"""``DocDbSource`` — the production discovery backend.

Two passes against ``api.allenneuraldynamics.org`` / ``metadata_index`` /
``data_assets``, read-only, via ``aind-data-access-api``:

* **Pass A** — authoritative, ``acquisition_type``/``session_type``-based. Covers
  both the v1 (``session.session_type``) and v2 (``acquisition.acquisition_type``)
  metadata generations.
* **Pass B** — a bounded legacy fallback on ``project_name``, for the ~67 sessions
  (all mid-2024) that predate typed acquisition metadata. Bounded by
  ``legacy_session_before`` so the fallback cannot silently grow to cover
  sessions that *should* have an acquisition type but are missing one — that
  would be a metadata bug upstream, not something to absorb quietly.

Requires the ``pipeline`` optional dependency group (``aind-data-access-api``).
"""

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Literal

from aind_data_access_api.document_db import MetadataDbClient

from ..models import SessionRef

logger = logging.getLogger(__name__)

_PROJECTION = {
    "_id": 1,
    "name": 1,
    "created": 1,
    "location": 1,
    "data_description.project_name": 1,
    "acquisition.acquisition_start_time": 1,
    "session.session_start_time": 1,
    "subject.subject_id": 1,
}


def _parse_dt(value: str | None) -> datetime | None:
    """Parse either serialisation seen live in ``acquisition_start_time``:
    ``2024-08-26 16:24:17.031552+00:00`` and ``2026-04-09T11:44:16Z``."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Could not parse session start timestamp %r", value)
        return None


class DocDbSource:
    """Query DocDB for VR-foraging raw sessions. See module docstring for the two passes."""

    name: Literal["docdb", "local", "manifest"] = "docdb"

    def __init__(
        self,
        *,
        host: str = "api.allenneuraldynamics.org",
        database: str = "metadata_index",
        collection: str = "data_assets",
        acquisition_types: list[str] | None = None,
        legacy_project_name: str | None = "Cognitive flexibility in patch foraging",
        legacy_session_before: str = "2026-01-01",
        client: MetadataDbClient | None = None,
    ) -> None:
        self._client = client or MetadataDbClient(host=host, database=database, collection=collection)
        self._acquisition_types = acquisition_types or ["AindVrForaging"]
        self._legacy_project_name = legacy_project_name
        self._legacy_session_before = legacy_session_before

    def discover(self, since: str | None) -> Iterator[SessionRef]:
        yield from self._pass_a(since)
        if self._legacy_project_name:
            yield from self._pass_b(since)

    # ------------------------------------------------------------------

    def _query(self, filter_query: dict[str, Any]) -> list[dict[str, Any]]:
        # `retrieve_docdb_records` paginates internally against the API Gateway but
        # returns one materialised list; true incremental streaming would require
        # the client's private per-page call. At the ~4700-record scale measured
        # for this collection (metadata-only, no blobs), this is a reasonable
        # trade-off against depending on a private API surface.
        return self._client.retrieve_docdb_records(
            filter_query=filter_query, projection=_PROJECTION, sort={"created": 1}, limit=0
        )

    def _to_ref(self, doc: dict[str, Any], *, discovered_by: str) -> SessionRef | None:
        name = doc.get("name")
        location = doc.get("location")
        if not name or not location:
            logger.warning("Skipping DocDB record missing name/location: _id=%s", doc.get("_id"))
            return None
        session_start = _parse_dt(doc.get("acquisition", {}).get("acquisition_start_time")) or _parse_dt(
            doc.get("session", {}).get("session_start_time")
        )
        return SessionRef(
            session_name=name,
            input_uri=location,
            asset_id=str(doc.get("_id")) if doc.get("_id") is not None else None,
            subject_id=doc.get("subject", {}).get("subject_id"),
            session_start=session_start,
            cursor=doc.get("created"),
            discovered_by=discovered_by,
        )

    def _pass_a(self, since: str | None) -> Iterator[SessionRef]:
        query: dict[str, Any] = {
            "data_description.data_level": "raw",
            "$or": [
                {"acquisition.acquisition_type": {"$in": self._acquisition_types}},
                {"session.session_type": {"$in": self._acquisition_types}},
            ],
        }
        if since:
            query["created"] = {"$gte": since}
        docs = self._query(query)
        logger.info("DocDB Pass A (type-based): %d sessions", len(docs))
        for doc in docs:
            ref = self._to_ref(doc, discovered_by="docdb:pass-a")
            if ref is not None:
                yield ref

    def _pass_b(self, since: str | None) -> Iterator[SessionRef]:
        query: dict[str, Any] = {
            "data_description.data_level": "raw",
            "data_description.project_name": self._legacy_project_name,
            "acquisition.acquisition_type": {"$exists": False},
            "session.session_type": {"$exists": False},
        }
        if since:
            query["created"] = {"$gte": since}
        docs = self._query(query)
        cutoff = _parse_dt(self._legacy_session_before) or datetime.max.replace(tzinfo=None)

        kept = 0
        for doc in docs:
            ref = self._to_ref(doc, discovered_by="docdb:pass-b")
            if ref is None:
                continue
            # Bounded on purpose: this fallback exists only for sessions that
            # predate typed acquisition metadata. If it starts matching newer
            # sessions, that is a metadata bug upstream, not something to absorb.
            if ref.session_start is not None and ref.session_start.replace(tzinfo=None) >= cutoff.replace(tzinfo=None):
                logger.warning(
                    "Pass B (legacy fallback) matched %s with session_start %s on/after cutoff %s — "
                    "an acquisition type is likely missing upstream, not a legacy session.",
                    ref.session_name,
                    ref.session_start,
                    self._legacy_session_before,
                )
                continue
            kept += 1
            yield ref
        logger.info(
            "DocDB Pass B (legacy fallback, project=%r): %d matched, %d kept",
            self._legacy_project_name,
            len(docs),
            kept,
        )
