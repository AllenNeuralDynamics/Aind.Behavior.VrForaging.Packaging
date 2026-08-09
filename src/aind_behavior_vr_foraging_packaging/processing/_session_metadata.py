"""Processor that extracts session-level identity metadata from the dataset's Session log."""

import datetime
import json
import logging
from pathlib import Path

import pandas as pd

from .._base import AbstractProcessor
from ..models import SessionMetadata

logger = logging.getLogger(__name__)

# Path relative to session root where the launcher writes the Session JSON.
_SESSION_JSON_RELPATH = Path("behavior") / "Logs" / "session_output.json"


class SessionMetadataProcessor(AbstractProcessor):
    """Produces a single-row DataFrame of session-level metadata.

    Tries the contraqctor ``Behavior/InputSchemas/Session`` stream first; falls
    back to reading ``behavior/Logs/session_output.json`` directly if the stream
    is unavailable.  Both sources expose the same ``subject`` and ``date`` keys.

    Raises if either required field (``subject``, ``date``) is absent from the
    loaded dict.  Error propagation is controlled by the caller (e.g.
    ``_process_one_session`` in :mod:`export_pipeline`).
    """

    __output_name__ = "session_metadata"

    def __init__(self, dataset, *, session_path: Path, raise_on_error: bool = False) -> None:
        super().__init__(dataset, raise_on_error=raise_on_error)
        self._session_path = Path(session_path)

    def _compute(self) -> pd.DataFrame:
        folder_name = self._session_path.name
        # Try the contraqctor stream; fall back to JSON only when the stream
        # itself is unavailable.  Data errors (missing required fields) propagate
        # immediately from _build_metadata regardless of which source was used.
        raw = self._fetch_stream_raw()
        if raw is None:
            raw = self._fetch_json_raw()  # FileNotFoundError if absent
        row = self._build_metadata(folder_name, raw)
        return pd.DataFrame([row.model_dump()])

    # ------------------------------------------------------------------
    # Raw-dict fetchers (return the dict; validation is in _build_metadata)
    # ------------------------------------------------------------------

    def _fetch_stream_raw(self) -> dict | None:
        """Return the raw dict from the contraqctor stream, or None if unavailable."""
        try:
            return self._dataset.at("Behavior").at("InputSchemas").at("Session").load().data
        except Exception as exc:
            logger.debug("Contraqctor stream unavailable for %s: %s", self._session_path.name, exc)
            return None

    def _fetch_json_raw(self) -> dict:
        """Return the raw dict from ``behavior/Logs/session_output.json``."""
        json_path = self._session_path / _SESSION_JSON_RELPATH
        with open(json_path, encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def _build_metadata(folder_name: str, raw: dict) -> SessionMetadata:
        """Extract required ``subject_id`` and ``date`` from a raw session dict.

        Raises :exc:`KeyError` if either field is absent.
        """
        if "subject" not in raw:
            raise KeyError(f"Required field 'subject' missing from session dict for {folder_name!r}")
        if not raw.get("date"):
            raise KeyError(f"Required field 'date' missing from session dict for {folder_name!r}")
        return SessionMetadata(
            session_id=folder_name,
            subject_id=str(raw["subject"]),
            date=datetime.date.fromisoformat(str(raw["date"])[:10]),
        )
