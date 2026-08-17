"""Processor that extracts session-level identity metadata from the dataset's Session log."""

import datetime
import logging
from pathlib import Path
from typing import Any, cast

import pandas as pd
from pydantic import BaseModel

from .._base import AbstractProcessor, DatasetProcessorError
from .._provenance import PackagingProvenance
from ..models import SessionMetadata

logger = logging.getLogger(__name__)


class SessionMetadataProcessor(AbstractProcessor):
    """Produces a single-row DataFrame of session-level metadata.

    ``session_id`` is always the session directory's name; the stream's own
    ``session_name`` field is ignored. ``subject`` and ``date`` come from the
    contraqctor ``Behavior/InputSchemas/Session`` stream, with no fallback.
    """

    __output_name__ = "session"

    def _compute(self) -> pd.DataFrame:
        raw = self._load_session_stream()
        row = self._build_metadata(raw, self._session_root().name, self.provenance)
        return pd.DataFrame([row.model_dump()])

    def _load_session_stream(self) -> dict[str, Any]:
        """Return the Session stream's payload as a plain dict."""
        data = self._session_stream().load().data
        return data.model_dump() if isinstance(data, BaseModel) else cast(dict[str, Any], data)

    def _session_stream(self) -> Any:
        return self._dataset.at("Behavior").at("InputSchemas").at("Session")

    def _session_root(self) -> Path:
        """Return the session root, found by walking up from the Session stream's path.

        Anchors on the ``behavior/`` component of
        ``<root>/behavior/Logs/session_input.json`` rather than counting parents, so
        moving the log deeper under ``behavior/`` cannot silently yield the wrong
        directory. Raises when the root cannot be recovered — there is no identity
        without it.
        """
        raw_path = getattr(self._session_stream().reader_params, "path", None)
        if raw_path is None:
            raise DatasetProcessorError("Session stream exposes no source path to take the session directory from")

        path = Path(raw_path)
        for parent in path.parents:
            if parent.name.lower() == "behavior":
                return parent.parent

        raise DatasetProcessorError(
            f"Session stream path {str(path)!r} has no 'behavior' component to locate the session root from"
        )

    @staticmethod
    def _build_metadata(raw: dict, session_id: str, provenance: PackagingProvenance) -> SessionMetadata:
        """Raises :exc:`KeyError` if ``subject`` or ``date`` is absent or empty."""
        for field in ("subject", "date"):
            if not raw.get(field):
                raise KeyError(f"Required field {field!r} missing from the contraqctor Session stream")
        return SessionMetadata(
            session_id=session_id,
            subject_id=str(raw["subject"]),
            date=datetime.datetime.fromisoformat(str(raw["date"])),
            dataset_version=provenance.dataset_version,
            data_contract_version=provenance.data_contract_version,
            packaging_version=provenance.packaging_version,
        )
