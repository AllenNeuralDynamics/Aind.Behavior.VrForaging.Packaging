"""Processor that extracts session-level identity metadata from the dataset's Session log."""

import datetime
import logging
from typing import Any, cast

import pandas as pd
from pydantic import BaseModel

from .._base import AbstractProcessor, session_root
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
        row = self._build_metadata(raw, session_root(self._dataset).name, self.provenance)
        return pd.DataFrame([row.model_dump()])

    def _load_session_stream(self) -> dict[str, Any]:
        """Return the Session stream's payload as a plain dict."""
        data = self._dataset.at("Behavior").at("InputSchemas").at("Session").load().data
        return data.model_dump() if isinstance(data, BaseModel) else cast(dict[str, Any], data)

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
