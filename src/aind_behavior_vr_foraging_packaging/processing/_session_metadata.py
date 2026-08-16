"""Processor that extracts session-level identity metadata from the dataset's Session log."""

import datetime
import logging
from typing import Any, cast

import pandas as pd
from pydantic import BaseModel

from .._base import AbstractProcessor
from .._provenance import PackagingProvenance
from ..models import SessionMetadata

logger = logging.getLogger(__name__)


class SessionMetadataProcessor(AbstractProcessor):
    """Produces a single-row DataFrame of session-level metadata.

    The contraqctor ``Behavior/InputSchemas/Session`` stream is the *only*
    source: ``session_name``, ``subject`` and ``date`` all come from it. There
    is deliberately no second source — no reading ``session_output.json`` off
    disk, no deriving identity from the directory name. A dataset whose Session
    stream cannot be loaded, or which does not name itself, is broken rather
    than legacy, and a fallback that silently disagrees with the stream is worse
    than a crash.

    Raises if the stream is unavailable or if any required field
    (``session_name``, ``subject``, ``date``) is absent. Isolating that failure
    from the rest of a batch is the caller's job — see
    :func:`~aind_behavior_vr_foraging_packaging.export_pipeline.process_sessions`.
    """

    __output_name__ = "session"

    def _compute(self) -> pd.DataFrame:
        raw = self._load_session_stream()
        row = self._build_metadata(raw, self.provenance)
        return pd.DataFrame([row.model_dump()])

    def _load_session_stream(self) -> dict[str, Any]:
        """Return the Session stream's payload as a plain dict.

        Propagates ``KeyError``/``FileNotFoundError`` rather than degrading:
        this processor has nothing meaningful to emit without the stream.
        """
        data = self._dataset.at("Behavior").at("InputSchemas").at("Session").load().data
        return data.model_dump() if isinstance(data, BaseModel) else cast(dict[str, Any], data)

    @staticmethod
    def _build_metadata(raw: dict, provenance: PackagingProvenance) -> SessionMetadata:
        """Extract the required identity fields from a raw session dict.

        Raises :exc:`KeyError` if any of ``session_name``, ``subject`` or
        ``date`` is absent or empty.
        """
        for field in ("session_name", "subject", "date"):
            if not raw.get(field):
                raise KeyError(f"Required field {field!r} missing from the contraqctor Session stream")
        return SessionMetadata(
            session_id=str(raw["session_name"]),
            subject_id=str(raw["subject"]),
            date=datetime.datetime.fromisoformat(str(raw["date"])),
            dataset_version=provenance.dataset_version,
            data_contract_version=provenance.data_contract_version,
            packaging_version=provenance.packaging_version,
        )
