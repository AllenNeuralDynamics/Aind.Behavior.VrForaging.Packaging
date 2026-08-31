"""Processor that extracts session-level identity metadata from the dataset's Session log."""

import datetime
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Json

from .._base import AbstractProcessor, cached_frame, session_root
from ..models import SessionMetadata

logger = logging.getLogger(__name__)


class SessionMetadataProcessor(AbstractProcessor):
    """Produces a single-row DataFrame of session-level metadata.

    ``session_id`` is always the session directory's name; the stream's own
    ``session_name`` field is ignored. ``subject`` and ``date`` come from the
    contraqctor ``Behavior/InputSchemas/Session`` stream, with no fallback.

    Also carries the raw ``session``, ``rig`` and ``task_logic`` config streams
    verbatim (see ``SessionMetadata``'s ``Json`` fields), for discoverability
    without a second pass over the dataset.
    """

    __output_name__ = "session"

    @cached_frame
    def _compute(self) -> pd.DataFrame:
        session_raw = self._normalize(self._load_input_schema("Session"))
        self._require_fields(session_raw, "subject", "date")
        row = SessionMetadata(
            session_id=session_root(self._dataset).name,
            subject_id=str(session_raw["subject"]),
            date=datetime.datetime.fromisoformat(str(session_raw["date"])),
            dataset_version=self.provenance.dataset_version,
            data_contract_version=self.provenance.data_contract_version,
            packaging_version=self.provenance.packaging_version,
            session=json.dumps(session_raw),
            rig=json.dumps(self._normalize(self._load_input_schema("Rig"))),
            task_logic=json.dumps(self._normalize(self._load_input_schema("TaskLogic"))),
        )
        return pd.DataFrame([row.model_dump()])

    def _load_input_schema(self, name: str) -> Any:
        """Return the ``Behavior/InputSchemas/<name>`` stream's raw payload, as loaded.

        Current-schema streams (``>= 1.0``) are ``PydanticModel``s; legacy streams
        are plain ``Json``, already a dict.
        """
        return self._dataset.at("Behavior").at("InputSchemas").at(name).load().data

    @staticmethod
    def _normalize(payload: Any) -> dict[str, Any]:
        """Normalize a stream payload to a plain, JSON-safe dict.

        Pydantic payloads are dumped with ``mode="json"`` so every field lands as
        a JSON-native type; the plain-dict (legacy) case is already JSON-safe,
        having come straight from ``json.load``.
        """
        return payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload

    @staticmethod
    def _require_fields(raw: dict, *fields: str) -> None:
        """Raises :exc:`KeyError` if any of *fields* is absent or empty in *raw*."""
        for field in fields:
            if not raw.get(field):
                raise KeyError(f"Required field {field!r} missing from the contraqctor Session stream")

    def write_parquet(self, output_dir: Path, filename: str | None = None) -> None:
        """Compute, then write, tagging ``SessionMetadata``'s ``Json`` fields with Parquet's native JSON logical type.

        Falls back to the default writer on a pyarrow build without ``json_``
        (added in pyarrow 19).
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        json_type_factory = getattr(pa, "json_", None)
        if json_type_factory is None:
            return super().write_parquet(output_dir, filename)

        df = self.compute()
        table = pa.Table.from_pandas(df)
        kv = {str(k).encode(): str(v).encode() for k, v in df.attrs.items()}
        table = table.replace_schema_metadata({**table.schema.metadata, **kv})

        json_fields = (
            name
            for name, field in SessionMetadata.model_fields.items()
            if any(isinstance(m, Json) for m in field.metadata)
        )
        for column in json_fields:
            index = table.schema.get_field_index(column)
            json_array = pa.array([json.dumps(v) for v in df[column]], type=json_type_factory())
            table = table.set_column(index, table.field(index).with_type(json_array.type), json_array)

        path = output_dir / (filename or f"{self.output_name}.parquet")
        pq.write_table(table, path)
