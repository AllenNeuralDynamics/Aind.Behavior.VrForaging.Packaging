"""Processor that extracts session-level identity metadata from the dataset's Session log."""

import datetime
import json
import logging
import typing as ty
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

import pandas as pd
from aind_behavior_curriculum.trainer import TrainerState
from contraqctor.contract import Dataset
from pydantic import BaseModel, Json, ValidationError

from .._base import AbstractProcessor, DatasetProcessorError, cached_frame, session_root
from ..models import SessionMetadata

logger = logging.getLogger(__name__)


def _is_json_marked(field: Any) -> bool:
    """True if a ``FieldInfo`` carries pydantic's ``Json`` marker, bare or under ``Optional``."""
    if any(isinstance(m, Json) for m in field.metadata):  # ty: ignore[invalid-argument-type]
        return True
    return any(
        isinstance(m, Json)  # ty: ignore[invalid-argument-type]
        for arg in ty.get_args(field.annotation)
        for m in getattr(arg, "__metadata__", ())
    )


class SessionMetadataProcessor(AbstractProcessor):
    """Single-row session identity: session_id/subject/date, raw session/rig/task_logic, curriculum state.

    ``session_id`` defaults to the session root's directory name. Pass it
    explicitly when that name is not the session's identity, e.g. when the raw
    data is mounted under a generic folder such as ``vr_foraging_raw``.
    """

    __output_name__ = "session"

    def __init__(
        self,
        dataset: Dataset,
        *,
        strict_parsing: bool = False,
        session_id: str | None = None,
    ) -> None:
        super().__init__(dataset, strict_parsing=strict_parsing)
        self._session_id = session_id

    @cached_frame
    def _compute(self) -> pd.DataFrame:
        session_raw = self._normalize(self._load_input_schema("Session"))
        self._require_fields(session_raw, "subject", "date")
        trainer_state = self._load_trainer_state()
        curriculum = (trainer_state or {}).get("curriculum", None)
        stage = (trainer_state or {}).get("stage", None)
        rig_raw = self._normalize(self._load_input_schema("Rig"))
        task_logic_raw = self._normalize(self._load_input_schema("TaskLogic"))
        row = SessionMetadata(
            session_id=self._session_id or session_root(self._dataset).name,
            subject_id=str(session_raw["subject"]),
            date=datetime.datetime.fromisoformat(str(session_raw["date"])),
            dataset_version=self.provenance.dataset_version,
            data_contract_version=self.provenance.data_contract_version,
            packaging_version=self.provenance.packaging_version,
            session=json.dumps(session_raw),
            rig=json.dumps(rig_raw),
            task_logic=json.dumps(task_logic_raw),
            curriculum_enabled=trainer_state.get("is_on_curriculum") if trainer_state else None,
            curriculum_name=curriculum.get("name") if curriculum else None,
            curriculum_stage_name=stage.get("name") if stage else None,
            trainer_state=json.dumps(trainer_state) if trainer_state is not None else None,
        )
        return pd.DataFrame([row.model_dump()])

    def _load_trainer_state(self) -> dict[str, Any] | None:
        """Validated ``behavior/trainer_state*.json`` payload, or ``None`` if absent."""
        behavior_dir = session_root(self._dataset) / "behavior"
        matches = list(behavior_dir.glob("trainer_state*.json"))
        if not matches:
            return None
        path = max(matches, key=lambda p: p.stat().st_mtime)
        if len(matches) > 1:
            logger.warning(
                "Multiple trainer_state files found under %s; using the most recently modified: %s",
                behavior_dir,
                path.name,
            )
        raw_text = path.read_text(encoding="utf-8")
        try:
            validated = TrainerState.model_validate_json(raw_text)
        except ValidationError as e:
            msg = f"{path} does not validate against aind_behavior_curriculum.trainer.TrainerState"
            if self.strict_parsing:
                raise DatasetProcessorError(msg) from e
            logger.warning("%s (%s); using the raw, unvalidated payload instead.", msg, e)
            return json.loads(raw_text)
        return self._normalize(validated)

    def _load_input_schema(self, name: str) -> Any:
        """Raw ``Behavior/InputSchemas/<name>`` payload: ``PydanticModel`` (current) or dict (legacy)."""
        return self._dataset.at("Behavior").at("InputSchemas").at(name).load().data

    @staticmethod
    def _normalize(payload: Any) -> dict[str, Any]:
        """Pydantic payload -> JSON-safe dict; a plain dict passes through unchanged."""
        return payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload

    @staticmethod
    def _require_fields(raw: dict, *fields: str) -> None:
        """Raises :exc:`KeyError` if any of *fields* is absent or empty in *raw*."""
        for field in fields:
            if not raw.get(field):
                raise KeyError(f"Required field {field!r} missing from the contraqctor Session stream")

    def to_arrow_table(self, df: pd.DataFrame | None = None) -> "pa.Table":
        """Convert raw session metadata to Arrow with explicit field types.

        ``df`` defaults to this processor's cached computation.
        """
        import pyarrow as pa

        df = self.compute() if df is None else df
        encoded = self._json_encoded_columns(df)
        frame = df.assign(**encoded)
        frame.attrs = dict(df.attrs)

        table = pa.Table.from_pandas(frame)
        kv = {str(k).encode(): str(v).encode() for k, v in frame.attrs.items()}
        table = table.replace_schema_metadata({**table.schema.metadata, **kv})

        json_type_factory = getattr(pa, "json_", None)
        if json_type_factory is None:
            return table

        scalar_arrow_type = {bool: pa.bool_(), str: pa.large_string()}

        for name, values in encoded.items():
            index = table.schema.get_field_index(name)
            # From the list, not frame[name]: pa.array ignores the extension type on
            # pandas' Arrow-backed string dtype, and renders None as NaN.
            json_array = pa.array(values, type=json_type_factory())
            table = table.set_column(index, table.field(index).with_type(json_array.type), json_array)

        for name, field in SessionMetadata.model_fields.items():
            if name not in table.column_names or name in encoded:
                continue
            index = table.schema.get_field_index(name)
            base_type = next((t for t in ty.get_args(field.annotation) if t is not type(None)), field.annotation)
            arrow_type = scalar_arrow_type.get(base_type)
            if arrow_type is not None:
                casted = table.column(index).cast(arrow_type)
                table = table.set_column(index, table.field(index).with_type(arrow_type), casted)

        return table

    def write_parquet(self, output_dir: Path, filename: str | None = None) -> None:
        """Write raw session metadata with its explicit Arrow schema."""
        import pyarrow.parquet as pq

        path = output_dir / (filename or f"{self.output_name}.parquet")
        pq.write_table(self.to_arrow_table(), path)

    @staticmethod
    def _json_encoded_columns(df: pd.DataFrame) -> dict[str, list[str | None]]:
        """Return model columns that must carry Arrow's JSON logical type.

        Pydantic parses model fields into live objects, so those are re-encoded.
        ``None`` stays null rather than ``"null"``.
        """
        return {
            name: [None if value is None else json.dumps(value) for value in df[name]]
            for name, field in SessionMetadata.model_fields.items()
            if name in df.columns and _is_json_marked(field)
        }
