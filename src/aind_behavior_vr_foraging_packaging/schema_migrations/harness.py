"""Run schema migrations against rows in a parquet corpus."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypeVar

import pyarrow.parquet as pq
from aind_behavior_services.session import Session
from aind_behavior_vr_foraging.rig import AindVrForagingRig
from aind_behavior_vr_foraging.task_logic import AindVrForagingTaskLogic
from pydantic import BaseModel, ValidationError

from .migrations import migrate_documents

DocumentKind = Literal["session", "rig", "task_logic", "bundle"]
JsonInput = str | bytes | bytearray | Mapping[str, Any] | BaseModel


@dataclass(frozen=True, slots=True)
class HarmonizedSchemas:
    """The three formally deserialized current schema models."""

    session: Session
    rig: AindVrForagingRig
    task_logic: AindVrForagingTaskLogic


@dataclass(frozen=True, slots=True)
class DocumentValidationFailure:
    """A failure produced while deserializing one document in a row."""

    document: DocumentKind
    message: str
    errors: tuple[Mapping[str, Any], ...] = ()


class SchemaHarmonizationError(ValueError):
    """One or more documents in a schema triplet could not be harmonized."""

    def __init__(self, failures: list[DocumentValidationFailure]) -> None:
        self.failures = tuple(failures)
        summary = "; ".join(f"{failure.document}: {failure.message}" for failure in failures)
        super().__init__(summary)


@dataclass(frozen=True, slots=True)
class HarmonizedRow:
    """A passing parquet row and its three output models."""

    row_number: int
    source_version: str
    session: Session
    rig: AindVrForagingRig
    task_logic: AindVrForagingTaskLogic


@dataclass(frozen=True, slots=True)
class HarnessFailure:
    """One document failure tied back to its parquet row and source version."""

    row_number: int
    source_version: str
    document: DocumentKind
    message: str
    errors: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class HarnessResult:
    """All successes and failures from one corpus pass."""

    total_rows: int
    rows: tuple[HarmonizedRow, ...]
    failures: tuple[HarnessFailure, ...]
    successful_rows: int | None = None

    @property
    def success_count(self) -> int:
        """Number of rows that produced all three current models."""
        return len(self.rows) if self.successful_rows is None else self.successful_rows

    @property
    def passed(self) -> bool:
        """Whether every input row produced all three models."""
        return not self.failures and self.success_count == self.total_rows

    def raise_for_failures(self) -> None:
        """Raise a compact assertion suitable for a corpus integration test."""
        if not self.failures:
            return
        counts: dict[tuple[str, DocumentKind], int] = {}
        for failure in self.failures:
            key = (failure.source_version, failure.document)
            counts[key] = counts.get(key, 0) + 1
        summary = ", ".join(f"{version}/{document}={count}" for (version, document), count in sorted(counts.items()))
        raise AssertionError(
            f"{len(self.failures)} schema failures across "
            f"{self.total_rows - self.success_count} of {self.total_rows} rows ({summary})"
        )


Harmonizer = Callable[..., HarmonizedSchemas]
_ModelT = TypeVar("_ModelT", bound=BaseModel)


def _json_bytes(value: JsonInput, *, document: DocumentKind) -> bytes:
    """Normalize a parquet JSON cell to bytes for ``model_validate_json``."""
    if isinstance(value, BaseModel):
        return value.model_dump_json().encode("utf-8")
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, Mapping):
        return json.dumps(value).encode("utf-8")
    raise TypeError(f"{document} must contain a JSON object or encoded JSON, got {type(value).__name__}")


def _json_object(value: JsonInput, *, document: DocumentKind) -> dict[str, Any]:
    """Decode an input JSON object without retaining references to caller data."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return json.loads(json.dumps(value))
    try:
        decoded = json.loads(bytes(value) if isinstance(value, bytearray) else value)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
        raise ValueError(f"{document} must contain valid JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise TypeError(f"{document} must contain a JSON object, got {type(decoded).__name__}")
    return decoded


def _validate_current_model(
    model_type: type[_ModelT], value: JsonInput, *, document: DocumentKind
) -> tuple[_ModelT | None, DocumentValidationFailure | None]:
    try:
        return model_type.model_validate_json(_json_bytes(value, document=document)), None
    except ValidationError as exc:
        return None, DocumentValidationFailure(
            document=document,
            message=str(exc),
            errors=tuple(exc.errors(include_url=False)),
        )
    except (TypeError, ValueError) as exc:
        return None, DocumentValidationFailure(document=document, message=str(exc))


def current_model_harmonizer(
    *,
    source_version: str,
    session: JsonInput,
    rig: JsonInput,
    task_logic: JsonInput,
) -> HarmonizedSchemas:
    """Apply current built-in Pydantic compatibility validators to one triplet.

    ``source_version`` is part of the stable harmonizer contract and will drive
    explicit migrations. The current baseline records it in the harness but
    otherwise lets each document's own version fields and validators govern
    deserialization.
    """
    del source_version
    session_model, session_failure = _validate_current_model(Session, session, document="session")
    rig_model, rig_failure = _validate_current_model(AindVrForagingRig, rig, document="rig")
    task_logic_model, task_logic_failure = _validate_current_model(
        AindVrForagingTaskLogic, task_logic, document="task_logic"
    )
    failures = [failure for failure in (session_failure, rig_failure, task_logic_failure) if failure is not None]

    if failures:
        raise SchemaHarmonizationError(failures)

    assert session_model is not None
    assert rig_model is not None
    assert task_logic_model is not None
    return HarmonizedSchemas(
        session=session_model,
        rig=rig_model,
        task_logic=task_logic_model,
    )


def schema_harmonizer(
    *,
    source_version: str,
    session: JsonInput,
    rig: JsonInput,
    task_logic: JsonInput,
) -> HarmonizedSchemas:
    """Migrate a historical schema triplet and deserialize current models."""
    try:
        migrated = migrate_documents(
            source_version=source_version,
            session=_json_object(session, document="session"),
            rig=_json_object(rig, document="rig"),
            task_logic=_json_object(task_logic, document="task_logic"),
        )
    except (TypeError, ValueError) as exc:
        raise SchemaHarmonizationError([DocumentValidationFailure(document="bundle", message=str(exc))]) from exc

    return current_model_harmonizer(
        source_version=source_version,
        session=migrated[0],
        rig=migrated[1],
        task_logic=migrated[2],
    )


def run_parquet_harness(
    parquet_path: Path | str,
    *,
    version_column: str = "dataset_version",
    session_column: str = "session",
    rig_column: str = "rig",
    task_logic_column: str = "task_logic",
    harmonizer: Harmonizer = schema_harmonizer,
    retain_rows: bool = True,
    batch_size: int = 64,
) -> HarnessResult:
    """Harmonize every schema triplet in a parquet file without failing fast.

    Only the four requested columns are loaded. A row is successful only when
    all three documents produce current models. Failures are collected per
    document so a broken session model cannot hide rig or task-logic problems.
    """
    columns = [version_column, session_column, rig_column, task_logic_column]
    rows: list[HarmonizedRow] = []
    failures: list[HarnessFailure] = []
    successful_rows = 0
    total_rows = 0

    parquet = pq.ParquetFile(parquet_path)
    for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
        batch_columns = [batch.column(index).to_pylist() for index in range(len(columns))]
        for values in zip(*batch_columns, strict=True):
            row_number = total_rows
            total_rows += 1
            version_value, session_value, rig_value, task_logic_value = values
            source_version = "<missing>" if version_value is None else str(version_value)
            if source_version == "<missing>" or not source_version.strip():
                failures.append(
                    HarnessFailure(
                        row_number=row_number,
                        source_version=source_version,
                        document="bundle",
                        message=f"Missing source version in column {version_column!r}",
                    )
                )
                continue

            try:
                schemas = harmonizer(
                    source_version=source_version,
                    session=session_value,
                    rig=rig_value,
                    task_logic=task_logic_value,
                )
            except SchemaHarmonizationError as exc:
                failures.extend(
                    HarnessFailure(
                        row_number=row_number,
                        source_version=source_version,
                        document=failure.document,
                        message=failure.message,
                        errors=failure.errors,
                    )
                    for failure in exc.failures
                )
                continue
            except Exception as exc:
                failures.append(
                    HarnessFailure(
                        row_number=row_number,
                        source_version=source_version,
                        document="bundle",
                        message=f"{type(exc).__name__}: {exc}",
                    )
                )
                continue

            successful_rows += 1
            if retain_rows:
                rows.append(
                    HarmonizedRow(
                        row_number=row_number,
                        source_version=source_version,
                        session=schemas.session,
                        rig=schemas.rig,
                        task_logic=schemas.task_logic,
                    )
                )

    return HarnessResult(
        total_rows=total_rows,
        rows=tuple(rows),
        failures=tuple(failures),
        successful_rows=successful_rows,
    )
