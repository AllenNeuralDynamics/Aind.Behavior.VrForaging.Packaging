"""Typed shapes shared across the pipeline package.

Kept deliberately small: :class:`SessionRef` is what a :class:`~.sources.Source`
yields (§3), :class:`Job` is a typed read of one ``jobs`` row (§5), and
``JobStatus``/``ErrorKind`` are the two controlled vocabularies the ledger's
state machine (§7) and failure classifier (§8) are built around.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

#: §7 state machine. ``completed`` + ``partial=True`` would mean output exists
#: but at least one processor failed anyway — currently unreachable, since any
#: processor exception now fails the whole session (`_sidecar.SidecarRecorder`).
#: Kept as a real state a future sidecar could still set deliberately.
JobStatus = Literal["pending", "running", "completed", "failed", "retrying", "dead", "skipped"]

#: §8 failure classification. Only ``data``/``code`` are terminal by default;
#: ``transient``/``infra``/``timeout`` retry with backoff (see ``ledger.RETRYABLE_KINDS``).
ErrorKind = Literal["transient", "infra", "timeout", "data", "code"]

#: §10. What produced the host path handed to the processor container.
StoreName = Literal["s3", "mount", "local"]


class SessionRef(BaseModel):
    """One discovered, complete, processable session (§3).

    Identity only — fetching the bytes is a store's job (§10), not a source's.
    """

    model_config = ConfigDict(frozen=True)

    session_name: str
    """Natural key; unique among raw assets (§3 — verified zero duplicates)."""
    input_uri: str
    """``s3://bucket/prefix`` or ``file:///path``."""
    asset_id: str | None = None
    """DocDB ``_id``, when the source knows one."""
    subject_id: str | None = None
    session_start: datetime | None = None
    """Acquisition/session start — NOT DocDB ``created`` (§3)."""
    cursor: str | None = None
    """Source-specific watermark value (``created``, for :class:`DocDbSource`)."""
    discovered_by: str = ""
    """``"docdb:pass-a"`` | ``"docdb:pass-b"`` | ``"local"`` — kept so the two
    DocDB passes stay separable after the fact."""


class Job(BaseModel):
    """Typed read of one ``jobs`` row (§5). Ledger writes go through dedicated
    methods (``claim``, ``rerun``, …); this is only for callers that want a
    structured view of a row rather than a raw ``sqlite3.Row``.
    """

    model_config = ConfigDict(frozen=True)

    job_id: str
    job_key: str
    kind: Literal["session", "aggregate"]
    release: str

    session_name: str | None = None
    subject_id: str | None = None
    session_start: str | None = None
    asset_id: str | None = None
    input_store: StoreName | None = None
    input_uri: str
    output_uri: str

    status: JobStatus
    partial: bool = False
    attempts: int = 0
    max_attempts: int = 3
    run_count: int = 0
    rerun_of: str | None = None
    priority: int = 0
    next_eligible_at: str | None = None
    worker_id: str | None = None
    lease_expires_at: str | None = None

    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    duration_s: float | None = None
    t_stage_s: float | None = None
    t_run_s: float | None = None
    t_publish_s: float | None = None

    exit_code: int | None = None
    error_kind: ErrorKind | None = None
    error: str | None = None
    sidecar: str | None = None
    log_uri: str | None = None
    staged_bytes: int | None = None
    read_files: int | None = None
    read_bytes: int | None = None
    output_bytes: int | None = None
    warn_count: int = 0
    failed_processors: str | None = None
    tags: str | None = None

    image_ref: str | None = None
    image_digest: str | None = None
    git_commit: str | None = None
    packaging_version: str | None = None
    data_contract_version: str | None = None
    dataset_version: str | None = None
