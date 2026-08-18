"""``output.metadata.json`` sidecar: a small, self-owned reproducibility record.

Written once per session, beside its parquet/NWB outputs, so every output tree
is self-describing. Deliberately **not** built on ``aind-data-schema``: this is a
record of *how a run went*, not a published data asset, and it has to be readable
by the worker across a container boundary with nothing but stdlib json. Every
field here is machine-derivable at runtime; nothing requires a human to remember
to set it.

It lives in this package, not in ``pipeline/``, because the dependency runs one
way. :class:`SidecarRecorder` plugs into ``process_session``'s generic
``on_output``/``on_error`` hooks; that function has never heard of this file.
"""

import hashlib
import importlib.metadata
import json
import logging
import os
import platform
import time
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, cast

import aind_behavior_vr_foraging
import pandas as pd
from pydantic import BaseModel, ConfigDict

from aind_behavior_vr_foraging_packaging._provenance import _PACKAGING_PKG, PackagingProvenance
from aind_behavior_vr_foraging_packaging.pipeline.batch import SESSION_TABLE

if TYPE_CHECKING:
    from aind_behavior_vr_foraging_packaging._base import AbstractProcessor

logger = logging.getLogger(__name__)

SIDECAR_NAME = "output.metadata.json"
"""Filename, fixed. The worker looks for exactly this name."""

_SIDECAR_SCHEMA_VERSION = "1.0.0"
_REPOSITORY = "https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging"

# Logger name prefixes counted by _WarnCounter. Both packages are "ours"; pynwb/hdmf
# emit unrelated boilerplate (e.g. one DynamicTable attribute-shadowing warning per
# table per session) that must not inflate the count. Not a logging-hierarchy
# relationship (the two names share no dotted parent), hence the plain prefix match.
_OWN_LOGGER_PREFIXES = ("aind_behavior_vr_foraging_packaging", "aind_behavior_vr_foraging")


class ContainerRef(BaseModel):
    """How the processor was packaged. Absent entirely for a bare-Python run."""

    model_config = ConfigDict(frozen=True)

    uri: str
    """``ghcr.io/…/…@sha256:…`` — immutable, authoritative."""
    tag: str | None
    """Human-friendly, movable."""
    digest: str
    """``sha256:…``, split out so the ledger can index it directly."""


class CodeRef(BaseModel):
    """Identifies exactly what code produced this output."""

    model_config = ConfigDict(frozen=True)

    repository: str
    version: str
    commit: str | None
    python_version: str
    container: ContainerRef | None
    provenance: Literal["pinned-digest", "unpinned"]
    """``"unpinned"`` == a local/dev run. Recorded explicitly so an unreproducible
    result is never mistaken for a reproducible one."""


class ProcessorResult(BaseModel):
    """Outcome of one processor (or the NWB step) for one session."""

    model_config = ConfigDict(frozen=True)

    name: str
    status: Literal["ok", "error"]
    rows: int | None = None
    output_file: str | None = None
    error: str | None = None
    """One-line; full traceback lives in the job log, not here."""
    warn_count: int = 0
    """WARNING records logged by our own code while this processor ran. A count,
    not a taxonomy — it says "look at the log", nothing more. See :class:`_WarnCounter`."""


class SessionOutputMetadata(BaseModel):
    """Sidecar written to ``output.metadata.json``, one per session."""

    model_config = ConfigDict(frozen=True)

    schema_version: Literal["1.0.0"] = _SIDECAR_SCHEMA_VERSION

    # ---- identity ----
    session_name: str
    subject_id: str | None = None
    session_start: datetime | None = None
    input_uri: str | None = None
    output_uri: str | None = None

    # ---- outcome ----
    status: Literal["ok", "partial", "error"]
    started_at: datetime
    finished_at: datetime
    duration_s: float
    processors: list[ProcessorResult]
    nwb: ProcessorResult | None = None

    # ---- reproducibility ----
    code: CodeRef
    versions: dict[str, str]
    """``PackagingProvenance.model_dump()``: packaging / data_contract / dataset."""
    parameters: dict[str, Any] = {}
    """The CLI flags actually used for this run."""
    staged: dict[str, Any] = {}
    """What the input store made available, when known (``input_store``, byte counts, …)."""

    # ---- server context, when run under the worker ----
    job_id: str | None = None
    worker_id: str | None = None

    @property
    def warn_count(self) -> int:
        """Total WARNING records across every processor and the NWB step."""
        total = sum(p.warn_count for p in self.processors)
        if self.nwb is not None:
            total += self.nwb.warn_count
        return total

    @property
    def failed_processors(self) -> list[str]:
        """Names of every processor (and ``"nwb"``, if applicable) that errored."""
        names = [p.name for p in self.processors if p.status == "error"]
        if self.nwb is not None and self.nwb.status == "error":
            names.append("nwb")
        return names


class _WarnCounter(logging.Handler):
    """Counts WARNING+ records from our own loggers while attached.

    A count, not a taxonomy: sortable in the ledger, and the log says what
    actually happened.
    Attach to the root logger — this filters by logger-name prefix internally
    rather than relying on the logging hierarchy, since the two packages we care
    about are not parent/child loggers despite the shared name prefix.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith(_OWN_LOGGER_PREFIXES):
            self.count += 1

    def reset(self) -> int:
        """Return the count since the last reset, and zero it."""
        n = self.count
        self.count = 0
        return n


def build_code_ref() -> CodeRef:
    """Build a :class:`CodeRef` from the running environment.

    ``container``/``provenance`` come from ``PROCESSOR_IMAGE_URI`` (set by the
    worker at ``docker run`` time) — never guessed. Its absence means a
    local/dev run, recorded as ``provenance="unpinned"`` rather than invented.
    """
    image_uri = os.environ.get("PROCESSOR_IMAGE_URI")
    container: ContainerRef | None = None
    provenance: Literal["pinned-digest", "unpinned"] = "unpinned"

    if image_uri and "@sha256:" in image_uri:
        digest = "sha256:" + image_uri.rsplit("@sha256:", 1)[1]
        container = ContainerRef(uri=image_uri, tag=os.environ.get("PROCESSOR_IMAGE_TAG"), digest=digest)
        provenance = "pinned-digest"
    elif image_uri:
        logger.warning("PROCESSOR_IMAGE_URI=%r has no @sha256:… digest; treating run as unpinned.", image_uri)

    return CodeRef(
        repository=_REPOSITORY,
        version=importlib.metadata.version(_PACKAGING_PKG),
        commit=os.environ.get("PROCESSOR_GIT_COMMIT"),
        python_version=platform.python_version(),
        container=container,
        provenance=provenance,
    )


def write_sidecar(path: Path, metadata: SessionOutputMetadata) -> None:
    """Write *metadata* to *path* as pretty-printed JSON, creating parent dirs.

    Written even when the session failed, so a *missing* sidecar unambiguously
    means the process died before it got this far.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(metadata.model_dump_json(indent=2), encoding="utf-8")


def session_completed_ok(session_dir: Path) -> bool:
    """Whether *session_dir* holds a complete session, per its own sidecar.

    Shaped as ``aggregate``'s ``include`` predicate. A session that failed still
    has a directory — deliberately, so the sidecar survives to be read — but its
    parquets may cover only the processors that ran before one raised, and those
    rows must not reach an aggregate silently.

    Fails **open** in both ambiguous cases. No sidecar at all means the session
    was produced by the plain ``vr-foraging-packaging`` CLI, which has no opinion
    on this; an unreadable one is a reason to look at the log, not to discard a
    session that may be perfectly good.
    """
    sidecar = session_dir / SIDECAR_NAME
    if not sidecar.exists():
        return True
    try:
        return json.loads(sidecar.read_text(encoding="utf-8")).get("status") == "ok"
    except (OSError, ValueError) as exc:
        logger.warning("%s: could not read sidecar (%s) — keeping session", session_dir.name, exc)
        return True


class AggregateOutputMetadata(BaseModel):
    """The aggregate output's own sidecar — same filename, same commit-marker role.

    Without it a reader cannot tell a complete aggregate from one caught mid-write,
    which matters far more now that it is rewritten continuously rather than once at
    the end of a campaign. It also answers "which sessions are in this?" without
    recomputing anything, and carries the watermark forward for the next run.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0.0"] = _SIDECAR_SCHEMA_VERSION
    kind: Literal["aggregate"] = "aggregate"
    release: str
    created_at: datetime
    watermark: str
    """Digest of the contributing set — see :func:`aggregate_watermark`."""
    sessions: dict[str, str]
    """``session_name`` → the token identifying *which run* of it contributed."""
    tables: dict[str, int] = {}
    """Table name → row count, for a cheap "did this actually change" read."""
    duration_s: float | None = None
    code: CodeRef


def session_token(sidecar: dict) -> str:
    """What identifies *which run* of a session produced the output on the store.

    ``job_id`` is the honest answer: a reprocess inserts a row with
    ``job_key(..., run_count + 1)``, so a recomputed session publishes a different
    one. Falls back to ``finished_at`` for sidecars written by the plain packaging
    CLI, which has no job. Last resort is a constant, which makes such a session
    invisible to change detection rather than making every run look changed.
    """
    return str(sidecar.get("job_id") or sidecar.get("finished_at") or "unversioned")


def aggregate_watermark(contributions: dict[str, str]) -> str:
    """Digest of ``{session_name: token}`` — the "has anything changed" key.

    Deliberately not a count or a max timestamp: both miss a *recompute*, which
    leaves the aggregate permanently stale for exactly the case that motivates
    re-aggregating. A digest over the whole set catches additions, recomputes and
    removals alike, and costs nothing on top of the listing already being done.
    """
    payload = "\n".join(f"{name}={contributions[name]}" for name in sorted(contributions))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


class SidecarRecorder:
    """Accumulates one session's outcome, writing the sidecar when the block ends.

    A context manager rather than a function so the file is written on the way
    out either way — a processor that raises still leaves a sidecar naming it,
    which is the only channel the server layer has for per-processor
    detail across the container boundary.

    Recording does not alter control flow: an exception still propagates, and
    the session is recorded as ``status="error"`` on its way past. ``path=None``
    disables the recorder entirely, so the caller needs no conditionals.
    """

    def __init__(self, path: Path | None, *, session_name: str, parameters: dict[str, Any] | None = None) -> None:
        self.path = Path(path) if path is not None else None
        self.session_name = session_name
        """Best known so far; the caller overwrites it once the dataset is loaded."""
        self.parameters = dict(parameters or {})

        self._warnings = _WarnCounter()
        self._results: list[ProcessorResult] = []
        self._nwb: ProcessorResult | None = None
        self._subject_id: str | None = None
        self._session_start: datetime | None = None
        self._dataset_version: str | None = None
        self._raised = False
        self._started_at = datetime.now(timezone.utc)
        self._t0 = time.monotonic()

    @property
    def enabled(self) -> bool:
        return self.path is not None

    def __enter__(self) -> "SidecarRecorder":
        self._started_at = datetime.now(timezone.utc)
        self._t0 = time.monotonic()
        if self.enabled:
            logging.getLogger().addHandler(self._warnings)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        if self.path is None:
            return False
        logging.getLogger().removeHandler(self._warnings)
        self._raised = exc_type is not None
        try:
            write_sidecar(self.path, self.build())
        except OSError:
            # Never mask the session's own failure with a bookkeeping error.
            logger.exception("Could not write %s", self.path)
        return False

    # -- recording -----------------------------------------------------------

    def dataset_loaded(self, version: str) -> None:
        """Note that the dataset opened, and at which schema version."""
        self._dataset_version = version

    def _identify(self, session_frame: pd.DataFrame) -> None:
        """Lift ``subject_id``/``date`` out of the computed ``session`` table.

        The sidecar repeats them so an output directory can be identified
        without opening a parquet file.
        """
        if session_frame.empty:
            return
        row = session_frame.iloc[0]
        if "subject_id" in session_frame.columns:
            self._subject_id = str(row["subject_id"])
        if "date" in session_frame.columns and pd.notna(row["date"]):
            self._session_start = cast(datetime, pd.Timestamp(row["date"]).to_pydatetime())

    def on_output(self, proc: "AbstractProcessor", frame: pd.DataFrame, path: Path | None) -> None:
        """Record one successful processor. Shaped to be passed straight to
        :func:`~aind_behavior_vr_foraging_packaging.pipeline.session.process_session`
        as its ``on_output`` hook."""
        self._results.append(
            ProcessorResult(
                name=proc.output_name,
                status="ok",
                rows=len(frame),
                output_file=path.name if path is not None else None,
                warn_count=self._warnings.reset(),
            )
        )
        if proc.output_name == SESSION_TABLE:
            self._identify(frame)

    def on_error(self, proc: "AbstractProcessor", exc: Exception) -> None:
        """Record one failed processor, **then re-raise**.

        ``process_session``'s contract is that an ``on_error`` returning normally
        means "skip this processor and carry on". Recording a failure is not a
        decision to tolerate it: a session missing a table is not a usable partial
        result, so the exception continues on its way and the container exits
        nonzero. The sidecar is the only thing that gains — it now names the
        processor that broke.
        """
        self._results.append(
            ProcessorResult(name=proc.output_name, status="error", error=str(exc), warn_count=self._warnings.reset())
        )
        raise exc

    def nwb_ok(self, dest: Path) -> None:
        self._nwb = ProcessorResult(name="nwb", status="ok", output_file=dest.name, warn_count=self._warnings.reset())

    def nwb_error(self, exc: BaseException) -> None:
        self._nwb = ProcessorResult(name="nwb", status="error", error=str(exc), warn_count=self._warnings.reset())

    # -- assembly ------------------------------------------------------------

    def _status(self) -> Literal["ok", "partial", "error"]:
        """One session-level verdict.

        Any processor failure fails the whole session, and so does an exception
        on the way out of the block — the two are the same event seen from
        inside and outside the loop, and a session missing a table is not a
        usable partial result.
        A dataset that never opened is likewise an error, not an empty success.

        ``"partial"`` is consequently unreachable from here; the type keeps it
        because every consumer already handles it and a caller absorbing
        failures through ``process_session``'s ``on_error`` could produce one.
        """
        if self._dataset_version is None or self._raised:
            return "error"
        if any(r.status == "error" for r in self._results) or (self._nwb is not None and self._nwb.status == "error"):
            return "error"
        return "ok"

    def build(self) -> SessionOutputMetadata:
        """Assemble the record. Safe to call more than once."""
        return SessionOutputMetadata(
            session_name=self.session_name,
            subject_id=self._subject_id,
            session_start=self._session_start,
            status=self._status(),
            started_at=self._started_at,
            finished_at=datetime.now(timezone.utc),
            duration_s=time.monotonic() - self._t0,
            processors=list(self._results),
            nwb=self._nwb,
            code=build_code_ref(),
            versions=PackagingProvenance(
                packaging_version=importlib.metadata.version(_PACKAGING_PKG),
                data_contract_version=aind_behavior_vr_foraging.__semver__,
                # "unknown" only when the dataset never opened, which is the one
                # case where there is no version to read.
                dataset_version=self._dataset_version or "unknown",
            ).model_dump(),
            parameters=self.parameters,
            job_id=os.environ.get("VRF_JOB_ID"),
            worker_id=os.environ.get("VRF_WORKER_ID"),
        )
