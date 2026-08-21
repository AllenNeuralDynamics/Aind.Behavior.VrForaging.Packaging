"""Docker execution + failure classification.

:func:`build_docker_args` builds the ``docker run`` argv for one job's
processor container. :func:`run` executes it with a timeout, capturing
combined stdout+stderr to a plain-text log file. :func:`classify` applies
the failure truth table to turn the exit code and sidecar into the ledger-facing
:class:`Verdict` — the worker never guesses from a raw string.
"""

import logging
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .config import ProcessorConfig
from .models import ErrorKind
from .sidecar import SIDECAR_NAME, SessionOutputMetadata

logger = logging.getLogger(__name__)

_OOM_EXIT_CODE = 137
_ERROR_MAX_LEN = 500


class RunnerConfigError(Exception):
    """The processor cannot be launched at all — no digest without
    ``allow_unpinned``, or Docker itself could not be invoked. Maps to
    ``error_kind='infra'``: it fails identically for every job, so it is an
    environment problem, not a per-session one."""


@dataclass(frozen=True)
class RunResult:
    exit_code: int | None
    timed_out: bool
    duration_s: float
    log_path: Path


@dataclass(frozen=True)
class Verdict:
    """The ledger-facing outcome of one job run."""

    status: Literal["completed", "failed"]
    partial: bool
    error_kind: ErrorKind | None
    error: str | None
    exit_code: int | None
    sidecar: SessionOutputMetadata | None
    sidecar_raw: str | None
    warn_count: int
    failed_processors: str


def _truncate(s: str, limit: int = _ERROR_MAX_LEN) -> str:
    return s if len(s) <= limit else s[: limit - 3] + "..."


def image_ref(processor: ProcessorConfig) -> str:
    """The ``repo@sha256:…`` or ``repo:tag`` string passed to ``docker run``.

    Pin by digest, never by tag — ``allow_unpinned`` exists only for
    local/dev runs and is recorded honestly in the sidecar as ``provenance:
    "unpinned"``, never guessed.
    """
    if processor.digest:
        return f"{processor.image}@{processor.digest}"
    if not processor.allow_unpinned:
        raise RunnerConfigError(
            "processor.digest is required (pin by digest, never by tag) unless processor.allow_unpinned is set"
        )
    logger.warning("Running %s unpinned (no digest) — provenance will be recorded as 'unpinned'.", processor.image)
    return f"{processor.image}:latest"


def build_docker_args(
    processor: ProcessorConfig,
    *,
    job_id: str,
    worker_id: str,
    work_volume: str,
    input_path_in_container: str,
    extra_mount: tuple[str, str] | None = None,
) -> list[str]:
    """Build the ``docker run`` argv for one job.

    *input_path_in_container* and *extra_mount* are resolved by the caller
    (``worker.py``) from the store's :class:`~.stores.PreparedInput` — this
    function only assembles the command line, so it is testable without a
    real store or a real Docker daemon.

    The named work volume (not a bind mount) is what makes the host-path trap
    a non-issue: ``-v work_volume:/work`` is resolved by the daemon *by
    name*, so there is no host path to get wrong. A store that returns a path
    outside the work volume (``mount``, or a pass-through ``local``) needs one
    extra identity-mapped bind mount, passed in as *extra_mount*.

    There is no session-name argument. The processor takes a session's identity
    from its input directory's own name, so the worker's job is to present the
    session *at* that name — which both branches of ``Worker._resolve_mount``
    guarantee. One rule, no override to keep in sync.

    No ``--network=none``, and it must not come back: ``contraqctor`` fetches Harp
    device schemas over HTTPS at load time, and offline every Harp group resolves to
    zero streams (``Data must be a list of DataStreams``). It still cannot upload —
    no credentials are passed and ``docker run`` inherits no environment.
    """
    ref = image_ref(processor)
    args = [
        "docker",
        "run",
        "--rm",
        "--name",
        f"vrf-{job_id}",
        f"--cpus={processor.cpus}",
        f"--memory={processor.memory}",
        "-v",
        f"{work_volume}:/work",
    ]
    if extra_mount is not None:
        host, container = extra_mount
        # `--mount` (comma-separated key=value), not `-v host:container:ro`: a
        # Windows host path (`C:\...`) has its own colon, which the short `-v`
        # form's colon-splitting parser rejects outright ("too many colons").
        args += ["--mount", f"type=bind,source={host},target={container},readonly"]
    if processor.digest:
        args += ["-e", f"PROCESSOR_IMAGE_URI={ref}"]
    args += [
        "-e",
        f"VRF_JOB_ID={job_id}",
        "-e",
        f"VRF_WORKER_ID={worker_id}",
        ref,
        # `process`: exactly one session, and it writes the sidecar. Aggregation is
        # a separate job kind over the published output tree, never
        # per-session work — nothing here can accidentally trigger it.
        "process",
        "--input-dir",
        input_path_in_container,
        "--output-dir",
        f"/work/{job_id}/out",
    ]
    if processor.write_nwb:
        args.append("--write-nwb")
    for name in processor.exclude_processors:
        # Repeated, not space-separated: pydantic-settings gives a list field an
        # `append` action, so `--exclude-processors a b` leaves `b` unrecognised
        # and the container exits 2 before running anything.
        args += ["--exclude-processors", name]
    return args


def run(args: list[str], *, job_id: str, log_path: Path, timeout_s: int) -> RunResult:
    """Execute ``docker run …``, capturing combined stdout+stderr to *log_path*
    (plain text). A nonzero container exit is a normal outcome, not an
    exception — :func:`classify` decides what it means. Only raises
    :class:`RunnerConfigError` if Docker itself could not be invoked at all
    (daemon down, binary missing).
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    timed_out = False
    exit_code: int | None = None
    try:
        with open(log_path, "w", encoding="utf-8") as log_file:
            try:
                proc = subprocess.run(args, stdout=log_file, stderr=subprocess.STDOUT, timeout=timeout_s, check=False)
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                # subprocess.run() only kills the `docker run` client process on
                # timeout, not the container it started — `--rm` alone does not
                # guarantee cleanup here, so stop the named container explicitly.
                subprocess.run(["docker", "kill", f"vrf-{job_id}"], capture_output=True, check=False)
    except OSError as exc:
        raise RunnerConfigError(f"Could not invoke docker: {exc}") from exc
    return RunResult(exit_code=exit_code, timed_out=timed_out, duration_s=time.monotonic() - t0, log_path=log_path)


def classify(result: RunResult, sidecar_path: Path, *, expected_digest: str | None) -> Verdict:
    """Apply the truth table: exit code × sidecar → one ledger-facing verdict.

    Both signals are needed, and neither alone is sufficient. The processor
    propagates any failure, so a broken session exits nonzero *and* leaves a
    sidecar naming what broke — the exit code says a failure happened, the
    sidecar says whether it happened to the *data* (a processor, hence
    ``error_kind='data'``) or to the run itself (``'code'``). A nonzero exit with
    a clean sidecar is a genuine contradiction and is resolved in the exit
    code's favour.

    Only the environmental verdicts are taken without consulting the sidecar,
    because in those cases there may not be one: timeout and OOM.
    """
    if result.timed_out:
        return Verdict(
            status="failed",
            partial=False,
            error_kind="timeout",
            error=f"container exceeded job_timeout_s ({result.duration_s:.0f}s elapsed)",
            exit_code=result.exit_code,
            sidecar=None,
            sidecar_raw=None,
            warn_count=0,
            failed_processors="",
        )
    if result.exit_code == _OOM_EXIT_CODE:
        return Verdict(
            status="failed",
            partial=False,
            error_kind="infra",
            error="container was OOM-killed (exit 137)",
            exit_code=result.exit_code,
            sidecar=None,
            sidecar_raw=None,
            warn_count=0,
            failed_processors="",
        )

    sidecar, sidecar_raw, parse_error = _read_sidecar(sidecar_path)
    if parse_error is not None:
        return Verdict(
            status="failed",
            partial=False,
            error_kind="code",
            error=parse_error,
            exit_code=result.exit_code,
            sidecar=None,
            sidecar_raw=sidecar_raw,
            warn_count=0,
            failed_processors="",
        )

    if sidecar is None:
        detail = (
            f"exit 0 but {SIDECAR_NAME} is missing — the process died before writing one"
            if result.exit_code == 0
            else f"container exited with code {result.exit_code} and wrote no {SIDECAR_NAME}"
        )
        return Verdict(
            status="failed",
            partial=False,
            error_kind="code",
            error=detail,
            exit_code=result.exit_code,
            sidecar=None,
            sidecar_raw=None,
            warn_count=0,
            failed_processors="",
        )

    def verdict(
        status: Literal["completed", "failed"],
        *,
        partial: bool = False,
        error_kind: ErrorKind | None = None,
        error: str | None = None,
    ) -> Verdict:
        return Verdict(
            status=status,
            partial=partial,
            error_kind=error_kind,
            error=_truncate(error) if error else None,
            exit_code=result.exit_code,
            sidecar=sidecar,
            sidecar_raw=sidecar_raw,
            warn_count=sidecar.warn_count,
            failed_processors=",".join(sidecar.failed_processors),
        )

    if (
        expected_digest is not None
        and sidecar.code.container is not None
        and sidecar.code.container.digest != expected_digest
    ):
        return verdict(
            "failed",
            error_kind="code",
            error=f"sidecar image digest {sidecar.code.container.digest} != launched digest {expected_digest}",
        )

    if sidecar.status == "error":
        first_error = next((p.error for p in sidecar.processors if p.error), None) or "session reported status=error"
        return verdict("failed", error_kind="data", error=first_error)

    if result.exit_code != 0:
        # The sidecar recorded no failure, yet the process still died — so
        # whatever broke happened outside the work the sidecar covers. Believe
        # the exit code; a session is not usable because a record says so.
        return verdict(
            "failed",
            error_kind="code",
            error=f"container exited with code {result.exit_code} but the sidecar reports status={sidecar.status!r}",
        )

    return verdict("completed", partial=sidecar.status == "partial")


def _read_sidecar(sidecar_path: Path) -> tuple[SessionOutputMetadata | None, str | None, str | None]:
    """Returns ``(sidecar, raw_json, parse_error)``. Exactly one of ``sidecar``/
    ``parse_error`` is set when the file exists; both are ``None`` if it is
    simply absent (a distinct, non-error case handled by the caller)."""
    if not sidecar_path.exists():
        return None, None, None
    raw = sidecar_path.read_text(encoding="utf-8")
    try:
        sidecar = SessionOutputMetadata.model_validate_json(raw)
    except Exception as exc:
        return None, raw, _truncate(f"sidecar failed validation: {type(exc).__name__}: {exc}")
    return sidecar, raw, None
