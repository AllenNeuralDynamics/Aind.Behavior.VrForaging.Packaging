"""The worker — claim loop, ingest timer, lease reaper, heartbeat (§4, §7, §16).

A single :class:`Worker` instance owns one ledger connection and drives one
job at a time end to end: claim → prepare input (§10) → run the processor
container (§12) → classify (§8) → publish output (§10b) → record the outcome.
Intra-worker concurrency (``worker.max_concurrent_jobs > 1`` executing jobs in
parallel within one process) is not implemented in this pass — run multiple
worker processes/replicas for parallelism today; the ledger's atomic claim
(§7) already makes that safe.
"""

import importlib.metadata
import logging
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

from .._provenance import _PACKAGING_PKG
from .._sidecar import SIDECAR_NAME
from . import runner
from .config import PipelineConfig
from .ledger import Ledger
from .models import Job
from .sources import get_source
from .stores import (
    InputStore,
    OutputStore,
    PreparedInput,
    StoreConfigError,
    StoreDataError,
    StoreTransientError,
    get_input_store,
    get_output_store,
)

if TYPE_CHECKING:
    from .sources import Source

logger = logging.getLogger(__name__)


class Worker:
    """Drives one job at a time against the ledger, stores, and the processor image.

    *work_dir* is this process's own filesystem view of the shared named work
    volume (§4a) — ``/work`` in the real, containerized deployment. Overridable
    for tests, which never launch a real sibling container.
    """

    def __init__(
        self,
        config: PipelineConfig,
        *,
        worker_id: str,
        work_dir: Path = Path("/work"),
        ledger: Ledger | None = None,
        input_store: InputStore | None = None,
        output_store: OutputStore | None = None,
    ) -> None:
        self.config = config
        self.worker_id = worker_id
        self.work_dir = Path(work_dir)
        self.logs_dir = Path(config.logging.dir)
        self.ledger = ledger or Ledger(config.worker.ledger)
        self.input_store = input_store or get_input_store(
            config.input.store, staging=config.staging, **self._input_store_kwargs()
        )
        self.output_store = output_store or get_output_store(config.output.store)

    def _input_store_kwargs(self) -> dict:
        if self.config.input.store == "local":
            return {"copy_files": self.config.input.copy_files}
        return {}

    def close(self) -> None:
        self.ledger.close()

    # ------------------------------------------------------------------
    # Preflight (§4a)
    # ------------------------------------------------------------------

    def doctor(self) -> list[str]:
        """Cheap-to-verify, expensive-to-discover-mid-campaign checks. Returns a
        list of problems found; empty means healthy. Covers the §4(a)
        volume-visibility assertion (the single highest-value guard in the
        design), Docker reachability, and an unpinned-image warning.
        """
        problems: list[str] = []

        try:
            self.work_dir.mkdir(parents=True, exist_ok=True)
            sentinel = self.work_dir / f".doctor-{self.worker_id}"
            sentinel.write_text("ok", encoding="utf-8")
            sentinel.unlink()
        except OSError as exc:
            problems.append(f"work_dir {self.work_dir} is not writable: {exc}")

        import subprocess

        try:
            proc = subprocess.run(
                ["docker", "version", "--format", "{{.Server.Version}}"], capture_output=True, timeout=10
            )
            if proc.returncode != 0:
                problems.append(f"docker daemon unreachable: {proc.stderr.decode(errors='replace').strip()}")
        except (OSError, subprocess.TimeoutExpired) as exc:
            problems.append(f"could not invoke docker: {exc}")

        if not self.config.processor.digest and not self.config.processor.allow_unpinned:
            problems.append("processor.digest is unset and processor.allow_unpinned is false — no image to launch")

        return problems

    # ------------------------------------------------------------------
    # Ingestion (§3, §6)
    # ------------------------------------------------------------------

    def processor_fingerprint(self) -> str:
        """§6: the pinned digest in production, else the packaging version for
        local runs without one — either way, a version bump changes every
        session's ``job_key`` automatically."""
        return self.config.processor.digest or importlib.metadata.version(_PACKAGING_PKG)

    def output_uri_for(self, session_name: str) -> str:
        base = self.config.output.uri.rstrip("/")
        return f"{base}/{self.config.release}/sessions/{session_name}/"

    def source(self) -> "Source":
        """Construct this worker's configured :class:`~.sources.Source` (§3)."""
        return get_source(self.config.ingestion.type, **self._source_kwargs())

    def ingest_once(self) -> int:
        """One discovery sweep: upsert every session the source reports since the
        last watermark. Routine ingestion is a no-op via ``job_key`` (§6) — the
        return value is how many were genuinely *new*."""
        source = self.source()
        since = self.ledger.get_watermark(source.name)
        fingerprint = self.processor_fingerprint()
        new_count = 0
        max_cursor = since
        for ref in source.discover(since):
            job_id = self.ledger.upsert_job(
                kind="session",
                release=self.config.release,
                asset_id=ref.asset_id,
                processor_fingerprint=fingerprint,
                input_store=self.config.input.store,
                input_uri=ref.input_uri,
                output_uri=self.output_uri_for(ref.session_name),
                session_name=ref.session_name,
                subject_id=ref.subject_id,
                session_start=ref.session_start.isoformat() if ref.session_start else None,
            )
            if job_id is not None:
                new_count += 1
            if ref.cursor and (max_cursor is None or ref.cursor > max_cursor):
                max_cursor = ref.cursor
        if max_cursor is not None:
            self.ledger.set_watermark(source.name, max_cursor)
        logger.info("Ingest sweep (%s): %d new job(s)", source.name, new_count)
        return new_count

    def _source_kwargs(self) -> dict:
        ing = self.config.ingestion
        if ing.type == "local":
            return {"root": ing.root, "name_pattern": ing.name_pattern}
        kwargs: dict = {"acquisition_types": ing.acquisition_types, "name_pattern": ing.name_pattern}
        if ing.legacy_fallback is not None:
            kwargs["legacy_project_name"] = ing.legacy_fallback.project_name
            kwargs["legacy_session_before"] = ing.legacy_fallback.session_before
        else:
            kwargs["legacy_project_name"] = None
        return kwargs

    # ------------------------------------------------------------------
    # Claim + process (§7, §8, §10, §10b)
    # ------------------------------------------------------------------

    def claim_and_process_one(self) -> bool:
        """Claim and fully process one job. Returns ``False`` if the queue was empty."""
        job = self.ledger.claim(self.worker_id, self.config.worker.lease_seconds)
        if job is None:
            return False
        try:
            self.process_job(job)
        except Exception:
            logger.exception("[%s] Unhandled error processing job %s", job.session_name, job.job_id)
            self.ledger.fail_job(job.job_id, error_kind="code", error="unhandled worker exception — see worker log")
        return True

    @staticmethod
    def _session_out_dir(job_dir: Path) -> Path:
        """Where ``vr-foraging-packaging session`` writes: straight into
        ``--output-dir``. There is no ``sessions/`` level, because one container
        processes exactly one session and has nothing to keep it apart from."""
        return job_dir / "out"

    def process_job(self, job: Job) -> None:
        job_dir = self.work_dir / job.job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.logs_dir / f"{job.job_id}.log"
        session_name = job.session_name or job_dir.name

        if not self.config.output.overwrite and self.output_store.exists(job.output_uri):
            logger.info("[%s] output already exists (overwrite=false) — skipping", job.session_name)
            self.ledger.skip_running(job.job_id, "output already exists (overwrite=false)")
            shutil.rmtree(job_dir, ignore_errors=True)
            return

        t_stage_s: float | None = None
        t_run_s: float | None = None
        prepared: PreparedInput | None = None

        try:
            t0 = time.monotonic()
            refs = self.input_store.list_objects(job.input_uri)
            prepared = self.input_store.prepare(job.input_uri, refs, self._stage_dir(job_dir, session_name))
            t_stage_s = time.monotonic() - t0
        except StoreDataError as exc:
            self.ledger.fail_job(job.job_id, error_kind="data", error=str(exc)[:500])
            shutil.rmtree(job_dir, ignore_errors=True)
            return
        except StoreConfigError as exc:
            self.ledger.fail_job(job.job_id, error_kind="infra", error=str(exc)[:500])
            shutil.rmtree(job_dir, ignore_errors=True)
            return
        except StoreTransientError as exc:
            self.ledger.fail_job(job.job_id, error_kind="transient", error=str(exc)[:500])
            shutil.rmtree(job_dir, ignore_errors=True)
            return

        try:
            input_in_container, extra_mount = self._resolve_mount(prepared, job_dir, session_name)
            args = runner.build_docker_args(
                self.config.processor,
                job_id=job.job_id,
                worker_id=self.worker_id,
                work_volume=self.config.worker.work_volume,
                input_path_in_container=input_in_container,
                extra_mount=extra_mount,
            )
            t0 = time.monotonic()
            result = runner.run(
                args, job_id=job.job_id, log_path=log_path, timeout_s=self.config.processor.job_timeout_s
            )
            t_run_s = time.monotonic() - t0
            verdict = runner.classify(
                result, self._session_out_dir(job_dir) / SIDECAR_NAME, expected_digest=self.config.processor.digest
            )
        except runner.RunnerConfigError as exc:
            self.ledger.fail_job(job.job_id, error_kind="infra", error=str(exc)[:500])
            shutil.rmtree(job_dir, ignore_errors=True)
            return
        finally:
            if prepared is not None:
                self.input_store.release(prepared)

        if verdict.status == "completed":
            self._finish_success(job, job_dir, log_path, verdict, t_stage_s, t_run_s, prepared)
        else:
            self.ledger.fail_job(
                job.job_id,
                error_kind=verdict.error_kind or "code",
                error=verdict.error or "unknown failure",
                exit_code=verdict.exit_code,
                sidecar=verdict.sidecar_raw,
                log_uri=str(log_path),
                warn_count=verdict.warn_count,
                failed_processors=verdict.failed_processors,
                t_stage_s=t_stage_s,
                t_run_s=t_run_s,
            )
            self._publish_failed_log(job, log_path)

        shutil.rmtree(job_dir, ignore_errors=True)

    def _finish_success(
        self,
        job: Job,
        job_dir: Path,
        log_path: Path,
        verdict: runner.Verdict,
        t_stage_s: float | None,
        t_run_s: float | None,
        prepared: PreparedInput | None,
    ) -> None:
        t0 = time.monotonic()
        try:
            manifest = self.output_store.publish(self._session_out_dir(job_dir), job.output_uri)
        except StoreTransientError as exc:
            self.ledger.fail_job(job.job_id, error_kind="transient", error=f"publish failed: {exc}"[:500])
            return
        t_publish_s = time.monotonic() - t0

        versions = verdict.sidecar.versions if verdict.sidecar else {}
        self.ledger.complete_job(
            job.job_id,
            partial=verdict.partial,
            exit_code=verdict.exit_code,
            sidecar=verdict.sidecar_raw,
            log_uri=str(log_path),
            warn_count=verdict.warn_count,
            failed_processors=verdict.failed_processors,
            t_stage_s=t_stage_s,
            t_run_s=t_run_s,
            t_publish_s=t_publish_s,
            staged_bytes=prepared.manifest.available_bytes if prepared else None,
            read_files=(verdict.sidecar.staged.get("read_files") if verdict.sidecar else None),
            read_bytes=(verdict.sidecar.staged.get("read_bytes") if verdict.sidecar else None),
            output_bytes=manifest.bytes,
            image_ref=self.config.processor.image,
            image_digest=self.config.processor.digest,
            git_commit=(verdict.sidecar.code.commit if verdict.sidecar else None),
            packaging_version=versions.get("packaging_version"),
            data_contract_version=versions.get("data_contract_version"),
            dataset_version=versions.get("dataset_version"),
        )

    #: Container-side parent directory for a session living outside the work
    #: volume (``mount``, or a pass-through ``local``). Deliberately not the host
    #: path reused verbatim: the "identity-mapped" requirement in §4a is about
    #: the *worker* and the *daemon* agreeing on the HOST-side string for
    #: `-v`/`--mount`'s source — the container-side target is this process's own
    #: business and need not (and, notably on a Windows host, cannot) look like
    #: the host path at all.
    _MOUNT_ROOT = "/mnt"

    @staticmethod
    def _stage_dir(job_dir: Path, session_name: str) -> Path:
        """Where a copying store stages the session: ``{job_dir}/in/{session_name}``.

        Named after the session, not just ``in``, for the same reason
        :attr:`_MOUNT_ROOT` gets a session-named child — see
        :meth:`_resolve_mount`.
        """
        return job_dir / "in" / session_name

    def _resolve_mount(
        self, prepared: PreparedInput, job_dir: Path, session_name: str
    ) -> tuple[str, tuple[str, str] | None]:
        """Decide the container-side input path, and whether an extra identity-mapped
        bind mount (§4a) is needed — true whenever the store handed back a path
        outside the shared work volume (``mount``, or a pass-through ``local``).

        Either way the path's **last component is the session name**, because that
        is where the processor reads a session's identity from: ``session_id`` in
        every table is the input directory's own name. Get this wrong and nothing
        errors — every table is simply stamped with the wrong session. It is the
        reason the container needs no ``--session-name`` flag, and the reason a
        pass-through ``mount`` cannot just be exposed at a fixed path.
        """
        host_path = prepared.host_path.resolve()
        if host_path == self._stage_dir(job_dir, session_name).resolve():
            return f"/work/{job_dir.name}/in/{session_name}", None
        target = f"{self._MOUNT_ROOT}/{session_name}"
        return target, (str(host_path), target)

    def _publish_failed_log(self, job: Job, log_path: Path) -> None:
        if not log_path.exists():
            return
        tmp_dir = self.work_dir / f"_failed_log_{job.job_id}"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(log_path, tmp_dir / "_log.txt")
            base = self.config.output.uri.rstrip("/")
            dest = f"{base}/{self.config.release}/{self.config.output.failed_log_prefix}{job.job_id}/"
            self.output_store.publish(tmp_dir, dest)
        except StoreTransientError as exc:
            logger.warning("Could not publish failed-job log for %s: %s", job.job_id, exc)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def heartbeat(self, *, running_jobs: int = 0) -> None:
        disk_free = shutil.disk_usage(self.work_dir).free if self.work_dir.exists() else None
        self.ledger.heartbeat(self.worker_id, running_jobs=running_jobs, disk_free_bytes=disk_free)

    def run_forever(self, *, once: bool = False) -> None:
        """The worker's main loop: reap expired leases, ingest on a timer, claim
        and process jobs, heartbeat. Sequential — one job at a time; see the
        module docstring on intra-worker concurrency."""
        last_ingest = 0.0
        while True:
            self.ledger.reap_expired_leases()
            self.heartbeat(running_jobs=0)

            now = time.monotonic()
            if now - last_ingest >= self.config.ingestion.interval_s:
                try:
                    self.ingest_once()
                except Exception:
                    logger.exception("Ingest sweep failed")
                last_ingest = now

            processed = self.claim_and_process_one()
            if once:
                return
            if not processed:
                time.sleep(self.config.worker.poll_interval_s)
