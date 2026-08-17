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
import os
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

from aind_behavior_vr_foraging_packaging._provenance import _PACKAGING_PKG

from . import runner
from .config import PipelineConfig
from .ledger import Ledger
from .models import Job
from .sidecar import SIDECAR_NAME
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

#: Environment variable naming the image THIS worker is running (§12), set on the
#: worker service in ``docker/compose.yaml``. Deliberately not ``PROCESSOR_IMAGE_URI``,
#: which ``runner.build_docker_args`` sets explicitly on each *child* container and
#: which ``sidecar.build_code_ref`` reads there: the two happen to hold the same
#: value today (one image, three entrypoints), and reusing the name would bake that
#: coincidence in and make each layer's provenance unattributable to it.
_WORKER_IMAGE_ENV = "VRF_WORKER_IMAGE_URI"

#: Filename a published job log is stored under, inside its own prefix.
_LOG_NAME = "_log.txt"

#: Work-volume directory a log is staged into before publishing (the output stores
#: publish a *directory*, and the log lives outside the work volume until then).
#: Prefixed so :meth:`Worker.sweep_work_dir` can tell it from a job directory and
#: still recover the job id it belongs to.
_LOG_STAGE_PREFIX = "_log_"

#: Job states whose work directory nothing will ever come back for, and which
#: :meth:`Worker.sweep_work_dir` may therefore reclaim. Deliberately not
#: exhaustive over ``JobStatus``: each remaining state has exactly one owner, and
#: the sweeper is not it. ``running`` is another worker's live job — the claim that
#: sets ``running`` happens before any ``mkdir``, which is what makes this check
#: race-free without resorting to mtime heuristics. ``pending``/``retrying`` belong
#: to the next attempt, whose entry-side cleanup reclaims the directory as its
#: first act; leaving them alone also closes the read-then-delete window a status
#: check would otherwise open against a job being claimed right now.
_RECLAIMABLE_STATUSES = frozenset({"completed", "failed", "dead", "skipped"})


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

        # The processor's pin is only half of the provenance chain. `allow_unpinned`
        # governs both halves: a run that is allowed to be unreproducible is allowed
        # to be unreproducible on both sides, and one that is not, is not.
        image = self.worker_image()
        if not self.config.processor.allow_unpinned:
            if image is None:
                problems.append(
                    f"{_WORKER_IMAGE_ENV} is unset — this worker cannot record which image it is running, "
                    "so nothing will say what staged, classified and published a campaign's output. "
                    "Set it on the worker service (see docker/compose.yaml)"
                )
            elif "@sha256:" not in image:
                problems.append(
                    f"{_WORKER_IMAGE_ENV}={image!r} is not digest-pinned — a tag is a moving target, "
                    "so the recorded value would not identify the code that ran"
                )

        free = self.free_disk_bytes()
        floor = self.config.worker.min_free_disk_bytes
        if free is not None and free < floor:
            problems.append(
                f"only {free / 1e9:.1f} GB free on {self.work_dir}, below "
                f"worker.min_free_disk_bytes ({floor / 1e9:.1f} GB) — the worker will refuse to claim"
            )

        # Reported, never acted on: `doctor` is a read-only preflight, and stranded
        # directories are the claim loop's business to reclaim (`sweep_work_dir`).
        # Neither is a problem in itself, so neither is appended above.
        reclaimable, unknown = self._triage_work_dir()
        if reclaimable:
            logger.info(
                "%d stranded work director(ies) under %s will be reclaimed by the next sweep",
                len(reclaimable),
                self.work_dir,
            )
        if unknown:
            logger.warning(
                "%d director(ies) under %s do not correspond to any job in this ledger and will be left alone: %s",
                len(unknown),
                self.work_dir,
                ", ".join(unknown[:10]),
            )
        if self.config.worker.keep_work_dir:
            logger.warning("worker.keep_work_dir is set — nothing will reclaim work directories. Debugging only.")

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
        """Run one claimed job, owning its work directory for the whole lifetime.

        Cleanup is on **entry** as well as exit, and the entry half is the
        load-bearing one. ``job_id`` is stable across attempts — reaping an expired
        lease returns the same row to ``pending``, so a previous attempt that died
        mid-write (SIGKILL, an OOM kill, a host reboot) left its wreckage at exactly
        the path this attempt is about to use, and ``mkdir(exist_ok=True)`` would
        adopt it. That is not merely untidy: :meth:`_finish_success` publishes
        ``out/`` wholesale, so a stale parquet from attempt N ships alongside
        attempt N+1's real output, and since the sidecar is rewritten every time,
        ``classify`` reports a clean success over it. Cleaning on entry is also the
        only ordering that survives a kill at all — whatever killed attempt N
        cannot stop attempt N+1 from starting clean, whereas no amount of
        ``try``/``finally`` can run after SIGKILL.

        The exit half is then just about disk: it bounds what the volume holds
        between sweeps. It lives in a ``finally`` so the six failure returns in
        :meth:`_run_job` cannot each forget it.
        """
        job_dir = self.work_dir / job.job_id
        shutil.rmtree(job_dir, ignore_errors=True)
        job_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._run_job(job, job_dir)
        finally:
            if self.config.worker.keep_work_dir:
                logger.warning(
                    "[%s] worker.keep_work_dir is set — leaving %s in place. Unset it for a campaign.",
                    job.session_name,
                    job_dir,
                )
            else:
                shutil.rmtree(job_dir, ignore_errors=True)

    def _run_job(self, job: Job, job_dir: Path) -> None:
        """One job, start to finish. Never cleans up *job_dir* — that is
        :meth:`process_job`'s job, on both sides of this call."""
        log_path = self.logs_dir / f"{job.job_id}.log"
        session_name = job.session_name or job_dir.name

        if not self.config.output.overwrite and self.output_store.exists(job.output_uri):
            logger.info("[%s] output already exists (overwrite=false) — skipping", job.session_name)
            self.ledger.skip_running(job.job_id, "output already exists (overwrite=false)")
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
            return
        except StoreConfigError as exc:
            self.ledger.fail_job(job.job_id, error_kind="infra", error=str(exc)[:500])
            return
        except StoreTransientError as exc:
            self.ledger.fail_job(job.job_id, error_kind="transient", error=str(exc)[:500])
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
            return
        finally:
            if prepared is not None:
                self.input_store.release(prepared)

        # Before recording either outcome, so `log_uri` is the published location
        # rather than a path on this worker's disk that nothing else can reach.
        log_uri = self._publish_log(job, log_path)

        if verdict.status == "completed":
            self._finish_success(job, job_dir, log_uri, verdict, t_stage_s, t_run_s, prepared)
        else:
            self.ledger.fail_job(
                job.job_id,
                error_kind=verdict.error_kind or "code",
                error=verdict.error or "unknown failure",
                exit_code=verdict.exit_code,
                sidecar=verdict.sidecar_raw,
                log_uri=log_uri,
                warn_count=verdict.warn_count,
                failed_processors=verdict.failed_processors,
                t_stage_s=t_stage_s,
                t_run_s=t_run_s,
            )

    def _finish_success(
        self,
        job: Job,
        job_dir: Path,
        log_uri: str | None,
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
            log_uri=log_uri,
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

    def _publish_log(self, job: Job, log_path: Path) -> str | None:
        """Publish one attempt's container log; return what to record as ``log_uri``.

        Every outcome goes to the same prefix, not just failures. The disk saved is
        incidental (~78 MB across a 4700-session campaign, measured); the point is
        that ``jobs.log_uri`` used to hold a worker-local path for a success and an
        output-store URI for a failure, so nothing could follow the column without
        first guessing which kind of string it had. Now it is one kind.

        The local copy is deleted only once the publish has actually succeeded. If
        it has not, the local path is recorded instead — a reachable log in the
        wrong place beats a URI pointing at nothing.
        """
        if not log_path.exists():
            return None
        if not self.config.logging.upload:
            return str(log_path)

        base = self.config.output.uri.rstrip("/")
        dest = f"{base}/{self.config.release}/{self.config.output.log_prefix}{job.job_id}/"
        # The output stores publish a directory, and the log lives in `logs_dir`,
        # outside the work volume — so it is staged into a directory of its own.
        stage_dir = self.work_dir / f"{_LOG_STAGE_PREFIX}{job.job_id}"
        shutil.rmtree(stage_dir, ignore_errors=True)
        stage_dir.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(log_path, stage_dir / _LOG_NAME)
            self.output_store.publish(stage_dir, dest)
        except (StoreTransientError, StoreConfigError, OSError) as exc:
            logger.warning(
                "Could not publish the log for %s (%s) — keeping the local copy at %s", job.job_id, exc, log_path
            )
            return str(log_path)
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)
        log_path.unlink(missing_ok=True)
        return f"{dest}{_LOG_NAME}"

    # ------------------------------------------------------------------
    # Work-volume housekeeping (§4a)
    # ------------------------------------------------------------------

    def _triage_work_dir(self) -> tuple[list[Path], list[str]]:
        """Partition the work volume's directories into "safe to reclaim" and "not
        ours". Read-only, so :meth:`doctor` can report what a sweep would remove
        without removing it.

        The ledger decides, because it is the only thing that knows whether another
        worker is using a directory right now — several workers share this volume,
        and deleting a live job's directory would be far worse than the leak it
        fixes. Anything whose name is not a job id this ledger recognises is left
        strictly alone and reported: a shared volume is the last place to delete
        things you cannot account for.
        """
        if not self.work_dir.is_dir():
            return [], []
        job_id_of: dict[Path, str] = {}
        for path in self.work_dir.iterdir():
            if not path.is_dir():
                continue
            name = path.name
            job_id_of[path] = name.removeprefix(_LOG_STAGE_PREFIX)
        statuses = self.ledger.job_statuses(sorted(set(job_id_of.values())))

        reclaimable: list[Path] = []
        unknown: list[str] = []
        for path, job_id in job_id_of.items():
            status = statuses.get(job_id)
            if status is None:
                unknown.append(path.name)
            elif status in _RECLAIMABLE_STATUSES:
                reclaimable.append(path)
        return sorted(reclaimable), sorted(unknown)

    def sweep_work_dir(self) -> int:
        """Reclaim work directories the ledger has finished with. Returns how many.

        The filesystem half of what :meth:`Ledger.reap_expired_leases` already does
        for the ledger, and needed for the same reason: a worker killed mid-job
        leaves a directory behind, and reaping repairs the row while knowing nothing
        about disk. Without this, a terminal job's directory is never reclaimed by
        anyone — its owner is never coming back for it — and the volume grows by one
        session per crash until a campaign stops on ENOSPC.
        """
        if self.config.worker.keep_work_dir:
            return 0
        reclaimable, unknown = self._triage_work_dir()
        for path in reclaimable:
            shutil.rmtree(path, ignore_errors=True)
        if reclaimable:
            logger.info("Reclaimed %d stranded work director(ies) under %s", len(reclaimable), self.work_dir)
        if unknown:
            logger.warning(
                "%d director(ies) under %s are not job ids this ledger knows (%s%s) — left untouched",
                len(unknown),
                self.work_dir,
                ", ".join(unknown[:5]),
                ", …" if len(unknown) > 5 else "",
            )
        return len(reclaimable)

    def free_disk_bytes(self) -> int | None:
        """Free space on the work volume, or ``None`` if it cannot be determined.

        Walks up to the nearest existing ancestor rather than giving up when
        ``work_dir`` itself does not exist yet — a first tick before any job has
        created it would otherwise report "unknown", and "unknown" does not block a
        claim. That turned the disk guard into a silent no-op on exactly the run
        where nothing has been created yet.
        """
        for candidate in (self.work_dir, *self.work_dir.parents):
            try:
                return shutil.disk_usage(candidate).free
            except OSError:
                continue
        return None

    def worker_image(self) -> str | None:
        """The image this worker is itself running, or ``None`` outside a container.

        Read from the environment rather than derived, for the same reason
        :func:`sidecar.build_code_ref` does it: a process cannot discover which image
        it was started from, and the only honest alternative to being told is to
        record nothing. Recorded on every heartbeat so the ledger answers "what code
        staged, classified and published this?" — which the ``jobs`` table's
        ``image_ref``/``image_digest`` do not, those being the *processor's*.
        """
        return os.environ.get(_WORKER_IMAGE_ENV) or None

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def heartbeat(self, *, running_jobs: int = 0) -> None:
        self.ledger.heartbeat(
            self.worker_id,
            running_jobs=running_jobs,
            disk_free_bytes=self.free_disk_bytes(),
            worker_image=self.worker_image(),
        )

    def run_forever(self, *, once: bool = False) -> None:
        """The worker's main loop: repair state, ingest on a timer, claim and process
        jobs, heartbeat. Sequential — one job at a time; see the module docstring on
        intra-worker concurrency."""
        last_ingest = 0.0
        while True:
            # Ledger repair and volume repair are the same phase, and in this order:
            # reaping is what moves a crashed job to a state the sweep may reclaim,
            # and the sweep can therefore clear the disk condition checked below
            # within the same tick.
            self.ledger.reap_expired_leases()
            self.sweep_work_dir()
            self.heartbeat(running_jobs=0)

            now = time.monotonic()
            if now - last_ingest >= self.config.ingestion.interval_s:
                try:
                    self.ingest_once()
                except Exception:
                    logger.exception("Ingest sweep failed")
                last_ingest = now

            if not self._disk_ok():
                if once:
                    return
                time.sleep(self.config.worker.poll_interval_s)
                continue

            processed = self.claim_and_process_one()
            if once:
                return
            if not processed:
                time.sleep(self.config.worker.poll_interval_s)

    def _disk_ok(self) -> bool:
        """Whether there is room to take on another job (§4a).

        Checked before claiming, not during: a job claimed onto a full volume dies
        on ENOSPC and burns one of ``max_attempts``, so an unguarded worker would
        chew through real sessions three attempts at a time rather than waiting for
        space. Refusing to claim leaves the queue exactly as it was.
        """
        free = self.free_disk_bytes()
        floor = self.config.worker.min_free_disk_bytes
        if free is None or free >= floor:
            return True
        logger.error(
            "Only %.1f GB free on %s, below worker.min_free_disk_bytes (%.1f GB) — not claiming any job",
            free / 1e9,
            self.work_dir,
            floor / 1e9,
        )
        return False
