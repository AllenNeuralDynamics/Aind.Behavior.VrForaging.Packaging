"""The worker — claim loop, ingest timer, lease reaper, heartbeat.

A single :class:`Worker` instance owns one ledger connection and drives one
job at a time end to end: claim → prepare input → run the processor
container → classify → publish output → record the outcome.
Intra-worker concurrency (``worker.max_concurrent_jobs > 1`` executing jobs in
parallel within one process) is not implemented in this pass — run multiple
worker processes/replicas for parallelism today; the ledger's atomic claim
already makes that safe.
"""

import importlib.metadata
import json
import logging
import os
import re
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from aind_behavior_vr_foraging_packaging._provenance import _PACKAGING_PKG

from . import runner
from .config import PipelineConfig
from .ledger import Ledger
from .models import Job
from .sidecar import SIDECAR_NAME, AggregateOutputMetadata, aggregate_watermark, build_code_ref, session_token
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

#: This worker's own image, set on the worker service in compose. Not
#: ``PROCESSOR_IMAGE_URI``: that is set per child container, and sharing the name would
#: make each layer's provenance unattributable to it.
_WORKER_IMAGE_ENV = "VRF_WORKER_IMAGE_URI"

_LOG_NAME = "_log.txt"

#: Log staging dir on the work volume. Prefixed so ``sweep_work_dir`` can tell it from a
#: job dir and still recover the job id.
_LOG_STAGE_PREFIX = "_log_"

#: States whose work dir nothing will come back for. ``running`` is a live job (claim
#: sets it before any ``mkdir``, so no mtime guesswork); ``pending``/``retrying`` belong
#: to the next attempt, whose entry-side cleanup reclaims it.
_RECLAIMABLE_STATUSES = frozenset({"completed", "failed", "dead", "skipped"})


#: Names the ``latest`` mirror, and the pattern every other child of ``aggregate/`` has
#: to match. Both are needed together: ``latest`` sorts *above* every date, because
#: digits precede letters — so ``max()`` over the children picks the mirror, not the
#: newest real aggregate. Anything scanning that prefix filters with ``_DAY_RE`` first.
_LATEST = "latest"
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _manifest_day(manifest: "AggregateOutputMetadata") -> str:
    """The UTC day an aggregate belongs to, from its own timestamp rather than from
    ``now`` — so the prefix a run writes and the prefix it logs cannot disagree if it
    straddles midnight."""
    return manifest.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _parquet_rows(payload: bytes) -> int:
    """Row count from parquet footer metadata — no column data read."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        return int(pq.ParquetFile(pa.BufferReader(payload)).metadata.num_rows)
    except Exception as exc:  # a row count is a nicety; never fail an aggregate over it
        logger.debug("Could not read a parquet row count: %s", exc)
        return -1


class Worker:
    """Drives one job at a time against the ledger, stores, and the processor image.

    *work_dir* is this process's own filesystem view of the shared named work
    volume — ``/work`` in the real, containerized deployment. Overridable
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
    # Preflight
    # ------------------------------------------------------------------

    def doctor(self) -> list[str]:
        """Cheap-to-verify, expensive-to-discover-mid-campaign checks. Returns a
        list of problems found; empty means healthy. Covers the
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

        # The processor's pin is half the chain; `allow_unpinned` governs both halves.
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

        # Reported, not acted on: doctor is read-only; reclaiming is the claim loop's job.
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
    # Ingestion
    # ------------------------------------------------------------------

    def processor_fingerprint(self) -> str:
        """The pinned digest in production, else the packaging version for
        local runs without one — either way, a version bump changes every
        session's ``job_key`` automatically."""
        return self.config.processor.digest or importlib.metadata.version(_PACKAGING_PKG)

    def output_uri_for(self, session_name: str) -> str:
        base = self.config.output.uri.rstrip("/")
        return f"{base}/{self.config.release}/sessions/{session_name}/"

    def source(self) -> "Source":
        """Construct this worker's configured :class:`~.sources.Source`."""
        return get_source(self.config.ingestion.type, **self._source_kwargs())

    def ingest_once(self) -> int:
        """One discovery sweep: upsert every session the source reports since the
        last watermark. Routine ingestion is a no-op via ``job_key`` — the
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
    # Claim + process
    # ------------------------------------------------------------------

    def claim_and_process_one(self) -> bool:
        """Claim and fully process one job. Returns ``False`` if the queue was empty."""
        job = self.ledger.claim(self.worker_id, self.config.worker.lease_seconds)
        if job is None:
            return False
        try:
            # Dispatch on kind: an aggregate job has no session to stage and no
            # container to launch, so it must not go down the session path.
            if job.kind == "aggregate":
                self.process_aggregate_job(job)
            else:
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
        """Run one claimed job, owning ``/work/{job_id}`` for its whole lifetime.

        Cleans on **entry** as well as exit. ``job_id`` is stable across attempts, so an
        attempt killed mid-write left partial output at this exact path, and ``publish``
        ships ``out/`` wholesale — the orphan would reach the store inside a session
        recorded as a clean success. Entry-side is also the only ordering that survives
        SIGKILL; the ``finally`` is disk hygiene.
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

        # Before recording, so `log_uri` is the published location, not a local path.
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
    #: path reused verbatim: the "identity-mapped" requirement is about
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
        bind mount is needed — true whenever the store handed back a path
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
        """Publish one attempt's log; return what to record as ``log_uri``.

        Every outcome goes to the same prefix, so ``log_uri`` means one kind of thing —
        it was a local path on success and a store URI on failure. On a failed publish
        the local copy survives and its path is recorded instead.
        """
        if not log_path.exists():
            return None
        if not self.config.logging.upload:
            return str(log_path)

        base = self.config.output.uri.rstrip("/")
        dest = f"{base}/{self.config.release}/{self.config.output.log_prefix}{job.job_id}/"
        # The stores publish a directory, and the log lives outside the work volume.
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
    # Aggregation
    # ------------------------------------------------------------------

    def sessions_prefix(self) -> str:
        base = self.config.output.uri.rstrip("/")
        return f"{base}/{self.config.release}/sessions/"

    def aggregate_uri(self) -> str:
        """The prefix all aggregates live under: one dated child per day, plus
        :meth:`aggregate_latest_uri`."""
        base = self.config.output.uri.rstrip("/")
        return f"{base}/{self.config.release}/aggregate/"

    def aggregate_day_uri(self, day: str) -> str:
        """``aggregate/YYYY-MM-DD/`` — where a run actually writes.

        Written first and then left alone, so every past aggregate stays exactly as it
        was published and a failed run cannot damage one. Storage is the cheap side of
        this trade: an aggregate a day is not a growth problem worth managing, and
        "the aggregate as of some date" is the question people actually ask.
        """
        return f"{self.aggregate_uri()}{day}/"

    def aggregate_latest_uri(self) -> str:
        """``aggregate/latest/`` — a full copy of the newest dated aggregate.

        A copy rather than a pointer file: a reader wanting current data should not have
        to fetch a pointer, parse it and follow it, and copying two small tables
        server-side costs almost nothing. It also means ``latest`` is readable by
        anything that can read a parquet path, with no convention to learn.
        """
        return f"{self.aggregate_uri()}{_LATEST}/"

    def aggregate_days(self) -> list[str]:
        """Every dated aggregate present, oldest first. Excludes the ``latest`` mirror."""
        return [name for name in self.output_store.list_children(self.aggregate_uri()) if _DAY_RE.match(name)]

    def contributing_sessions(self) -> dict[str, str]:
        """``{session_name: token}`` for every completed session of this release.

        One listing pass, which already reads each sidecar — so the change-detection
        token comes for free.
        """
        return {
            name: session_token(sidecar)
            for name, sidecar in self.output_store.iter_completed(self.sessions_prefix())
            if sidecar.get("status") == "ok"
        }

    def aggregation_due(self, now: datetime | None = None) -> bool:
        """Whether today's scheduled aggregation has come round and not yet happened.

        Catch-up rather than strict: a worker that was down at 03:00 aggregates at its
        next tick instead of skipping the day, which for a data product is the useful
        behaviour. "Already happened" is read from the ledger, not from process state,
        so a restart cannot re-trigger a run.
        """
        agg = self.config.aggregation
        if not agg.enabled:
            return False
        now_local = now.astimezone(ZoneInfo(agg.timezone)) if now else datetime.now(ZoneInfo(agg.timezone))
        scheduled = agg.scheduled_time(now_local)
        if now_local < scheduled:
            return False
        last = self.ledger.latest_job_created_at(self.config.release, "aggregate")
        return last is None or datetime.fromisoformat(last) < scheduled

    def enqueue_aggregate(self, *, force: bool = False) -> tuple[str | None, str, int]:
        """Queue an aggregate job unless the contributing set is unchanged.

        Returns ``(job_id_or_None, watermark, n_sessions)``. The dedupe is the ledger's
        own ``job_key`` uniqueness, with the watermark standing in for the processor
        fingerprint: an unchanged set produces the same key and the insert is a no-op —
        the same mechanism that makes routine ingestion idempotent, rather than a
        second bespoke one. *force* appends a nonce so the key is always new.
        """
        contributions = self.contributing_sessions()
        watermark = aggregate_watermark(contributions)
        if not contributions:
            # Nothing published yet. Queueing here would put a job in the queue that can
            # only ever no-op, and on a fresh release that is every tick.
            return None, watermark, 0
        fingerprint = f"{watermark}+force-{uuid.uuid4().hex[:8]}" if force else watermark
        job_id = self.ledger.upsert_job(
            kind="aggregate",
            release=self.config.release,
            asset_id=None,
            processor_fingerprint=fingerprint,
            input_store=self.config.output.store,
            input_uri=self.sessions_prefix(),
            output_uri=self.aggregate_uri(),
            session_name=None,
        )
        return job_id, watermark, len(contributions)

    def process_aggregate_job(self, job: Job) -> None:
        """Run one aggregate job.

        No work dir, unlike :meth:`process_job`: aggregation streams parquet from the
        output store straight back to it, so it never touches the work volume and has
        no partial state on disk for a killed attempt to leave behind.
        """
        self._run_aggregate(job)

    def _run_aggregate(self, job: Job) -> None:
        """Read every completed session's tables, concatenate them, publish.

        In-process: no container, so nothing to pin and no image plumbing. The
        concatenation is the packaging library's, shared with its ``aggregate``
        subcommand rather than reimplemented here.
        """
        from aind_behavior_vr_foraging_packaging.pipeline.batch import aggregate_tables

        started = time.monotonic()
        try:
            contributions = self.contributing_sessions()
            if not contributions:
                logger.info("Nothing to aggregate for release %s", self.config.release)
                self.ledger.complete_job(job.job_id, partial=False)
                return

            sessions_prefix = self.sessions_prefix()

            def _read(session_name: str, table: str) -> bytes | None:
                return self.output_store.read_object(f"{sessions_prefix}{session_name}/{table}.parquet")

            tables = aggregate_tables(contributions, _read)
            if not tables:
                # `aggregate_tables` returns nothing rather than raising when it finds no
                # identity table. Publishing that would replace a good aggregate with an
                # empty one and report success.
                self.ledger.fail_job(
                    job.job_id,
                    error_kind="data",
                    error=f"aggregation produced no tables from {len(contributions)} session(s) — not publishing",
                )
                return

            manifest = AggregateOutputMetadata(
                release=self.config.release,
                created_at=datetime.now(timezone.utc),
                watermark=aggregate_watermark(contributions),
                sessions=contributions,
                tables={name: _parquet_rows(payload) for name, payload in tables.items()},
                duration_s=round(time.monotonic() - started, 3),
                code=build_code_ref(),
            )
            written = self._publish_aggregate(tables, manifest)
        except (StoreTransientError, StoreConfigError) as exc:
            self.ledger.fail_job(job.job_id, error_kind="transient", error=str(exc)[:500])
            return
        except StoreDataError as exc:
            self.ledger.fail_job(job.job_id, error_kind="data", error=str(exc)[:500])
            return

        self.ledger.complete_job(
            job.job_id,
            partial=False,
            output_bytes=written,
            t_publish_s=round(time.monotonic() - started, 3),
            packaging_version=importlib.metadata.version(_PACKAGING_PKG),
        )
        logger.info(
            "Aggregated %d session(s) into %s and %s: %s",
            len(manifest.sessions),
            self.aggregate_day_uri(_manifest_day(manifest)),
            self.aggregate_latest_uri(),
            ", ".join(f"{k}={v}" for k, v in sorted(manifest.tables.items())),
        )

    def _publish_aggregate(self, tables: dict[str, bytes], manifest: AggregateOutputMetadata) -> int:
        """Write today's dated aggregate, then mirror it to ``latest``. Returns bytes written.

        Ordered so that nothing a reader may be relying on is touched until the new
        aggregate exists in full:

        1. clear today's dated prefix — only ever a previous attempt from today, since
           past days are immutable;
        2. write the tables, then the marker **last**. These are individual object
           writes, not a :meth:`publish`, so nothing else imposes that order: without
           the marker going last there is no way to tell a finished aggregate from one
           caught with a single table uploaded;
        3. only now replace ``latest``, marker last again.

        A failure at any point leaves every previous day intact and, at worst, ``latest``
        absent — which reads as "rebuilding", not as a torn aggregate, and the newest
        dated prefix is still there to read instead.
        """
        day = _manifest_day(manifest)
        dated = self.aggregate_day_uri(day)
        marker = manifest.model_dump_json(indent=2).encode("utf-8")

        self.output_store.delete_prefix(dated)
        written = sum(
            self.output_store.write_object(f"{dated}{name}.parquet", payload) for name, payload in tables.items()
        )
        written += self.output_store.write_object(f"{dated}{SIDECAR_NAME}", marker)

        latest = self.aggregate_latest_uri()
        self.output_store.delete_prefix(latest)
        self.output_store.copy_prefix(dated, latest)
        return written

    def read_aggregate_manifest(self) -> dict | None:
        """The ``latest`` mirror's manifest, or ``None`` when there is no complete one.

        Reads the mirror rather than picking the newest dated child, because the mirror
        is only written once its source is complete — so its marker vouches for the
        whole promotion, not just for one prefix.
        """
        latest = self.aggregate_latest_uri()
        raw = self.output_store.read_object(f"{latest}{SIDECAR_NAME}")
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            logger.warning("The aggregate manifest at %s is unreadable", latest)
            return None

    # ------------------------------------------------------------------
    # Work-volume housekeeping
    # ------------------------------------------------------------------

    def _triage_work_dir(self) -> tuple[list[Path], list[str]]:
        """Split the volume's dirs into "safe to reclaim" and "not ours". Read-only, so
        :meth:`doctor` can report what a sweep would remove.

        The ledger decides: workers share this volume, and deleting a live job's dir is
        worse than the leak. Unrecognised names are reported, never removed.
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
        """Reclaim work dirs the ledger has finished with. Returns how many.

        The filesystem half of :meth:`Ledger.reap_expired_leases`, which repairs the row
        and knows nothing about disk — without this the volume grows one session per crash.
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
        """Free space on the work volume, or ``None`` if undeterminable.

        Walks up to the nearest existing ancestor: a missing ``work_dir`` reports
        "unknown", and unknown does not block a claim — a silent no-op on the first tick.
        """
        for candidate in (self.work_dir, *self.work_dir.parents):
            try:
                return shutil.disk_usage(candidate).free
            except OSError:
                continue
        return None

    def worker_image(self) -> str | None:
        """This worker's own image, or ``None`` outside a container.

        Told, not derived — a process cannot discover its own image. Recorded per
        heartbeat, since ``jobs.image_ref`` is the *processor's*.
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
            # Reap before sweep: reaping is what moves a crashed job into a state the
            # sweep may reclaim, so the disk check below can benefit in the same tick.
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

            try:
                if self.aggregation_due():
                    # Only queues; the claim loop below runs it like any other job, so
                    # the lease is what stops N replicas aggregating at once.
                    job_id, watermark, n = self.enqueue_aggregate()
                    if job_id is not None:
                        logger.info("Queued aggregate job %s (%d session(s), watermark %s)", job_id, n, watermark)
            except Exception:
                logger.exception("Queueing aggregation failed")

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
        """Whether there is room for another job.

        Before claiming, not during: a job claimed onto a full volume dies on ENOSPC and
        burns one of ``max_attempts``. Refusing to claim leaves the queue untouched.
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
