"""Unit tests for the Worker (§4, §7) — a fake runner, no real Docker daemon."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal
from unittest.mock import patch

import pytest
from processing_server.config import PipelineConfig
from processing_server.runner import RunResult, Verdict
from processing_server.staging import InputManifest
from processing_server.stores import PreparedInput, PublishManifest, StoreTransientError
from processing_server.stores.input_local import LocalInputStore
from processing_server.stores.output_local import LocalOutputStore
from processing_server.worker import _LOG_STAGE_PREFIX, _WORKER_IMAGE_ENV, Worker

_EMPTY_MANIFEST = InputManifest(store="local", available_files=0, available_bytes=0, include=[], exclude=[])


class _LogPublishFails(LocalOutputStore):
    """A store that publishes output normally but never manages to publish a log.

    Selective, because the two publishes have different consequences and a store
    that failed both could not tell them apart: losing the output fails the job,
    while losing the log must not.
    """

    name: Literal["s3", "local"] = "local"

    def publish(self, src: Path, dest_uri: str) -> PublishManifest:
        if Path(src).name.startswith(_LOG_STAGE_PREFIX):
            raise StoreTransientError("log store unreachable")
        return super().publish(src, dest_uri)


def _config(tmp_path, **overrides) -> PipelineConfig:
    data = {
        "release": "rel1",
        "output": {"store": "local", "uri": str(tmp_path / "out")},
        "input": {"store": "local"},
        "worker": {"ledger": str(tmp_path / "jobs.sqlite")},
        "processor": {"allow_unpinned": True},
        # Both default to real absolute paths under /var/lib/vrf (production
        # convention) — without this override, writing a fake job's log here
        # fails outright on a CI runner with no permission to create /var/lib/vrf.
        "logging": {"dir": str(tmp_path / "logs")},
    }
    data.update(overrides)
    return PipelineConfig(**data)


def _make_session(root: Path, name: str = "sess_A") -> Path:
    d = root / name
    (d / "behavior").mkdir(parents=True)
    (d / "data_description.json").write_text("{}")
    (d / "behavior" / "Block.json").write_text("{}")
    return d


def _make_worker(tmp_path, config, output_store=None) -> Worker:
    return Worker(
        config,
        worker_id="w1",
        work_dir=tmp_path / "work",
        input_store=LocalInputStore(staging=config.staging),
        output_store=output_store or LocalOutputStore(),
    )


def _sidecar_payload(session_name: str = "sess_A") -> dict:
    return {
        "schema_version": "1.0.0",
        "session_name": session_name,
        "status": "ok",
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:01:00Z",
        "duration_s": 5.0,
        "processors": [{"name": "sites", "status": "ok", "warn_count": 0}],
        "code": {
            "repository": "r",
            "version": "1.0.0",
            "commit": None,
            "python_version": "3.13",
            "container": None,
            "provenance": "unpinned",
        },
        "versions": {"packaging_version": "1.0.0", "data_contract_version": "1.0.0", "dataset_version": "0.6.1"},
        "staged": {},
    }


def _completed_verdict(out_dir: Path) -> Verdict:
    """Stand in for a container that ran cleanly: write what it would have written
    (one table plus the sidecar) and report the verdict `classify` would return."""
    from processing_server.sidecar import SessionOutputMetadata

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "sites.parquet").write_bytes(b"x")
    payload = _sidecar_payload()
    (out_dir / "output.metadata.json").write_text(json.dumps(payload))
    return Verdict(
        status="completed",
        partial=False,
        error_kind=None,
        error=None,
        exit_code=0,
        sidecar=SessionOutputMetadata.model_validate(payload),
        sidecar_raw=json.dumps(payload),
        warn_count=0,
        failed_processors="",
    )


def _failed_verdict(out_dir: Path) -> Verdict:
    """A named processor failed — terminal `data`, and the container still exited
    nonzero (recording is not tolerance)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    return Verdict(
        status="failed",
        partial=False,
        error_kind="data",
        error="sites: boom",
        exit_code=1,
        sidecar=None,
        sidecar_raw=None,
        warn_count=0,
        failed_processors="sites",
    )


def _one_pending_job(worker: Worker):
    """Ingest a single local session and claim it."""
    worker.ingest_once()
    job = worker.ledger.claim(worker.worker_id, 60)
    assert job is not None
    return job


@contextmanager
def _fake_container(verdict_factory=_completed_verdict, *, run=None) -> Iterator[None]:
    """Patch `runner.run`/`runner.classify`. Everything up to the container boundary
    — staging, mount resolution, publishing, the ledger — runs for real."""

    def default_run(args, *, job_id, log_path, timeout_s):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("fake log")
        return RunResult(exit_code=0, timed_out=False, duration_s=1.0, log_path=log_path)

    def fake_classify(result, sidecar_path, *, expected_digest):
        # `sidecar_path` is `_session_out_dir(...)/output.metadata.json`, so its
        # parent is where the container would have written its tables.
        return verdict_factory(sidecar_path.parent)

    with (
        patch("processing_server.worker.runner.run", side_effect=run or default_run),
        patch("processing_server.worker.runner.classify", side_effect=fake_classify),
    ):
        yield


def _process(worker: Worker, job, verdict_factory=_completed_verdict, *, run=None) -> None:
    with _fake_container(verdict_factory, run=run):
        worker.process_job(job)


class TestFingerprintAndOutputUri:
    def test_processor_fingerprint_falls_back_to_packaging_version(self, tmp_path):
        config = _config(tmp_path)
        worker = _make_worker(tmp_path, config)
        assert worker.processor_fingerprint()  # non-empty, no digest configured

    def test_processor_fingerprint_prefers_digest(self, tmp_path):
        config = _config(tmp_path, processor={"digest": "sha256:abc", "allow_unpinned": True})
        worker = _make_worker(tmp_path, config)
        assert worker.processor_fingerprint() == "sha256:abc"

    def test_output_uri_for_shape(self, tmp_path):
        config = _config(tmp_path)
        worker = _make_worker(tmp_path, config)
        uri = worker.output_uri_for("sess_A")
        assert uri == f"{config.output.uri}/rel1/sessions/sess_A/"


class TestResolveMount:
    def test_staged_under_work_volume_needs_no_extra_mount(self, tmp_path):
        config = _config(tmp_path)
        worker = _make_worker(tmp_path, config)
        job_dir = worker.work_dir / "job1"
        staged = worker._stage_dir(job_dir, "sess_A")
        staged.mkdir(parents=True)
        prepared = PreparedInput(host_path=staged, read_only=True, manifest=_EMPTY_MANIFEST)
        in_container, extra = worker._resolve_mount(prepared, job_dir, "sess_A")
        assert in_container == "/work/job1/in/sess_A"
        assert extra is None

    def test_outside_work_volume_needs_identity_mapped_mount(self, tmp_path):
        """The container-side target is not the host path reused — the host path is
        only meaningful to the daemon resolving the mount `source`, and (notably on
        Windows) is not a valid in-container path."""
        config = _config(tmp_path)
        worker = _make_worker(tmp_path, config)
        job_dir = worker.work_dir / "job1"
        job_dir.mkdir(parents=True)
        mounted = tmp_path / "data" / "raw" / "sess_A"
        mounted.mkdir(parents=True)
        prepared = PreparedInput(host_path=mounted, read_only=True, manifest=_EMPTY_MANIFEST)
        in_container, extra = worker._resolve_mount(prepared, job_dir, "sess_A")
        assert in_container == f"{Worker._MOUNT_ROOT}/sess_A"
        assert extra == (str(mounted.resolve()), f"{Worker._MOUNT_ROOT}/sess_A")

    @pytest.mark.parametrize("staged", [True, False])
    def test_container_path_always_ends_in_the_session_name(self, tmp_path, staged):
        """The invariant that replaces `--session-name`: the processor stamps every
        table's `session_id` with its input directory's own name, so a mount point
        or staging directory named anything else silently mislabels the whole
        session — no error, just wrong data."""
        worker = _make_worker(tmp_path, _config(tmp_path))
        job_dir = worker.work_dir / "job1"
        job_dir.mkdir(parents=True)
        host_path = worker._stage_dir(job_dir, "sess_A") if staged else tmp_path / "elsewhere" / "sess_A"
        host_path.mkdir(parents=True)
        prepared = PreparedInput(host_path=host_path, read_only=True, manifest=_EMPTY_MANIFEST)
        in_container, _ = worker._resolve_mount(prepared, job_dir, "sess_A")
        assert in_container.rsplit("/", 1)[-1] == "sess_A"


class TestIngestOnce:
    def test_ingests_local_sessions(self, tmp_path):
        raw = tmp_path / "raw"
        _make_session(raw, "behavior_1_2025-01-01_00-00-00")
        _make_session(raw, "behavior_2_2025-01-02_00-00-00")
        config = _config(tmp_path, ingestion={"type": "local", "root": str(raw)})
        worker = _make_worker(tmp_path, config)
        try:
            n = worker.ingest_once()
            assert n == 2
            assert len(worker.ledger.list_jobs()) == 2
        finally:
            worker.close()


class TestProcessJob:
    def test_successful_job_completes_and_publishes(self, tmp_path):
        raw = tmp_path / "raw"
        _make_session(raw, "behavior_1_2025-01-01_00-00-00")
        config = _config(tmp_path, ingestion={"type": "local", "root": str(raw)})
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)
            _process(worker, job)

            final = worker.ledger.get_job(job.job_id)
            assert final is not None
            assert final.status == "completed"
            assert final.partial is False
            assert final.output_bytes is not None

            published = Path(config.output.uri) / "rel1" / "sessions" / "behavior_1_2025-01-01_00-00-00"
            assert (published / "output.metadata.json").exists()
            assert (published / "sites.parquet").exists()
        finally:
            worker.close()

    def test_overwrite_false_skips_when_output_exists(self, tmp_path):
        raw = tmp_path / "raw"
        _make_session(raw, "behavior_1_2025-01-01_00-00-00")
        config = _config(
            tmp_path,
            ingestion={"type": "local", "root": str(raw)},
            output={
                "store": "local",
                "uri": str(tmp_path / "out"),
                "overwrite": False,
            },
        )
        worker = _make_worker(tmp_path, config)
        try:
            worker.ingest_once()
            job = worker.ledger.claim("w1", 60)
            assert job is not None
            assert job.session_name is not None

            # Pre-populate the output as if a previous run already completed it.
            dest = Path(config.output.uri) / "rel1" / "sessions" / job.session_name
            dest.mkdir(parents=True)
            (dest / "output.metadata.json").write_text("{}")

            worker.process_job(job)
            final = worker.ledger.get_job(job.job_id)
            assert final is not None
            assert final.status == "skipped"
        finally:
            worker.close()


def _local_config(tmp_path, **overrides) -> PipelineConfig:
    raw = tmp_path / "raw"
    if not raw.exists():
        _make_session(raw, "behavior_1_2025-01-01_00-00-00")
    overrides.setdefault("ingestion", {"type": "local", "root": str(raw)})
    return _config(tmp_path, **overrides)


class TestWorkDirLifecycle:
    """§4a — who owns a job directory, and when it stops existing."""

    def test_stale_output_from_a_previous_attempt_is_not_published(self, tmp_path):
        """The bug entry-side cleanup exists for, and the only one here that is about
        correctness rather than disk.

        `job_id` is stable across attempts, so an attempt killed mid-write leaves its
        partial output at exactly the path the retry will use. `publish` ships `out/`
        wholesale and the sidecar is rewritten every time, so without the entry-side
        `rmtree` the orphan reaches the output store inside a session the ledger
        records as a clean success — no error anywhere.
        """
        config = _local_config(tmp_path)
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)

            # Attempt 1: died after writing one table, before the sidecar.
            orphan_dir = worker.work_dir / job.job_id / "out"
            orphan_dir.mkdir(parents=True)
            (orphan_dir / "orphan.parquet").write_bytes(b"stale")

            _process(worker, job)

            final = worker.ledger.get_job(job.job_id)
            assert final is not None and final.status == "completed"
            published = Path(config.output.uri) / "rel1" / "sessions" / "behavior_1_2025-01-01_00-00-00"
            assert (published / "sites.parquet").exists()
            assert not (published / "orphan.parquet").exists()
        finally:
            worker.close()

    @pytest.mark.parametrize("verdict_factory", [_completed_verdict, _failed_verdict])
    def test_work_dir_is_gone_whatever_the_verdict(self, tmp_path, verdict_factory):
        config = _local_config(tmp_path)
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)
            _process(worker, job, verdict_factory)
            assert not (worker.work_dir / job.job_id).exists()
        finally:
            worker.close()

    def test_work_dir_is_gone_even_when_the_run_raises(self, tmp_path):
        """The reason cleanup is in a `finally` rather than at the end of the body:
        `claim_and_process_one` catches an unhandled error and records it, and used to
        leave the directory behind while doing so."""
        config = _local_config(tmp_path)
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)

            def exploding_run(args, *, job_id, log_path, timeout_s):
                raise RuntimeError("docker daemon died mid-run")

            with pytest.raises(RuntimeError):
                _process(worker, job, run=exploding_run)
            assert not (worker.work_dir / job.job_id).exists()
        finally:
            worker.close()

    def test_keep_work_dir_preserves_it(self, tmp_path):
        config = _local_config(tmp_path, worker={"ledger": str(tmp_path / "jobs.sqlite"), "keep_work_dir": True})
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)
            _process(worker, job)
            kept = worker.work_dir / job.job_id
            assert (kept / "out" / "sites.parquet").exists()
        finally:
            worker.close()

    def test_entry_cleanup_still_runs_under_keep_work_dir(self, tmp_path):
        """`keep_work_dir` suppresses reclamation, never correctness — a preserved
        directory must not leak into the next attempt's output."""
        config = _local_config(tmp_path, worker={"ledger": str(tmp_path / "jobs.sqlite"), "keep_work_dir": True})
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)
            stale = worker.work_dir / job.job_id / "out"
            stale.mkdir(parents=True)
            (stale / "orphan.parquet").write_bytes(b"stale")
            _process(worker, job)
            published = Path(config.output.uri) / "rel1" / "sessions" / "behavior_1_2025-01-01_00-00-00"
            assert not (published / "orphan.parquet").exists()
        finally:
            worker.close()


class TestSweepWorkDir:
    """§4a — the filesystem half of `reap_expired_leases`."""

    @staticmethod
    def _job_with_status(worker: Worker, status: str) -> str:
        job_id = worker.ledger.upsert_job(
            kind="session",
            release="rel1",
            asset_id=f"asset-{status}",
            processor_fingerprint="fp",
            input_store="local",
            input_uri="file:///x",
            output_uri="file:///y",
            session_name=f"sess_{status}",
        )
        assert job_id is not None
        worker.ledger._conn.execute("UPDATE jobs SET status=? WHERE job_id=?", (status, job_id))
        (worker.work_dir / job_id).mkdir(parents=True)
        return job_id

    @pytest.mark.parametrize("status", ["completed", "failed", "dead", "skipped"])
    def test_terminal_jobs_are_reclaimed(self, tmp_path, status):
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            worker.work_dir.mkdir(parents=True, exist_ok=True)
            job_id = self._job_with_status(worker, status)
            assert worker.sweep_work_dir() == 1
            assert not (worker.work_dir / job_id).exists()
        finally:
            worker.close()

    @pytest.mark.parametrize("status", ["running", "pending", "retrying"])
    def test_live_and_retryable_jobs_are_left_alone(self, tmp_path, status):
        """`running` is another worker's live job — the claim that sets it happens
        before any `mkdir`, which is what makes the status check race-free without
        mtime guesswork. `pending`/`retrying` belong to the next attempt, whose own
        entry-side cleanup reclaims the directory."""
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            worker.work_dir.mkdir(parents=True, exist_ok=True)
            job_id = self._job_with_status(worker, status)
            assert worker.sweep_work_dir() == 0
            assert (worker.work_dir / job_id).exists()
        finally:
            worker.close()

    def test_unknown_directories_are_never_deleted(self, tmp_path):
        """Several workers share this volume. Anything whose name is not a job id this
        ledger knows is reported, not removed."""
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            stranger = worker.work_dir / "not-a-job-id"
            stranger.mkdir(parents=True)
            assert worker.sweep_work_dir() == 0
            assert stranger.exists()
            _, unknown = worker._triage_work_dir()
            assert unknown == ["not-a-job-id"]
        finally:
            worker.close()

    def test_log_staging_dirs_follow_their_job(self, tmp_path):
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            worker.work_dir.mkdir(parents=True, exist_ok=True)
            done = self._job_with_status(worker, "completed")
            live = self._job_with_status(worker, "running")
            (worker.work_dir / f"{_LOG_STAGE_PREFIX}{done}").mkdir()
            (worker.work_dir / f"{_LOG_STAGE_PREFIX}{live}").mkdir()
            worker.sweep_work_dir()
            assert not (worker.work_dir / f"{_LOG_STAGE_PREFIX}{done}").exists()
            assert (worker.work_dir / f"{_LOG_STAGE_PREFIX}{live}").exists()
        finally:
            worker.close()

    def test_keep_work_dir_disables_the_sweep(self, tmp_path):
        """Otherwise the sweeper would reclaim, within one loop tick, exactly what the
        flag was set to preserve."""
        config = _local_config(tmp_path, worker={"ledger": str(tmp_path / "jobs.sqlite"), "keep_work_dir": True})
        worker = _make_worker(tmp_path, config)
        try:
            worker.work_dir.mkdir(parents=True, exist_ok=True)
            job_id = self._job_with_status(worker, "completed")
            assert worker.sweep_work_dir() == 0
            assert (worker.work_dir / job_id).exists()
        finally:
            worker.close()


class TestDiskGuard:
    def test_disk_ok_is_false_below_the_floor(self, tmp_path):
        config = _local_config(
            tmp_path, worker={"ledger": str(tmp_path / "jobs.sqlite"), "min_free_disk_bytes": 10_000}
        )
        worker = _make_worker(tmp_path, config)
        try:
            with patch.object(Worker, "free_disk_bytes", return_value=9_999):
                assert worker._disk_ok() is False
            with patch.object(Worker, "free_disk_bytes", return_value=10_000):
                assert worker._disk_ok() is True
        finally:
            worker.close()

    def test_unknown_free_space_does_not_block(self, tmp_path):
        """`None` means the check could not be performed, which is not evidence of a
        full disk — refusing to work on it would strand a whole campaign."""
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            with patch.object(Worker, "free_disk_bytes", return_value=None):
                assert worker._disk_ok() is True
        finally:
            worker.close()

    def test_free_space_is_measured_before_the_work_dir_exists(self, tmp_path):
        """Otherwise the guard is a silent no-op on a fresh worker: `disk_usage` raises
        on a missing directory, and "unknown" does not block a claim."""
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            assert not worker.work_dir.exists()
            assert worker.free_disk_bytes() is not None
        finally:
            worker.close()

    def test_a_full_volume_leaves_the_queue_untouched(self, tmp_path):
        """The point of checking before claiming: a job claimed onto a full volume
        dies on ENOSPC and burns one of `max_attempts`."""
        config = _local_config(
            tmp_path, worker={"ledger": str(tmp_path / "jobs.sqlite"), "min_free_disk_bytes": 1 << 62}
        )
        worker = _make_worker(tmp_path, config)
        try:
            worker.ingest_once()
            with _fake_container():
                worker.run_forever(once=True)
            jobs = worker.ledger.list_jobs()
            assert [j.status for j in jobs] == ["pending"]
            assert jobs[0].attempts == 0
        finally:
            worker.close()


class TestLogPublishing:
    def test_log_uri_points_into_the_output_store(self, tmp_path):
        config = _local_config(tmp_path)
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)
            _process(worker, job)
            final = worker.ledger.get_job(job.job_id)
            assert final is not None and final.log_uri is not None
            published_log = Path(config.output.uri) / "rel1" / "logs" / job.job_id / "_log.txt"
            assert published_log.read_text() == "fake log"
            assert Path(final.log_uri) == published_log
            # The local copy is gone only because the publish succeeded.
            assert not (Path(config.logging.dir) / f"{job.job_id}.log").exists()
        finally:
            worker.close()

    def test_failures_get_a_log_in_the_same_place_as_successes(self, tmp_path):
        """The whole point of the change: `log_uri` used to be a worker-local path on
        success and an output-store URI on failure, so nothing could follow the column
        without first guessing which it had."""
        config = _local_config(tmp_path)
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)
            _process(worker, job, _failed_verdict)
            final = worker.ledger.get_job(job.job_id)
            assert final is not None
            assert final.status == "failed"
            assert final.log_uri is not None
            assert Path(final.log_uri) == Path(config.output.uri) / "rel1" / "logs" / job.job_id / "_log.txt"
        finally:
            worker.close()

    def test_a_failed_publish_keeps_the_local_copy(self, tmp_path):
        """A reachable log in the wrong place beats a URI pointing at nothing."""
        config = _local_config(tmp_path)
        worker = _make_worker(tmp_path, config, output_store=_LogPublishFails())
        try:
            job = _one_pending_job(worker)
            _process(worker, job)
            final = worker.ledger.get_job(job.job_id)
            assert final is not None
            assert final.status == "completed"  # the output published fine
            local = Path(config.logging.dir) / f"{job.job_id}.log"
            assert local.exists()
            assert final.log_uri == str(local)
        finally:
            worker.close()

    def test_upload_false_records_the_local_path(self, tmp_path):
        config = _local_config(tmp_path, logging={"dir": str(tmp_path / "logs"), "upload": False})
        worker = _make_worker(tmp_path, config)
        try:
            job = _one_pending_job(worker)
            _process(worker, job)
            final = worker.ledger.get_job(job.job_id)
            local = Path(config.logging.dir) / f"{job.job_id}.log"
            assert final is not None and final.log_uri == str(local)
            assert local.exists()
        finally:
            worker.close()


class TestWorkerProvenance:
    """§12 — the half of the chain the processor's digest does not cover."""

    def test_heartbeat_records_the_workers_own_image(self, tmp_path, monkeypatch):
        ref = "ghcr.io/x/y@sha256:" + "a" * 64
        monkeypatch.setenv(_WORKER_IMAGE_ENV, ref)
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            worker.heartbeat()
            row = worker.ledger.get_worker("w1")
            assert row is not None and row["worker_image"] == ref
        finally:
            worker.close()

    def test_unset_image_is_recorded_as_nothing_not_guessed(self, tmp_path, monkeypatch):
        monkeypatch.delenv(_WORKER_IMAGE_ENV, raising=False)
        worker = _make_worker(tmp_path, _local_config(tmp_path))
        try:
            worker.heartbeat()
            row = worker.ledger.get_worker("w1")
            assert row is not None and row["worker_image"] is None
        finally:
            worker.close()

    @pytest.mark.parametrize(
        ("value", "expect_problem"),
        [
            (None, True),
            ("ghcr.io/x/y:latest", True),
            ("ghcr.io/x/y@sha256:" + "a" * 64, False),
        ],
    )
    def test_doctor_gates_an_unrecordable_worker(self, tmp_path, monkeypatch, value, expect_problem):
        """Caught before a campaign, not after: an unpinned worker is only visible in
        hindsight, when the ledger cannot say what published 4700 sessions."""
        if value is None:
            monkeypatch.delenv(_WORKER_IMAGE_ENV, raising=False)
        else:
            monkeypatch.setenv(_WORKER_IMAGE_ENV, value)
        config = _local_config(tmp_path, processor={"digest": "sha256:" + "b" * 64})
        worker = _make_worker(tmp_path, config)
        try:
            problems = [p for p in worker.doctor() if _WORKER_IMAGE_ENV in p]
            assert bool(problems) is expect_problem
        finally:
            worker.close()

    def test_allow_unpinned_governs_both_halves(self, tmp_path, monkeypatch):
        """A run allowed to be unreproducible is allowed to be unreproducible on both
        sides — otherwise every local `doctor` fails on a check no laptop can pass."""
        monkeypatch.delenv(_WORKER_IMAGE_ENV, raising=False)
        worker = _make_worker(tmp_path, _local_config(tmp_path))  # allow_unpinned: True
        try:
            assert not [p for p in worker.doctor() if _WORKER_IMAGE_ENV in p]
        finally:
            worker.close()
