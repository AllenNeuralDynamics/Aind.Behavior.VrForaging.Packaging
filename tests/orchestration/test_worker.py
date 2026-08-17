"""Unit tests for the Worker (§4, §7) — a fake runner, no real Docker daemon."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from aind_behavior_vr_foraging_packaging.orchestration.config import PipelineConfig
from aind_behavior_vr_foraging_packaging.orchestration.runner import RunResult, Verdict
from aind_behavior_vr_foraging_packaging.orchestration.staging import InputManifest
from aind_behavior_vr_foraging_packaging.orchestration.stores import PreparedInput
from aind_behavior_vr_foraging_packaging.orchestration.stores.input_local import LocalInputStore
from aind_behavior_vr_foraging_packaging.orchestration.stores.output_local import LocalOutputStore
from aind_behavior_vr_foraging_packaging.orchestration.worker import Worker

_EMPTY_MANIFEST = InputManifest(store="local", available_files=0, available_bytes=0, include=[], exclude=[])


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


def _make_worker(tmp_path, config) -> Worker:
    return Worker(
        config,
        worker_id="w1",
        work_dir=tmp_path / "work",
        input_store=LocalInputStore(staging=config.staging),
        output_store=LocalOutputStore(),
    )


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
    def _completed_verdict(self, out_dir: Path) -> Verdict:
        sidecar_path = out_dir / "output.metadata.json"
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        (out_dir / "sites.parquet").write_bytes(b"x")
        payload = {
            "schema_version": "1.0.0",
            "session_name": "sess_A",
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
        sidecar_path.write_text(json.dumps(payload))
        from aind_behavior_vr_foraging_packaging._sidecar import SessionOutputMetadata

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

    def test_successful_job_completes_and_publishes(self, tmp_path):
        raw = tmp_path / "raw"
        _make_session(raw, "behavior_1_2025-01-01_00-00-00")
        config = _config(tmp_path, ingestion={"type": "local", "root": str(raw)})
        worker = _make_worker(tmp_path, config)
        try:
            worker.ingest_once()
            job = worker.ledger.claim("w1", 60)
            assert job is not None

            def fake_run(args, *, job_id, log_path, timeout_s):
                # Real `runner.run` only executes the container; `classify` is what
                # reads the sidecar afterward. Nothing needs writing here.
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text("fake log")
                return RunResult(exit_code=0, timed_out=False, duration_s=1.0, log_path=log_path)

            def fake_classify(result, sidecar_path, *, expected_digest):
                # `sidecar_path` is `_session_out_dir(...)/output.metadata.json` —
                # write the fake container's output there, matching what
                # `vr-foraging-packaging session` actually produces.
                return self._completed_verdict(sidecar_path.parent)

            with (
                patch("aind_behavior_vr_foraging_packaging.orchestration.worker.runner.run", side_effect=fake_run),
                patch(
                    "aind_behavior_vr_foraging_packaging.orchestration.worker.runner.classify",
                    side_effect=fake_classify,
                ),
            ):
                worker.process_job(job)

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
