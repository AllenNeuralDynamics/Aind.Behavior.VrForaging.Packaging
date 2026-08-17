"""Unit tests for pipeline.runner — docker argv construction and the §8 truth table."""

import json

import pytest
from processing_server.config import ProcessorConfig
from processing_server.runner import (
    RunnerConfigError,
    RunResult,
    build_docker_args,
    classify,
    image_ref,
)


def _write_sidecar(path, **overrides):
    data = {
        "schema_version": "1.0.0",
        "session_name": "sess_A",
        "status": "ok",
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:01:00Z",
        "duration_s": 60.0,
        "processors": [{"name": "sites", "status": "ok", "warn_count": 0}],
        "code": {
            "repository": "repo",
            "version": "1.0.0",
            "commit": None,
            "python_version": "3.13.0",
            "container": {"uri": "ghcr.io/x/y@sha256:abc", "tag": "latest", "digest": "sha256:abc"},
            "provenance": "pinned-digest",
        },
        "versions": {"packaging_version": "1.0.0", "data_contract_version": "1.0.0", "dataset_version": "0.6.1"},
    }
    data.update(overrides)
    path.write_text(json.dumps(data))


class TestImageRef:
    def test_pinned(self):
        cfg = ProcessorConfig(digest="sha256:abc")
        assert image_ref(cfg) == f"{cfg.image}@sha256:abc"

    def test_unpinned_requires_allow_unpinned(self):
        cfg = ProcessorConfig()
        with pytest.raises(RunnerConfigError):
            image_ref(cfg)

    def test_unpinned_allowed(self):
        cfg = ProcessorConfig(allow_unpinned=True)
        assert image_ref(cfg) == f"{cfg.image}:latest"


def _args(
    cfg: ProcessorConfig,
    *,
    input_path_in_container: str = "/work/job1/in/sess_A",
    extra_mount: tuple[str, str] | None = None,
) -> list[str]:
    return build_docker_args(
        cfg,
        job_id="job1",
        worker_id="w1",
        work_volume="vrf_work",
        input_path_in_container=input_path_in_container,
        extra_mount=extra_mount,
    )


class TestBuildDockerArgs:
    def test_shape(self):
        cfg = ProcessorConfig(digest="sha256:abc", write_nwb=True)
        args = _args(cfg)
        assert args[:3] == ["docker", "run", "--rm"]
        assert "--network=none" in args
        assert "vrf_work:/work" in args
        assert f"PROCESSOR_IMAGE_URI={cfg.image}@sha256:abc" in args
        assert "VRF_JOB_ID=job1" in args
        assert "VRF_WORKER_ID=w1" in args
        assert "--write-nwb" in args
        assert "/work/job1/out" in args

    def test_runs_the_process_subcommand_first(self):
        """`process` must be the first argument after the image ref — the CLI
        dispatches on it. It is the only subcommand that runs a single session and
        writes a sidecar; `work` or `serve` here would start a whole worker inside
        what is meant to be one ephemeral job."""
        cfg = ProcessorConfig(digest="sha256:abc")
        args = _args(cfg)
        image_at = args.index(f"{cfg.image}@sha256:abc")
        assert args[image_at + 1] == "process"

    def test_passes_no_session_name_and_no_sidecar_flag(self):
        """Identity comes from the input directory's own name, so there is no name
        argument to get out of sync (`Worker._resolve_mount`). And `process` always
        writes a sidecar — it is the reason the subcommand exists — so there is no
        flag to forget either."""
        args = _args(ProcessorConfig(allow_unpinned=True))
        assert "--session-name" not in args
        assert "--single-session" not in args
        assert "--skip-aggregation" not in args
        assert "--write-sidecar" not in args

    def test_extra_mount_added_for_out_of_volume_path(self):
        """`--mount` (not `-v host:container:ro`): a Windows host path has its own
        colon, which the short form's colon-splitting parser rejects outright."""
        cfg = ProcessorConfig(allow_unpinned=True)
        args = _args(
            cfg,
            input_path_in_container="/mnt/sess_A",
            extra_mount=(r"C:\data\raw\sess_A", "/mnt/sess_A"),
        )
        assert r"type=bind,source=C:\data\raw\sess_A,target=/mnt/sess_A,readonly" in args

    def test_no_pinned_env_var_when_unpinned(self):
        args = _args(ProcessorConfig(allow_unpinned=True))
        assert not any(a.startswith("PROCESSOR_IMAGE_URI=") for a in args)

    def test_the_argv_actually_parses_as_the_container_command(self):
        """The container contract, checked against the real parser rather than
        asserted flag by flag. Every other test here only proves a string is in a
        list; nothing but this catches an argument the CLI will reject — and it
        rejects them by exiting 2, having processed nothing, with the reason only
        in the container log.
        """
        from processing_server.cli import build_parser
        from processing_server.runner import image_ref

        cfg = ProcessorConfig(digest="sha256:abc", write_nwb=True, exclude_processors=["sniffing", "licks"])
        argv = _args(cfg, input_path_in_container="/mnt/behavior_1_2025-01-01_00-00-00")
        container_args = argv[argv.index(image_ref(cfg)) + 1 :]

        args = build_parser().parse_args(container_args)

        assert args.command == "process"
        assert args.input_dir.name == "behavior_1_2025-01-01_00-00-00"  # the session id it will stamp
        assert args.output_dir.as_posix().endswith("/work/job1/out")
        assert args.write_nwb is True
        assert args.write_parquet is True
        assert args.exclude_processors == ["sniffing", "licks"]

    def test_exclude_processors_forwarded_one_flag_per_name(self):
        """Repeated, not space-separated. pydantic-settings gives a list field an
        `append` action, so `--exclude-processors a b` leaves `b` unrecognised and
        the container exits 2 having processed nothing."""
        cfg = ProcessorConfig(allow_unpinned=True, exclude_processors=["sniffing", "licks"])
        args = _args(cfg)
        assert args.count("--exclude-processors") == 2
        at = args.index("--exclude-processors")
        assert args[at : at + 4] == ["--exclude-processors", "sniffing", "--exclude-processors", "licks"]


class TestClassify:
    def test_timeout(self, tmp_path):
        result = RunResult(exit_code=None, timed_out=True, duration_s=3600.0, log_path=tmp_path / "x.log")
        verdict = classify(result, tmp_path / "output.metadata.json", expected_digest=None)
        assert verdict.status == "failed"
        assert verdict.error_kind == "timeout"

    def test_oom(self, tmp_path):
        result = RunResult(exit_code=137, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, tmp_path / "output.metadata.json", expected_digest=None)
        assert verdict.status == "failed"
        assert verdict.error_kind == "infra"

    def test_exit_0_sidecar_ok(self, tmp_path):
        sidecar = tmp_path / "output.metadata.json"
        _write_sidecar(sidecar, status="ok")
        result = RunResult(exit_code=0, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest="sha256:abc")
        assert verdict.status == "completed"
        assert verdict.partial is False

    def test_exit_0_sidecar_partial(self, tmp_path):
        sidecar = tmp_path / "output.metadata.json"
        _write_sidecar(sidecar, status="partial", processors=[{"name": "sites", "status": "error", "error": "boom"}])
        result = RunResult(exit_code=0, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest="sha256:abc")
        assert verdict.status == "completed"
        assert verdict.partial is True
        assert verdict.failed_processors == "sites"

    def test_exit_0_sidecar_missing(self, tmp_path):
        result = RunResult(exit_code=0, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, tmp_path / "output.metadata.json", expected_digest=None)
        assert verdict.status == "failed"
        assert verdict.error_kind == "code"
        assert verdict.error is not None
        assert "missing" in verdict.error.lower()

    def test_exit_0_sidecar_status_error(self, tmp_path):
        sidecar = tmp_path / "output.metadata.json"
        _write_sidecar(sidecar, status="error", processors=[{"name": "sites", "status": "error", "error": "bad data"}])
        result = RunResult(exit_code=0, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest="sha256:abc")
        assert verdict.status == "failed"
        assert verdict.error_kind == "data"
        assert verdict.error == "bad data"

    def test_exit_0_sidecar_fails_validation(self, tmp_path):
        sidecar = tmp_path / "output.metadata.json"
        sidecar.write_text("{not valid json")
        result = RunResult(exit_code=0, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest=None)
        assert verdict.status == "failed"
        assert verdict.error_kind == "code"

    def test_exit_0_digest_mismatch(self, tmp_path):
        sidecar = tmp_path / "output.metadata.json"
        _write_sidecar(sidecar, status="ok")
        result = RunResult(exit_code=0, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest="sha256:DIFFERENT")
        assert verdict.status == "failed"
        assert verdict.error_kind == "code"
        assert verdict.error is not None
        assert "digest" in verdict.error.lower()

    def test_nonzero_exit_with_sidecar_error_is_data_not_code(self, tmp_path):
        """The normal shape of a failed session: the processor propagates, so the
        container exits nonzero *and* the sidecar names what broke. `data`, not
        `code` — this is the session's data failing to parse, not our run being
        misconfigured, and it is the distinction the ledger's `error_kind` column
        exists to sort on."""
        sidecar = tmp_path / "output.metadata.json"
        _write_sidecar(
            sidecar, status="error", processors=[{"name": "sites", "status": "error", "error": "crashed hard"}]
        )
        result = RunResult(exit_code=1, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest=None)
        assert verdict.status == "failed"
        assert verdict.error_kind == "data"
        assert verdict.error is not None
        assert "crashed hard" in verdict.error
        assert verdict.failed_processors == "sites"

    def test_nonzero_exit_with_clean_sidecar_believes_the_exit_code(self, tmp_path):
        """A contradiction: the sidecar recorded no failure, yet the process died.
        Whatever broke happened outside the work the sidecar covers, so the run is
        failed regardless of what the record claims."""
        sidecar = tmp_path / "output.metadata.json"
        _write_sidecar(sidecar, status="ok")
        result = RunResult(exit_code=1, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest=None)
        assert verdict.status == "failed"
        assert verdict.error_kind == "code"
        assert verdict.error is not None
        assert "status='ok'" in verdict.error

    def test_nonzero_exit_no_sidecar(self, tmp_path):
        result = RunResult(exit_code=1, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, tmp_path / "output.metadata.json", expected_digest=None)
        assert verdict.status == "failed"
        assert verdict.error_kind == "code"
        assert verdict.error is not None
        assert "1" in verdict.error

    def test_warn_count_and_failed_processors_carried_through_on_success(self, tmp_path):
        sidecar = tmp_path / "output.metadata.json"
        _write_sidecar(sidecar, status="ok", processors=[{"name": "sites", "status": "ok", "warn_count": 3}])
        result = RunResult(exit_code=0, timed_out=False, duration_s=10.0, log_path=tmp_path / "x.log")
        verdict = classify(result, sidecar, expected_digest="sha256:abc")
        assert verdict.warn_count == 3
        assert verdict.failed_processors == ""
