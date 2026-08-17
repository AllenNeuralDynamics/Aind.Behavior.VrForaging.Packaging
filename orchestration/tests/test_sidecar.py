"""Unit tests for sidecar.py — the output.metadata.json model and helpers (§9)."""

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from aind_behavior_vr_foraging_orchestration.sidecar import (
    SIDECAR_NAME,
    CodeRef,
    ProcessorResult,
    SessionOutputMetadata,
    SidecarRecorder,
    _WarnCounter,
    build_code_ref,
    session_completed_ok,
    write_sidecar,
)


def _minimal_metadata(**overrides) -> SessionOutputMetadata:
    defaults = dict(
        session_name="behavior_1_2025-01-01_00-00-00",
        status="ok",
        started_at="2026-01-01T00:00:00Z",
        finished_at="2026-01-01T00:01:00Z",
        duration_s=60.0,
        processors=[ProcessorResult(name="sites", status="ok", rows=10, output_file="sites.parquet")],
        code=CodeRef(
            repository="repo",
            version="1.0.0",
            commit=None,
            python_version="3.13.0",
            container=None,
            provenance="unpinned",
        ),
        versions={"packaging_version": "1.0.0", "data_contract_version": "1.0.0", "dataset_version": "0.6.1"},
    )
    defaults.update(overrides)
    # model_validate (not SessionOutputMetadata(**defaults)): defaults is a plain
    # dict merged from heterogeneous per-field values, so a static checker can't
    # verify it field-by-field against the constructor's keyword signature.
    return SessionOutputMetadata.model_validate(defaults)


class TestWarnCounter:
    def test_counts_own_package_warnings(self):
        counter = _WarnCounter()
        logger = logging.getLogger("aind_behavior_vr_foraging_packaging.processing._site_table")
        logger.addHandler(counter)
        logger.warning("odor onset before interval")
        logger.warning("another one")
        logger.removeHandler(counter)
        assert counter.count == 2

    def test_counts_upstream_data_contract_warnings_too(self):
        counter = _WarnCounter()
        logger = logging.getLogger("aind_behavior_vr_foraging.data_contract")
        logger.addHandler(counter)
        logger.warning("no dedicated data contract")
        logger.removeHandler(counter)
        assert counter.count == 1

    def test_ignores_third_party_noise(self):
        """pynwb/hdmf boilerplate must not inflate the count (§16)."""
        counter = _WarnCounter()
        logger = logging.getLogger("hdmf.common.table")
        logger.addHandler(counter)
        logger.warning("attribute 'name' already exists on DynamicTable")
        logger.removeHandler(counter)
        assert counter.count == 0

    def test_ignores_info_and_below(self):
        counter = _WarnCounter()
        logger = logging.getLogger("aind_behavior_vr_foraging_packaging.foo")
        logger.addHandler(counter)
        logger.info("just fyi")
        logger.removeHandler(counter)
        assert counter.count == 0

    def test_reset_returns_and_zeroes(self):
        counter = _WarnCounter()
        logger = logging.getLogger("aind_behavior_vr_foraging_packaging.foo")
        logger.addHandler(counter)
        logger.warning("one")
        n = counter.reset()
        logger.removeHandler(counter)
        assert n == 1
        assert counter.count == 0


class TestBuildCodeRef:
    def test_unpinned_when_no_image_uri(self, monkeypatch):
        monkeypatch.delenv("PROCESSOR_IMAGE_URI", raising=False)
        ref = build_code_ref()
        assert ref.provenance == "unpinned"
        assert ref.container is None

    def test_pinned_when_digest_present(self, monkeypatch):
        monkeypatch.setenv(
            "PROCESSOR_IMAGE_URI", "ghcr.io/allenneuraldynamics/aind-behavior-vr-foraging-packaging@sha256:abc123"
        )
        monkeypatch.setenv("PROCESSOR_IMAGE_TAG", "sha-abc123")
        ref = build_code_ref()
        assert ref.provenance == "pinned-digest"
        assert ref.container is not None
        assert ref.container.digest == "sha256:abc123"
        assert ref.container.tag == "sha-abc123"

    def test_malformed_uri_without_digest_falls_back_to_unpinned(self, monkeypatch):
        monkeypatch.setenv("PROCESSOR_IMAGE_URI", "ghcr.io/foo/bar:latest")
        ref = build_code_ref()
        assert ref.provenance == "unpinned"
        assert ref.container is None

    def test_commit_from_env(self, monkeypatch):
        monkeypatch.delenv("PROCESSOR_IMAGE_URI", raising=False)
        monkeypatch.setenv("PROCESSOR_GIT_COMMIT", "deadbeef")
        assert build_code_ref().commit == "deadbeef"


class TestSessionOutputMetadata:
    def test_warn_count_sums_processors_and_nwb(self):
        meta = _minimal_metadata(
            processors=[
                ProcessorResult(name="sites", status="ok", warn_count=2),
                ProcessorResult(name="licks", status="ok", warn_count=1),
            ],
            nwb=ProcessorResult(name="nwb", status="ok", warn_count=3),
        )
        assert meta.warn_count == 6

    def test_warn_count_zero_by_default(self):
        meta = _minimal_metadata()
        assert meta.warn_count == 0

    def test_failed_processors_lists_only_errors(self):
        meta = _minimal_metadata(
            status="partial",
            processors=[
                ProcessorResult(name="sites", status="ok"),
                ProcessorResult(name="licks", status="error", error="boom"),
            ],
        )
        assert meta.failed_processors == ["licks"]

    def test_failed_processors_includes_nwb(self):
        meta = _minimal_metadata(status="partial", nwb=ProcessorResult(name="nwb", status="error", error="boom"))
        assert meta.failed_processors == ["nwb"]

    def test_frozen(self):
        meta = _minimal_metadata()
        # setattr, not `meta.status = ...`: pydantic's frozen __setattr__ raises
        # either way, but a static checker can verify a dynamic setattr call
        # while a direct assignment to a known-read-only field is flagged outright.
        with pytest.raises(Exception):  # pydantic ValidationError on frozen model
            setattr(meta, "status", "error")

    def test_round_trips_through_json(self):
        meta = _minimal_metadata()
        restored = SessionOutputMetadata.model_validate_json(meta.model_dump_json())
        assert restored == meta


class TestWriteSidecar:
    def test_writes_parseable_json_and_creates_parents(self, tmp_path):
        meta = _minimal_metadata()
        path = tmp_path / "nested" / "output.metadata.json"
        write_sidecar(path, meta)
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["session_name"] == meta.session_name
        assert data["schema_version"] == "1.0.0"


def _proc(name: str):
    """Minimal stand-in for an AbstractProcessor — the recorder only reads
    ``output_name``."""
    return SimpleNamespace(output_name=name)


class TestSidecarRecorder:
    """The status roll-up, which is what the ledger sorts on. Ordinary success and
    per-processor failures are covered end-to-end in tests/pipeline/test_session.py;
    these are the cases only reachable at this level."""

    def _recorder(self, tmp_path, **kwargs) -> SidecarRecorder:
        return SidecarRecorder(tmp_path / SIDECAR_NAME, session_name="sess_A", **kwargs)

    def test_nothing_attempted_is_ok(self, tmp_path):
        """Vacuously true, and deliberately: every processor being excluded by
        config is a valid run, not a failure."""
        rec = self._recorder(tmp_path)
        with rec:
            rec.dataset_loaded("0.6.1")
        assert rec.build().status == "ok"

    def test_failure_outside_the_processor_loop_is_still_an_error(self, tmp_path):
        """No ProcessorResult says anything went wrong, yet the block exited on an
        exception — the NWB step, or resolving the session root. Reporting `ok` here
        would hand the worker a clean sidecar for a container that exited nonzero."""
        rec = self._recorder(tmp_path)
        with pytest.raises(RuntimeError), rec:
            rec.dataset_loaded("0.6.1")
            rec.on_output(_proc("sites"), pd.DataFrame({"x": [1]}), Path("sites.parquet"))
            raise RuntimeError("nwb write failed")

        written = json.loads((tmp_path / SIDECAR_NAME).read_text(encoding="utf-8"))
        assert written["status"] == "error"
        assert [p["status"] for p in written["processors"]] == ["ok"]

    def test_a_failed_nwb_step_fails_the_session(self, tmp_path):
        rec = self._recorder(tmp_path)
        with rec:
            rec.dataset_loaded("0.6.1")
            rec.nwb_error(ValueError("no metadata jsons"))
        assert rec.build().status == "error"

    def test_disabled_recorder_writes_nothing(self, tmp_path):
        rec = SidecarRecorder(None, session_name="sess_A")
        with rec:
            rec.dataset_loaded("0.6.1")
        assert list(tmp_path.iterdir()) == []

    def test_warnings_are_attributed_to_the_processor_that_logged_them(self, tmp_path):
        rec = self._recorder(tmp_path)
        log = logging.getLogger("aind_behavior_vr_foraging_packaging.processing.test")
        with rec:
            rec.dataset_loaded("0.6.1")
            log.warning("first")
            log.warning("second")
            rec.on_output(_proc("sites"), pd.DataFrame({"x": [1]}), Path("sites.parquet"))
            log.warning("third")
            rec.on_output(_proc("licks"), pd.DataFrame({"x": [1]}), Path("licks.parquet"))

        counts = {p.name: p.warn_count for p in rec.build().processors}
        assert counts == {"sites": 2, "licks": 1}


class TestSessionCompletedOk:
    """`aggregate`'s include predicate. Fails OPEN in both ambiguous cases."""

    def _session(self, tmp_path, name: str, status: str | None) -> Path:
        d = tmp_path / name
        d.mkdir(parents=True)
        if status is not None:
            (d / SIDECAR_NAME).write_text(json.dumps({"status": status}), encoding="utf-8")
        return d

    @pytest.mark.parametrize(("status", "expected"), [("ok", True), ("error", False), ("partial", False)])
    def test_reads_the_recorded_status(self, tmp_path, status, expected):
        assert session_completed_ok(self._session(tmp_path, "s", status)) is expected

    def test_no_sidecar_is_kept(self, tmp_path):
        """Produced by the plain `vr-foraging-packaging` CLI, which has no opinion
        on this — the check must not become an implicit requirement for a sidecar."""
        assert session_completed_ok(self._session(tmp_path, "s", None)) is True

    def test_unreadable_sidecar_is_kept(self, tmp_path):
        d = self._session(tmp_path, "s", None)
        (d / SIDECAR_NAME).write_text("{not json", encoding="utf-8")
        assert session_completed_ok(d) is True
