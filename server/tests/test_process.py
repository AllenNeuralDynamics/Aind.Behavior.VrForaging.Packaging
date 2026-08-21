"""Unit tests for `vr-foraging-server process` — what runs in the container.

The seam being tested is the one the split created: `process_one_session` drives
the published `process_session` through its generic hooks and turns the result
into a sidecar. No dataset I/O, no Docker.
"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from processing_server.process import process_one_session
from processing_server.sidecar import SIDECAR_NAME


def _proc(name: str, *, raises: bool = False) -> MagicMock:
    m = MagicMock()
    m.output_name = name
    if raises:
        m.compute.side_effect = ValueError(f"{name} blew up")
    else:
        df = pd.DataFrame({"session_id": ["sess_A"], "subject_id": ["808728"], "date": ["2025-01-01T10:00:00"]})
        df.attrs.update({"packaging_version": "t", "data_contract_version": "1.0.0", "dataset_version": "0.6.1"})
        m.compute.return_value = df
    return m


def _dataset_rooted_at(root) -> MagicMock:
    ds = MagicMock()
    ds.version = "0.6.1"
    ds.at.return_value.at.return_value.at.return_value.reader_params.path = str(
        root / "behavior" / "Logs" / "session_input.json"
    )
    return ds


def _run(tmp_path, procs, *, load_error=None, **kwargs):
    raw = tmp_path / "raw" / "sess_A"
    raw.mkdir(parents=True, exist_ok=True)
    load = patch(
        "aind_behavior_vr_foraging.data_contract.dataset",
        **({"side_effect": load_error} if load_error else {"return_value": _dataset_rooted_at(raw)}),
    )
    with (
        load,
        patch("aind_behavior_vr_foraging_packaging.pipeline.session.create_processors", return_value=procs),
    ):
        return process_one_session(raw, tmp_path / "out", **kwargs)


def _read(tmp_path) -> dict:
    return json.loads((tmp_path / "out" / SIDECAR_NAME).read_text(encoding="utf-8"))


class TestProcessOneSession:
    def test_records_every_processor_on_success(self, tmp_path):
        metadata = _run(tmp_path, [_proc("session"), _proc("sites")])

        assert metadata.status == "ok"
        written = _read(tmp_path)
        assert {p["name"]: p["status"] for p in written["processors"]} == {"session": "ok", "sites": "ok"}
        assert [p["output_file"] for p in written["processors"]] == ["session.parquet", "sites.parquet"]
        assert written["session_name"] == "sess_A"
        assert written["versions"]["dataset_version"] == "0.6.1"

    def test_identity_is_lifted_from_the_session_table(self, tmp_path):
        """So an output directory can be identified without opening a parquet."""
        _run(tmp_path, [_proc("session")])

        written = _read(tmp_path)
        assert written["subject_id"] == "808728"
        assert written["session_start"].startswith("2025-01-01T10:00:00")

    def test_written_when_a_processor_raises_and_the_failure_still_propagates(self, tmp_path):
        """The whole reason the sidecar exists: the exception is not swallowed, so
        the container exits nonzero — and the record naming the culprit is on disk
        anyway, which is the only way the worker learns which processor broke."""
        with pytest.raises(ValueError, match="sites blew up"):
            _run(tmp_path, [_proc("session"), _proc("sites", raises=True), _proc("licks")])

        written = _read(tmp_path)
        assert written["status"] == "error"
        assert {p["name"]: p["status"] for p in written["processors"]} == {"session": "ok", "sites": "error"}
        assert "blew up" in next(p["error"] for p in written["processors"] if p["name"] == "sites")
        # `licks` never ran — recording a failure is not tolerating it.
        assert not (tmp_path / "out" / "licks.parquet").exists()

    def test_written_when_the_dataset_never_loads(self, tmp_path):
        """The case most worth reporting, and the reason the dataset is loaded here
        rather than inside process_session: there is no dataset left to ask for a
        version, so the session name has to come from the input path."""
        with pytest.raises(ValueError, match="bad session"):
            _run(tmp_path, [], load_error=ValueError("bad session"))

        written = _read(tmp_path)
        assert written["status"] == "error"
        assert written["processors"] == []
        assert written["session_name"] == "sess_A"
        assert written["versions"]["dataset_version"] == "unknown"

    def test_records_the_flags_the_run_actually_used(self, tmp_path):
        _run(tmp_path, [_proc("sites")], strict_parsing=True, exclude=["licks"])

        params = _read(tmp_path)["parameters"]
        assert params["strict_parsing"] is True
        assert params["exclude"] == ["licks"]

    def test_carries_job_context_from_the_environment(self, tmp_path, monkeypatch):
        """Set by the worker at `docker run` time, so a published output can be
        traced back to the ledger row that produced it."""
        monkeypatch.setenv("VRF_JOB_ID", "job-42")
        monkeypatch.setenv("VRF_WORKER_ID", "worker-1")

        _run(tmp_path, [_proc("sites")])

        written = _read(tmp_path)
        assert (written["job_id"], written["worker_id"]) == ("job-42", "worker-1")

    def test_no_parquet_still_records_rows(self, tmp_path):
        """`--no-write-parquet` is a dry run: the frames are computed and counted,
        nothing reaches disk but the sidecar."""
        _run(tmp_path, [_proc("sites")], write_parquet=False)

        written = _read(tmp_path)
        assert written["processors"][0]["rows"] == 1
        assert written["processors"][0]["output_file"] is None
        assert not (tmp_path / "out" / "sites.parquet").exists()
