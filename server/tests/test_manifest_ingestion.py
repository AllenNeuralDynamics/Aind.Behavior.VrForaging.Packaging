"""Unit tests for ``ingestion.type: manifest`` and ``worker.exit_when_drained``.

Two things are being pinned here, and the second is the dangerous one:

* the manifest is validated **up front**, so a bad file fails at startup rather than
  after 1700 sessions;
* a drained run exits for the right reason. "Nothing left to claim" and "the work is
  finished" are different states, and conflating them produces a green run that
  processed nothing — the single failure mode worth the most tests in this file.
"""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from processing_server.config import PipelineConfig
from processing_server.models import Job
from processing_server.sources.manifest import ManifestError, ManifestSource
from processing_server.stores.output_local import LocalOutputStore
from processing_server.worker import Worker

_SESSIONS = [
    {"session_name": "707349_2024-04-17_10-34-09", "location": "s3://aind-open-data/707349_2024-04-17_10-34-09"},
    {"session_name": "behavior_808728_2025-12-10_20-40-41", "location": "s3://private-bucket/behavior_808728"},
]


def _manifest(tmp_path: Path, payload) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _config(tmp_path, manifest: Path, **overrides) -> PipelineConfig:
    data = {
        "release": "rel1",
        "ingestion": {"type": "manifest", "manifest_file": str(manifest), "interval_s": 999_999},
        "input": {"store": "local"},
        "output": {"store": "local", "uri": str(tmp_path / "out")},
        "worker": {"ledger": str(tmp_path / "jobs.sqlite"), "poll_interval_s": 0, "exit_when_drained": True},
        "processor": {"allow_unpinned": True},
        "logging": {"dir": str(tmp_path / "logs")},
        "aggregation": {"enabled": False},
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(data.get(key), dict):
            data[key] = {**data[key], **value}
        else:
            data[key] = value
    return PipelineConfig(**data)


class TestManifestParsing:
    def test_reads_the_documented_shape(self, tmp_path):
        source = ManifestSource(_manifest(tmp_path, {"sessions": _SESSIONS}))
        refs = list(source.discover(None))
        assert [r.session_name for r in refs] == [s["session_name"] for s in _SESSIONS]
        assert [r.input_uri for r in refs] == [s["location"] for s in _SESSIONS]
        assert len(source) == 2

    def test_a_bare_list_is_accepted(self, tmp_path):
        assert len(ManifestSource(_manifest(tmp_path, _SESSIONS))) == 2

    def test_subject_id_comes_from_the_session_name(self, tmp_path):
        source = ManifestSource(_manifest(tmp_path, {"sessions": _SESSIONS}))
        assert [r.subject_id for r in source.discover(None)] == ["707349", "808728"]

    def test_no_cursor_is_emitted(self, tmp_path):
        """A finite static set has no watermark. Emitting one would give the run a way to
        skip part of the list it was handed."""
        source = ManifestSource(_manifest(tmp_path, {"sessions": _SESSIONS}))
        assert all(r.cursor is None for r in source.discover(None))

    def test_since_is_ignored(self, tmp_path):
        source = ManifestSource(_manifest(tmp_path, {"sessions": _SESSIONS}))
        assert len(list(source.discover("2099-01-01T00:00:00+00:00"))) == 2

    def test_rejected_siblings_are_reported_not_processed(self, tmp_path, caplog):
        path = _manifest(
            tmp_path,
            {"sessions": _SESSIONS, "ambiguous": [], "unmatched": ["715867_2024-05-02_12-20-15"]},
        )
        with caplog.at_level("WARNING"):
            assert len(ManifestSource(path)) == 2
        assert "unmatched" in caplog.text and "715867_2024-05-02_12-20-15" in caplog.text

    def test_an_entry_without_a_location_is_skipped(self, tmp_path, caplog):
        path = _manifest(tmp_path, {"sessions": [*_SESSIONS, {"session_name": "707349_2024-04-18_10-35-08"}]})
        with caplog.at_level("WARNING"):
            assert len(ManifestSource(path)) == 2
        assert "missing session_name or location" in caplog.text

    def test_a_misnamed_session_is_skipped(self, tmp_path, caplog):
        path = _manifest(
            tmp_path, {"sessions": [*_SESSIONS, {"session_name": "not-a-session", "location": "s3://b/x"}]}
        )
        with caplog.at_level("WARNING"):
            assert len(ManifestSource(path)) == 2
        assert "does not match the expected session-name pattern" in caplog.text

    def test_duplicates_are_collapsed_once(self, tmp_path):
        """`job_key` would absorb a duplicate anyway, but then the count this source
        reports would not be the number of sessions that get processed."""
        path = _manifest(tmp_path, {"sessions": [*_SESSIONS, _SESSIONS[0]]})
        assert len(ManifestSource(path)) == 2

    def test_a_duplicate_with_a_different_location_is_flagged(self, tmp_path, caplog):
        clash = {"session_name": _SESSIONS[0]["session_name"], "location": "s3://somewhere-else/x"}
        with caplog.at_level("WARNING"):
            source = ManifestSource(_manifest(tmp_path, {"sessions": [*_SESSIONS, clash]}))
        assert "listed twice with different locations" in caplog.text
        locations = {r.session_name: r.input_uri for r in source.discover(None)}
        assert locations[_SESSIONS[0]["session_name"]] == _SESSIONS[0]["location"], "the first location should win"


class TestABadManifestFailsAtStartup:
    """Every one of these would otherwise become a run that quietly processes nothing."""

    def test_a_missing_file_raises(self, tmp_path):
        with pytest.raises(ManifestError, match="does not exist"):
            ManifestSource(tmp_path / "nope.json")

    def test_unparsable_json_raises(self, tmp_path):
        path = tmp_path / "m.json"
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(ManifestError, match="not readable JSON"):
            ManifestSource(path)

    def test_an_object_without_sessions_raises(self, tmp_path):
        with pytest.raises(ManifestError, match="without a 'sessions' key"):
            ManifestSource(_manifest(tmp_path, {"items": _SESSIONS}))

    def test_a_scalar_top_level_raises(self, tmp_path):
        with pytest.raises(ManifestError, match="must hold an object or a list"):
            ManifestSource(_manifest(tmp_path, 3))

    def test_an_empty_session_list_raises(self, tmp_path):
        with pytest.raises(ManifestError, match="no usable sessions"):
            ManifestSource(_manifest(tmp_path, {"sessions": []}))

    def test_a_list_of_only_unusable_entries_raises(self, tmp_path):
        with pytest.raises(ManifestError, match="no usable sessions"):
            ManifestSource(_manifest(tmp_path, {"sessions": [{"session_name": "x"}, "y"]}))

    def test_the_type_requires_the_file_to_be_configured(self, tmp_path):
        with pytest.raises(ValidationError, match="manifest_file is unset"):
            PipelineConfig(
                release="rel1",
                ingestion={"type": "manifest"},
                output={"store": "local", "uri": str(tmp_path / "out")},
                worker={"ledger": str(tmp_path / "jobs.sqlite")},
                processor={"allow_unpinned": True},
                logging={"dir": str(tmp_path / "logs")},
            )

    def test_doctor_reports_a_bad_manifest(self, tmp_path):
        config = _config(tmp_path, tmp_path / "gone.json")
        worker = Worker(config, worker_id="w1", work_dir=tmp_path / "work", output_store=LocalOutputStore())
        try:
            assert any("does not exist" in p for p in worker.doctor())
        finally:
            worker.close()


# ---------------------------------------------------------------------------
# exit_when_drained
# ---------------------------------------------------------------------------


class _Worker(Worker):
    """A worker whose session processing is decided by the test, not by Docker."""

    outcomes: dict[str, str] = {}

    def process_job(self, job: Job) -> None:
        outcome = self.outcomes.get(job.session_name or "", "completed")
        if outcome == "completed":
            self.ledger.complete_job(job.job_id, partial=False)
        elif outcome == "skipped":
            self.ledger.skip_running(job.job_id, "output exists")
        else:
            self.ledger.fail_job(job.job_id, error_kind="data", error="test-induced")


def _worker(tmp_path, outcomes=None, **overrides) -> _Worker:
    manifest = _manifest(tmp_path, {"sessions": _SESSIONS})
    worker = _Worker(
        _config(tmp_path, manifest, **overrides),
        worker_id="w1",
        work_dir=tmp_path / "work",
        output_store=LocalOutputStore(),
    )
    worker.outcomes = outcomes or {}
    return worker


class TestDrainedRunExitCode:
    def test_all_sessions_complete_exits_zero(self, tmp_path):
        worker = _worker(tmp_path)
        try:
            assert worker.run_forever() == 0
            assert worker.ledger.status_counts("rel1") == {"completed": 2}
        finally:
            worker.close()

    def test_any_failure_exits_nonzero(self, tmp_path):
        """The chosen policy: one bad session fails the run. Which one is in the ledger."""
        worker = _worker(tmp_path, {_SESSIONS[0]["session_name"]: "failed"})
        try:
            assert worker.run_forever() == 1
            counts = worker.ledger.status_counts("rel1")
            assert counts.get("completed") == 1 and sum(counts.values()) == 2
        finally:
            worker.close()

    def test_a_skipped_session_is_not_a_failure(self, tmp_path):
        """`skipped` means the output was already there and `overwrite` is false."""
        worker = _worker(tmp_path, {_SESSIONS[0]["session_name"]: "skipped"})
        try:
            assert worker.run_forever() == 0
        finally:
            worker.close()

    def test_it_does_not_exit_while_a_job_is_still_pending(self, tmp_path):
        """The guard that matters: `_finish_if_drained` reads the ledger rather than
        trusting "the claim loop found nothing", because a retrying job with a future
        `next_eligible_at` is unclaimable *and* outstanding."""
        worker = _worker(tmp_path)
        try:
            worker.ingest_once()
            assert worker.ledger.count_active("rel1") == 2
            assert worker._finish_if_drained() is None
        finally:
            worker.close()

    def test_zero_jobs_is_an_error_not_a_clean_drain(self, tmp_path):
        """An empty ledger after a successful sweep means the run processed nothing —
        an unmounted file, a wrong release name. Exiting 0 here is how that hides."""
        worker = _worker(tmp_path)
        try:
            assert worker.ledger.status_counts("rel1") == {}
            assert worker._finish_if_drained() == 1
        finally:
            worker.close()

    def test_disabled_by_default_so_a_server_keeps_running(self, tmp_path):
        worker = _worker(tmp_path, worker={"exit_when_drained": False})
        try:
            assert worker.config.worker.exit_when_drained is False
            worker.ingest_once()
            while worker.claim_and_process_one():
                pass
            # Drained, but the flag is off: nothing should conclude the run.
            assert worker.ledger.count_active("rel1") == 0
            assert worker.run_forever(once=True) == 0
        finally:
            worker.close()


class TestAggregationOnDrain:
    def test_it_aggregates_before_exiting(self, tmp_path):
        """A batch run finishing at 14:00 must not exit without the table it was run to
        produce, so the drain path deliberately bypasses the 03:00 wall clock."""
        worker = _worker(tmp_path, aggregation={"enabled": True, "at": "03:00"})
        try:
            _publish_two_sessions(worker)
            assert worker.run_forever() == 0
            days = worker.aggregate_days()
            assert len(days) == 1, f"no aggregate was written on drain: {days}"
            assert (Path(worker.config.output.uri) / "rel1" / "aggregate" / "latest" / "session.parquet").exists()
        finally:
            worker.close()

    def test_it_is_not_gated_on_the_scheduled_hour(self, tmp_path):
        worker = _worker(tmp_path, aggregation={"enabled": True, "at": "23:59"})
        try:
            _publish_two_sessions(worker)
            assert worker.aggregation_due() is False, "the schedule should not be open — that is the point"
            assert worker.run_forever() == 0
            assert len(worker.aggregate_days()) == 1
        finally:
            worker.close()

    def test_a_failed_aggregation_fails_the_run(self, tmp_path):
        worker = _worker(tmp_path, aggregation={"enabled": True})
        try:
            _publish_two_sessions(worker, real_parquet=False)
            assert worker.run_forever() == 1
            assert worker.aggregate_days() == []
        finally:
            worker.close()

    def test_disabled_aggregation_still_exits_zero(self, tmp_path):
        worker = _worker(tmp_path, aggregation={"enabled": False})
        try:
            _publish_two_sessions(worker)
            assert worker.run_forever() == 0
            assert worker.aggregate_days() == []
        finally:
            worker.close()


def _publish_two_sessions(worker: Worker, *, real_parquet: bool = True) -> None:
    """Put published output where the aggregate reads from, as the session path would."""
    from processing_server.stores import SIDECAR_NAME

    for index, entry in enumerate(_SESSIONS):
        name = entry["session_name"]
        root = Path(worker.config.output.uri) / worker.config.release / "sessions" / name
        root.mkdir(parents=True, exist_ok=True)
        if real_parquet:
            import pandas as pd

            pd.DataFrame([{"session_id": name}]).to_parquet(root / "session.parquet")
            pd.DataFrame({"site_index": [0, 1, 2]}).to_parquet(root / "sites.parquet")
        else:
            (root / "sites.parquet").write_bytes(b"not-parquet")
        (root / SIDECAR_NAME).write_text(json.dumps({"session_name": name, "status": "ok", "job_id": f"j{index}"}))


class TestFailFast:
    """Stop at the first session that fails for good, rather than working the rest."""

    def test_it_stops_on_the_first_terminal_failure(self, tmp_path):
        worker = _worker(tmp_path, {_SESSIONS[0]["session_name"]: "failed"}, worker={"fail_fast": True})
        try:
            assert worker.run_forever() == 1
            counts = worker.ledger.status_counts("rel1")
            assert counts.get("failed") == 1
            assert counts.get("pending") == 1, f"the run continued past the failure: {counts}"
        finally:
            worker.close()

    def test_without_it_the_rest_still_runs(self, tmp_path):
        """Same failure, flag off: every remaining session is still attempted."""
        worker = _worker(tmp_path, {_SESSIONS[0]["session_name"]: "failed"})
        try:
            assert worker.run_forever() == 1
            counts = worker.ledger.status_counts("rel1")
            assert counts.get("failed") == 1 and counts.get("completed") == 1
            assert "pending" not in counts
        finally:
            worker.close()

    def test_nothing_is_aggregated_on_the_abort(self, tmp_path):
        worker = _worker(
            tmp_path,
            {_SESSIONS[0]["session_name"]: "failed"},
            worker={"fail_fast": True},
            aggregation={"enabled": True},
        )
        try:
            _publish_two_sessions(worker)
            assert worker.run_forever() == 1
            assert worker.aggregate_days() == [], (
                "an aggregate over a partial set would be published as though the run had finished"
            )
        finally:
            worker.close()

    def test_a_clean_run_is_unaffected(self, tmp_path):
        worker = _worker(tmp_path, worker={"fail_fast": True})
        try:
            assert worker.run_forever() == 0
            assert worker.ledger.status_counts("rel1") == {"completed": 2}
        finally:
            worker.close()

    def test_a_retrying_job_does_not_trigger_it(self, tmp_path):
        """A transient failure with attempts left is not a failure yet — otherwise
        fail_fast fires on a blip in S3 rather than on bad data or broken code."""
        worker = _worker(tmp_path, worker={"fail_fast": True})
        try:
            worker.ingest_once()
            job = worker.ledger.claim("w1", 600)
            assert job is not None
            status = worker.ledger.fail_job(job.job_id, error_kind="transient", error="s3 blip")
            assert status == "retrying", "the fixture no longer produces a retry — the test proves nothing"
            assert worker._abort_on_failure() is None
            assert worker._terminal_failures() == 0
        finally:
            worker.close()


class TestSessionStartIsBackfilled:
    """A manifest carries a name and a URI, so the ledger row starts with no acquisition
    time. The authoritative value is inside the session, and the processor's sidecar
    already computes it — so it is written back on completion rather than guessed from
    the directory name at discovery."""

    def test_discovery_leaves_it_blank_rather_than_guessing(self, tmp_path):
        source = ManifestSource(_manifest(tmp_path, {"sessions": _SESSIONS}))
        assert all(r.session_start is None for r in source.discover(None))

    def test_completion_fills_it_in_from_the_sidecar(self, tmp_path):
        worker = _worker(tmp_path)
        try:
            worker.ingest_once()
            job = worker.ledger.claim("w1", 600)
            assert job is not None
            assert job.session_start is None, "nothing should have supplied this at discovery"
            worker.ledger.complete_job(
                job.job_id,
                partial=False,
                session_start="2024-04-17T10:34:09+00:00",
                subject_id="707349",
            )
            done = worker.ledger.get_job(job.job_id)
            assert done is not None
            assert done.session_start is not None
            assert str(done.session_start).startswith("2024-04-17T10:34:09")
        finally:
            worker.close()

    def test_an_existing_value_is_never_overwritten(self, tmp_path):
        """DocDB reads a metadata index and knows this at discovery. The sidecar's value
        must not silently replace it — a backfill, not a correction."""
        worker = _worker(tmp_path)
        try:
            job_id = worker.ledger.upsert_job(
                kind="session",
                release="rel1",
                asset_id=None,
                processor_fingerprint="fp",
                input_store="s3",
                input_uri="s3://b/x",
                output_uri="out",
                session_name="707349_2024-04-19_10-43-00",
                subject_id="707349",
                session_start="2024-04-19T10:43:00+00:00",
            )
            assert job_id is not None
            worker.ledger.force_claim(job_id, "w1", 600)
            worker.ledger.complete_job(job_id, partial=False, session_start="1999-01-01T00:00:00+00:00")
            done = worker.ledger.get_job(job_id)
            assert done is not None and done.session_start is not None
            assert str(done.session_start).startswith("2024-04-19"), "the discovery-time value was clobbered"
        finally:
            worker.close()

    def test_a_sidecar_without_one_leaves_the_column_null(self, tmp_path):
        worker = _worker(tmp_path)
        try:
            worker.ingest_once()
            job = worker.ledger.claim("w1", 600)
            assert job is not None
            worker.ledger.complete_job(job.job_id, partial=False)
            done = worker.ledger.get_job(job.job_id)
            assert done is not None and done.session_start is None
        finally:
            worker.close()
