"""Unit tests for pipeline/batch.py — no real dataset I/O required."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging._base import session_root
from aind_behavior_vr_foraging_packaging.pipeline.batch import (
    AGGREGATED_TABLES,
    SESSION_TABLE,
    aggregate,
    process_sessions,
)
from aind_behavior_vr_foraging_packaging.processing import SessionMetadataProcessor

# ---------------------------------------------------------------------------
# process_sessions — any processor failure fails the whole session
# ---------------------------------------------------------------------------


def _mock_proc(name: str, *, raises: bool = False) -> MagicMock:
    proc = MagicMock()
    proc.output_name = name
    if raises:
        proc.compute.side_effect = ValueError(f"{name} blew up")
    else:
        df = pd.DataFrame({"x": [1]})
        df.attrs.update({"packaging_version": "test", "data_contract_version": "1.0.0", "dataset_version": "0.6.1"})
        proc.compute.return_value = df
    return proc


def _mock_dataset(root: Path | None = None) -> MagicMock:
    """Mock dataset whose Session stream path makes session_root() resolve to *root*."""
    ds = MagicMock()
    ds.version = "0.6.1"
    root = root or Path("raw") / "sess_A"
    ds.at.return_value.at.return_value.at.return_value.reader_params.path = str(
        root / "behavior" / "Logs" / "session_input.json"
    )
    return ds


class TestAnyProcessorFailureFailsTheSession:
    def test_processor_failure_propagates(self, tmp_path):
        """A processor raising aborts the run rather than yielding a partial
        session: anything escaping compute() is unexpected by definition (the
        strict_parsing convention), so there is nothing safe to salvage."""
        raw = tmp_path / "raw" / "sess_A"
        raw.mkdir(parents=True)
        procs = [_mock_proc("session"), _mock_proc("sites"), _mock_proc("licks", raises=True)]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", return_value=_mock_dataset(raw)),
            patch("aind_behavior_vr_foraging_packaging.pipeline.session.create_processors", return_value=procs),
            pytest.raises(ValueError, match="licks blew up"),
        ):
            process_sessions([raw], tmp_path / "out")

    def test_session_kept_when_every_processor_succeeds(self, tmp_path):
        raw = tmp_path / "raw" / "sess_A"
        raw.mkdir(parents=True)
        procs = [_mock_proc("session"), _mock_proc("sites"), _mock_proc("licks")]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", return_value=_mock_dataset(raw)),
            patch("aind_behavior_vr_foraging_packaging.pipeline.session.create_processors", return_value=procs),
        ):
            written = process_sessions([raw], tmp_path / "out")

        assert written == [tmp_path / "out" / "sessions" / "sess_A"]
        assert (tmp_path / "out" / "sessions" / "sess_A" / "sites.parquet").exists()

    def test_processor_construction_failure_propagates(self, tmp_path):
        """A bad rig config crashing create_processors() aborts the batch — it is
        not isolated per session. Constructing a processor list is not a data
        anomaly, so strict_parsing has no say in it."""
        good = tmp_path / "raw" / "sess_good"
        bad = tmp_path / "raw" / "sess_bad"
        good.mkdir(parents=True)
        bad.mkdir(parents=True)

        def _create_processors(ds, **kwargs):
            if session_root(ds) == bad:
                raise RuntimeError("malformed rig config")
            return [_mock_proc("session"), _mock_proc("sites")]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", side_effect=_mock_dataset),
            patch(
                "aind_behavior_vr_foraging_packaging.pipeline.session.create_processors",
                side_effect=_create_processors,
            ),
            pytest.raises(RuntimeError, match="malformed rig config"),
        ):
            process_sessions([bad, good], tmp_path / "out")


class TestParallelSessions:
    """max_workers > 1 fans out over a ThreadPoolExecutor; nothing else changes."""

    def test_every_session_is_written(self, tmp_path):
        raws = [tmp_path / "raw" / f"sess_{n}" for n in "ABC"]
        for r in raws:
            r.mkdir(parents=True)

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", side_effect=_mock_dataset),
            patch(
                "aind_behavior_vr_foraging_packaging.pipeline.session.create_processors",
                side_effect=lambda ds, **kw: [_mock_proc("session"), _mock_proc("sites")],
            ),
        ):
            written = process_sessions(raws, tmp_path / "out", max_workers=3)

        assert {p.name for p in written} == {"sess_A", "sess_B", "sess_C"}
        for r in raws:
            assert (tmp_path / "out" / "sessions" / r.name / "sites.parquet").exists()

    def test_a_failing_session_still_aborts_the_batch(self, tmp_path):
        """`fut.result()` re-raises, so concurrency does not quietly become
        isolation — the property the sequential path is held to must survive here."""
        good = tmp_path / "raw" / "sess_good"
        bad = tmp_path / "raw" / "sess_bad"
        good.mkdir(parents=True)
        bad.mkdir(parents=True)

        def _create_processors(ds, **kwargs):
            failing = session_root(ds) == bad
            return [_mock_proc("session"), _mock_proc("sites", raises=failing)]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", side_effect=_mock_dataset),
            patch(
                "aind_behavior_vr_foraging_packaging.pipeline.session.create_processors",
                side_effect=_create_processors,
            ),
            pytest.raises(ValueError, match="sites blew up"),
        ):
            process_sessions([good, bad], tmp_path / "out", max_workers=2)


# ---------------------------------------------------------------------------
# aggregate()
# ---------------------------------------------------------------------------


def _write_fake_session(sessions_dir: Path, session_id: str, subject_id: str) -> None:
    """Write minimal parquet files for a fake session."""
    d = sessions_dir / session_id
    d.mkdir(parents=True)

    pd.DataFrame([{"session_id": session_id, "subject_id": subject_id, "date": "2025-01-01"}]).to_parquet(
        d / "session.parquet", index=False
    )

    pd.DataFrame({"site": [1, 2, 3]}).to_parquet(d / "sites.parquet", index=False)
    pd.DataFrame({"t": range(5)}).to_parquet(d / "licks.parquet", index=False)


class TestAggregateIncludePredicate:
    """`aggregate` cannot tell a complete session from one abandoned partway
    through — both are just a directory of parquets. A caller that can, says so."""

    def test_excluded_sessions_are_left_out(self, tmp_path):
        sessions_dir = tmp_path / "sessions"
        _write_fake_session(sessions_dir, "sess_good", "sub1")
        _write_fake_session(sessions_dir, "sess_bad", "sub1")

        aggregate(sessions_dir, tmp_path, include=lambda d: d.name != "sess_bad")

        assert set(pd.read_parquet(tmp_path / "session.parquet")["session_id"]) == {"sess_good"}

    def test_no_predicate_aggregates_everything(self, tmp_path):
        sessions_dir = tmp_path / "sessions"
        _write_fake_session(sessions_dir, "sess_A", "sub1")
        _write_fake_session(sessions_dir, "sess_B", "sub1")

        aggregate(sessions_dir, tmp_path)

        assert set(pd.read_parquet(tmp_path / "session.parquet")["session_id"]) == {"sess_A", "sess_B"}

    def test_everything_excluded_writes_nothing_and_does_not_raise(self, tmp_path):
        sessions_dir = tmp_path / "sessions"
        _write_fake_session(sessions_dir, "sess_A", "sub1")

        aggregate(sessions_dir, tmp_path, include=lambda _: False)

        assert not (tmp_path / "session.parquet").exists()


# --- What gets aggregated is fixed, not configurable -----------------------


def test_aggregated_tables():
    assert AGGREGATED_TABLES == ("session", "sites")


def test_session_table_name_matches_the_processor_that_writes_it():
    """SESSION_TABLE is hardcoded so the constant stays a plain `str`; this pins
    it to the processor so the two cannot drift apart silently."""
    assert SESSION_TABLE == SessionMetadataProcessor.__output_name__


def test_large_per_sample_streams_are_not_aggregated(tmp_path):
    """licks/sniffing/position_velocity are read one session at a time, so they
    are deliberately absent from the experiment-level output."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    aggregate(sessions_dir, tmp_path)

    assert not (tmp_path / "licks.parquet").exists()


def test_per_session_files_are_never_deleted(tmp_path):
    """Aggregation copies rows out; it does not destroy the source. The
    per-session files are what re-aggregation reads back."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    aggregate(sessions_dir, tmp_path)

    for table in ("session", "sites", "licks"):
        assert (sessions_dir / "sess_A" / f"{table}.parquet").exists(), table


# --- Normal output ---------------------------------------------------------


def test_aggregate_writes_both_tables(tmp_path):
    """Both names in AGGREGATED_TABLES become one flat file spanning all sessions."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub2")

    aggregate(sessions_dir, tmp_path)

    sessions = pd.read_parquet(tmp_path / "session.parquet")
    assert len(sessions) == 2  # one identity row per session
    assert set(sessions.columns) >= {"session_id", "subject_id", "date"}

    sites = pd.read_parquet(tmp_path / "sites.parquet")
    assert len(sites) == 6  # 3 rows × 2 sessions
    assert set(sites["session_id"].unique()) == {"sess_A", "sess_B"}


def test_aggregate_session_id_joins_across_tables(tmp_path):
    """Regression: session.parquet and the flat tables must key on the same
    value. They once disagreed — session.parquet carried the stream's
    `session_name` while every other table carried the directory name — which
    silently broke every join."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "behavior_815103_2025-11-05_22-52-21", "815103")

    aggregate(sessions_dir, tmp_path)

    sessions = pd.read_parquet(tmp_path / "session.parquet")
    sites = pd.read_parquet(tmp_path / "sites.parquet")
    assert set(sessions["session_id"]) == set(sites["session_id"].unique())
    assert set(sites["session_id"].unique()) == {"behavior_815103_2025-11-05_22-52-21"}


def test_aggregate_is_rerunnable(tmp_path):
    """Phase 2 can run twice over the same sessions/ — the flat file is
    overwritten, not appended to. This is what the `aggregate` subcommand relies on."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    aggregate(sessions_dir, tmp_path)
    aggregate(sessions_dir, tmp_path)

    assert len(pd.read_parquet(tmp_path / "sites.parquet")) == 3  # 3 rows, not 6
    assert len(pd.read_parquet(tmp_path / "session.parquet")) == 1


# --- Degenerate inputs -----------------------------------------------------


def test_aggregate_missing_table_is_skipped(tmp_path):
    """A session that lacks a table file does not crash aggregation."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    # sess_B intentionally has no sites.parquet
    d = sessions_dir / "sess_B"
    d.mkdir(parents=True)
    pd.DataFrame([{"session_id": "sess_B", "subject_id": "sub1", "date": "2025-01-02"}]).to_parquet(
        d / "session.parquet", index=False
    )

    aggregate(sessions_dir, tmp_path)  # must not raise

    df = pd.read_parquet(tmp_path / "sites.parquet")
    assert len(df) == 3  # only from sess_A
    assert set(df["session_id"].unique()) == {"sess_A"}


def test_missing_session_table_aborts_aggregation(tmp_path):
    """Without the identity table there is nothing to join on, so the rest of
    the aggregate is not written either."""
    sessions_dir = tmp_path / "sessions"
    d = sessions_dir / "sess_A"
    d.mkdir(parents=True)
    pd.DataFrame({"site": [1, 2, 3]}).to_parquet(d / "sites.parquet", index=False)

    aggregate(sessions_dir, tmp_path)  # must not raise

    assert not (tmp_path / "session.parquet").exists()
    assert not (tmp_path / "sites.parquet").exists()


def test_aggregate_empty_sessions_dir(tmp_path):
    """aggregate() on an empty sessions/ dir logs a warning and returns cleanly."""
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    aggregate(sessions_dir, tmp_path)  # must not raise
    assert not (tmp_path / "session.parquet").exists()


# ---------------------------------------------------------------------------
# clean — scoped to what this pipeline wrote
# ---------------------------------------------------------------------------


def _prior_run(output_dir: Path) -> None:
    """Lay down what a previous batch run would have left behind."""
    _write_fake_session(output_dir / "sessions", "sess_old", "sub1")
    for table in AGGREGATED_TABLES:
        pd.DataFrame({"x": [1]}).to_parquet(output_dir / f"{table}.parquet", index=False)


def _run_one_session(raw: Path, output_dir: Path, **kwargs):
    with (
        patch("aind_behavior_vr_foraging.data_contract.dataset", side_effect=_mock_dataset),
        patch(
            "aind_behavior_vr_foraging_packaging.pipeline.session.create_processors",
            side_effect=lambda ds, **kw: [_mock_proc("session"), _mock_proc("sites")],
        ),
    ):
        return process_sessions([raw], output_dir, **kwargs)


def test_clean_removes_the_previous_runs_outputs(tmp_path):
    raw = tmp_path / "raw" / "sess_new"
    raw.mkdir(parents=True)
    out = tmp_path / "out"
    _prior_run(out)

    _run_one_session(raw, out, clean=True)

    assert not (out / "sessions" / "sess_old").exists(), "stale session survived"
    assert (out / "sessions" / "sess_new").exists()
    # Stale aggregates are removed; Phase 2 rewrites them, and this run does not.
    for table in AGGREGATED_TABLES:
        assert not (out / f"{table}.parquet").exists(), table


def test_clean_leaves_files_it_did_not_write(tmp_path):
    """Regression: `clean` used to `rmtree(output_dir)`, which took anything else
    living there with it. `--log-file <output-dir>/run.log` is the obvious case
    (and what the docs suggest) — on Windows it failed outright, because the
    handler holds the file open; on POSIX the log silently vanished mid-run."""
    raw = tmp_path / "raw" / "sess_new"
    raw.mkdir(parents=True)
    out = tmp_path / "out"
    out.mkdir()
    log = out / "run.log"
    log.write_text("previous line\n", encoding="utf-8")
    (out / "notes.txt").write_text("mine", encoding="utf-8")

    with log.open("a", encoding="utf-8") as fh:  # held open, as a FileHandler would
        _run_one_session(raw, out, clean=True)
        fh.write("during run\n")

    assert log.read_text(encoding="utf-8") == "previous line\nduring run\n"
    assert (out / "notes.txt").read_text(encoding="utf-8") == "mine"


def test_no_clean_keeps_the_previous_runs_outputs(tmp_path):
    raw = tmp_path / "raw" / "sess_new"
    raw.mkdir(parents=True)
    out = tmp_path / "out"
    _prior_run(out)

    _run_one_session(raw, out, clean=False)

    assert (out / "sessions" / "sess_old").exists()
    assert (out / "sessions" / "sess_new").exists()
