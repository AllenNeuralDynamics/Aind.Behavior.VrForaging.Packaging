"""Unit tests for export_pipeline.py — no real dataset I/O required."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging.export_pipeline import (
    DEFAULT_AGGREGATOR,
    AggregationRule,
    Aggregator,
    aggregate,
    process_sessions,
)

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


def _mock_dataset() -> MagicMock:
    ds = MagicMock()
    ds.version = "0.6.1"
    return ds


class TestAnyProcessorFailureFailsTheSession:
    def test_session_dropped_and_directory_removed_when_a_processor_fails(self, tmp_path):
        raw = tmp_path / "raw" / "sess_A"
        raw.mkdir(parents=True)
        procs = [_mock_proc("session"), _mock_proc("sites"), _mock_proc("licks", raises=True)]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", return_value=_mock_dataset()),
            patch("aind_behavior_vr_foraging_packaging.export_pipeline.create_processors", return_value=procs),
        ):
            written = process_sessions([raw], tmp_path / "out", raise_on_error=False)

        assert written == []
        assert not (tmp_path / "out" / "sessions" / "sess_A").exists()

    def test_session_kept_when_every_processor_succeeds(self, tmp_path):
        raw = tmp_path / "raw" / "sess_A"
        raw.mkdir(parents=True)
        procs = [_mock_proc("session"), _mock_proc("sites"), _mock_proc("licks")]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", return_value=_mock_dataset()),
            patch("aind_behavior_vr_foraging_packaging.export_pipeline.create_processors", return_value=procs),
        ):
            written = process_sessions([raw], tmp_path / "out", raise_on_error=False)

        assert written == [tmp_path / "out" / "sessions" / "sess_A"]
        assert (tmp_path / "out" / "sessions" / "sess_A" / "sites.parquet").exists()

    def test_raise_on_error_true_propagates_immediately(self, tmp_path):
        raw = tmp_path / "raw" / "sess_A"
        raw.mkdir(parents=True)
        procs = [_mock_proc("session"), _mock_proc("sites", raises=True)]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", return_value=_mock_dataset()),
            patch("aind_behavior_vr_foraging_packaging.export_pipeline.create_processors", return_value=procs),
            pytest.raises(ValueError, match="blew up"),
        ):
            process_sessions([raw], tmp_path / "out", raise_on_error=True)

    def test_processor_construction_failure_is_isolated_from_other_sessions(self, tmp_path):
        """A bad rig config crashing create_processors() for one session must not
        abort the rest of a multi-session batch."""
        good = tmp_path / "raw" / "sess_good"
        bad = tmp_path / "raw" / "sess_bad"
        good.mkdir(parents=True)
        bad.mkdir(parents=True)

        def _load_dataset(path):
            ds = _mock_dataset()
            ds.session_root = path
            return ds

        def _create_processors(ds, **kwargs):
            if ds.session_root == bad:
                raise RuntimeError("malformed rig config")
            return [_mock_proc("session"), _mock_proc("sites")]

        with (
            patch("aind_behavior_vr_foraging.data_contract.dataset", side_effect=_load_dataset),
            patch(
                "aind_behavior_vr_foraging_packaging.export_pipeline.create_processors",
                side_effect=_create_processors,
            ),
        ):
            written = process_sessions([bad, good], tmp_path / "out", raise_on_error=False)

        assert written == [tmp_path / "out" / "sessions" / "sess_good"]
        assert not (tmp_path / "out" / "sessions" / "sess_bad").exists()


# ---------------------------------------------------------------------------
# AggregationRule / Aggregator
# ---------------------------------------------------------------------------


def test_aggregation_rule_fields():
    rule = AggregationRule(table="licks")
    assert rule.table == "licks"


def test_aggregator_default():
    tables = {r.table for r in DEFAULT_AGGREGATOR.rules}
    assert "sites" in tables
    assert "licks" not in tables


# ---------------------------------------------------------------------------
# aggregate() helpers
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


# ---------------------------------------------------------------------------
# aggregate() tests
# ---------------------------------------------------------------------------


def test_aggregate_session_parquet(tmp_path):
    """session.parquet is always created from per-session session.parquet files."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub1")

    aggregate(sessions_dir, tmp_path, DEFAULT_AGGREGATOR)

    sessions = pd.read_parquet(tmp_path / "session.parquet")
    assert len(sessions) == 2
    assert set(sessions.columns) >= {"session_id", "subject_id", "date"}


def test_aggregate_flat_table(tmp_path):
    """Tables are written as a single flat parquet file at output_dir/{table}.parquet."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub2")

    agg = Aggregator(rules=[AggregationRule("sites")])
    aggregate(sessions_dir, tmp_path, agg)

    assert (tmp_path / "sites.parquet").exists(), "sites.parquet should exist"
    df = pd.read_parquet(tmp_path / "sites.parquet")
    assert len(df) == 6  # 3 rows × 2 sessions
    assert "session_id" in df.columns
    assert set(df["session_id"].unique()) == {"sess_A", "sess_B"}


def test_aggregate_missing_table_is_skipped(tmp_path):
    """A session that lacks a table file does not crash aggregation."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    # sess_B intentionally has no licks.parquet
    d = sessions_dir / "sess_B"
    d.mkdir(parents=True)
    pd.DataFrame([{"session_id": "sess_B", "subject_id": "sub1", "date": "2025-01-02"}]).to_parquet(
        d / "session.parquet", index=False
    )

    agg = Aggregator(rules=[AggregationRule("licks")])
    aggregate(sessions_dir, tmp_path, agg)  # must not raise

    df = pd.read_parquet(tmp_path / "licks.parquet")
    assert len(df) == 5  # only from sess_A
    assert set(df["session_id"].unique()) == {"sess_A"}


def test_aggregate_empty_sessions_dir(tmp_path):
    """aggregate() on an empty sessions/ dir logs a warning and returns cleanly."""
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    agg = Aggregator(rules=[AggregationRule("sites")])
    aggregate(sessions_dir, tmp_path, agg)  # must not raise
    assert not (tmp_path / "session.parquet").exists()


def test_aggregate_session_id_column_present(tmp_path):
    """session_id is injected into every aggregated flat file."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    agg = Aggregator(rules=[AggregationRule("sites")])
    aggregate(sessions_dir, tmp_path, agg)

    df = pd.read_parquet(tmp_path / "sites.parquet")
    assert "session_id" in df.columns


def test_aggregate_rerun_is_idempotent(tmp_path):
    """Running aggregate() twice with cleanup=False overwrites the flat file, same row count."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    # cleanup=False keeps per-session files so a second run can read them
    agg = Aggregator(rules=[AggregationRule("sites", cleanup=False)])
    aggregate(sessions_dir, tmp_path, agg)
    aggregate(sessions_dir, tmp_path, agg)  # second run — flat file is overwritten

    df = pd.read_parquet(tmp_path / "sites.parquet")
    assert len(df) == 3  # 3 rows, not 6


def test_aggregate_cleanup_removes_per_session_files(tmp_path):
    """cleanup=True deletes per-session parquet files after writing the flat file."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    agg = Aggregator(rules=[AggregationRule("sites", cleanup=True)])
    aggregate(sessions_dir, tmp_path, agg)

    assert (tmp_path / "sites.parquet").exists()
    assert not (sessions_dir / "sess_A" / "sites.parquet").exists()


def test_aggregate_no_cleanup_keeps_per_session_files(tmp_path):
    """cleanup=False leaves per-session parquet files in place."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    agg = Aggregator(rules=[AggregationRule("sites", cleanup=False)])
    aggregate(sessions_dir, tmp_path, agg)

    assert (tmp_path / "sites.parquet").exists()
    assert (sessions_dir / "sess_A" / "sites.parquet").exists()
