"""Unit tests for export_pipeline.py — no real dataset I/O required."""

from pathlib import Path

import pandas as pd

from aind_behavior_vr_foraging_packaging.export_pipeline import (
    DEFAULT_AGGREGATOR,
    AggregationLevel,
    AggregationRule,
    Aggregator,
    aggregate,
)

# ---------------------------------------------------------------------------
# AggregationRule / Aggregator
# ---------------------------------------------------------------------------


def test_aggregation_rule_fields():
    rule = AggregationRule(table="licks", level=AggregationLevel.SUBJECT)
    assert rule.table == "licks"
    assert rule.level == AggregationLevel.SUBJECT


def test_aggregator_default():
    tables = {r.table for r in DEFAULT_AGGREGATOR.rules}
    assert "trials" in tables
    assert "licks" not in tables  # subject-level rules are opt-in, not in the default


def test_aggregation_level_values():
    assert AggregationLevel.SUBJECT == "subject"
    assert AggregationLevel.DATASET == "dataset"


# ---------------------------------------------------------------------------
# aggregate() helpers
# ---------------------------------------------------------------------------


def _write_fake_session(sessions_dir: Path, session_id: str, subject_id: str) -> None:
    """Write minimal parquet files for a fake session."""
    d = sessions_dir / session_id
    d.mkdir(parents=True)

    pd.DataFrame([{"session_id": session_id, "subject_id": subject_id, "date": "2025-01-01"}]).to_parquet(
        d / "session_metadata.parquet", index=False
    )

    pd.DataFrame({"trial": [1, 2, 3]}).to_parquet(d / "trials.parquet", index=False)
    pd.DataFrame({"t": range(5)}).to_parquet(d / "licks.parquet", index=False)


# ---------------------------------------------------------------------------
# aggregate() tests
# ---------------------------------------------------------------------------


def test_aggregate_sessions_parquet(tmp_path):
    """sessions.parquet is always created from session_metadata files."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub1")

    aggregate(sessions_dir, tmp_path, DEFAULT_AGGREGATOR)

    sessions = pd.read_parquet(tmp_path / "sessions.parquet")
    assert len(sessions) == 2
    assert set(sessions.columns) >= {"session_id", "subject_id", "date"}


def test_aggregate_dataset_level(tmp_path):
    """DATASET-level tables concatenate to output_dir/{table}.parquet with session_id column."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub2")

    agg = Aggregator(rules=[AggregationRule("trials", AggregationLevel.DATASET)])
    aggregate(sessions_dir, tmp_path, agg)

    df = pd.read_parquet(tmp_path / "trials.parquet")
    assert len(df) == 6  # 3 rows × 2 sessions
    assert "session_id" in df.columns
    assert set(df["session_id"].unique()) == {"sess_A", "sess_B"}


def test_aggregate_subject_level(tmp_path):
    """SUBJECT-level tables go to output_dir/subjects/{subject_id}/{table}.parquet."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub1")
    _write_fake_session(sessions_dir, "sess_C", "sub2")

    agg = Aggregator(rules=[AggregationRule("licks", AggregationLevel.SUBJECT)])
    aggregate(sessions_dir, tmp_path, agg)

    sub1 = pd.read_parquet(tmp_path / "subjects" / "sub1" / "licks.parquet")
    assert len(sub1) == 10  # 5 rows × 2 sessions
    assert "session_id" in sub1.columns

    sub2 = pd.read_parquet(tmp_path / "subjects" / "sub2" / "licks.parquet")
    assert len(sub2) == 5


def test_aggregate_missing_table_is_skipped(tmp_path):
    """A session that lacks a table file does not crash aggregation."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    # sess_B intentionally has no licks.parquet
    d = sessions_dir / "sess_B"
    d.mkdir(parents=True)
    pd.DataFrame([{"session_id": "sess_B", "subject_id": "sub1", "date": "2025-01-02"}]).to_parquet(
        d / "session_metadata.parquet", index=False
    )

    agg = Aggregator(rules=[AggregationRule("licks", AggregationLevel.SUBJECT)])
    aggregate(sessions_dir, tmp_path, agg)  # must not raise

    sub1 = pd.read_parquet(tmp_path / "subjects" / "sub1" / "licks.parquet")
    assert len(sub1) == 5  # only from sess_A


def test_aggregate_empty_sessions_dir(tmp_path):
    """aggregate() on an empty sessions/ dir logs a warning and returns cleanly."""
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    agg = Aggregator(rules=[AggregationRule("trials", AggregationLevel.DATASET)])
    aggregate(sessions_dir, tmp_path, agg)  # must not raise
    assert not (tmp_path / "sessions.parquet").exists()


def test_aggregate_session_id_column_prepended(tmp_path):
    """session_id is the first column in every aggregated frame."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")

    agg = Aggregator(rules=[AggregationRule("trials", AggregationLevel.DATASET)])
    aggregate(sessions_dir, tmp_path, agg)

    df = pd.read_parquet(tmp_path / "trials.parquet")
    assert df.columns[0] == "session_id"
