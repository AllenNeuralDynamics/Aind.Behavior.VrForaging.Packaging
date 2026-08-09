"""Integration tests for the two-phase experiment export pipeline.

Runs against all locally cached sessions from ``datasets.yml``.
Requires the ``integration`` marker::

    uv run pytest -m integration tests/integration/test_experiment_export.py -v -s
"""

from pathlib import Path

import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging.export_pipeline import (
    DEFAULT_AGGREGATOR,
    aggregate,
    process_sessions,
)

pytestmark = pytest.mark.integration


def test_full_export_pipeline(all_cached_session_paths: list[Path], tmp_path: Path) -> None:
    """Full two-phase pipeline: process → aggregate → assert output structure."""
    if not all_cached_session_paths:
        pytest.skip("No cached session data available")

    output_dir = tmp_path / "export"
    sessions_dir = output_dir / "sessions"

    # ------------------------------------------------------------------ #
    # Phase 1
    # ------------------------------------------------------------------ #
    written = process_sessions(all_cached_session_paths, output_dir, raise_on_error=False)

    assert len(written) == len(all_cached_session_paths), (
        f"Expected {len(all_cached_session_paths)} session dirs, got {len(written)}"
    )

    for session_path in written:
        assert (session_path / "trials.parquet").exists(), f"Missing trials.parquet in {session_path.name}"
        assert (session_path / "session_metadata.parquet").exists(), (
            f"Missing session_metadata.parquet in {session_path.name}"
        )

    # ------------------------------------------------------------------ #
    # Phase 2
    # ------------------------------------------------------------------ #
    aggregate(sessions_dir, output_dir, DEFAULT_AGGREGATOR)

    # sessions.parquet: one row per session, required columns present
    assert (output_dir / "sessions.parquet").exists()
    sessions = pd.read_parquet(output_dir / "sessions.parquet")
    assert len(sessions) == len(written), f"sessions.parquet has {len(sessions)} rows; expected {len(written)}"
    assert {"session_id", "subject_id", "date"}.issubset(set(sessions.columns)), (
        f"Missing columns in sessions.parquet: {set(sessions.columns)}"
    )

    # dataset-level: trials.parquet exists and has session_id traceability column
    assert (output_dir / "trials.parquet").exists()
    all_trials = pd.read_parquet(output_dir / "trials.parquet")
    assert "session_id" in all_trials.columns
    assert not all_trials.empty

    # row-count consistency: concatenated rows == sum of per-session rows
    per_session_total = sum(
        len(pd.read_parquet(sessions_dir / sid / "trials.parquet"))
        for sid in sessions["session_id"]
        if (sessions_dir / sid / "trials.parquet").exists()
    )
    assert len(all_trials) == per_session_total, (
        f"trials.parquet row count mismatch: {len(all_trials)} vs per-session sum {per_session_total}"
    )

    # subject-level: at least one subjects/ subfolder with licks.parquet
    subjects_dir = output_dir / "subjects"
    assert subjects_dir.exists()
    subject_dirs = [d for d in subjects_dir.iterdir() if d.is_dir()]
    assert len(subject_dirs) > 0, "No subject directories created under subjects/"
    for sub_dir in subject_dirs:
        assert (sub_dir / "licks.parquet").exists(), f"Missing licks.parquet for subject {sub_dir.name}"


def test_skip_aggregation_writes_only_sessions(all_cached_session_paths: list[Path], tmp_path: Path) -> None:
    """Phase 1 only: sessions/ is written; no top-level parquets exist."""
    if not all_cached_session_paths:
        pytest.skip("No cached session data available")

    output_dir = tmp_path / "export"
    process_sessions(all_cached_session_paths, output_dir, raise_on_error=False)

    assert (output_dir / "sessions").exists()
    assert not (output_dir / "sessions.parquet").exists(), (
        "sessions.parquet should not exist when aggregation is skipped"
    )
    assert not (output_dir / "trials.parquet").exists(), "trials.parquet should not exist when aggregation is skipped"


def test_exclude_processor(all_cached_session_paths: list[Path], tmp_path: Path) -> None:
    """Excluding 'sniffing' means no sniffing.parquet in any session directory."""
    if not all_cached_session_paths:
        pytest.skip("No cached session data available")

    output_dir = tmp_path / "export"
    process_sessions(
        all_cached_session_paths,
        output_dir,
        exclude_processors=["sniffing"],
        raise_on_error=False,
    )

    sessions_dir = output_dir / "sessions"
    for session_path in sessions_dir.iterdir():
        if not session_path.is_dir():
            continue
        assert not (session_path / "sniffing.parquet").exists(), (
            f"sniffing.parquet should not exist in {session_path.name}"
        )


def test_rerun_aggregation_only(all_cached_session_paths: list[Path], tmp_path: Path) -> None:
    """Phase 2 can be re-run independently after Phase 1 has written sessions/."""
    if not all_cached_session_paths:
        pytest.skip("No cached session data available")

    output_dir = tmp_path / "export"
    sessions_dir = output_dir / "sessions"

    # Phase 1
    process_sessions(all_cached_session_paths, output_dir, raise_on_error=False)

    # Phase 2 — first run
    aggregate(sessions_dir, output_dir, DEFAULT_AGGREGATOR)
    first_mtime = (output_dir / "sessions.parquet").stat().st_mtime

    # Phase 2 — second run (re-aggregate without re-processing)
    aggregate(sessions_dir, output_dir, DEFAULT_AGGREGATOR)
    second_mtime = (output_dir / "sessions.parquet").stat().st_mtime

    # File was overwritten on the second run
    assert second_mtime >= first_mtime

    sessions = pd.read_parquet(output_dir / "sessions.parquet")
    assert len(sessions) == len(all_cached_session_paths)
