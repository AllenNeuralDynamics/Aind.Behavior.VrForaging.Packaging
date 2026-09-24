"""Regression test for the compact schema-transition corpus."""

from pathlib import Path

from aind_behavior_vr_foraging_packaging.schema_migrations import run_parquet_harness

TRANSITION_ASSET = Path(__file__).parent / "assets" / "session_schema_transitions.parquet"


def test_schema_transition_asset():
    """Every curated migration example must produce three current models."""
    result = run_parquet_harness(TRANSITION_ASSET, retain_rows=False)

    result.raise_for_failures()
    assert result.passed
    assert result.total_rows == 11
