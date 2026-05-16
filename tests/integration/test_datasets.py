"""Parametrized integration tests — one test per entry in datasets.yml.

Run with::

    uv run pytest -m integration

The suite is gated behind the ``integration`` marker so the default
``uv run pytest`` invocation is unaffected.
"""

from urllib.parse import urlparse

import pandas as pd
import pytest

from aind_behavior_vr_foraging_nwb.processing import TrialTableProcessor

from .conftest import CACHE_ROOT, _manifest
from .model import DatasetEntry

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Parametrize over manifest entries
# ---------------------------------------------------------------------------

_entries = _manifest.datasets
_ids = [e.id for e in _entries]

# ---------------------------------------------------------------------------
# Invariant assertion helper
# ---------------------------------------------------------------------------


def _assert_trials_table_invariants(sites_df: pd.DataFrame, entry: DatasetEntry) -> None:
    """Check invariants for the trial table."""
    inv = entry.expected
    if inv is None:
        return

    if inv.n_sites is not None:
        actual = len(sites_df)
        assert actual == inv.n_sites, (
            f"{entry.id}: expected n_sites={inv.n_sites}, got {actual}\nRationale: {entry.rationale}"
        )

    if inv.n_choices is not None:
        actual = int(sites_df["has_choice"].fillna(False).astype(bool).sum())
        assert actual == inv.n_choices, (
            f"{entry.id}: expected n_choices={inv.n_choices}, got {actual}\nRationale: {entry.rationale}"
        )

    if inv.n_rewards is not None:
        actual = int(sites_df["has_reward"].fillna(False).astype(bool).sum())
        assert actual == inv.n_rewards, (
            f"{entry.id}: expected n_rewards={inv.n_rewards}, got {actual}\nRationale: {entry.rationale}"
        )

    if inv.n_blocks is not None:
        actual = int(sites_df["block_index"].nunique(dropna=True))
        assert actual == inv.n_blocks, (
            f"{entry.id}: expected n_blocks={inv.n_blocks}, got {actual}\nRationale: {entry.rationale}"
        )

    if inv.n_patches is not None:
        actual = int(sites_df["patch_index"].nunique(dropna=True))
        assert actual == inv.n_patches, (
            f"{entry.id}: expected n_patches={inv.n_patches}, got {actual}\nRationale: {entry.rationale}"
        )


# ---------------------------------------------------------------------------
# Test function
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entry", _entries, ids=_ids)
def test_trials_table(entry, request):
    """Test the trial table processing logic using already downloaded datasets."""
    if entry.xfail:
        request.applymarker(
            pytest.mark.xfail(
                strict=True,
                reason=entry.xfail_reason or "marked xfail in manifest",
            )
        )

    try:
        from aind_behavior_vr_foraging.data_contract import dataset

        parsed = urlparse(entry.uri)
        session_path = CACHE_ROOT / parsed.netloc / parsed.path.strip("/")

        ds = dataset(session_path)
        processor = TrialTableProcessor(ds, raise_on_error=True)
        sites = processor.process_to_sites()
        sites_df = pd.DataFrame([s.model_dump() for s in sites])

        if entry.expected is not None:
            _assert_trials_table_invariants(sites_df, entry)

        assert not sites_df.empty, f"{entry.id}: trial table is unexpectedly empty"

    except Exception as e:
        pytest.fail(f"Dataset {entry.id} failed trial table test.\nRationale: {entry.rationale}\nError: {e}")
