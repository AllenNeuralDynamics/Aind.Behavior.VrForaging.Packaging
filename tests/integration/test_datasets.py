"""Parametrized integration tests — one test per entry in datasets.yml.

Run with::

    uv run pytest -m integration

The suite is gated behind the ``integration`` marker so the default
``uv run pytest`` invocation is unaffected.
"""

import pytest

from .conftest import CACHE_ROOT, _manifest, download_dataset

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Parametrize over manifest entries
# ---------------------------------------------------------------------------

_entries = _manifest.datasets
_ids = [e.id for e in _entries]


# ---------------------------------------------------------------------------
# Invariant assertion helper
# ---------------------------------------------------------------------------


def _assert_invariants(session, nwb, entry) -> None:  # type: ignore[no-untyped-def]
    """Dispatch per-field invariant checks from *entry.expected*."""
    from aind_behavior_vr_foraging_nwb.processing import TrialTableProcessor

    inv = entry.expected

    # We need the trial table for n_trials, n_blocks, and n_patches.
    # process_to_sites() returns the full list without modifying the NWB file.
    processor = TrialTableProcessor(session.dataset)

    if inv.n_trials is not None or inv.n_blocks is not None or inv.n_patches is not None:
        sites = processor.process_to_sites()

        if inv.n_trials is not None:
            actual = len(sites)
            assert actual == inv.n_trials, (
                f"{entry.id}: expected n_trials={inv.n_trials}, got {actual}\nRationale: {entry.rationale}"
            )

        if inv.n_blocks is not None:
            actual = len({s.block_index for s in sites})
            assert actual == inv.n_blocks, (
                f"{entry.id}: expected n_blocks={inv.n_blocks}, got {actual}\nRationale: {entry.rationale}"
            )

        if inv.n_patches is not None:
            actual = len({s.patch_index for s in sites})
            assert actual == inv.n_patches, (
                f"{entry.id}: expected n_patches={inv.n_patches}, got {actual}\nRationale: {entry.rationale}"
            )

    if inv.nwb_validates:
        import pynwb

        errors = pynwb.validate(nwb)
        assert not errors, f"{entry.id}: pynwb validation failed.\nErrors: {errors}\nRationale: {entry.rationale}"


# ---------------------------------------------------------------------------
# Test function
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entry", _entries, ids=_ids)
def test_dataset(entry, s3_client, request) -> None:  # type: ignore[no-untyped-def]
    """Smoke-test the parser against a real dataset and check declared invariants."""
    if entry.xfail:
        request.applymarker(
            pytest.mark.xfail(
                strict=True,
                reason=entry.xfail_reason or "marked xfail in manifest",
            )
        )

    try:
        local_path = download_dataset(s3_client, entry, CACHE_ROOT)
        from aind_behavior_vr_foraging_nwb.nwb_file import NwbSession

        session = NwbSession(local_path)
        nwb = session.process()
    except Exception as e:
        pytest.fail(f"Dataset {entry.id} failed parser smoke test.\nRationale: {entry.rationale}\nError: {e}")

    if entry.expected is not None:
        _assert_invariants(session, nwb, entry)
