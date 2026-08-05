"""Parametrized integration tests — one test per entry in datasets.yml.

Run with::

    uv run pytest -m integration

The suite is gated behind the ``integration`` marker so the default
``uv run pytest`` invocation is unaffected.
"""

from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging.session_pipeline import create_processors, get_site_table_processor, run_session

from .conftest import CACHE_ROOT, _manifest
from .model import DatasetEntry

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Parametrize over manifest entries
# ---------------------------------------------------------------------------

_entries = _manifest.datasets
_ids = [e.id for e in _entries]

# ---------------------------------------------------------------------------
# Dataset loading helper
# ---------------------------------------------------------------------------


def _session_path(entry: DatasetEntry) -> Path:
    """Local cache path of the session root for *entry*."""
    parsed = urlparse(entry.uri)
    return CACHE_ROOT / parsed.netloc / parsed.path.strip("/")


def _load_dataset(entry: DatasetEntry):
    """Load the cached dataset for *entry*, letting the loader infer its version."""
    from aind_behavior_vr_foraging.data_contract import dataset

    return dataset(_session_path(entry))


# ---------------------------------------------------------------------------
# Invariant assertion helper
# ---------------------------------------------------------------------------


def _assert_sites_table_invariants(sites_df: pd.DataFrame, entry: DatasetEntry) -> None:
    """Check invariants for the site table."""
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
def test_sites_table(entry, request):
    """Test the site table processing logic using already downloaded datasets."""
    if entry.xfail:
        request.applymarker(
            pytest.mark.xfail(
                strict=True,
                reason=entry.xfail_reason or "marked xfail in manifest",
            )
        )

    try:
        ds = _load_dataset(entry)
        processor = get_site_table_processor(ds, raise_on_error=entry.raise_on_error)
        sites = processor.process_to_sites()
        sites_df = pd.DataFrame([s.model_dump() for s in sites])

        if entry.expected is not None:
            _assert_sites_table_invariants(sites_df, entry)

        assert not sites_df.empty, f"{entry.id}: site table is unexpectedly empty"

    except Exception as e:
        pytest.fail(f"Dataset {entry.id} failed site table test.\nRationale: {entry.rationale}\nError: {e}")


@pytest.mark.parametrize("entry", _entries, ids=_ids)
def test_full_pipeline(entry, request, tmp_path):
    """Smoke-test the full parquet pipeline: all 6 processors must run without crashing.

    Exercises ``run_session`` end-to-end. Parquet files are written to
    ``tmp_path`` (auto-cleaned by pytest). Only the ``sites`` output is
    asserted non-empty; other streams may legitimately be empty for some
    sessions.

    Uses ``run_session``'s default ``raise_on_error=False``: the pipeline runs
    every processor, and some sessions legitimately lack optional SoftwareEvents
    streams (e.g. ForceGiveReward, PatchRewardAmount). An absent optional stream
    should not fail this smoke test.
    """
    if entry.xfail:
        request.applymarker(
            pytest.mark.xfail(
                strict=True,
                reason=entry.xfail_reason or "marked xfail in manifest",
            )
        )

    try:
        ds = _load_dataset(entry)
        outputs = run_session(ds, tmp_path)
        assert not outputs["sites"].empty, f"{entry.id}: sites table is unexpectedly empty"

    except Exception as e:
        pytest.fail(f"Dataset {entry.id} failed full pipeline test.\nRationale: {entry.rationale}\nError: {e}")


@pytest.mark.parametrize("entry", _entries, ids=_ids)
def test_nwb_session(entry, request, tmp_path):
    """Smoke-test the NWB path end-to-end: build, nwbize with all processors, write zarr, read back.

    Complements ``test_full_pipeline`` (which only covers the parquet path). The
    base file comes from ``aind_nwb_utils.create_base_nwb_file``, so this is what
    exercises the real ``data_description``/``subject``/``acquisition`` jsons in
    each cached session — synthetic unit-test fixtures cannot catch an upstream
    metadata key rename.

    Set ``expected.nwb_validates: true`` on a manifest entry to additionally
    require the written file to pass ``pynwb.validate``.
    """
    if entry.xfail:
        request.applymarker(
            pytest.mark.xfail(
                strict=True,
                reason=entry.xfail_reason or "marked xfail in manifest",
            )
        )

    from hdmf_zarr import NWBZarrIO

    from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession

    try:
        ds = _load_dataset(entry)
        session = NwbSession(_session_path(entry), dataset=ds)

        nwb = session.run(*create_processors(ds))

        # Identity fields derived from the session's own metadata jsons.
        assert nwb.session_id, f"{entry.id}: session_id is empty"
        assert nwb.identifier, f"{entry.id}: identifier is empty"
        assert nwb.session_start_time is not None, f"{entry.id}: session_start_time is missing"
        assert nwb.session_start_time.tzinfo is not None, f"{entry.id}: session_start_time is not timezone-aware"
        assert nwb.subject is not None, f"{entry.id}: subject was not populated"
        assert nwb.subject.subject_id, f"{entry.id}: subject_id is empty"

        # At minimum the trial table must have landed as a trials table.
        assert nwb.trials is not None, f"{entry.id}: no trials table on the NWB file"
        assert len(nwb.trials) > 0, f"{entry.id}: trials table is unexpectedly empty"

        out = tmp_path / "session.nwb.zarr"
        session.write_nwb_zarr(out)
        assert out.exists(), f"{entry.id}: {out.name} was not written"

        with NWBZarrIO(out.as_posix(), "r") as io:
            round_tripped = io.read()
            assert round_tripped.session_id == nwb.session_id
            assert len(round_tripped.trials) == len(nwb.trials)

            if entry.expected is not None and entry.expected.nwb_validates:
                import pynwb

                errors = pynwb.validate(io=io)
                assert not errors, f"{entry.id}: pynwb validation reported {len(errors)} error(s):\n" + "\n".join(
                    str(err) for err in errors
                )

    except Exception as e:
        pytest.fail(f"Dataset {entry.id} failed NWB session test.\nRationale: {entry.rationale}\nError: {e}")
