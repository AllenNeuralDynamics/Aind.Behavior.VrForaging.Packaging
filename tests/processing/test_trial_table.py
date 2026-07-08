"""Tests for the trial table index precomputation logic.

These tests verify that the vectorized groupby().cumcount() approach
correctly computes all site-, patch-, and block-level indices.
The tests replicate the same pandas operations used in
TrialTableProcessor.process_to_sites without requiring a real dataset.
"""

import pandas as pd
import pytest


def _build_merged(sites: pd.DataFrame, patches: pd.DataFrame, blocks: pd.DataFrame) -> pd.DataFrame:
    """Replicate the merge + index precomputation from process_to_sites.

    This mirrors the exact sequence of operations in TrialTableProcessor.process_to_sites
    so that tests exercise the same code path.
    """
    patches = patches.copy()
    blocks = blocks.copy()

    patches["patch_count"] = range(len(patches))
    blocks["block_count"] = range(len(blocks))

    # Merge nearest patch (backward in time)
    merged = pd.merge_asof(
        sites.sort_index(),
        patches[["data", "patch_count"]].rename(columns={"data": "patch_data"}).sort_index(),
        left_index=True,
        right_index=True,
        direction="backward",
        suffixes=("", "_patch"),
    )

    # Merge nearest block (backward in time)
    merged = pd.merge_asof(
        merged.sort_index(),
        blocks[["block_count"]].sort_index(),
        left_index=True,
        right_index=True,
        direction="backward",
    )

    # --- Precompute all trial indices (same as production code) ---
    merged["site_label"] = merged["data"].apply(lambda d: d["label"])
    merged["patch_label"] = merged["patch_data"].apply(lambda d: d["label"])

    # Site-level indices
    merged["_site_index_in_patch"] = merged.groupby("patch_count").cumcount()
    merged["_site_index_in_block"] = merged.groupby("block_count").cumcount()
    merged["_site_index_by_type"] = merged.groupby("site_label").cumcount()
    merged["_site_index_in_patch_by_type"] = merged.groupby(["patch_count", "site_label"]).cumcount()
    merged["_site_index_in_block_by_type"] = merged.groupby(["block_count", "site_label"]).cumcount()

    # Patch-level indices (computed on patches, then mapped back to sites via patch_count)
    patches_with_blocks = pd.merge_asof(
        patches.sort_index(),
        blocks[["block_count"]].sort_index(),
        left_index=True,
        right_index=True,
        direction="backward",
    )
    patches_with_blocks["patch_label"] = patches_with_blocks["data"].apply(lambda d: d["label"])
    patches_with_blocks["_patch_index_in_block"] = patches_with_blocks.groupby("block_count").cumcount()
    patches_with_blocks["_patch_index_by_type"] = patches_with_blocks.groupby("patch_label").cumcount()
    patches_with_blocks["_patch_index_in_block_by_type"] = patches_with_blocks.groupby(
        ["block_count", "patch_label"]
    ).cumcount()
    merged = merged.join(
        patches_with_blocks.set_index("patch_count")[
            ["_patch_index_in_block", "_patch_index_by_type", "_patch_index_in_block_by_type"]
        ],
        on="patch_count",
    )

    return merged


def _make_site_data(label: str) -> dict:
    return {"label": label, "start_position": 0, "length": 10, "odor_specification": None}


def _make_patch_data(label: str) -> dict:
    return {"label": label, "odor_specification": None}


def _make_block_data() -> dict:
    return {}


class TestMergeAssignment:
    """Tests that merge_asof correctly assigns patches and blocks to sites."""

    @pytest.fixture
    def session(self):
        """A session with 2 blocks, 4 patches, and 9 sites.

        Block 0 (t=0.0):
          Patch 0, label=A (t=1.0):   Site R(t=2), Site I(t=3), Site R(t=4)
          Patch 1, label=B (t=5.0):   Site R(t=6), Site R(t=7)
        Block 1 (t=8.0):
          Patch 2, label=A (t=9.0):   Site R(t=10), Site I(t=11)
          Patch 3, label=A (t=12.0):  Site R(t=13), Site R(t=14)
        """
        blocks = pd.DataFrame(
            {"data": [_make_block_data(), _make_block_data()]},
            index=[0.0, 8.0],
        )
        patches = pd.DataFrame(
            {"data": [_make_patch_data("A"), _make_patch_data("B"), _make_patch_data("A"), _make_patch_data("A")]},
            index=[1.0, 5.0, 9.0, 12.0],
        )
        sites = pd.DataFrame(
            {
                "data": [
                    _make_site_data("Reward"),
                    _make_site_data("Inter"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Inter"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                ]
            },
            index=[2.0, 3.0, 4.0, 6.0, 7.0, 10.0, 11.0, 13.0, 14.0],
        )
        return _build_merged(sites, patches, blocks)

    def test_patch_count_assignment(self, session):
        """Each site is assigned to the most recent patch via backward merge."""
        assert list(session["patch_count"]) == [0, 0, 0, 1, 1, 2, 2, 3, 3]

    def test_block_count_assignment(self, session):
        """Each site is assigned to the most recent block via backward merge."""
        assert list(session["block_count"]) == [0, 0, 0, 0, 0, 1, 1, 1, 1]

    def test_index_preserved(self, session):
        """The time-based index must survive all merge/join operations."""
        assert list(session.index) == [2.0, 3.0, 4.0, 6.0, 7.0, 10.0, 11.0, 13.0, 14.0]


class TestSiteLevelIndices:
    """Tests for site-level index columns computed via groupby().cumcount()."""

    @pytest.fixture
    def session(self):
        """Same layout as TestMergeAssignment."""
        blocks = pd.DataFrame(
            {"data": [_make_block_data(), _make_block_data()]},
            index=[0.0, 8.0],
        )
        patches = pd.DataFrame(
            {"data": [_make_patch_data("A"), _make_patch_data("B"), _make_patch_data("A"), _make_patch_data("A")]},
            index=[1.0, 5.0, 9.0, 12.0],
        )
        sites = pd.DataFrame(
            {
                "data": [
                    _make_site_data("Reward"),
                    _make_site_data("Inter"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Inter"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                ]
            },
            index=[2.0, 3.0, 4.0, 6.0, 7.0, 10.0, 11.0, 13.0, 14.0],
        )
        return _build_merged(sites, patches, blocks)

    def test_site_index_in_patch(self, session):
        """Sites numbered 0, 1, 2, ... within each patch."""
        #         p0--------  p1----  p2----  p3----
        expected = [0, 1, 2, 0, 1, 0, 1, 0, 1]
        assert list(session["_site_index_in_patch"]) == expected

    def test_site_index_in_block(self, session):
        """Sites numbered 0, 1, 2, ... within each block."""
        #         b0--------------  b1--------------
        expected = [0, 1, 2, 3, 4, 0, 1, 2, 3]
        assert list(session["_site_index_in_block"]) == expected

    def test_site_index_by_type(self, session):
        """Global site count, only among sites with the same label.

        R=Reward, I=Inter
        """
        #         R  I  R  R  R  R  I  R  R
        expected = [0, 0, 1, 2, 3, 4, 1, 5, 6]
        assert list(session["_site_index_by_type"]) == expected

    def test_site_index_in_patch_by_type(self, session):
        """Site count within patch, only among same label.

        Patch 0 has R,I,R. Patch 1 has R,R. Patch 2 has R,I. Patch 3 has R,R.
        """
        #         R  I  R  R  R  R  I  R  R
        expected = [0, 0, 1, 0, 1, 0, 0, 0, 1]
        assert list(session["_site_index_in_patch_by_type"]) == expected

    def test_site_index_in_block_by_type(self, session):
        """Site count within block, only among same label.

        Block 0 has R,I,R,R,R (3 R's before block change, then 2 more).
        Block 1 has R,I,R,R.
        """
        #         R  I  R  R  R  R  I  R  R
        expected = [0, 0, 1, 2, 3, 0, 0, 1, 2]
        assert list(session["_site_index_in_block_by_type"]) == expected


class TestPatchLevelIndices:
    """Tests for patch-level indices computed on patches_with_blocks then joined to sites."""

    @pytest.fixture
    def session(self):
        """Same layout as TestMergeAssignment."""
        blocks = pd.DataFrame(
            {"data": [_make_block_data(), _make_block_data()]},
            index=[0.0, 8.0],
        )
        patches = pd.DataFrame(
            {"data": [_make_patch_data("A"), _make_patch_data("B"), _make_patch_data("A"), _make_patch_data("A")]},
            index=[1.0, 5.0, 9.0, 12.0],
        )
        sites = pd.DataFrame(
            {
                "data": [
                    _make_site_data("Reward"),
                    _make_site_data("Inter"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Inter"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                ]
            },
            index=[2.0, 3.0, 4.0, 6.0, 7.0, 10.0, 11.0, 13.0, 14.0],
        )
        return _build_merged(sites, patches, blocks)

    def test_patch_index_in_block(self, session):
        """Patch position within its block, broadcast to all sites of that patch.

        Block 0: Patch 0 → 0, Patch 1 → 1
        Block 1: Patch 2 → 0, Patch 3 → 1
        """
        expected = [0, 0, 0, 1, 1, 0, 0, 1, 1]
        assert list(session["_patch_index_in_block"]) == expected

    def test_patch_index_by_type(self, session):
        """Global patch count, only among patches with the same label.

        Patch 0 (A) → 0, Patch 1 (B) → 0, Patch 2 (A) → 1, Patch 3 (A) → 2
        """
        expected = [0, 0, 0, 0, 0, 1, 1, 2, 2]
        assert list(session["_patch_index_by_type"]) == expected

    def test_patch_index_in_block_by_type(self, session):
        """Patch count within block, only among same label.

        Block 0: Patch 0 (A, 0th A), Patch 1 (B, 0th B)
        Block 1: Patch 2 (A, 0th A), Patch 3 (A, 1st A)
        """
        expected = [0, 0, 0, 0, 0, 0, 0, 1, 1]
        assert list(session["_patch_index_in_block_by_type"]) == expected


class TestSingleBlockSinglePatch:
    """Edge case: minimal session with one block, one patch, all same site type."""

    @pytest.fixture
    def session(self):
        """Single block, single patch, 3 Reward sites."""
        blocks = pd.DataFrame({"data": [_make_block_data()]}, index=[0.0])
        patches = pd.DataFrame({"data": [_make_patch_data("A")]}, index=[1.0])
        sites = pd.DataFrame(
            {"data": [_make_site_data("Reward"), _make_site_data("Reward"), _make_site_data("Reward")]},
            index=[2.0, 3.0, 4.0],
        )
        return _build_merged(sites, patches, blocks)

    def test_all_site_indices_sequential(self, session):
        """With one group everywhere, all indices should just be 0, 1, 2."""
        for col in [
            "_site_index_in_patch",
            "_site_index_in_block",
            "_site_index_by_type",
            "_site_index_in_patch_by_type",
            "_site_index_in_block_by_type",
        ]:
            assert list(session[col]) == [0, 1, 2], f"Failed for {col}"

    def test_all_patch_indices_zero(self, session):
        """With one patch, all patch-level indices should be 0."""
        for col in [
            "_patch_index_in_block",
            "_patch_index_by_type",
            "_patch_index_in_block_by_type",
        ]:
            assert list(session[col]) == [0, 0, 0], f"Failed for {col}"


class TestSimultaneousBlockAndPatchChange:
    """Test correct behavior when block and patch boundaries coincide.

    This is the scenario that was buggy in the original imperative code:
    when both block and patch change on the same site, the patch-by-type-in-block
    counter was incremented then immediately reset, producing index -1.
    """

    @pytest.fixture
    def session(self):
        """Two blocks, each starting with a new patch simultaneously.

        Block 0 (t=0.0) + Patch 0, label=A (t=0.0):  Site R(t=1), Site R(t=2)
        Block 1 (t=3.0) + Patch 1, label=A (t=3.0):  Site R(t=4), Site R(t=5)
        """
        blocks = pd.DataFrame({"data": [_make_block_data(), _make_block_data()]}, index=[0.0, 3.0])
        patches = pd.DataFrame({"data": [_make_patch_data("A"), _make_patch_data("A")]}, index=[0.0, 3.0])
        sites = pd.DataFrame(
            {
                "data": [
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                ]
            },
            index=[1.0, 2.0, 4.0, 5.0],
        )
        return _build_merged(sites, patches, blocks)

    def test_patch_index_in_block(self, session):
        """First patch in each block should be 0."""
        expected = [0, 0, 0, 0]
        assert list(session["_patch_index_in_block"]) == expected

    def test_patch_index_in_block_by_type(self, session):
        """First A-patch in each block should be 0 (NOT -1 as the old bug produced)."""
        expected = [0, 0, 0, 0]
        assert list(session["_patch_index_in_block_by_type"]) == expected

    def test_patch_index_by_type(self, session):
        """Global A-patch count: Patch 0 → 0, Patch 1 → 1."""
        expected = [0, 0, 1, 1]
        assert list(session["_patch_index_by_type"]) == expected

    def test_site_index_in_block(self, session):
        """Sites reset to 0 at each block boundary."""
        expected = [0, 1, 0, 1]
        assert list(session["_site_index_in_block"]) == expected

    def test_site_index_in_patch(self, session):
        """Sites reset to 0 at each patch boundary."""
        expected = [0, 1, 0, 1]
        assert list(session["_site_index_in_patch"]) == expected

    def test_site_index_in_block_by_type(self, session):
        """Reward sites reset to 0 at each block boundary."""
        expected = [0, 1, 0, 1]
        assert list(session["_site_index_in_block_by_type"]) == expected


class TestManyPatchTypesInBlock:
    """Test with alternating patch types within a single block."""

    @pytest.fixture
    def session(self):
        """Single block with patches A, B, A, B.

        Block 0 (t=0.0):
          Patch 0, label=A (t=1.0):  Site R(t=2)
          Patch 1, label=B (t=3.0):  Site R(t=4)
          Patch 2, label=A (t=5.0):  Site R(t=6)
          Patch 3, label=B (t=7.0):  Site R(t=8)
        """
        blocks = pd.DataFrame({"data": [_make_block_data()]}, index=[0.0])
        patches = pd.DataFrame(
            {"data": [_make_patch_data("A"), _make_patch_data("B"), _make_patch_data("A"), _make_patch_data("B")]},
            index=[1.0, 3.0, 5.0, 7.0],
        )
        sites = pd.DataFrame(
            {
                "data": [
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                    _make_site_data("Reward"),
                ]
            },
            index=[2.0, 4.0, 6.0, 8.0],
        )
        return _build_merged(sites, patches, blocks)

    def test_patch_index_in_block(self, session):
        """Sequential patch position within the block."""
        expected = [0, 1, 2, 3]
        assert list(session["_patch_index_in_block"]) == expected

    def test_patch_index_by_type(self, session):
        """Global count by label: A→0,1  B→0,1."""
        expected = [0, 0, 1, 1]
        assert list(session["_patch_index_by_type"]) == expected

    def test_patch_index_in_block_by_type(self, session):
        """Count by label within block: A→0,1  B→0,1."""
        expected = [0, 0, 1, 1]
        assert list(session["_patch_index_in_block_by_type"]) == expected
