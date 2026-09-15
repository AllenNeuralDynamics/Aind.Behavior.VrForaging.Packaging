"""Tests for LegacySiteTableProcessor.

Pure-logic tests only — no contraqctor dataset required.
Integration with a real legacy dataset is covered by tests/integration/datasets.yml.
"""

import pytest

from aind_behavior_vr_foraging_packaging._base import DatasetProcessorError
from aind_behavior_vr_foraging_packaging.processing._legacy_site_table import (
    _LEGACY_OLFACTOMETER_CHANNEL_COUNT,
    LegacySiteTableProcessor,
)


def _uninit_processor() -> LegacySiteTableProcessor:
    """Return a LegacySiteTableProcessor without calling __init__ (avoids needing a real dataset)."""
    return LegacySiteTableProcessor.__new__(LegacySiteTableProcessor)


class TestLegacyOdorConcentration:
    def test_none_returns_zeros(self):
        result = _uninit_processor()._process_odor_concentration(None, 3)
        assert result == [0.0, 0.0, 0.0]

    def test_single_channel_assigned_at_index_0(self):
        result = _uninit_processor()._process_odor_concentration({"index": 0, "concentration": 1.0}, 3)
        assert result == [1.0, 0.0, 0.0]

    def test_single_channel_assigned_at_index_1(self):
        result = _uninit_processor()._process_odor_concentration({"index": 1, "concentration": 0.75}, 3)
        assert result == [0.0, 0.75, 0.0]

    def test_missing_concentration_defaults_to_zero(self):
        result = _uninit_processor()._process_odor_concentration({"index": 2}, 3)
        assert result == [0.0, 0.0, 0.0]

    def test_invalid_index_type_raises_type_error(self):
        with pytest.raises(TypeError, match="index must be an int"):
            _uninit_processor()._process_odor_concentration({"index": "oops", "concentration": 0.5}, 3)

    def test_result_length_matches_n_channels(self):
        result = _uninit_processor()._process_odor_concentration({"index": 0, "concentration": 0.5}, 5)
        assert len(result) == 5


class TestLegacyChannelCount:
    def test_constant_is_three(self):
        assert _LEGACY_OLFACTOMETER_CHANNEL_COUNT == 3

    def test_method_returns_three(self):
        from unittest.mock import MagicMock

        assert _uninit_processor()._get_olfactometer_channel_count(MagicMock()) == 3


class TestLegacyIsStoppedAndVelocity:
    def test_parse_is_stopped_returns_none(self):
        from unittest.mock import MagicMock

        assert LegacySiteTableProcessor._parse_is_stopped(MagicMock()) is None

    def test_parse_velocity_returns_none(self):
        from unittest.mock import MagicMock

        assert _uninit_processor()._parse_velocity(MagicMock()) is None


class TestLegacyLoadBlocksFallback:
    """Test that _load_blocks falls back to ActivePatch when Block stream is absent."""

    def _make_fake_dataset(self, *, has_block_stream: bool):
        import pandas as pd

        patch_data = pd.DataFrame({"data": [{"label": "A"}, {"label": "B"}]}, index=[1.0, 2.0])
        block_data = pd.DataFrame({"data": [{}]}, index=[0.0])

        class _FakeLoad:
            def __init__(self, df):
                self.data = df

        class _FakeStream:
            def __init__(self, df=None, *, raise_key=False):
                self._df = df
                self._raise = raise_key

            def load(self):
                if self._raise:
                    raise KeyError("Block")
                return _FakeLoad(self._df)

        class _FakeSoftwareEvents:
            def at(self, name):
                if name == "Block":
                    return _FakeStream(block_data, raise_key=not has_block_stream)
                if name == "ActivePatch":
                    return _FakeStream(patch_data)
                raise KeyError(name)

        class _FakeBehavior:
            def at(self, name):
                if name == "SoftwareEvents":
                    return _FakeSoftwareEvents()
                raise KeyError(name)

        class _FakeDataset:
            def at(self, name):
                if name == "Behavior":
                    return _FakeBehavior()
                raise KeyError(name)

        return _FakeDataset()

    def test_uses_block_stream_when_available(self):
        ds = self._make_fake_dataset(has_block_stream=True)
        result = LegacySiteTableProcessor._load_blocks(ds)  # type: ignore[arg-type]
        assert "block_count" in result.columns
        assert list(result["block_count"]) == [0]

    def test_falls_back_to_active_patch_when_block_missing(self):
        ds = self._make_fake_dataset(has_block_stream=False)
        result = LegacySiteTableProcessor._load_blocks(ds)  # type: ignore[arg-type]
        assert "block_count" in result.columns
        # All patches collapse into block 0 — no block info means one block.
        assert list(result["block_count"]) == [0, 0]


class TestLegacyPatchStateAtRewardTransitionRace:
    """_parse_patch_state_at_reward must tag a reward reading with the *new* patch's
    PatchId even when the reward event was logged a few milliseconds before the
    ActivePatch event announcing that patch — a real ordering seen in raw sessions
    (up to ~5ms), which a plain backward asof-merge misses, mislabeling the new
    patch's first reading with the outgoing patch's PatchId.
    """

    def _make_fake_dataset(self, *, amount, available, probability, active_patch):
        import pandas as pd

        class _FakeLoad:
            def __init__(self, df):
                self.data = df

        class _FakeStream:
            def __init__(self, df):
                self._df = df

            def load(self):
                return _FakeLoad(self._df)

        class _RaisingStream:
            def load(self):
                raise KeyError("PatchStateAtReward")

        class _FakeSoftwareEvents:
            def at(self, name):
                streams = {
                    "PatchRewardAmount": amount,
                    "PatchRewardAvailable": available,
                    "PatchRewardProbability": probability,
                    "ActivePatch": active_patch,
                }
                if name == "PatchStateAtReward":
                    return _RaisingStream()
                if name in streams:
                    return _FakeStream(streams[name])
                raise KeyError(name)

        class _FakeBehavior:
            def at(self, name):
                if name == "SoftwareEvents":
                    return _FakeSoftwareEvents()
                raise KeyError(name)

        class _FakeDataset:
            def at(self, name):
                if name == "Behavior":
                    return _FakeBehavior()
                raise KeyError(name)

        return _FakeDataset(), pd

    def test_reward_slightly_before_active_patch_gets_new_patch_id(self):
        """Reward at t=10.000 (patch 0's own reading) logged 3ms before the ActivePatch
        event announcing patch 1 at t=10.003 — a bare backward merge would tag it PatchId=0.
        """
        pd = __import__("pandas")

        amount = pd.DataFrame({"data": [5.0, 5.0]}, index=[1.0, 10.000])
        available = pd.DataFrame({"data": [1000.0, 1000.0]}, index=[1.0, 10.000])
        probability = pd.DataFrame({"data": [0.6, 0.9]}, index=[1.0, 10.000])
        active_patch = pd.DataFrame(
            {"data": [{"state_index": 0}, {"state_index": 1}]}, index=[0.5, 10.003]
        )

        ds, _ = self._make_fake_dataset(
            amount=amount, available=available, probability=probability, active_patch=active_patch
        )
        result = _uninit_processor()._parse_patch_state_at_reward(ds)  # type: ignore[arg-type]

        assert result.loc[10.000, "PatchId"] == 1
        assert result.loc[10.000, "Probability"] == 0.9

    def test_genuinely_separate_later_patch_is_not_pulled_in(self):
        """A reward far (minutes) before the next patch transition must still resolve
        to the earlier (currently active) patch — the grace period must not bleed into
        an unrelated, later patch transition.
        """
        pd = __import__("pandas")

        amount = pd.DataFrame({"data": [5.0]}, index=[10.0])
        available = pd.DataFrame({"data": [1000.0]}, index=[10.0])
        probability = pd.DataFrame({"data": [0.6]}, index=[10.0])
        active_patch = pd.DataFrame(
            {"data": [{"state_index": 0}, {"state_index": 1}]}, index=[0.5, 300.0]
        )

        ds, _ = self._make_fake_dataset(
            amount=amount, available=available, probability=probability, active_patch=active_patch
        )
        result = _uninit_processor()._parse_patch_state_at_reward(ds)  # type: ignore[arg-type]

        assert result.loc[10.0, "PatchId"] == 0


class TestLegacyVersionCheck:
    """Verify __init__ rejects datasets at version >= 0.6.0."""

    def test_rejects_version_0_6_0(self):
        from unittest.mock import MagicMock

        mock_ds = MagicMock()
        mock_ds.version = "0.6.0"
        with pytest.raises(DatasetProcessorError, match="LegacySiteTableProcessor only supports datasets"):
            LegacySiteTableProcessor(mock_ds)

    def test_rejects_newer_version(self):
        from unittest.mock import MagicMock

        mock_ds = MagicMock()
        mock_ds.version = "0.7.1"
        with pytest.raises(DatasetProcessorError, match="LegacySiteTableProcessor only supports datasets"):
            LegacySiteTableProcessor(mock_ds)
