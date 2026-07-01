"""Tests for LegacyTrialTableProcessor.

Pure-logic tests only — no contraqctor dataset required.
Integration with a real legacy dataset is covered by tests/integration/datasets.yml.
"""

import pytest

from aind_behavior_vr_foraging_packaging.processing._legacy_trial_table import (
    _LEGACY_OLFACTOMETER_CHANNEL_COUNT,
    LegacyTrialTableProcessor,
)
from aind_behavior_vr_foraging_packaging.processing._trial_table import DatasetProcessorError


def _uninit_processor() -> LegacyTrialTableProcessor:
    """Return a LegacyTrialTableProcessor without calling __init__ (avoids needing a real dataset)."""
    return LegacyTrialTableProcessor.__new__(LegacyTrialTableProcessor)


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
        assert _uninit_processor()._get_olfactometer_channel_count(None) == 3  # type: ignore[arg-type]


class TestLegacyIsStoppedAndVelocity:
    def test_parse_is_stopped_returns_none(self):
        assert LegacyTrialTableProcessor._parse_is_stopped(None) is None  # type: ignore[arg-type]

    def test_parse_velocity_returns_none(self):
        assert _uninit_processor()._parse_velocity(None) is None  # type: ignore[arg-type]


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
        result = LegacyTrialTableProcessor._load_blocks(ds)  # type: ignore[arg-type]
        assert "block_count" in result.columns
        assert list(result["block_count"]) == [0]

    def test_falls_back_to_active_patch_when_block_missing(self):
        ds = self._make_fake_dataset(has_block_stream=False)
        result = LegacyTrialTableProcessor._load_blocks(ds)  # type: ignore[arg-type]
        assert "block_count" in result.columns
        # All patches collapse into block 0 — no block info means one block.
        assert list(result["block_count"]) == [0, 0]


class TestLegacyVersionCheck:
    """Verify __init__ rejects datasets at version >= 0.6.0."""

    def test_rejects_version_0_6_0(self):
        from unittest.mock import MagicMock

        mock_ds = MagicMock()
        mock_ds.version = "0.6.0"
        with pytest.raises(DatasetProcessorError, match="LegacyTrialTableProcessor only supports datasets"):
            LegacyTrialTableProcessor(mock_ds)

    def test_rejects_newer_version(self):
        from unittest.mock import MagicMock

        mock_ds = MagicMock()
        mock_ds.version = "0.7.1"
        with pytest.raises(DatasetProcessorError, match="LegacyTrialTableProcessor only supports datasets"):
            LegacyTrialTableProcessor(mock_ds)
