"""Tests for SoftwareEventsProcessor."""

import json
import typing as ty
from unittest.mock import MagicMock

import pandas as pd

from aind_behavior_vr_foraging_packaging.processing._software_events import SoftwareEventsProcessor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sw_stream(name: str, rows: list[tuple[float, ty.Any]]) -> MagicMock:
    """Create a mock SoftwareEvents stream that passes isinstance checks."""
    import contraqctor.contract as _dc

    index = pd.Index([t for t, _ in rows], name="Time")
    df = pd.DataFrame({"data": [d for _, d in rows]}, index=index)

    # spec= makes isinstance(mock, SoftwareEvents) return True
    mock = MagicMock(spec=_dc.json.SoftwareEvents)
    mock.name = name
    mock.is_collection = False
    mock.has_error = False
    mock.data = df
    mock.collect_errors.return_value = []
    return mock


def _make_processor(streams: list) -> SoftwareEventsProcessor:
    """Build a processor whose SoftwareEvents collection yields *streams*."""
    sw_collection = MagicMock()
    sw_collection.is_collection = True
    sw_collection.load_all.return_value = None
    sw_collection.iter_all.return_value = iter(streams)

    behavior = MagicMock()
    behavior.at.return_value = sw_collection

    dataset = MagicMock()
    dataset.at.return_value = behavior

    proc = SoftwareEventsProcessor.__new__(SoftwareEventsProcessor)
    proc._dataset = dataset
    proc._raise_on_error = False
    return proc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSoftwareEventsCompute:
    def test_returns_dataframe_with_correct_columns(self):
        proc = _make_processor([_make_sw_stream("ActiveSite", [(1.0, {"label": "RewardSite"})])])
        df = proc.compute()
        assert isinstance(df, pd.DataFrame)
        assert "event_name" in df.columns
        assert "data" in df.columns

    def test_index_named_timestamp(self):
        proc = _make_processor([_make_sw_stream("GiveReward", [(2.0, {"amount": 5.0})])])
        df = proc.compute()
        assert df.index.name == "timestamp"

    def test_data_column_is_valid_json(self):
        payload = {"label": "RewardSite", "start_position": 0.0}
        proc = _make_processor([_make_sw_stream("ActiveSite", [(1.0, payload)])])
        df = proc.compute()
        assert json.loads(df.iloc[0]["data"]) == payload

    def test_multiple_streams_concatenated(self):
        proc = _make_processor(
            [
                _make_sw_stream("ActiveSite", [(1.0, {"label": "RewardSite"}), (3.0, {"label": "InterSite"})]),
                _make_sw_stream("GiveReward", [(2.0, {"amount": 5.0})]),
            ]
        )
        df = proc.compute()
        assert len(df) == 3
        assert set(df["event_name"].unique()) == {"ActiveSite", "GiveReward"}

    def test_rows_sorted_by_timestamp(self):
        proc = _make_processor(
            [
                _make_sw_stream("GiveReward", [(5.0, {"amount": 5.0})]),
                _make_sw_stream("ActiveSite", [(1.0, {"label": "R"}), (3.0, {"label": "I"})]),
            ]
        )
        df = proc.compute()
        assert list(df.index) == sorted(df.index.tolist())

    def test_empty_when_no_streams(self):
        proc = _make_processor([])
        df = proc.compute()
        assert df.empty
        assert df.index.name == "timestamp"
        assert set(df.columns) == {"event_name", "data"}

    def test_non_dict_data_serialised_as_json(self):
        proc = _make_processor([_make_sw_stream("RngSeed", [(1.0, 42)])])
        df = proc.compute()
        assert json.loads(df.iloc[0]["data"]) == 42

    def test_parquet_round_trip(self, tmp_path):
        payload = {"label": "RewardSite", "start_position": 10.0}
        proc = _make_processor([_make_sw_stream("ActiveSite", [(1.0, payload)])])
        df = proc.compute()
        path = tmp_path / "software_events.parquet"
        df.to_parquet(path)
        loaded = pd.read_parquet(path)
        assert loaded.iloc[0]["event_name"] == "ActiveSite"
        assert json.loads(loaded.iloc[0]["data"]) == payload

    def test_output_name_is_software_events(self):
        proc = SoftwareEventsProcessor.__new__(SoftwareEventsProcessor)
        assert proc.output_name == "software_events"
