"""Tests for EventsProcessor."""

import typing as t
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging.processing._events import EventsProcessor
from aind_behavior_vr_foraging_packaging.processing._helper import nearest_positions, parse_force_reward

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_processor(raise_on_error: bool = False) -> EventsProcessor:
    proc = EventsProcessor.__new__(EventsProcessor)
    proc._dataset = MagicMock()
    proc._dataset.version = "0.6.0"  # required by AbstractProcessor.compute() for dataset_version
    proc._raise_on_error = raise_on_error
    return proc


def _make_force_reward_dataset(index, data):
    """Dataset stub whose Behavior/SoftwareEvents/ForceGiveReward stream returns the given rows."""

    class _Stream:
        def __init__(self, df: pd.DataFrame) -> None:
            self.data = df
            self.has_data = not df.empty

    class _Node:
        def load(self) -> _Stream:
            return _Stream(pd.DataFrame({"data": data}, index=np.asarray(index, dtype=float)))

    class _SoftwareEvents:
        def at(self, name: str) -> _Node:
            assert name == "ForceGiveReward"
            return _Node()

    class _Behavior:
        def at(self, name: str) -> _SoftwareEvents:
            assert name == "SoftwareEvents"
            return _SoftwareEvents()

    class _Dataset:
        def at(self, name: str) -> _Behavior:
            assert name == "Behavior"
            return _Behavior()

    return _Dataset()


def _make_absent_stream_dataset():
    """Dataset stub matching a pre-0.6.0 schema: ForceGiveReward isn't a declared node at all.

    Verified against the real data contract: ``dataset.at("SoftwareEvents").at("ForceGiveReward")``
    raises ``KeyError`` (not ``FileNotFoundError``) when the node was never registered for that
    dataset version -- see ``contraqctor.contract.base._At.__call__``.
    """

    class _SoftwareEvents:
        def at(self, name: str) -> t.NoReturn:
            raise KeyError(f"Stream with name: '{name}' not found in data streams.")

    class _Behavior:
        def at(self, name: str) -> _SoftwareEvents:
            assert name == "SoftwareEvents"
            return _SoftwareEvents()

    class _Dataset:
        def at(self, name: str) -> _Behavior:
            assert name == "Behavior"
            return _Behavior()

    return _Dataset()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNearestPositions:
    """Positional nearest-neighbour lookup used to snap software events onto valve openings."""

    def test_exact_below_and_above(self):
        sorted_values = np.array([0.0, 10.0, 20.0])
        query = np.array([-5.0, 0.0, 4.0, 6.0, 20.0, 100.0])
        assert nearest_positions(sorted_values, query).tolist() == [0, 0, 0, 1, 2, 2]

    def test_tie_breaks_toward_earlier(self):
        sorted_values = np.array([0.0, 10.0])
        assert nearest_positions(sorted_values, np.array([5.0])).tolist() == [0]


class TestParseForceReward:
    """The ForceGiveReward stream may be genuinely absent (pre-0.6.0) as well as empty or present."""

    def test_absent_node_returns_empty(self):
        # KeyError from .at() -- the node isn't declared at all for this dataset version.
        result = parse_force_reward(_make_absent_stream_dataset())
        assert result.empty
        assert list(result.columns) == ["data"]

    def test_present_with_data(self):
        dataset = _make_force_reward_dataset(index=[1.0, 2.0], data=["a", "b"])
        result = parse_force_reward(dataset)
        assert result.index.tolist() == [1.0, 2.0]
        assert result["data"].tolist() == ["a", "b"]


class TestManualWaterDelivery:
    """ForceGiveReward events resolve to the valve openings left after automatic deliveries claim theirs."""

    @staticmethod
    def _patch(monkeypatch, valve_times, auto_times, auto_amounts=None):
        amounts = [3.0] * len(auto_times) if auto_amounts is None else auto_amounts
        monkeypatch.setattr(
            "aind_behavior_vr_foraging_packaging.processing._events.parse_water_delivery",
            lambda ds: pd.Series(True, index=np.asarray(valve_times, dtype=float)),
        )
        monkeypatch.setattr(
            "aind_behavior_vr_foraging_packaging.processing._events.parse_reward_metadata",
            lambda ds: pd.DataFrame({"data": amounts}, index=np.asarray(auto_times, dtype=float)),
        )

    def test_returns_hardware_valve_times_with_payload(self, monkeypatch):
        # Valve opens at 1.0 (auto), 2.01 (force), 3.0 (auto), 4.02 (force). Force events are
        # software-timestamped slightly off the valve; the returned rows are indexed by valve time
        # but keep the original event payload.
        self._patch(monkeypatch, valve_times=[1.0, 2.01, 3.0, 4.02], auto_times=[1.0, 3.0])
        dataset = _make_force_reward_dataset(index=[2.0, 4.0], data=["a", "b"])
        result = EventsProcessor._parse_manual_water_delivery(dataset)
        assert result.index.tolist() == [2.01, 4.02]
        assert result["data"].tolist() == ["a", "b"]

    def test_forced_reward_adjacent_to_automatic(self, monkeypatch):
        # A forced reward fires right next to an automatic one. Removing the automatic valve open
        # first prevents the force event from being mis-attributed to the automatic delivery.
        self._patch(monkeypatch, valve_times=[1.0, 1.05], auto_times=[1.0])
        dataset = _make_force_reward_dataset(index=[1.04], data=["a"])
        result = EventsProcessor._parse_manual_water_delivery(dataset)
        assert result.index.tolist() == [1.05]

    def test_zero_amount_give_reward_does_not_claim_valve(self, monkeypatch):
        # GiveReward with amount 0 is not a real delivery, so it must not consume a valve open.
        self._patch(monkeypatch, valve_times=[5.0], auto_times=[5.0], auto_amounts=[0.0])
        dataset = _make_force_reward_dataset(index=[5.0], data=["a"])
        result = EventsProcessor._parse_manual_water_delivery(dataset)
        assert result.index.tolist() == [5.0]

    def test_no_unclaimed_valve_returns_empty(self, monkeypatch):
        self._patch(monkeypatch, valve_times=[1.0], auto_times=[1.0])
        dataset = _make_force_reward_dataset(index=[1.0], data=["a"])
        result = EventsProcessor._parse_manual_water_delivery(dataset)
        assert result.empty

    def test_no_force_reward_events_returns_empty(self, monkeypatch):
        self._patch(monkeypatch, valve_times=[1.0, 2.0], auto_times=[1.0])
        dataset = _make_force_reward_dataset(index=[], data=[])
        result = EventsProcessor._parse_manual_water_delivery(dataset)
        assert result.empty

    def test_absent_stream_returns_empty(self):
        # Pre-0.6.0 schemas do not register ForceGiveReward.
        result = EventsProcessor._parse_manual_water_delivery(_make_absent_stream_dataset())
        assert result.empty
        assert list(result.columns) == ["data"]


class TestEventsCompute:
    """_compute() iterates _EVENT_SOURCES, tags each source's rows with its event_name, and concatenates."""

    def test_concatenates_sources_sorted_by_timestamp(self, monkeypatch):
        proc = _make_processor()
        sources = [
            ("B", lambda ds: pd.DataFrame({"data": ["b"]}, index=[5.0])),
            ("A", lambda ds: pd.DataFrame({"data": ["a1", "a2"]}, index=[1.0, 3.0])),
        ]
        monkeypatch.setattr(EventsProcessor, "_EVENT_SOURCES", sources)
        df = proc.compute()
        assert list(df.index) == [1.0, 3.0, 5.0]
        assert list(df["event_name"]) == ["A", "A", "B"]
        assert list(df["data"]) == ["a1", "a2", "b"]
        assert df.index.name == "timestamp"

    def test_empty_source_is_skipped(self, monkeypatch):
        proc = _make_processor()
        monkeypatch.setattr(EventsProcessor, "_EVENT_SOURCES", [("Empty", lambda ds: pd.DataFrame(columns=["data"]))])
        df = proc.compute()
        assert df.empty
        assert df.index.name == "timestamp"

    def test_no_sources_returns_empty(self, monkeypatch):
        proc = _make_processor()
        monkeypatch.setattr(EventsProcessor, "_EVENT_SOURCES", [])
        df = proc.compute()
        assert df.empty
        assert set(df.columns) == {"event_name", "data"}

    def test_failing_source_is_skipped_and_logged(self, monkeypatch):
        proc = _make_processor(raise_on_error=False)

        def _boom(ds):
            raise ValueError("no stream")

        sources = [("Broken", _boom), ("Ok", lambda ds: pd.DataFrame({"data": ["x"]}, index=[1.0]))]
        monkeypatch.setattr(EventsProcessor, "_EVENT_SOURCES", sources)
        df = proc.compute()
        assert list(df["event_name"]) == ["Ok"]

    def test_failing_source_raises_when_raise_on_error(self, monkeypatch):
        proc = _make_processor(raise_on_error=True)

        def _boom(ds):
            raise ValueError("no stream")

        monkeypatch.setattr(EventsProcessor, "_EVENT_SOURCES", [("Broken", _boom)])
        with pytest.raises(ValueError):
            proc.compute()

    def test_output_name_is_events(self):
        proc = EventsProcessor.__new__(EventsProcessor)
        assert proc.output_name == "events"
