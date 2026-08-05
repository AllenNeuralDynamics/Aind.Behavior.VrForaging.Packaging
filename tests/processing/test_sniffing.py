"""Tests for SniffingProcessor."""

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging.processing._sniffing import SniffingProcessor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_processor(timestamps, voltage, sampling_rate_hz=100.0) -> SniffingProcessor:
    """Processor whose compute() returns a fixed sniff frame, as _compute() would build it."""
    df = pd.DataFrame(
        {"voltage": np.asarray(voltage, dtype=float)},
        index=pd.Index(np.asarray(timestamps, dtype=float), name="timestamp"),
    )
    if sampling_rate_hz is not None:
        df.attrs["sampling_rate_hz"] = sampling_rate_hz

    proc = SniffingProcessor.__new__(SniffingProcessor)
    proc._dataset = MagicMock()
    proc._dataset.version = "0.6.0"  # required by AbstractProcessor.compute() for dataset_version
    proc._raise_on_error = False
    proc._resampling_frequency_hz = None
    proc.compute = lambda: df  # bypass the harp stream read; nwbize() only consumes the frame
    return proc


def _empty_nwb():
    """A minimal real NWBFile — the pynwb TimeSeries validation is the point of these tests."""
    from datetime import datetime, timezone

    from pynwb import NWBFile

    return NWBFile(
        session_description="test",
        identifier="test",
        session_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSniffingNwbize:
    """nwbize() adds a sniffing TimeSeries to the behavior processing module."""

    def test_adds_timeseries_to_behavior_module(self):
        proc = _make_processor([0.0, 0.01, 0.02], [1.0, 2.0, 3.0])
        nwb = proc.nwbize(_empty_nwb())

        module = nwb.processing["behavior"]
        ts = module["sniffing"]
        assert ts.unit == "V"
        assert ts.data.tolist() == [1.0, 2.0, 3.0]

    def test_uses_timestamps_not_rate(self):
        """Regression: passing timestamps together with starting_time/rate raises in pynwb.

        NWB treats the two as mutually exclusive representations, so nwbize() must pick one.
        """
        proc = _make_processor([0.5, 0.51, 0.52], [1.0, 2.0, 3.0])
        ts = proc.nwbize(_empty_nwb()).processing["behavior"]["sniffing"]

        assert ts.timestamps is not None
        assert list(ts.timestamps) == [0.5, 0.51, 0.52]
        assert ts.rate is None
        assert ts.starting_time is None

    def test_description_records_sampling_rate(self):
        proc = _make_processor([0.0, 0.01], [1.0, 2.0], sampling_rate_hz=250.0)
        ts = proc.nwbize(_empty_nwb()).processing["behavior"]["sniffing"]
        assert "250.0 Hz" in ts.description

    def test_reuses_existing_behavior_module(self):
        """A processor that runs after another must not create a second behavior module."""
        from pynwb.base import ProcessingModule

        nwb = _empty_nwb()
        nwb.add_processing_module(ProcessingModule(name="behavior", description="existing"))

        proc = _make_processor([0.0, 0.01], [1.0, 2.0])
        nwb = proc.nwbize(nwb)

        assert list(nwb.processing.keys()) == ["behavior"]
        assert nwb.processing["behavior"].description == "existing"
        assert "sniffing" in nwb.processing["behavior"].data_interfaces

    def test_returns_the_nwb_file(self):
        proc = _make_processor([0.0, 0.01], [1.0, 2.0])
        nwb = _empty_nwb()
        assert proc.nwbize(nwb) is nwb

    @pytest.mark.parametrize("attrs_rate", [None, 0.0])
    def test_missing_or_zero_sampling_rate_still_writes(self, attrs_rate):
        """df.attrs may lack sampling_rate_hz; that must not block the TimeSeries."""
        proc = _make_processor([0.0, 0.01], [1.0, 2.0], sampling_rate_hz=attrs_rate)
        ts = proc.nwbize(_empty_nwb()).processing["behavior"]["sniffing"]
        assert list(ts.timestamps) == [0.0, 0.01]
