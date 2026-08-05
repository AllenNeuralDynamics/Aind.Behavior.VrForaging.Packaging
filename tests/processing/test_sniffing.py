"""Tests for ``SniffingProcessor``.

The processor is run end-to-end on a synthetic dataset: a 4 Hz sine wave
sampled at 200 Hz, wrapped in a real ``contraqctor`` ``Dataset`` whose
``RawVoltage`` stream is served from memory. Nothing on the processor is
stubbed, so ``nwbize()`` exercises the real resample/band-pass path and the
real pynwb ``TimeSeries`` validation.
"""

import typing as ty

import numpy as np
import pandas as pd
from contraqctor.contract import Dataset, DataStream, DataStreamCollection

from aind_behavior_vr_foraging_packaging.processing._sniffing import SniffingProcessor

SAMPLING_RATE_HZ = 200.0
SINE_FREQUENCY_HZ = 4.0  # inside the 0.2-20 Hz breathing band the processor keeps
DURATION_S = 5.0


class _InMemoryStream(DataStream):
    def _reader(self, frame: pd.DataFrame) -> pd.DataFrame:
        return frame


def _sine_dataset() -> Dataset:
    """Dataset exposing ``Behavior::HarpSniffDetector::RawVoltage`` as a sine wave."""
    timestamps = np.arange(0.0, DURATION_S, 1.0 / SAMPLING_RATE_HZ)
    raw_voltage = pd.DataFrame(
        {"RawVoltage": np.sin(2 * np.pi * SINE_FREQUENCY_HZ * timestamps), "MessageType": "EVENT"},
        index=pd.Index(timestamps, name="timestamp"),
    )
    sniff_detector = DataStreamCollection(
        "HarpSniffDetector", [_InMemoryStream("RawVoltage", reader_params=raw_voltage)]
    )
    behavior = DataStreamCollection("Behavior", [sniff_detector])
    return Dataset("SyntheticSniffDataset", version="0.6.0", data_streams=[behavior])


def test_nwbize(nwb_file: ty.Any) -> None:
    """The sniff signal lands in the behavior module as a timestamped TimeSeries."""
    processor = SniffingProcessor(_sine_dataset(), resampling_frequency_hz=SAMPLING_RATE_HZ)

    result = processor.nwbize(nwb_file)

    assert result is nwb_file
    time_series = result.processing["behavior"]["sniffing"]
    assert time_series.unit == "V"
    assert f"{SAMPLING_RATE_HZ} Hz" in time_series.description
    # NWB treats timestamps and starting_time/rate as mutually exclusive; passing
    # both raises in pynwb, so nwbize() must commit to timestamps alone.
    assert time_series.rate is None
    assert time_series.starting_time is None
    assert len(time_series.timestamps) == len(time_series.data)
    # the band-pass keeps the 4 Hz sine, so the signal is not flattened away
    assert np.ptp(time_series.data) > 0.1
