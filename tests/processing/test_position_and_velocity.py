"""Tests for the raw (pre-downsampling) position/velocity computations.

Both encoder paths derive velocity as displacement / dt between consecutive harp timestamps, so a
repeated timestamp (dt == 0) must not produce an infinite velocity. The dataset is mocked down to
the single stream each path reads.
"""

from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from aind_behavior_vr_foraging_packaging.processing._legacy_position_and_velocity import (
    LegacyPositionAndVelocityProcessor,
)
from aind_behavior_vr_foraging_packaging.processing._position_and_velocity import PositionAndVelocityProcessor

# Unit wheel circumference per pulse keeps positions equal to encoder counts.
_CALIBRATION = {"wheel_diameter": 1 / np.pi, "pulses_per_revolution": 1, "invert_direction": False}


def _dataset(device: str, register: str, times: list[float], counts: list[int]) -> MagicMock:
    data = pd.DataFrame({"Encoder": counts, "MessageType": "EVENT"}, index=pd.Index(times, name="Time"))
    dataset = MagicMock()
    dataset.at("Behavior").at(device).load().at(register).load().data = data
    return dataset


def test_treadmill_duplicate_timestamp_keeps_last_sample() -> None:
    dataset = _dataset("HarpTreadmill", "SensorData", [0.0, 1.0, 1.0, 2.0], [0, 1, 2, 4])
    df = PositionAndVelocityProcessor.compute_position_and_velocity_from_treadmill(
        dataset, {"harp_treadmill": {"calibration": _CALIBRATION}}
    )
    assert np.isfinite(df["velocity"]).all()
    assert df.index.tolist() == [0.0, 1.0, 2.0]
    assert df["position"].tolist() == [0.0, 2.0, 4.0]
    assert df["velocity"].tolist() == [0.0, 2.0, 2.0]


def test_analog_duplicate_timestamp_sums_displacement() -> None:
    dataset = _dataset("HarpBehavior", "AnalogData", [0.0, 1.0, 1.0, 2.0], [0, 1, 1, 2])
    df = LegacyPositionAndVelocityProcessor.compute_position_and_velocity_from_analog_data(
        dataset, {"treadmill": {"settings": _CALIBRATION}}
    )
    assert np.isfinite(df["velocity"]).all()
    assert df.index.tolist() == [0.0, 1.0, 2.0]
    assert df["position"].tolist() == [0.0, 2.0, 4.0]
    assert df["velocity"].tolist() == [0.0, 2.0, 2.0]
