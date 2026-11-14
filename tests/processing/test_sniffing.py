"""Tests for ``SniffingProcessor``.

The processor is run end-to-end on a synthetic dataset: a 4 Hz sine wave
sampled at 200 Hz, wrapped in a real ``contraqctor`` ``Dataset`` whose
``RawVoltage`` stream is served from memory. Nothing on the processor is
stubbed, so ``nwbize()`` exercises the real resample/band-pass path and the
real pynwb ``TimeSeries`` validation.
"""

import types
import typing as ty

import numpy as np
import pandas as pd
from contraqctor.contract import Dataset, DataStream, DataStreamCollection

from aind_behavior_vr_foraging_packaging.processing._sniffing import SniffingProcessor

SAMPLING_RATE_HZ = 200.0
SINE_FREQUENCY_HZ = 4.0  # inside the 0.2-20 Hz breathing band the processor keeps
DURATION_S = 5.0


class _InMemoryStream(DataStream):
    def _reader(self, params: pd.DataFrame) -> pd.DataFrame:
        return params


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


# ---------------------------------------------------------------------------
# Absent sniff-detector data: schema version doesn't declare the node (KeyError),
# or the node is declared but its data file is missing on disk (FileNotFoundError).
# ---------------------------------------------------------------------------


class _MissingFileStream(DataStream):
    """A stream whose reader always raises ``FileNotFoundError``, simulating a
    session whose ``SniffDetector.harp/SniffDetector_32.bin`` file is absent."""

    def _reader(self, params: ty.Any) -> pd.DataFrame:
        raise FileNotFoundError("SniffDetector_32.bin")


def _missing_file_dataset() -> Dataset:
    """Dataset where ``HarpSniffDetector`` is declared but its ``RawVoltage`` file is missing."""
    sniff_detector = DataStreamCollection("HarpSniffDetector", [_MissingFileStream("RawVoltage", reader_params={})])
    behavior = DataStreamCollection("Behavior", [sniff_detector])
    return Dataset("SyntheticSniffDatasetMissingFile", version="0.6.0", data_streams=[behavior])


def _no_sniff_detector_dataset() -> Dataset:
    """Dataset where ``HarpSniffDetector`` is not a declared node at all."""
    behavior = DataStreamCollection("Behavior", [])
    return Dataset("SyntheticSniffDatasetNoDetector", version="0.6.0", data_streams=[behavior])


def test_compute_returns_empty_frame_when_file_missing() -> None:
    processor = SniffingProcessor(_missing_file_dataset())

    df = processor.compute()

    assert df.empty
    assert list(df.columns) == ["voltage"]


def test_compute_returns_empty_frame_when_stream_not_declared() -> None:
    processor = SniffingProcessor(_no_sniff_detector_dataset())

    df = processor.compute()

    assert df.empty
    assert list(df.columns) == ["voltage"]


def test_nwbize_is_noop_when_no_sniff_detector_data(nwb_file: ty.Any) -> None:
    """No sniff-detector data means no TimeSeries -- and no empty 'behavior' module either."""
    processor = SniffingProcessor(_missing_file_dataset())

    result = processor.nwbize(nwb_file)

    assert result is nwb_file
    assert "behavior" not in result.processing


def test_write_parquet_skips_file_when_no_sniff_detector_data(tmp_path: ty.Any) -> None:
    """An absent device leaves no sniffing.parquet at all, not an empty one."""
    processor = SniffingProcessor(_missing_file_dataset())

    processor.write_parquet(tmp_path)

    assert not (tmp_path / "sniffing.parquet").exists()


def test_write_parquet_writes_file_when_sniff_detector_data_present(tmp_path: ty.Any) -> None:
    """Sanity check: the skip is specific to the absent case, not a regression for real data."""
    processor = SniffingProcessor(_sine_dataset(), resampling_frequency_hz=SAMPLING_RATE_HZ)

    processor.write_parquet(tmp_path)

    assert (tmp_path / "sniffing.parquet").exists()


# ---------------------------------------------------------------------------
# Rig-based shortcut: a v1-schema rig that already declares no sniff detector
# skips HarpSniffDetector entirely, without even attempting to load it.
# ---------------------------------------------------------------------------


class _ConstantStream(DataStream):
    """A stream whose reader returns *reader_params* verbatim, whatever its type."""

    def _reader(self, params: ty.Any) -> ty.Any:
        return params


class _UnreachableStream(DataStream):
    """A stream whose reader must never be called -- proves a shortcut skipped loading it."""

    def _reader(self, params: ty.Any) -> pd.DataFrame:
        raise AssertionError(
            "HarpSniffDetector should not have been loaded: the rig-based shortcut should have short-circuited first."
        )


def _dataset_with_rig(
    *, dataset_version: str, harp_sniff_detector: ty.Any, sniff_detector_stream: DataStream
) -> Dataset:
    """Dataset exposing a stubbed ``InputSchemas::Rig`` alongside a ``HarpSniffDetector`` stream.

    ``harp_sniff_detector`` stands in for the rig's own field of the same name; a real
    ``AindVrForagingRig`` isn't needed since the processor only reads that one attribute.
    """
    rig = types.SimpleNamespace(harp_sniff_detector=harp_sniff_detector)
    input_schemas = DataStreamCollection("InputSchemas", [_ConstantStream("Rig", reader_params=rig)])
    sniff_detector = DataStreamCollection("HarpSniffDetector", [sniff_detector_stream])
    behavior = DataStreamCollection("Behavior", [sniff_detector, input_schemas])
    return Dataset("SyntheticSniffDatasetWithRig", version=dataset_version, data_streams=[behavior])


def test_rig_shortcut_skips_load_for_v1_dataset_with_no_sniff_detector() -> None:
    """A v1 rig that already says 'no sniff detector' never touches HarpSniffDetector at all."""
    dataset = _dataset_with_rig(
        dataset_version="1.2.1",
        harp_sniff_detector=None,
        sniff_detector_stream=_UnreachableStream("RawVoltage", reader_params={}),
    )
    processor = SniffingProcessor(dataset)

    df = processor.compute()

    assert df.empty
    assert list(df.columns) == ["voltage"]


def test_rig_shortcut_falls_through_when_rig_reports_sniff_detector_present() -> None:
    """A v1 rig that does declare a sniff detector still goes on to load the real signal."""
    timestamps = np.arange(0.0, DURATION_S, 1.0 / SAMPLING_RATE_HZ)
    raw_voltage = pd.DataFrame(
        {"RawVoltage": np.sin(2 * np.pi * SINE_FREQUENCY_HZ * timestamps), "MessageType": "EVENT"},
        index=pd.Index(timestamps, name="timestamp"),
    )
    dataset = _dataset_with_rig(
        dataset_version="1.2.1",
        harp_sniff_detector=object(),  # anything not None means "present"
        sniff_detector_stream=_InMemoryStream("RawVoltage", reader_params=raw_voltage),
    )
    processor = SniffingProcessor(dataset, resampling_frequency_hz=SAMPLING_RATE_HZ)

    df = processor.compute()

    assert not df.empty


def test_rig_shortcut_falls_through_when_rig_stream_is_absent() -> None:
    """A v1 dataset with no declared Rig node just skips the shortcut, not the whole processor."""
    timestamps = np.arange(0.0, DURATION_S, 1.0 / SAMPLING_RATE_HZ)
    raw_voltage = pd.DataFrame(
        {"RawVoltage": np.sin(2 * np.pi * SINE_FREQUENCY_HZ * timestamps), "MessageType": "EVENT"},
        index=pd.Index(timestamps, name="timestamp"),
    )
    sniff_detector = DataStreamCollection(
        "HarpSniffDetector", [_InMemoryStream("RawVoltage", reader_params=raw_voltage)]
    )
    behavior = DataStreamCollection("Behavior", [sniff_detector])  # no InputSchemas/Rig at all
    dataset = Dataset("SyntheticSniffDatasetNoRig", version="1.2.1", data_streams=[behavior])
    processor = SniffingProcessor(dataset, resampling_frequency_hz=SAMPLING_RATE_HZ)

    df = processor.compute()

    assert not df.empty


def test_rig_shortcut_is_scoped_to_v1_only() -> None:
    """A pre-v1 dataset's rig claim is ignored -- the real (present) stream is still used."""
    timestamps = np.arange(0.0, DURATION_S, 1.0 / SAMPLING_RATE_HZ)
    raw_voltage = pd.DataFrame(
        {"RawVoltage": np.sin(2 * np.pi * SINE_FREQUENCY_HZ * timestamps), "MessageType": "EVENT"},
        index=pd.Index(timestamps, name="timestamp"),
    )
    dataset = _dataset_with_rig(
        dataset_version="0.6.0",
        harp_sniff_detector=None,  # would trigger the shortcut on a v1 dataset -- must not here
        sniff_detector_stream=_InMemoryStream("RawVoltage", reader_params=raw_voltage),
    )
    processor = SniffingProcessor(dataset, resampling_frequency_hz=SAMPLING_RATE_HZ)

    df = processor.compute()

    assert not df.empty
