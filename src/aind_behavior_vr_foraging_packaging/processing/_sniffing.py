import logging
import typing as ty

import contraqctor.contract
import numpy as np
import pandas as pd
from scipy.interpolate import interp1d
from scipy.signal import butter, filtfilt

from .._base import AbstractProcessor

logger = logging.getLogger(__name__)


class SniffingProcessor(AbstractProcessor):
    __output_name__ = "sniffing"

    def __init__(
        self, dataset: contraqctor.contract.Dataset, *, resampling_frequency_hz: float | None = None, **kwargs
    ):
        super().__init__(dataset=dataset, **kwargs)
        self._resampling_frequency_hz = resampling_frequency_hz

    def _compute(self) -> pd.DataFrame:
        """Returns DataFrame with 'voltage' (V) indexed by harp time.
        Sampling rate stored in df.attrs['sampling_rate_hz'].
        """
        sniff, fs = self.compute_sniff_signal(self.dataset)
        df = sniff.rename("voltage").to_frame()
        df.attrs["sampling_rate_hz"] = fs
        return df

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Add sniffing TimeSeries to *nwb_file*."""
        from pynwb import TimeSeries
        from pynwb.base import ProcessingModule

        module = nwb_file.processing.get("behavior")
        if module is None:
            module = ProcessingModule(name="behavior", description="Processing module for behavior data")
            nwb_file.add_processing_module(module)

        df = self.compute()
        fs = float(df.attrs.get("sampling_rate_hz", 0.0))
        module.add(
            TimeSeries(
                name="sniffing",
                data=df["voltage"].values,
                unit="V",
                starting_time=float(df.index[0]),
                rate=fs,
                timestamps=df.index.values,
                description="Filtered breathing/sniff signal derived from the sniff detector raw voltage.",
            )
        )
        return nwb_file

    def compute_sniff_signal(self, dataset: contraqctor.contract.Dataset) -> ty.Tuple[pd.Series, float]:
        """Computes the filtered breathing/sniff signal from the sniff detector raw voltage.

        The raw voltage is resampled onto a uniform time grid and then passed
        through a 0.2-20 Hz band-pass filter to isolate the breathing band, as
        done in ``contraqctor.qc.harp.sniff_detector``. The grid spacing is set
        by the processor's ``resampling_frequency_hz`` when provided, otherwise
        it defaults to the median sampling rate of the raw voltage.

        Args:
            dataset: A contraqctor Dataset providing access to the
                ``HarpSniffDetector`` device.

        Returns:
            A tuple of the filtered sniff signal (indexed by harp timestamp in
            seconds) and the sampling frequency (Hz) used for resampling.
        """
        raw = ty.cast(
            pd.DataFrame,
            dataset.at("Behavior").at("HarpSniffDetector").load().at("RawVoltage").load().data,
        )
        raw = raw[raw["MessageType"] == "EVENT"]["RawVoltage"]

        timestamps = np.asarray(raw.index.values, dtype=float)
        signal = np.asarray(raw.values, dtype=float)

        if self._resampling_frequency_hz is not None:
            fs = self._resampling_frequency_hz
        else:
            fs = 1.0 / float(np.median(np.diff(timestamps)))
        dt = 1.0 / fs
        t_uniform = np.arange(timestamps[0], timestamps[-1], dt)

        interp_func = interp1d(timestamps, signal, kind="linear", bounds_error=False, fill_value="extrapolate")
        y_uniform = interp_func(t_uniform)

        b_band, a_band = butter(2, [0.2, 20], "bandpass", fs=fs)
        y_filtered = filtfilt(b_band, a_band, y_uniform)

        return pd.Series(y_filtered, index=t_uniform), fs
