import logging
import typing as ty

import contraqctor.contract
import numpy as np
import pandas as pd

from .._base import AbstractProcessor

logger = logging.getLogger(__name__)


class LicksProcessor(AbstractProcessor):
    __output_name__ = "licks"

    def __init__(self, dataset: contraqctor.contract.Dataset, *, refractory_period_s: float | None = 0.01, **kwargs):
        super().__init__(dataset=dataset, **kwargs)
        self._refractory_period_s = refractory_period_s

    def compute(self) -> pd.DataFrame:
        """Returns DataFrame with 'is_lick_onset' (bool) indexed by harp time."""
        licks = self._compute_lick_state(self.dataset)
        return licks.rename("is_lick_onset").to_frame()

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Add lick TimeSeries to *nwb_file*."""
        from pynwb import TimeSeries
        from pynwb.base import ProcessingModule

        _nwb = ty.cast(ty.Any, nwb_file)
        module = _nwb.processing.get("behavior")
        if module is None:
            module = ProcessingModule(name="behavior", description="Processing module for behavior data")
            _nwb.add_processing_module(module)

        df = self.compute()
        module.add(
            TimeSeries(
                name="licks",
                data=df["is_lick_onset"].values,
                unit="n/a",
                timestamps=df.index.values,
                description="Lick onset/offset transitions (True = lick onset, False = lick offset).",
            )
        )
        return nwb_file

    def _compute_lick_state(self, dataset: contraqctor.contract.Dataset) -> pd.Series:
        """Load the lickometer state and compute the lick onset/offset series.

        Args:
            dataset: A contraqctor Dataset providing access to the
                ``HarpLickometer`` device.

        Returns:
            A boolean Series named ``"IsLickOnset"`` indexed by harp timestamp
            (seconds), where ``True`` marks a lick onset and ``False`` a lick
            offset.
        """
        data = ty.cast(
            pd.DataFrame,
            dataset.at("Behavior").at("HarpLickometer").load().at("LickState").load().data,
        )
        data = data[data["MessageType"] == "EVENT"]
        return self._lick_onsets_from_state(data["Channel0"].astype(bool), self._refractory_period_s)

    @staticmethod
    def _lick_onsets_from_state(lick_state: pd.Series, refractory_period_s: float | None) -> pd.Series:
        """Compute the lick onset/offset transition series from a boolean lick state.

        Only distinct state changes are kept, so the resulting boolean series
        alternates between ``True`` (lick onset) and ``False`` (lick offset),
        starting on the first onset, as done in ``contraqctor.qc.harp.lickety_split``.
        Lick onsets whose gap to the preceding onset is below
        ``refractory_period_s`` are treated as spurious double-detections and
        removed together with their paired offset.

        Args:
            lick_state: A boolean Series of the raw lick state (``True`` while a
                lick is detected) indexed by harp timestamp (seconds).
            refractory_period_s: Minimum spacing between onsets; ``None`` or ``0``
                disables refractory filtering.

        Returns:
            A boolean Series named ``"IsLickOnset"`` indexed by harp timestamp
            (seconds), where ``True`` marks a lick onset and ``False`` a lick
            offset.
        """
        # Keep only distinct state transitions: True = lick onset, False = lick offset.
        is_onset = lick_state[lick_state != lick_state.shift()].astype(bool)
        is_onset.name = "IsLickOnset"

        # Start the series on the first lick onset so it begins with an onset.
        onset_positions = np.flatnonzero(is_onset.values)
        if len(onset_positions) == 0:
            return is_onset.iloc[:0]
        is_onset = is_onset.iloc[onset_positions[0] :]

        if not refractory_period_s:
            return is_onset

        flags = is_onset.values
        onset_positions = np.flatnonzero(flags)
        onset_times = is_onset.index.values[onset_positions]
        violating = np.flatnonzero(np.diff(onset_times) < refractory_period_s) + 1
        if len(violating) == 0:
            return is_onset

        keep = np.ones(len(flags), dtype=bool)
        bad_onsets = onset_positions[violating]
        keep[bad_onsets] = False

        paired_offsets = bad_onsets + 1
        paired_offsets = paired_offsets[paired_offsets < len(flags)]
        paired_offsets = paired_offsets[~flags[paired_offsets]]
        keep[paired_offsets] = False

        return is_onset.iloc[keep]
