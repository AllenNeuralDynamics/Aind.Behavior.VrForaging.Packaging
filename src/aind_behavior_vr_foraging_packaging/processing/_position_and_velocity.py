import logging
import typing as ty

import contraqctor.contract
import numpy as np
import pandas as pd
from pydantic import BaseModel

from .._base import AbstractProcessor, cached_frame

logger = logging.getLogger(__name__)


class PositionAndVelocityProcessor(AbstractProcessor):
    __output_name__ = "position_velocity"

    def __init__(self, dataset: contraqctor.contract.Dataset, *, sampling_rate_hz: float | None = None, **kwargs):
        super().__init__(dataset=dataset, **kwargs)
        self._sampling_rate_hz = sampling_rate_hz

    @cached_frame
    def _compute(self) -> pd.DataFrame:
        """Returns DataFrame with 'position' (cm) and 'velocity' (cm/s) indexed by harp time."""
        return self.compute_position_and_velocity(self.dataset, downsample_to_hz=self._sampling_rate_hz)

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Add a single ``position_velocity`` DynamicTable to *nwb_file*.

        The table has three columns: ``timestamp`` (harp time, seconds), ``position`` (cm) and
        ``velocity`` (cm/s).
        """
        from pynwb.base import ProcessingModule
        from pynwb.core import DynamicTable

        module = nwb_file.processing.get("behavior")
        if module is None:
            module = ProcessingModule(name="behavior", description="Processing module for behavior data")
            nwb_file.add_processing_module(module)

        df = self.compute()
        table = DynamicTable.from_dataframe(
            name=self.__output_name__,
            table_description="Treadmill-derived position (cm) and velocity (cm/s) by harp timestamp (s)",
            df=pd.DataFrame(
                {
                    "timestamp": df.index.values,
                    "position": df["position"].values,
                    "velocity": df["velocity"].values,
                }
            ),
        )
        module.add(table)
        return nwb_file

    def compute_position_and_velocity(
        self, dataset: contraqctor.contract.Dataset, *, downsample_to_hz: float | None
    ) -> pd.DataFrame:
        """Computes position and velocity from treadmill encoder data"""
        dataset.at("Behavior").at("InputSchemas").load_all()

        rig_settings = dataset.at("Behavior").at("InputSchemas").at("Rig").load().data
        rig_settings = rig_settings.model_dump() if isinstance(rig_settings, BaseModel) else rig_settings

        try:
            df = self.compute_position_and_velocity_from_treadmill(dataset, rig_settings)
        except KeyError as e:
            e.add_note(
                "Missing calibration data for HarpTreadmill in rig settings. Cannot compute position and velocity."
            )
            raise

        if downsample_to_hz is None:
            return df

        df.sort_index(inplace=True)
        df.index = pd.to_timedelta(df.index, unit="s")
        dt = pd.to_timedelta(1.0 / downsample_to_hz, unit="s")
        df = df.resample(dt, label="right", closed="right").mean()
        df.dropna(inplace=True)
        df.index = df.index.total_seconds()  # Convert back to harp time!

        return df

    @staticmethod
    def compute_position_and_velocity_from_treadmill(
        dataset: contraqctor.contract.Dataset,
        rig_config: dict,
    ) -> pd.DataFrame:
        """Compute position and velocity from treadmill encoder data.

        Args:
            dataset: A contraqctor Dataset providing access to HarpTreadmill data.
            rig_config: Rig configuration dict. Must contain a
                ``harp_treadmill.calibration`` entry with ``wheel_diameter``
                (cm), ``pulses_per_revolution``, and ``invert_direction``.

        Returns:
            DataFrame with ``position`` (cm) and ``velocity`` (cm/s) columns,
            indexed by harp timestamp (seconds).
        """
        calibration = rig_config.get("harp_treadmill", {}).get("calibration")
        if calibration is None:
            raise KeyError("Missing harp_treadmill.calibration in rig_config.")
        calibration = calibration.get("output", calibration)
        wheel_diameter: float = calibration["wheel_diameter"]
        pulses_per_revolution: float = calibration["pulses_per_revolution"]
        invert_direction: bool = calibration["invert_direction"]
        converting_factor = wheel_diameter * np.pi / pulses_per_revolution * (-1 if invert_direction else 1)
        treadmill_data = ty.cast(
            pd.DataFrame,
            dataset.at("Behavior").at("HarpTreadmill").load().at("SensorData").load().data,
        )
        encoder = treadmill_data.query("MessageType == 'EVENT'")["Encoder"].copy()
        position = (encoder - encoder.iloc[0]) * converting_factor
        displacement = position.diff().fillna(0)
        velocity = displacement / position.index.to_series().diff().fillna(1)
        return pd.DataFrame({"position": position, "velocity": velocity})
