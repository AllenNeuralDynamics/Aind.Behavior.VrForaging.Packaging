import logging
import typing as ty

import contraqctor.contract
import numpy as np
import pandas as pd
from pydantic import BaseModel

from ._position_and_velocity import PositionAndVelocityProcessor

logger = logging.getLogger(__name__)


class LegacyPositionAndVelocityProcessor(PositionAndVelocityProcessor):
    """PositionAndVelocityProcessor for VR foraging datasets with schema version < 0.6.0.

    The only difference from PositionAndVelocityProcessor is support for v0.3
    datasets where no HarpTreadmill device exists. In those sessions the encoder
    was wired to HarpBehavior.AnalogData.Encoder (already-differential counts at
    1 kHz) and rig calibration lives under ``treadmill.settings`` rather than
    ``harp_treadmill.calibration``.

    For v0.4+ datasets this processor is identical to PositionAndVelocityProcessor:
    it uses HarpTreadmill.SensorData via the parent static method unchanged.
    """

    def compute_position_and_velocity(
        self,
        dataset: contraqctor.contract.Dataset,
        *,
        downsample_to_hz: ty.Optional[float] = 250.0,
    ) -> pd.DataFrame:
        dataset.at("Behavior").at("InputSchemas").load_all()
        rig_settings = dataset.at("Behavior").at("InputSchemas").at("Rig").load().data
        rig_settings = rig_settings.model_dump() if isinstance(rig_settings, BaseModel) else rig_settings

        try:
            # v0.4+ path: delegate entirely to the parent static method (no changes)
            df = self.compute_position_and_velocity_from_treadmill(dataset, rig_settings)
        except (KeyError, FileNotFoundError):
            # v0.3 path: no HarpTreadmill; encoder lives in HarpBehavior.AnalogData
            logger.info("HarpTreadmill not available; falling back to HarpBehavior.AnalogData (v0.3 encoder path).")
            df = self.compute_position_and_velocity_from_analog_data(dataset, rig_settings)

        if downsample_to_hz is None:
            return df

        df.sort_index(inplace=True)
        df.index = pd.to_timedelta(df.index, unit="s")
        dt = pd.to_timedelta(1.0 / downsample_to_hz, unit="s")
        df = df.resample(dt, label="right", closed="right").mean()
        df.dropna(inplace=True)
        df.index = df.index.total_seconds()
        return df

    @staticmethod
    def compute_position_and_velocity_from_analog_data(
        dataset: contraqctor.contract.Dataset,
        rig_config: dict,
    ) -> pd.DataFrame:
        """Compute position and velocity from HarpBehavior.AnalogData (v0.3 datasets).

        AnalogData.Encoder contains already-differential counts timestamped per read.
        Velocity and position are computed the same way as the parent's treadmill
        path: displacement / actual dt, using the harp timestamps directly.

        Args:
            dataset: Dataset providing access to HarpBehavior data.
            rig_config: Rig config dict. Supports the ``treadmill.settings``
                nesting used in v0.3 as well as the current ``harp_treadmill.calibration``
                schema, with both snake_case and camelCase key aliases.

        Returns:
            DataFrame with ``position`` (cm) and ``velocity`` (cm/s) columns,
            indexed by harp timestamp (seconds).
        """
        wheel_diameter, ppr, invert = _extract_legacy_treadmill_calibration(rig_config)
        converter = wheel_diameter * np.pi / ppr * (-1 if invert else 1)

        analog_data = ty.cast(
            pd.DataFrame,
            dataset.at("Behavior").at("HarpBehavior").load().at("AnalogData").load().data,
        )
        encoder = analog_data[analog_data["MessageType"] == "EVENT"]["Encoder"].astype(float)

        # Already-differential counts: displacement per sample
        displacement = encoder * converter
        position = displacement.cumsum()
        position -= position.iloc[0]
        velocity = displacement / encoder.index.to_series().diff().fillna(1)

        return pd.DataFrame({"position": position.values, "velocity": velocity.values}, index=encoder.index)


def _extract_legacy_treadmill_calibration(rig_config: dict) -> tuple[float, float, bool]:
    """Extract (wheel_diameter, pulses_per_revolution, invert_direction) from a legacy rig config.

    Handles multiple nesting schemas found across rig versions:
      - v0.3: ``treadmill.settings.*``
      - v0.4: ``harp_treadmill.calibration.*`` (flat, no ``.output`` wrapper)
      - v0.5+: ``harp_treadmill.calibration.output.*``
    Also accepts camelCase aliases (``wheelDiameter``, ``pulsesPerRevolution``, ``invertDirection``).
    """
    cal = (
        rig_config.get("harp_treadmill", {}).get("calibration", {}).get("output")
        or rig_config.get("harp_treadmill", {}).get("calibration")
        or rig_config.get("treadmill", {}).get("settings")
        or rig_config.get("treadmill", {})
    )
    if not cal:
        raise KeyError(
            "Could not find treadmill calibration in rig config. "
            "Checked: harp_treadmill.calibration[.output], treadmill.settings"
        )

    wheel_diameter = cal.get("wheel_diameter") or cal.get("wheelDiameter")
    ppr = cal.get("pulses_per_revolution") or cal.get("pulsesPerRevolution")
    invert = cal.get("invert_direction") or cal.get("invertDirection") or False

    if wheel_diameter is None or ppr is None:
        raise KeyError(f"Missing wheel_diameter or pulses_per_revolution in calibration: {cal}")

    return float(wheel_diameter), float(ppr), bool(invert)
