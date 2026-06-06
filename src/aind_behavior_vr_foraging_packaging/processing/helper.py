import typing as t

import numpy as np
import pandas as pd


def compute_position_and_velocity_from_treadmill(
    dataset: t.Any,
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
    treadmill_data = t.cast(
        pd.DataFrame,
        dataset.at("Behavior").at("HarpTreadmill").load().at("SensorData").load().data,
    )
    encoder = treadmill_data.query("MessageType == 'EVENT'")["Encoder"].copy()
    position = (encoder - encoder.iloc[0]) * converting_factor
    displacement = position.diff().fillna(0)
    velocity = displacement / position.index.to_series().diff().fillna(1)
    return pd.DataFrame({"position": position, "velocity": velocity})


def get_closest_from_timestamp(
    timestamps: np.ndarray,
    df: pd.DataFrame | pd.Series,
    *,
    search_mode: t.Literal["closest", "next", "previous"] = "closest",
) -> np.ndarray:
    """
    For each timestamp in `timestamps`, find the index in df.index that is:
      - 'closest': closest in value
      - 'next': the first index >= timestamp
      - 'previous': the last index <= timestamp

    Returns an array of indices from df.index.
    """
    df_index = np.asarray(df.index.values)

    # Use numpy searchsorted for efficient lookup
    timestamps = np.asarray(timestamps)
    if search_mode == "closest":
        idx_left = np.searchsorted(df_index, timestamps, side="left")
        idx_right = np.clip(idx_left - 1, 0, len(df_index) - 1)
        idx_left = np.clip(idx_left, 0, len(df_index) - 1)
        left_diff = np.abs(df_index[idx_left] - timestamps)
        right_diff = np.abs(df_index[idx_right] - timestamps)
        use_left = left_diff <= right_diff
        idxs = np.where(use_left, idx_left, idx_right)
    elif search_mode == "next":
        idxs = np.searchsorted(df_index, timestamps, side="left")
        idxs = np.clip(idxs, 0, len(df_index) - 1)
    elif search_mode == "previous":
        idxs = np.searchsorted(df_index, timestamps, side="right") - 1
        idxs = np.clip(idxs, 0, len(df_index) - 1)
    else:
        raise ValueError(f"Unknown search_mode: {search_mode}")
    return df.index[idxs]


_TSliceable = t.TypeVar("_TSliceable", pd.DataFrame, pd.Series)


def slice_by_index(df: _TSliceable, start_time: float, end_time: float, *, end_inclusive: bool = False) -> _TSliceable:
    """
    Subsets the DataFrame to only include rows within the specified range.
    Assumes the DataFrame index is a datetime-like index.
    """
    _df = t.cast(t.Any, df)
    end_mask = df.index <= end_time if end_inclusive else df.index < end_time
    return _df[(df.index >= start_time) & end_mask]
