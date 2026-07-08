import typing as t

import contraqctor
import numpy as np
import pandas as pd


def parse_water_delivery(dataset: contraqctor.contract.Dataset) -> pd.Series:
    """Hardware valve-open times (``HarpBehavior/OutputSet``, ``SupplyPort0`` writes)."""
    water_delivery = dataset.at("Behavior").at("HarpBehavior").load().at("OutputSet").load().data
    water_delivery = water_delivery[(water_delivery["MessageType"] == "WRITE") & (water_delivery["SupplyPort0"])][
        "SupplyPort0"
    ]
    return water_delivery


def parse_reward_metadata(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
    """Automatic (contingent) reward deliveries (``SoftwareEvents/GiveReward``)."""
    return dataset.at("Behavior").at("SoftwareEvents").at("GiveReward").load().data


def parse_force_reward(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
    """Forced/manual reward events (``SoftwareEvents/ForceGiveReward``).

    Not a declared node at all in pre-0.6.0 schemas (``KeyError`` from ``.at()``); guarded the same
    as a declared-but-missing file (``FileNotFoundError``), since either way there's no data to read.
    """
    try:
        stream = dataset.at("Behavior").at("SoftwareEvents").at("ForceGiveReward").load()
    except (KeyError, FileNotFoundError):
        return pd.DataFrame(columns=["data"])
    return stream.data if stream.has_data else pd.DataFrame(columns=["data"])


def nearest_positions(sorted_values: np.ndarray, query: np.ndarray) -> np.ndarray:
    """Position in ``sorted_values`` (ascending, non-empty) nearest each ``query``; ties go earlier."""
    right = np.searchsorted(sorted_values, query, side="left")
    left = np.clip(right - 1, 0, sorted_values.size - 1)
    right = np.clip(right, 0, sorted_values.size - 1)
    use_left = np.abs(sorted_values[left] - query) <= np.abs(sorted_values[right] - query)
    return np.where(use_left, left, right)


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
        idxs = nearest_positions(df_index, timestamps)
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
