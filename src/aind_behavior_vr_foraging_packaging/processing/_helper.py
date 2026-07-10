import logging
import typing as t

import contraqctor
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


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


def parse_manual_water_delivery(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
    """Forced/manual reward events (``ForceGiveReward``), re-timestamped to their valve-open time.

    ``GiveReward`` deliveries claim their nearest valve open; each ``ForceGiveReward`` is then
    matched to its nearest remaining one, recovering a hardware time for the software event.

    This is the single source of truth for forced/manual rewards: :class:`EventsProcessor` emits
    these rows as ``ManualWaterDelivery`` events, and :class:`TrialTableProcessor` derives
    ``Site.has_forced_rewards`` by binning these hardware times into site intervals. Returns a
    ``data``-column frame indexed by hardware valve-open time (harp seconds), or an empty frame
    when there are no forced rewards / no valve openings to match.
    """
    force_reward = parse_force_reward(dataset)
    if force_reward.empty:
        return pd.DataFrame(columns=["data"])

    valve_opens = np.sort(parse_water_delivery(dataset).index.to_numpy(dtype=float))
    if valve_opens.size == 0:
        return pd.DataFrame(columns=["data"])

    # Step 1: account for automatic deliveries -- mark their nearest valve opens as consumed.
    reward_metadata = parse_reward_metadata(dataset)
    auto_times = reward_metadata.index[reward_metadata["data"].fillna(0) != 0].to_numpy(dtype=float)
    consumed = np.zeros(valve_opens.size, dtype=bool)
    if auto_times.size:
        consumed[nearest_positions(valve_opens, auto_times)] = True

    # Step 2: whatever valve opens are left get matched to the forced-reward events.
    leftover = valve_opens[~consumed]
    if leftover.size == 0:
        logger.warning("Found %d ForceGiveReward event(s) but no unclaimed valve openings.", len(force_reward))
        return pd.DataFrame(columns=["data"])

    matched = force_reward.copy()
    matched.index = leftover[nearest_positions(leftover, force_reward.index.to_numpy(dtype=float))]
    matched = matched[~matched.index.duplicated(keep="first")].sort_index()
    if len(matched) != len(force_reward):
        logger.warning(
            "Matched %d ForceGiveReward event(s) to %d distinct valve opening(s).", len(force_reward), len(matched)
        )
    return matched


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
