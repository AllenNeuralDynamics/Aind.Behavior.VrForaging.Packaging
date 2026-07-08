import logging
import typing as t

import contraqctor
import numpy as np
import pandas as pd

from .._base import AbstractProcessor
from ._helper import nearest_positions, parse_force_reward, parse_reward_metadata, parse_water_delivery

logger = logging.getLogger(__name__)


class EventsProcessor(AbstractProcessor):
    """Collects derived/computed events into a single tall table, alongside SoftwareEventsProcessor's raw streams.

    Each row is one event, with columns ``event_name`` (str) and ``data`` (the event's payload),
    indexed by ``timestamp`` (harp seconds). Unlike SoftwareEventsProcessor, sources here are
    computed from one or more underlying streams rather than being a straight passthrough.

    To add a new event source: write a ``@staticmethod(dataset) -> pd.DataFrame`` returning a
    ``data``-column frame indexed by timestamp, and append it to ``_EVENT_SOURCES``.
    """

    __output_name__ = "events"

    @staticmethod
    def _parse_manual_water_delivery(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        """Forced/manual reward events (``ForceGiveReward``), re-timestamped to their valve-open time.

        ``GiveReward`` deliveries claim their nearest valve open; each ``ForceGiveReward`` is then
        matched to its nearest remaining one, recovering a hardware time for the software event.
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

    _EVENT_SOURCES: t.ClassVar[list[tuple[str, t.Callable[[contraqctor.contract.Dataset], pd.DataFrame]]]] = [
        ("ManualWaterDelivery", _parse_manual_water_delivery),
    ]

    def _compute(self) -> pd.DataFrame:
        """Returns all derived events sorted by timestamp.

        Returns
        -------
        pd.DataFrame
            Index ``"timestamp"`` (harp seconds). Columns: ``event_name``, ``data``.
        """
        frames: list[pd.DataFrame] = []
        for name, source in self._EVENT_SOURCES:
            try:
                df = source(self.dataset)
            except Exception as exc:
                if self.raise_on_error:
                    raise
                logger.warning("Skipping event source %s: %s", name, exc)
                continue
            if df.empty:
                continue
            frames.append(pd.DataFrame({"event_name": name, "data": df["data"]}, index=df.index))

        if not frames:
            empty = pd.DataFrame(columns=["event_name", "data"])
            empty.index.name = "timestamp"
            return empty

        result = pd.concat(frames).sort_index()
        result.index.name = "timestamp"
        return result
