import logging
import typing as t

import contraqctor
import pandas as pd

from .._base import AbstractProcessor
from ._helper import parse_manual_water_delivery

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

    _EVENT_SOURCES: t.ClassVar[list[tuple[str, t.Callable[[contraqctor.contract.Dataset], pd.DataFrame]]]] = [
        ("ManualWaterDelivery", parse_manual_water_delivery),
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

    def nwbize(self, nwb_file: t.Any) -> t.Any:
        """Add the derived events to *nwb_file* as an ndx-events ``EventsTable``.

        The tall ``compute()`` frame maps one-to-one onto the table: the index becomes the required
        ``timestamp`` column, and ``event_name``/``data`` become columns. ``data`` is JSON-serialized
        to stay within NWB's string dtypes. No table is added when there are no derived events.
        """
        import json

        from ndx_events import EventsTable

        df = self.compute()
        if df.empty:
            return nwb_file

        table = EventsTable(name="events", description="Events derived/computed from one or more raw streams.")
        table.add_column(name="event_name", description="Name of the derived event source.")
        table.add_column(name="data", description="JSON-serialized event payload.")
        for timestamp, row in df.iterrows():
            table.add_row(
                data={
                    "timestamp": float(t.cast(float, timestamp)),
                    "event_name": str(row["event_name"]),
                    "data": json.dumps(row["data"], default=str),
                }
            )

        nwb_file.add_events_table(table)
        return nwb_file
