import json
import logging
import typing as ty

import pandas as pd

from .._base import AbstractProcessor, cached_frame

logger = logging.getLogger(__name__)


class SoftwareEventsProcessor(AbstractProcessor):
    """Events logged by the task software, one row per event.

    All SoftwareEvents streams are collected into a single tall DataFrame.

    Software timestamps are low precision; do not use them to align to physiology data.

    | Column | Type | Unit | Description |
    | --- | --- | --- | --- |
    | `timestamp` | `float` | second | Harp time. |
    | `event_name` | `string` | | Name of the event stream, e.g. `ActiveSite`. |
    | `data` | `string` | | JSON-serialized payload. Its structure depends on the event, so parse it per event name. |

    The ``data`` column is polymorphic. Parse it back with::

          df["data"].apply(json.loads)
          # or flatten a specific event type:
          active_sites = df[df["event_name"] == "ActiveSite"]
          pd.json_normalize(active_sites["data"].apply(json.loads).tolist())

    Rows are sorted by harp timestamp (the DataFrame index, named ``"timestamp"``).
    """

    __output_name__ = "software_events"

    @cached_frame
    def _compute(self) -> pd.DataFrame:
        """Returns all software events sorted by timestamp.

        Returns
        -------
        pd.DataFrame
            Index ``"timestamp"`` (harp seconds). Columns: ``event_name``, ``data``.
        """
        import contraqctor.contract as _dc

        sw_collection: ty.Any = self._dataset.at("Behavior").at("SoftwareEvents")
        sw_collection.load_all(strict=False)

        frames: list[pd.DataFrame] = []
        for stream in sw_collection.iter_all():
            if stream.is_collection:
                continue
            if stream.has_error:
                if self._strict_parsing:
                    raise ValueError(f"Stream {stream.name} error: {stream.collect_errors()}")
                logger.debug("Skipping %s: %s", stream.name, stream.collect_errors())
                continue
            if not isinstance(stream, _dc.json.SoftwareEvents):
                continue

            df = ty.cast(pd.DataFrame, stream.data)
            if df.empty:
                # An event type that never fired contributes no rows, and its frame
                # may not even carry a "data" column. Not an error.
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "event_name": stream.name,
                        "data": df["data"].apply(lambda d: json.dumps(d, default=str)),
                    },
                    index=df.index,
                )
            )

        if not frames:
            empty = pd.DataFrame(columns=["event_name", "data"])
            empty.index.name = "timestamp"
            return empty

        result = pd.concat(frames).sort_index()
        result.index.name = "timestamp"
        return result

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Add each SoftwareEvents stream as a separate DynamicTable acquisition.

        The NWB representation keeps one table per event type (mirroring the
        original stream structure) rather than the single tall table produced by
        ``compute()``.  The ``data`` column is JSON-serialized to remain
        compatible with NWB's string dtypes.
        """
        import json as _json

        import contraqctor.contract as _dc
        import pynwb

        from ..acquisition.helper import clean_dataframe_for_nwb

        sw_collection: ty.Any = self._dataset.at("Behavior").at("SoftwareEvents")
        sw_collection.load_all(strict=False)

        for stream in sw_collection.iter_all():
            if stream.is_collection:
                continue
            if stream.has_error:
                if self._strict_parsing:
                    raise ValueError(f"Stream {stream.name} error: {stream.collect_errors()}")
                logger.debug("Skipping %s: %s", stream.name, stream.collect_errors())
                continue
            if not isinstance(stream, _dc.json.SoftwareEvents):
                continue

            name = stream.resolved_name.replace("::", ".")
            df = ty.cast(pd.DataFrame, stream.data).copy()
            if df.empty:
                continue
            df["data"] = df["data"].apply(lambda d: _json.dumps(d, default=str))
            table = pynwb.core.DynamicTable.from_dataframe(
                name=name,
                table_description=stream.description,
                df=clean_dataframe_for_nwb(df.reset_index()),
            )
            nwb_file.add_acquisition(table)

        return nwb_file
