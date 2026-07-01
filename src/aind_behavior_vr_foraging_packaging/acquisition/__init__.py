import logging
import typing as ty

import contraqctor.contract as data_contract
import pandas as pd

from .._base import AbstractProcessor

logger = logging.getLogger(__name__)


class AcquisitionProcessor(AbstractProcessor):
    __output_name__ = "acquisition"

    def __init__(self, dataset: data_contract.Dataset, *, raise_on_error: bool = False) -> None:
        super().__init__(dataset, raise_on_error=raise_on_error)

    def compute(self) -> pd.DataFrame:
        """Returns a tall DataFrame of all acquisition streams.

        Includes a ``_stream_name`` column (e.g. ``"Behavior.HarpBehavior.PwmStart"``)
        so the parquet can be filtered per stream downstream.
        """
        _ = self._dataset.load_all(strict=False)
        frames = []
        for stream in self._dataset.iter_all():
            if stream.is_collection or stream.has_error:
                if stream.is_collection:
                    continue
                logger.debug("Stream %s has error: %s", stream.name, stream.collect_errors())
                if self._raise_on_error:
                    raise ValueError(f"Stream {stream.name} has error")
                continue
            name = stream.resolved_name.replace("::", ".")
            try:
                df = stream.data.reset_index()
                df["_stream_name"] = name
                frames.append(df)
            except Exception as exc:
                if self._raise_on_error:
                    raise
                logger.debug("Could not load stream %s: %s", stream.name, exc)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Add all acquisition streams as DynamicTables to *nwb_file*."""
        import contraqctor.contract as data_contract
        import pynwb

        from .helper import clean_dataframe_for_nwb

        _ = self._dataset.load_all(strict=False)
        for stream in self._dataset.iter_all():
            if stream.is_collection:
                err = stream.collect_errors()
                if err:
                    logger.debug("Collection stream %s has errors: %s", stream.name, err)
                    if self._raise_on_error:
                        raise ValueError(f"Collection stream {stream.name} has errors: {err}")
                continue

            name = stream.resolved_name.replace("::", ".")
            try:
                if stream.has_error:
                    logger.debug("Stream %s has error: %s", stream.name, stream.collect_errors())
                    if self._raise_on_error:
                        raise ValueError(f"Stream {stream.name} has error: {stream.collect_errors()}")
                    continue
                if isinstance(stream, (data_contract.harp.HarpRegister, data_contract.csv.Csv)):
                    table = pynwb.core.DynamicTable.from_dataframe(
                        name=name, table_description=stream.description, df=stream.data.reset_index()
                    )
                    nwb_file.add_acquisition(table)
                elif isinstance(stream, data_contract.json.SoftwareEvents):
                    table = pynwb.core.DynamicTable.from_dataframe(
                        name=name,
                        table_description=stream.description,
                        df=clean_dataframe_for_nwb(stream.data.reset_index()),
                    )
                    nwb_file.add_acquisition(table)
                elif isinstance(stream, data_contract.json.PydanticModel):
                    nwb_file.add_acquisition(
                        pynwb.core.DynamicTable(
                            name=name,
                            description=stream.data.model_dump_json(),
                        )
                    )
                else:
                    raise ValueError(f"Stream {stream.name} has unsupported type {type(stream)}")
            except Exception as exc:
                if self._raise_on_error:
                    raise
                logger.debug("Error processing stream %s: %s", stream.name, exc)
        return nwb_file
