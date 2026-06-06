import logging

import contraqctor.contract as data_contract
import pynwb
from ndx_events import NdxEventsNWBFile

from .._base import AbstractProcessor
from . import helper

logger = logging.getLogger(__name__)


class AcquisitionProcessor(AbstractProcessor):
    def __init__(self, dataset: data_contract.Dataset, *, raise_on_error: bool = False) -> None:
        super().__init__(dataset, raise_on_error=raise_on_error)

    def process(self, nwb_file: NdxEventsNWBFile) -> NdxEventsNWBFile:
        _ = self._dataset.load_all(strict=False)

        for stream in self._dataset.iter_all():
            if stream.is_collection:  # only process leaf nodes into nwb
                err = stream.collect_errors()
                if err:
                    logger.debug(f"Collection stream {stream.name} has errors: {err}")
                    if self.raise_on_error:
                        raise ValueError(f"Collection stream {stream.name} has errors: {err}")
                continue

            name = stream.resolved_name.replace("::", ".")
            try:
                if stream.has_error:
                    logger.debug(f"Stream {stream.name} has error: {stream.collect_errors()}")
                    if self.raise_on_error:
                        raise ValueError(f"Stream {stream.name} has error: {stream.collect_errors()}")
                if isinstance(stream, (data_contract.harp.HarpRegister, data_contract.csv.Csv)):
                    dynamic_table = pynwb.core.DynamicTable.from_dataframe(
                        name=name,
                        table_description=stream.description,
                        df=stream.data.reset_index(),
                    )
                    nwb_file.add_acquisition(dynamic_table)
                elif isinstance(stream, (data_contract.json.SoftwareEvents)):
                    data = helper.clean_dataframe_for_nwb(stream.data.reset_index())
                    dynamic_table = pynwb.core.DynamicTable.from_dataframe(
                        name=name, table_description=stream.description, df=data
                    )
                    nwb_file.add_acquisition(dynamic_table)

                elif isinstance(stream, data_contract.json.PydanticModel):
                    nwb_file.add_acquisition(
                        pynwb.core.DynamicTable(
                            name=name,
                            description=stream.data.model_dump_json(),
                        )
                    )
                else:
                    raise ValueError(f"Stream {stream.name} has unsupported type {type(stream)}, skipping.")
            except Exception as e:
                if self.raise_on_error:
                    logger.debug(f"Error processing stream {stream.name}: {e}")
                    raise
                else:
                    logger.debug(f"Error processing stream {stream.name}: {e}")
        return nwb_file
