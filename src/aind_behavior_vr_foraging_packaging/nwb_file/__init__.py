import logging
from pathlib import Path
from typing import Optional

import aind_behavior_vr_foraging.data_contract
import contraqctor.contract as data_contract
from aind_nwb_utils.utils import create_base_nwb_file
from hdmf_zarr import NWBZarrIO
from pynwb import NWBFile

from .._base import AbstractProcessor
from .._provenance import PackagingProvenance

logger = logging.getLogger(__name__)


class NwbSession:
    def __init__(
        self,
        root_path: Path,
        *,
        dataset: Optional[data_contract.Dataset] = None,
        base_nwb_file: Optional[NWBFile] = None,
    ) -> None:
        self._root_path = root_path
        self._dataset = dataset if dataset else aind_behavior_vr_foraging.data_contract.dataset(root_path)
        self._base_nwb_file = base_nwb_file
        self._nwb_file: Optional[NWBFile] = None

    @property
    def dataset(self) -> data_contract.Dataset:
        return self._dataset

    @property
    def root_path(self) -> Path:
        return self._root_path

    @property
    def nwb_file(self) -> NWBFile:
        if self._nwb_file is None:
            raise ValueError("NWB file has not been created yet. Call process() to create it before accessing.")
        return self._nwb_file

    def process(self) -> NWBFile:
        if self._nwb_file is None:
            self._nwb_file = self._create_nwb_file()
        return self._nwb_file

    def run(self, *processors: AbstractProcessor) -> NWBFile:
        nwb = self.process()
        logging.info("Running %s processors on NWB file...", len(processors))
        for processor in processors:
            logging.info("Running nwbize: %s", processor.__class__.__name__)
            nwb = processor.nwbize(nwb)
        return nwb

    @property
    def provenance(self) -> dict[str, str]:
        """Versions describing how this file was produced, keyed as in ``df.attrs``."""
        return PackagingProvenance.build(self._dataset).model_dump()

    def _create_nwb_file(self) -> NWBFile:
        nwb_file = self._base_nwb_file if self._base_nwb_file is not None else create_base_nwb_file(self.root_path)
        # Provenance otherwise only reaches the parquet outputs (df.attrs). The same
        # versions land here, so the two outputs of a session can be checked against
        # each other. was_generated_by is write-once, and create_base_nwb_file has
        # already set it to aind-nwb-utils' own entry, so extend it in place.
        entries = [[key, value] for key, value in self.provenance.items()]
        if nwb_file.was_generated_by is None:
            nwb_file.was_generated_by = entries
        else:
            nwb_file.was_generated_by.extend(entries)
        return nwb_file

    def write_nwb_zarr(self, output: Path) -> None:
        if self._nwb_file is None:
            raise ValueError("NWB file has not been created yet. Call process() to create it before writing.")

        with NWBZarrIO(Path(output).as_posix(), "w") as io:
            io.write(self._nwb_file)
        logger.info(f"NWB zarr successfully written to path {output}")
