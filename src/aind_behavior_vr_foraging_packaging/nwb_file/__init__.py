import dataclasses
import logging
from pathlib import Path
from typing import Optional

import aind_behavior_vr_foraging.data_contract
import contraqctor.contract as data_contract
import semver
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.subject import Subject
from aind_nwb_utils.utils import get_subject_nwb_object
from hdmf_zarr import NWBZarrIO
from ndx_events import NdxEventsNWBFile

from .._base import AbstractProcessor

logger = logging.getLogger(__name__)


class NwbSession:
    def __init__(self, root_path: Path, *, dataset: Optional[data_contract.Dataset] = None) -> None:
        self._root_path = root_path
        self._dataset = dataset if dataset else aind_behavior_vr_foraging.data_contract.dataset(root_path)
        self._aind_data_schema = self._get_aind_data_schema_json()
        self._nwb_file: Optional[NdxEventsNWBFile] = None

    @property
    def dataset(self) -> data_contract.Dataset:
        return self._dataset

    @property
    def dataset_version(self) -> semver.Version:
        return semver.Version.parse(str(self._dataset.version))

    @property
    def aind_data_schema(self) -> "_AindDataSchemaJson":
        return self._aind_data_schema

    @property
    def root_path(self) -> Path:
        return self._root_path

    @property
    def nwb_file(self) -> NdxEventsNWBFile:
        if self._nwb_file is None:
            raise ValueError("NWB file has not been created yet. Call process() to create it before accessing.")
        return self._nwb_file

    def process(self) -> NdxEventsNWBFile:
        if self._nwb_file is None:
            self._nwb_file = self._create_nwb_file()
        return self._nwb_file

    def run(self, *processors: AbstractProcessor) -> NdxEventsNWBFile:
        nwb = self.process()
        logging.info("Running %s processors on NWB file...", len(processors))
        for processor in processors:
            logging.info("Running processor %s...", processor.__class__.__name__)
            nwb = processor.process(nwb)
        return nwb

    def _get_aind_data_schema_json(self) -> "_AindDataSchemaJson":
        jsons = _AindDataSchemaJson.from_doc_db(Path(self.root_path).name)
        # jsons = _AindDataSchemaJson.from_root_path(self.root_path)
        logger.debug("Found primary data %s", jsons.data_description.name)
        return jsons

    def _create_nwb_file(self) -> NdxEventsNWBFile:
        nwb_file = NdxEventsNWBFile(
            session_id=self.aind_data_schema.data_description.name,
            session_description=f"Dataset version: {self.dataset_version}",
            session_start_time=self.aind_data_schema.acquisition.acquisition_start_time,
            identifier=self.aind_data_schema.data_description.subject_id,
            subject=get_subject_nwb_object(
                self.aind_data_schema.data_description.model_dump(mode="json"),
                self.aind_data_schema.subject.model_dump(mode="json"),
            ),
        )
        return nwb_file

    def write_nwb_zarr(self, output: Path) -> None:
        if self._nwb_file is None:
            raise ValueError("NWB file has not been created yet. Call process() to create it before writing.")

        with NWBZarrIO(Path(output).as_posix(), "w") as io:
            io.write(self._nwb_file)
        logger.info(f"NWB zarr successfully written to path {output}")


@dataclasses.dataclass(kw_only=True)
class _AindDataSchemaJson:
    acquisition: Acquisition
    instrument: Instrument
    subject: Subject
    data_description: DataDescription

    @classmethod
    def from_root_path(cls, root_path: Path) -> "_AindDataSchemaJson":
        acquisition_json_path = tuple(root_path.glob("*acquisition*.json"))
        data_description_json_path = tuple(root_path.glob("*data_description*.json"))
        subject_json_path = tuple(root_path.glob("*subject*.json"))
        instrument_json_path = tuple(root_path.glob("*instrument*.json"))

        assert len(acquisition_json_path) == 1, (
            f"Expected exactly 1 acquisition.json, found {len(acquisition_json_path)}"
        )
        assert len(instrument_json_path) == 1, f"Expected exactly 1 instrument.json, found {len(instrument_json_path)}"
        assert len(subject_json_path) == 1, f"Expected exactly 1 subject.json, found {len(subject_json_path)}"
        assert len(data_description_json_path) == 1, (
            f"Expected exactly 1 data_description.json, found {len(data_description_json_path)}"
        )

        return cls(
            acquisition=Acquisition.model_validate_json(acquisition_json_path[0].read_text()),
            instrument=Instrument.model_validate_json(instrument_json_path[0].read_text()),
            subject=Subject.model_validate_json(subject_json_path[0].read_text()),
            data_description=DataDescription.model_validate_json(data_description_json_path[0].read_text()),
        )

    @classmethod
    def from_doc_db(cls, session_id: str) -> "_AindDataSchemaJson":
        from aind_data_access_api.document_db import MetadataDbClient

        client = MetadataDbClient(host="api.allenneuraldynamics.org", version="v2")
        records = client.fetch_records_by_filter_list(filter_key="name", filter_values=[session_id])
        if len(records) == 0:
            raise ValueError(f"No records found in document DB for session_id {session_id}")
        return cls(
            acquisition=Acquisition.model_validate(records[0]["acquisition"]),
            instrument=Instrument.model_validate(records[0]["instrument"]),
            subject=Subject.model_validate(records[0]["subject"]),
            data_description=DataDescription.model_validate(records[0]["data_description"]),
        )
