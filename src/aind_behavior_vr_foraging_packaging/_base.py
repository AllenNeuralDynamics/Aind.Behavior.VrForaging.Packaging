import abc
import typing as ty

import aind_behavior_vr_foraging
import semver
from contraqctor.contract import Dataset
from ndx_events import NdxEventsNWBFile


class AbstractProcessor(abc.ABC):
    def __init__(self, dataset: Dataset, *, raise_on_error: bool = False) -> None:
        self._dataset = dataset
        self._raise_on_error = raise_on_error

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @property
    def dataset_version(self) -> semver.Version:
        return self._parse_version(self.dataset.version)

    @property
    def parser_version(self) -> semver.Version:
        return semver.Version.parse(aind_behavior_vr_foraging.__semver__)

    @staticmethod
    def _parse_version(value: str | semver.Version) -> semver.Version:
        if isinstance(value, semver.Version):
            return value
        return semver.Version.parse(value)

    @abc.abstractmethod
    def process(self, nwb_file: NdxEventsNWBFile) -> NdxEventsNWBFile:
        raise NotImplementedError("Subclasses must implement the process method.")

    def with_raise_errors(self, raise_on_error: bool = True) -> ty.Self:
        self._raise_on_error = raise_on_error
        return self

    @property
    def raise_on_error(self) -> bool:
        return self._raise_on_error
