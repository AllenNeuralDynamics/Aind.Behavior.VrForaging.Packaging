import abc
import re
import typing as ty

import aind_behavior_vr_foraging
import pandas as pd
import semver
from contraqctor.contract import Dataset


def _class_name_to_snake(name: str) -> str:
    """Convert a CamelCase class name to snake_case, e.g. ``LicksProcessor`` → ``licks_processor``."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


class AbstractProcessor(abc.ABC):
    #: Override in subclasses to set a canonical parquet filename stem (e.g. ``"trials"``).
    #: When ``None`` (the default), ``output_name`` falls back to a snake_case of the class name.
    __output_name__: ty.ClassVar[str | None] = None

    @property
    def output_name(self) -> str:
        """Canonical name used as the parquet filename stem.

        Returns ``__output_name__`` if defined on the class, otherwise a
        snake_case of the class name (e.g. ``LicksProcessor`` → ``licks_processor``).
        """
        return self.__class__.__output_name__ or _class_name_to_snake(type(self).__name__)

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
    def compute(self) -> pd.DataFrame:
        """Compute this processor's output as a DataFrame.

        This is the primary, NWB-agnostic output. Suitable for saving directly
        to parquet. Column names and dtypes are stable across versions.
        """
        raise NotImplementedError

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Write this processor's output to *nwb_file* and return it.

        Default implementation is a no-op. Override in subclasses that have
        an NWB representation. May call ``compute()`` internally; the two
        methods are intentionally independent (no shared state).
        """
        return nwb_file

    def with_raise_errors(self, raise_on_error: bool = True) -> ty.Self:
        self._raise_on_error = raise_on_error
        return self

    @property
    def raise_on_error(self) -> bool:
        return self._raise_on_error
