import abc
import re
import typing as ty

import pandas as pd
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

    @abc.abstractmethod
    def _compute(self) -> pd.DataFrame:
        """Compute this processor's output as a DataFrame.

        Subclasses implement this method. Callers should use :meth:`compute`,
        which wraps ``_compute`` and stamps provenance metadata into ``df.attrs``.
        """
        raise NotImplementedError

    def compute(self) -> pd.DataFrame:
        """Return the processor's output DataFrame with provenance metadata in attrs.

        Calls :meth:`_compute`, then stamps ``df.attrs`` with the session-level
        provenance keys from :func:`~aind_behavior_vr_foraging_packaging._provenance.packaging_provenance`
        plus a processor-specific ``processor`` key (this class's name).

        Attrs already set by ``_compute`` (e.g. ``sampling_rate_hz`` from
        :class:`SniffingProcessor`) are preserved via ``setdefault``.
        """
        from ._provenance import PackagingProvenance

        df = self._compute()
        for k, v in PackagingProvenance.build(self._dataset).model_dump().items():
            df.attrs.setdefault(k, v)
        df.attrs.setdefault("processor", type(self).__name__)
        return df

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
