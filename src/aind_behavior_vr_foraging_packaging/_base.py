import abc
import functools
import re
import typing as ty
from functools import cached_property

import pandas as pd
from contraqctor.contract import Dataset

from ._provenance import PackagingProvenance


def _class_name_to_snake(name: str) -> str:
    """Convert a CamelCase class name to snake_case, e.g. ``LicksProcessor`` → ``licks_processor``."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def cached_frame(fn: ty.Callable[[ty.Any], pd.DataFrame]) -> ty.Callable[[ty.Any], pd.DataFrame]:
    """Memoize a processor's :meth:`~AbstractProcessor._compute` for the instance's lifetime.

    Opt-in per processor — deliberately *not* applied by :class:`AbstractProcessor`
    to everything. Decorate ``_compute`` only where both hold:

    1. ``nwbize()`` re-enters ``compute()``, so the frame is built twice per
       session whenever ``--write-nwb`` is set, and
    2. building it is expensive enough for that to matter.

    Five of the seven processors qualify; ``SoftwareEventsProcessor`` builds its
    NWB tables straight from the raw streams and ``SessionMetadataProcessor`` has
    no ``nwbize`` at all, so neither gains anything.

    Each call returns a **copy**, so the invariant documented on
    :meth:`AbstractProcessor.nwbize` — that ``compute()`` and ``nwbize()`` share
    no state — still holds exactly. Callers may mutate what they get back without
    reaching into the cache or into each other. Copying a frame is far cheaper
    than re-parsing the underlying streams, so the saving survives.

    The cache lives on the instance (``self.__dict__``), and processors are
    constructed per session by
    :func:`~aind_behavior_vr_foraging_packaging.session_pipeline.create_processors`,
    so it dies with the session. There is no cross-session staleness to manage and
    nothing to invalidate. An exception is *not* cached: a failed ``_compute``
    leaves the cache empty and the next call retries.
    """

    key = getattr(fn, "__name__", "_compute")

    @functools.wraps(fn)
    def wrapper(self: ty.Any) -> pd.DataFrame:
        cache: dict[str, pd.DataFrame] = self.__dict__.setdefault("_frame_cache", {})
        if key not in cache:
            cache[key] = fn(self)
        return cache[key].copy()

    return wrapper


class AbstractProcessor(abc.ABC):
    #: Override in subclasses to set a canonical parquet filename stem (e.g. ``"sites"``).
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

    @cached_property
    def provenance(self) -> PackagingProvenance:
        """Provenance snapshot for this processor's dataset.

        Cached so that :meth:`compute` and version-check code in subclasses
        share a single :class:`~aind_behavior_vr_foraging_packaging._provenance.PackagingProvenance`
        instance rather than rebuilding it on every call.
        """

        return PackagingProvenance.build(self._dataset)

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
        provenance keys from :class:`~aind_behavior_vr_foraging_packaging._provenance.PackagingProvenance`
        plus a processor-specific ``processor`` key (this class's name).

        Attrs already set by ``_compute`` (e.g. ``sampling_rate_hz`` from
        :class:`SniffingProcessor`) are preserved via ``setdefault``.
        """
        df = self._compute()
        for k, v in self.provenance.model_dump().items():
            df.attrs.setdefault(k, v)
        df.attrs.setdefault("processor", type(self).__name__)
        return df

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Write this processor's output to *nwb_file* and return it.

        Default implementation is a no-op. Override in subclasses that have
        an NWB representation. May call ``compute()`` internally; the two
        methods are intentionally independent (no shared state).

        That independence costs a second full ``_compute()`` per session when
        both outputs are written (``--write-nwb``). Processors for which that
        is expensive decorate ``_compute`` with
        :func:`~aind_behavior_vr_foraging_packaging._base.cached_frame`, which
        removes the recomputation while preserving the no-shared-state
        guarantee — every call still hands back its own copy.
        """
        return nwb_file

    def with_raise_errors(self, raise_on_error: bool = True) -> ty.Self:
        self._raise_on_error = raise_on_error
        return self

    @property
    def raise_on_error(self) -> bool:
        """Whether *known* data anomalies raise instead of being logged and worked around.

        The flag covers only anomalies a processor explicitly checks for and can name,
        and only where a degraded-but-meaningful output exists. The convention is::

            if <specific condition detected>:
                msg = "<what was violated>"
                if self.raise_on_error:
                    raise DatasetProcessorError(msg)
                logger.warning("%s; <what is used instead>.", msg)

        It does **not** gate general exceptions. Never write
        ``except Exception: ... if self.raise_on_error: raise`` — with the flag off (the
        default) that swallows real bugs, API drift and corrupt files as though they were
        data quirks, dropping the processor's output while the run still reports success.
        Catch only the exception types that signal an *expected* condition, narrowly —
        e.g. ``except (KeyError, FileNotFoundError)`` for a stream a given schema version
        does not declare — and let everything else propagate.

        Failures that leave nothing meaningful to emit (e.g. absent treadmill calibration,
        without which position cannot be computed at all) should raise unconditionally
        rather than consult this flag: there is no degraded output to fall back to.

        Isolating one failure from the rest of a run is the caller's job, not the flag's.
        :func:`~aind_behavior_vr_foraging_packaging.export_pipeline.process_sessions`
        catches whatever a processor raises, so a single bad session or processor never
        aborts a batch.
        """
        return self._raise_on_error
