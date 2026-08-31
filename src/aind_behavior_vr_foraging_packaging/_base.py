import abc
import functools
import re
import typing as ty
from functools import cached_property
from pathlib import Path

import pandas as pd
from contraqctor.contract import Dataset

from ._provenance import PackagingProvenance


class DatasetProcessorError(Exception):
    """Raised by a processor for a data condition it explicitly checks for and names.

    See :attr:`AbstractProcessor.strict_parsing` and
    ``docs/knowledge/conventions/error-policy.md``.
    """


def session_root(dataset: Dataset) -> Path:
    """Return the session's root directory, derived from the Session stream's path.

    The contraqctor ``Dataset`` does not carry the root it was loaded from, so it
    is recovered by walking up from ``<root>/behavior/Logs/session_input.json``.
    Anchors on the ``behavior/`` component rather than counting parents, so moving
    the log deeper under ``behavior/`` cannot silently yield the wrong directory.

    The directory's ``name`` is the session's ``session_id`` everywhere in the
    package, so a failure here is fatal rather than degradable.
    """
    stream = dataset.at("Behavior").at("InputSchemas").at("Session")
    raw_path = getattr(stream.reader_params, "path", None)
    if raw_path is None:
        raise DatasetProcessorError("Session stream exposes no source path to take the session directory from")

    path = Path(raw_path)
    for parent in path.parents:
        if parent.name.lower() == "behavior":
            return parent.parent

    raise DatasetProcessorError(
        f"Session stream path {str(path)!r} has no 'behavior' component to locate the session root from"
    )


def _class_name_to_snake(name: str) -> str:
    """Convert a CamelCase class name to snake_case, e.g. ``LicksProcessor`` → ``licks_processor``."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def cached_frame(fn: ty.Callable[[ty.Any], pd.DataFrame]) -> ty.Callable[[ty.Any], pd.DataFrame]:
    """Memoize a processor's :meth:`~AbstractProcessor._compute` for the instance's lifetime.

    Opt-in per processor — deliberately *not* applied by :class:`AbstractProcessor`
    to everything. Decorate ``_compute`` only where both hold:

    1. ``nwbize()`` and/or ``write_parquet()`` re-enter ``compute()`` independently
       of whatever the caller already computed, so the frame can be built more
       than once per session, and
    2. building it is expensive enough for that to matter.

    Each call returns a **copy**, so the invariant that ``compute()``,
    ``nwbize()`` and ``write_parquet()`` share no state still holds exactly.
    Callers may mutate what they get back without reaching into the cache or
    into each other. Copying a frame is far cheaper than re-parsing the
    underlying streams, so the saving survives.

    The cache lives on the instance (``self.__dict__``), and processors are
    constructed per session by
    :func:`~aind_behavior_vr_foraging_packaging.pipeline.session.create_processors`,
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


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write *df* to *path* as parquet, promoting ``df.attrs`` to schema metadata.

    All keys in ``df.attrs`` are written both in the pandas metadata blob
    (for pandas round-trips) and as top-level key-value entries in the parquet
    schema (readable from DuckDB, R arrow, Polars, Spark, etc.).

    The canonical implementation, usable directly by callers with no processor
    instance at hand — e.g. :mod:`~aind_behavior_vr_foraging_packaging.pipeline.batch`'s
    multi-session aggregation, which reads parquets back from disk and
    re-combines them — and it's also :meth:`AbstractProcessor.write_parquet`'s
    default implementation, overridable per processor.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df)
    kv = {str(k).encode(): str(v).encode() for k, v in df.attrs.items()}
    table = table.replace_schema_metadata({**table.schema.metadata, **kv})
    pq.write_table(table, path)


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

    def __init__(self, dataset: Dataset, *, strict_parsing: bool = False) -> None:
        self._dataset = dataset
        self._strict_parsing = strict_parsing

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
        an NWB representation. May call ``compute()`` internally; ``compute()``,
        ``nwbize()`` and :meth:`write_parquet` are intentionally independent of
        one another (no shared state).

        That independence costs a second (or third) full ``_compute()`` per
        session whenever more than one of them runs. Processors for which that
        is expensive decorate ``_compute`` with
        :func:`~aind_behavior_vr_foraging_packaging._base.cached_frame`, which
        removes the recomputation while preserving the no-shared-state
        guarantee — every call still hands back its own copy.
        """
        return nwb_file

    def write_parquet(self, output_dir: Path, filename: str | None = None) -> None:
        """Compute this processor's output and write it under *output_dir* as parquet.

        Calls :meth:`compute` internally rather than taking a DataFrame — see
        :meth:`nwbize` for why the output-writing methods are independent of
        one another and of whatever the caller already computed.

        *filename* defaults to ``f"{self.output_name}.parquet"`` when not given,
        matching the pipeline's own naming convention. Default implementation
        delegates to the module-level :func:`write_parquet`. Override wholesale
        in a subclass to customize the arrow table before it's written — e.g.
        tagging a column with a non-default logical type.
        """
        write_parquet(self.compute(), output_dir / (filename or f"{self.output_name}.parquet"))

    def with_strict_parsing(self, strict_parsing: bool = True) -> ty.Self:
        self._strict_parsing = strict_parsing
        return self

    @property
    def strict_parsing(self) -> bool:
        """Whether *known* data anomalies raise instead of being logged and worked around.

        The flag covers only anomalies a processor explicitly checks for and can name,
        and only where a degraded-but-meaningful output exists. The convention is::

            if <specific condition detected>:
                msg = "<what was violated>"
                if self.strict_parsing:
                    raise DatasetProcessorError(msg)
                logger.warning("%s; <what is used instead>.", msg)

        It does **not** gate general exceptions. Never write
        ``except Exception: ... if self.strict_parsing: raise`` — with the flag off (the
        default) that swallows real bugs, API drift and corrupt files as though they were
        data quirks, dropping the processor's output while the run still reports success.
        Catch only the exception types that signal an *expected* condition, narrowly —
        e.g. ``except (KeyError, FileNotFoundError)`` for a stream a given schema version
        does not declare — and let everything else propagate.

        Failures that leave nothing meaningful to emit (e.g. absent treadmill calibration,
        without which position cannot be computed at all) should raise unconditionally
        rather than consult this flag: there is no degraded output to fall back to.

        Isolating one failure from the rest of a run is the caller's job, not the flag's.
        :func:`~aind_behavior_vr_foraging_packaging.pipeline.batch.process_sessions`
        catches whatever a processor raises, so a single bad session or processor never
        aborts a batch.
        """
        return self._strict_parsing
