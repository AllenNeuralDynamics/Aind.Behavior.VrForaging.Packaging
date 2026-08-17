"""Top-level pipeline factory.

Selects the correct processor set for a dataset version and returns it ready
to pass to ``NwbSession.run()``. Version dispatch is automatic: datasets with
schema version < 0.6.0 receive the legacy processor variants.

See ``docs/guides/session-from-disk.md`` for usage examples.
"""

import logging
from collections.abc import Callable, Sequence
from pathlib import Path

import pandas as pd
import semver
from contraqctor.contract import Dataset

from ._base import AbstractProcessor
from .processing import (
    EventsProcessor,
    LegacyPositionAndVelocityProcessor,
    LegacySiteTableProcessor,
    LicksProcessor,
    PositionAndVelocityProcessor,
    SessionMetadataProcessor,
    SiteTableProcessor,
    SniffingProcessor,
    SoftwareEventsProcessor,
)

logger = logging.getLogger(__name__)

_LEGACY_VERSION_CUTOFF = semver.Version(major=0, minor=6, patch=0)


def create_processors(
    dataset: Dataset,
    *,
    strict_parsing: bool = False,
) -> list[AbstractProcessor]:
    """Return the ordered processor list for *dataset*, dispatching on version.

    Parameters
    ----------
    dataset:
        The loaded contraqctor Dataset. Its ``.version`` attribute determines
        which processor variants are selected.
    strict_parsing:
        Passed through to every processor. When ``True``, any parsing anomaly
        raises; when ``False`` (default) it logs a warning and continues.

    Returns
    -------
    list[AbstractProcessor]
        Processors in the order they must be applied; ``session`` is always
        first. :class:`~.processing.SessionMetadataProcessor` is unconditional —
        it derives the session root from the dataset's own Session stream, so
        there is nothing for the caller to supply and no reason to omit it.
    """

    processors: list[AbstractProcessor] = [
        SessionMetadataProcessor(dataset, strict_parsing=strict_parsing),
        resolve_position_velocity_processor(dataset, strict_parsing=strict_parsing),
        resolve_site_table_processor(dataset, strict_parsing=strict_parsing),
        LicksProcessor(dataset, strict_parsing=strict_parsing),
        SniffingProcessor(dataset, strict_parsing=strict_parsing),
        SoftwareEventsProcessor(dataset, strict_parsing=strict_parsing),
        EventsProcessor(dataset, strict_parsing=strict_parsing),
    ]
    return processors


def resolve_site_table_processor(
    dataset: Dataset,
    *,
    strict_parsing: bool = False,
) -> SiteTableProcessor | LegacySiteTableProcessor:
    """Return the correct site-table processor for *dataset*'s version."""
    version = semver.Version.parse(str(dataset.version))
    cls = LegacySiteTableProcessor if version < _LEGACY_VERSION_CUTOFF else SiteTableProcessor
    return cls(dataset, strict_parsing=strict_parsing)


def resolve_position_velocity_processor(
    dataset: Dataset,
    *,
    sampling_rate_hz: float | None = 250.0,
    strict_parsing: bool = False,
) -> PositionAndVelocityProcessor | LegacyPositionAndVelocityProcessor:
    """Return the correct position/velocity processor for *dataset*'s version."""
    version = semver.Version.parse(str(dataset.version))
    cls = LegacyPositionAndVelocityProcessor if version < _LEGACY_VERSION_CUTOFF else PositionAndVelocityProcessor
    return cls(dataset, sampling_rate_hz=sampling_rate_hz, strict_parsing=strict_parsing)


def process_session(
    dataset: Dataset,
    output_dir: Path,
    *,
    strict_parsing: bool = False,
    processors: Sequence[AbstractProcessor] | None = None,
    on_error: Callable[[AbstractProcessor, Exception], None] | None = None,
    log_prefix: str = "",
) -> dict[str, pd.DataFrame]:
    """Run all processors and save their outputs as parquet files.

    Each processor's ``output_name`` attribute determines its parquet filename,
    e.g. ``sites.parquet``, ``position_velocity.parquet``, etc.

    Parameters
    ----------
    dataset:
        Loaded contraqctor Dataset. Its version determines which processor
        variants are selected (legacy vs current).
    output_dir:
        Directory where parquet files are written. Created if absent.
    strict_parsing:
        Passed to all processors.
    processors:
        Use this exact, already-constructed processor list instead of calling
        :func:`create_processors` internally. For a caller that has already
        filtered/selected its own processor list (e.g.
        ``export_pipeline._process_one_session``'s ``--include-processors``/
        ``--exclude-processors`` handling) — *strict_parsing* is then
        irrelevant to processor construction and only still applies to this
        function's own behavior. ``None`` (default) preserves the original
        behavior: build the list via
        ``create_processors(dataset, strict_parsing=strict_parsing)``.
    on_error:
        Called as ``on_error(processor, exception)`` when a processor's
        ``compute()`` raises, instead of letting the exception propagate.
        The callback decides what happens next: returning normally skips that
        processor and continues with the rest; re-raising aborts the run
        immediately (the callback's own raised exception propagates out of
        this function). ``None`` (default) means an exception propagates
        immediately, same as before this parameter existed — every existing
        caller keeps its current behavior unchanged.
    log_prefix:
        Prepended to this function's own log lines — e.g. ``f"[{session_id}] "``
        for a caller processing many sessions in one batch, so per-processor
        progress stays grep-able by session. Empty by default.

    Returns
    -------
    dict[str, pd.DataFrame]
        DataFrames for every processor that computed successfully, keyed by
        ``output_name``. A processor whose ``compute()`` raised and whose
        failure was absorbed by *on_error* (rather than re-raised) is simply
        absent from this dict.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    selected = processors if processors is not None else create_processors(dataset, strict_parsing=strict_parsing)

    all_data: dict[str, pd.DataFrame] = {}
    for proc in selected:
        name = proc.output_name
        logger.info("%scompute: %s → %s.parquet", log_prefix, proc.__class__.__name__, name)
        try:
            # compute() stamps provenance attrs automatically (see AbstractProcessor.compute)
            df = proc.compute()
        except Exception as exc:
            if on_error is None:
                raise
            on_error(proc, exc)
            continue
        _write_parquet(df, output_dir / f"{name}.parquet")
        all_data[name] = df
        logger.info("%s  saved %d rows", log_prefix, len(df))

    return all_data


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write *df* to *path*, promoting ``df.attrs`` to first-class parquet metadata.

    All keys in ``df.attrs`` are written both in the pandas metadata blob
    (for pandas round-trips) and as top-level key-value entries in the parquet
    schema (readable from DuckDB, R arrow, Polars, Spark, etc.).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df)
    kv = {str(k).encode(): str(v).encode() for k, v in df.attrs.items()}
    table = table.replace_schema_metadata({**table.schema.metadata, **kv})
    pq.write_table(table, path)
