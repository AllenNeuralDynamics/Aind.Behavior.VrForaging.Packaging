"""Top-level pipeline factory.

Selects the correct processor set for a dataset version and returns it ready
to pass to ``NwbSession.run()``. Version dispatch is automatic: datasets with
schema version < 0.6.0 receive the legacy processor variants.

See ``scripts/example_parquet_pipeline.py`` for usage examples.
"""

import logging
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
    SniffingProcessor,
    SoftwareEventsProcessor,
    SiteTableProcessor,
)

logger = logging.getLogger(__name__)

_LEGACY_VERSION_CUTOFF = semver.Version(major=0, minor=6, patch=0)


def create_processors(
    dataset: Dataset,
    *,
    session_path: Path | None = None,
    raise_on_error: bool = False,
) -> list[AbstractProcessor]:
    """Return the ordered processor list for *dataset*, dispatching on version.

    Parameters
    ----------
    dataset:
        The loaded contraqctor Dataset. Its ``.version`` attribute determines
        which processor variants are selected.
    session_path:
        When provided, a :class:`~.processing.SessionMetadataProcessor` is
        prepended to the list using this path as the session root.
    raise_on_error:
        Passed through to every processor. When ``True``, any parsing anomaly
        raises; when ``False`` (default) it logs a warning and continues.

    Returns
    -------
    list[AbstractProcessor]
        Processors in the order they must be applied. When *session_path* is
        given, ``session`` is always first.
    """
    version = semver.Version.parse(str(dataset.version))
    is_legacy = version < _LEGACY_VERSION_CUTOFF

    if is_legacy:
        logger.info("Dataset version %s < %s — using legacy processors.", version, _LEGACY_VERSION_CUTOFF)
        site_table_cls = LegacySiteTableProcessor
        pos_vel_cls = LegacyPositionAndVelocityProcessor
    else:
        logger.info("Dataset version %s — using current processors.", version)
        site_table_cls = SiteTableProcessor
        pos_vel_cls = PositionAndVelocityProcessor

    procs: list[AbstractProcessor] = []
    if session_path is not None:
        procs.append(SessionMetadataProcessor(dataset, session_path=session_path, raise_on_error=raise_on_error))
    procs += [
        site_table_cls(dataset, raise_on_error=raise_on_error),
        pos_vel_cls(dataset, raise_on_error=raise_on_error),
        LicksProcessor(dataset, raise_on_error=raise_on_error),
        SniffingProcessor(dataset, raise_on_error=raise_on_error),
        SoftwareEventsProcessor(dataset, raise_on_error=raise_on_error),
        EventsProcessor(dataset, raise_on_error=raise_on_error),
    ]
    return procs


def get_site_table_processor(
    dataset: Dataset,
    *,
    raise_on_error: bool = False,
) -> SiteTableProcessor | LegacySiteTableProcessor:
    """Return the correct site-table processor for *dataset*'s version."""
    version = semver.Version.parse(str(dataset.version))
    cls = LegacySiteTableProcessor if version < _LEGACY_VERSION_CUTOFF else SiteTableProcessor
    return cls(dataset, raise_on_error=raise_on_error)


def get_position_velocity_processor(
    dataset: Dataset,
    *,
    sampling_rate_hz: float | None = 250.0,
    raise_on_error: bool = False,
) -> PositionAndVelocityProcessor | LegacyPositionAndVelocityProcessor:
    """Return the correct position/velocity processor for *dataset*'s version."""
    version = semver.Version.parse(str(dataset.version))
    cls = LegacyPositionAndVelocityProcessor if version < _LEGACY_VERSION_CUTOFF else PositionAndVelocityProcessor
    return cls(dataset, sampling_rate_hz=sampling_rate_hz, raise_on_error=raise_on_error)


def run_session(
    dataset: Dataset,
    output_dir: Path,
    *,
    session_path: Path | None = None,
    raise_on_error: bool = False,
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
    session_path:
        When provided, a ``session`` processor is prepended. Pass the
        session root directory (same value used to load *dataset*).
    raise_on_error:
        Passed to all processors.

    Returns
    -------
    dict[str, pd.DataFrame]
        Computed DataFrames keyed by each processor's ``output_name``.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_data: dict[str, pd.DataFrame] = {}
    for proc in create_processors(
        dataset,
        session_path=session_path,
        raise_on_error=raise_on_error,
    ):
        name = proc.output_name
        logger.info("compute: %s → %s.parquet", proc.__class__.__name__, name)
        # compute() stamps provenance attrs automatically (see AbstractProcessor.compute)
        df = proc.compute()
        _write_parquet(df, output_dir / f"{name}.parquet")
        all_data[name] = df
        logger.info("  saved %d rows", len(df))

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
