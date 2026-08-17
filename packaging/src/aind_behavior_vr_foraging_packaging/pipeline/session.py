"""Single-session pipeline: version dispatch, fan-out, and output writing.

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

from .._base import AbstractProcessor, session_root
from ..processing import (
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
    include: Sequence[str] = (),
    exclude: Sequence[str] = (),
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
    include:
        If non-empty, keep only processors whose ``output_name`` is listed.
    exclude:
        Drop processors whose ``output_name`` is listed. Applied after
        *include*, so an name in both is dropped.

    Returns
    -------
    list[AbstractProcessor]
        Processors in the order they must be applied; ``session`` is always
        first and is never filtered out — it carries the session's identity, so
        every other table would lose its join key without it.
        :class:`~.processing.SessionMetadataProcessor` is also unconditional in
        the sense that it needs no arguments: it derives the session root from
        the dataset's own Session stream.
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
    return filter_processors(processors, include=include, exclude=exclude)


def filter_processors(
    processors: Sequence[AbstractProcessor],
    *,
    include: Sequence[str] = (),
    exclude: Sequence[str] = (),
) -> list[AbstractProcessor]:
    """Select processors by ``output_name``, always keeping ``session``.

    Empty *include* means "keep everything"; *exclude* is applied second.
    ``session`` survives both, because dropping it would leave every other
    table without the identity row it joins to.
    """
    include_set, exclude_set = frozenset(include), frozenset(exclude)

    def _keep(proc: AbstractProcessor) -> bool:
        name = proc.output_name
        if name == SessionMetadataProcessor.__output_name__:
            return True
        if include_set and name not in include_set:
            logger.debug("skip %s (not in include list)", name)
            return False
        if name in exclude_set:
            logger.debug("skip %s (excluded)", name)
            return False
        return True

    return [p for p in processors if _keep(p)]


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
    dataset: Dataset | Path | str,
    output_dir: Path | str = ".",
    *,
    strict_parsing: bool = False,
    include: Sequence[str] = (),
    exclude: Sequence[str] = (),
    processors: Sequence[AbstractProcessor] | None = None,
    on_error: Callable[[AbstractProcessor, Exception], None] | None = None,
    on_output: Callable[[AbstractProcessor, pd.DataFrame, Path | None], None] | None = None,
    write_parquet: bool = True,
    write_nwb: bool = False,
) -> dict[str, pd.DataFrame]:
    """Run every processor and write the outputs chosen by *write_parquet* / *write_nwb*.

    Each processor's ``output_name`` attribute determines its parquet filename,
    e.g. ``sites.parquet``, ``position_velocity.parquet``, etc.

    The two output formats are independent switches over the same computed
    frames. Every processor runs regardless — the flags choose what reaches disk,
    not what is computed — so the returned dict is the same either way.

    Log lines are prefixed with the session id, taken from the dataset, so
    per-processor progress stays grep-able when many sessions run in one batch.

    Parameters
    ----------
    dataset:
        A loaded contraqctor Dataset, or the path to a raw session directory to
        load one from. Its version determines which processor variants are
        selected (legacy vs current).
    output_dir:
        Directory where outputs are written; defaults to the current working
        directory. Created if absent, unless both *write_parquet* and
        *write_nwb* are ``False``, in which case nothing is written and no
        directory is made.
    strict_parsing:
        Passed to all processors.
    include, exclude:
        Processor ``output_name`` filters, forwarded to
        :func:`create_processors`. ``session`` is never filtered out. Ignored
        when *processors* is given, since that list is already final.
    processors:
        Use this exact, already-constructed processor list instead of calling
        :func:`create_processors` internally — the escape hatch for a custom or
        third-party processor. *strict_parsing*, *include* and *exclude* are
        then irrelevant to processor construction. ``None`` (default) builds
        the list from the dataset.
    on_error:
        Called as ``on_error(processor, exception)`` when a processor's
        ``compute()`` raises, instead of letting the exception propagate.
        The callback decides what happens next: returning normally skips that
        processor and continues with the rest; re-raising aborts the run
        immediately (the callback's own raised exception propagates out of
        this function). ``None`` (default) means an exception propagates
        immediately, same as before this parameter existed — every existing
        caller keeps its current behavior unchanged.
    on_output:
        Called as ``on_output(processor, frame, path)`` after each processor
        succeeds, where *path* is the parquet written or ``None`` if
        *write_parquet* is off. Together with *on_error* this is the full record
        of what happened, processor by processor, which is more than the return
        value can say: a run that fails partway returns nothing at all.

        Deliberately generic. The server layer builds its
        ``output.metadata.json`` on top of this pair, and this function stays
        unaware of that file, its schema, and the package that defines it.
    write_parquet:
        When ``True`` (default), write one ``output_dir/{output_name}.parquet``
        per processor, with provenance promoted into the parquet schema. Set to
        ``False`` to compute without touching disk — the frames still come back
        in the return value.
    write_nwb:
        When ``True``, write ``output_dir/{session_id}.nwb.zarr`` from the same
        processor list, so one filtered selection can produce both output
        formats. Requires the AIND metadata JSON files in the session root; a
        session missing them fails the NWB step, and that failure propagates
        like any other. Defaults to ``False``.

    Returns
    -------
    dict[str, pd.DataFrame]
        DataFrames for every processor that computed successfully, keyed by
        ``output_name`` — independent of which formats were written. A processor
        whose ``compute()`` raised and whose failure was absorbed by *on_error*
        (rather than re-raised) is simply absent from this dict.
    """
    if isinstance(dataset, (str, Path)):
        from aind_behavior_vr_foraging.data_contract import dataset as load_dataset

        dataset = load_dataset(Path(dataset))

    output_dir = Path(output_dir)
    if write_parquet or write_nwb:
        output_dir.mkdir(parents=True, exist_ok=True)

    root = session_root(dataset)
    selected = (
        processors
        if processors is not None
        else create_processors(dataset, strict_parsing=strict_parsing, include=include, exclude=exclude)
    )

    all_data: dict[str, pd.DataFrame] = {}
    for proc in selected:
        name = proc.output_name
        logger.info("[%s] compute: %s → %s", root.name, proc.__class__.__name__, name)
        try:
            # compute() stamps provenance attrs automatically (see AbstractProcessor.compute)
            df = proc.compute()
        except Exception as exc:
            if on_error is None:
                raise
            on_error(proc, exc)
            continue
        all_data[name] = df
        dest: Path | None = None
        if write_parquet:
            dest = output_dir / f"{name}.parquet"
            _write_parquet(df, dest)
            logger.info("[%s]   saved %d rows → %s.parquet", root.name, len(df), name)
        else:
            logger.info("[%s]   %d rows (parquet skipped)", root.name, len(df))
        if on_output is not None:
            on_output(proc, df, dest)

    if write_nwb:
        _write_nwb_zarr(dataset, root, output_dir, selected)

    return all_data


def _write_nwb_zarr(
    dataset: Dataset,
    root: Path,
    output_dir: Path,
    processors: Sequence[AbstractProcessor],
) -> Path:
    """Build and write one NWB-Zarr store to ``output_dir/{session_id}.nwb.zarr``.

    Held to the same rule as the processors: a session whose NWB step failed is
    not a usable partial result, so nothing is caught here.
    """
    import shutil

    from ..nwb_file import NwbSession

    dest = output_dir / f"{root.name}.nwb.zarr"

    session = NwbSession(root, dataset=dataset)
    session.run(*processors)
    # NWBZarrIO("w") does not reliably clear a pre-existing store; remove it
    # first so a re-run never mixes old and new objects.
    if dest.exists():
        shutil.rmtree(dest)
    session.write_nwb_zarr(dest)

    logger.info("[%s] NWB → %s", root.name, dest.name)
    return dest


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
