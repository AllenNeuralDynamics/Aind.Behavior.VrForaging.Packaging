"""Multi-session export: run the session pipeline over many sessions, then aggregate.

Phase 1 — :func:`process_sessions`: iterate raw session directories → per-session parquets.
Phase 2 — :func:`aggregate`: read per-session parquets → experiment-level parquets.

The two phases are independent: Phase 2 reads only what Phase 1 left on disk, so
a slow Phase 1 does not have to be repeated to re-cut the aggregates. The
``vr-foraging-packaging`` CLI in :mod:`.cli` exposes each as its own subcommand.
"""

import logging
import shutil
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

from .session import process_session

logger = logging.getLogger(__name__)


SESSION_TABLE = "session"
AGGREGATED_TABLES: tuple[str, ...] = (SESSION_TABLE, "sites")


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------


def process_sessions(
    dataset_paths: Iterable[Path],
    output_dir: Path,
    *,
    include_processors: Sequence[str] = (),
    exclude_processors: Sequence[str] = (),
    strict_parsing: bool = False,
    max_workers: int = 1,
    clean: bool = True,
    write_parquet: bool = True,
    write_nwb: bool = False,
) -> list[Path]:
    """Run :func:`~.pipeline.session.process_session` over many session directories.

    Phase 1 of the export. This layer is deliberately thin: everything about a
    single session — loading the dataset, building and filtering the processor
    list, writing parquet and NWB — belongs to ``process_session``. What is
    genuinely multi-session, and therefore lives here, is *clean*, *max_workers*,
    and the ``sessions/{session_id}/`` layout.

    Everything propagates. Anything escaping a processor is unexpected by
    definition, so neither a failing session nor a failing batch is caught here
    (see ``docs/knowledge/conventions/error-policy.md``).

    Parameters
    ----------
    dataset_paths:
        Iterable of paths, each pointing to the root directory of one raw session.
    output_dir:
        Root of the experiment export. Per-session files go to
        ``output_dir/sessions/{session_id}/``.
    include_processors, exclude_processors, strict_parsing, write_parquet, write_nwb:
        Per-session options, forwarded unchanged to
        :func:`~.pipeline.session.process_session` (as *include* / *exclude* /
        *strict_parsing* / *write_parquet* / *write_nwb*). This layer adds no
        behaviour of its own to any of them, so their semantics are documented
        once, there.

        Note that ``write_parquet=False`` leaves Phase 2 nothing to aggregate:
        :func:`aggregate` reads the per-session parquets back off disk.
    max_workers:
        Number of parallel threads. ``1`` (default) runs sessions sequentially.
        Values ``> 1`` process up to *max_workers* sessions concurrently via
        :class:`~concurrent.futures.ThreadPoolExecutor`.
    clean:
        When ``True`` (default), delete the outputs a previous run of *this*
        function left in *output_dir* — the ``sessions/`` tree and the
        aggregated ``{table}.parquet`` files — so a re-run never mixes results
        from two invocations. Set to ``False`` to resume a partial run.

        It deliberately does **not** wipe *output_dir* itself. Anything else
        living there is not ours to delete: a log file being written to
        ``output_dir/run.log`` is the obvious case, and on Windows removing an
        open file fails outright.

    Returns
    -------
    list[Path]
        Paths to the written session directories (``output_dir/sessions/{session_id}``).
    """
    paths = [Path(p) for p in dataset_paths]
    output_dir = Path(output_dir)
    sessions_dir = output_dir / "sessions"

    if clean:
        _clear_previous_outputs(output_dir, sessions_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def _submit(raw_path: Path) -> Path:
        """Run one session and return the directory it was written to.

        The only genuinely multi-session decision is where a session's output
        goes; everything else is ``process_session``'s, and propagates from it.
        """
        session_out = sessions_dir / raw_path.name
        process_session(
            raw_path,
            session_out,
            strict_parsing=strict_parsing,
            include=include_processors,
            exclude=exclude_processors,
            write_parquet=write_parquet,
            write_nwb=write_nwb,
        )
        return session_out

    if max_workers == 1:
        return [_submit(p) for p in paths]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_submit, p): p for p in paths}
        # fut.result() re-raises whatever the session raised.
        return [fut.result() for fut in as_completed(futures)]


def _clear_previous_outputs(output_dir: Path, sessions_dir: Path) -> None:
    """Remove what a previous run of this pipeline wrote, and nothing else.

    Scoped to the paths this module owns rather than deleting *output_dir*
    wholesale: the output directory is a location the caller chose, and may hold
    things we did not put there — a log file being written to
    ``output_dir/run.log`` most obviously, which on Windows cannot be removed
    while open and on POSIX would vanish mid-run.
    """
    if sessions_dir.exists():
        shutil.rmtree(sessions_dir)
        logger.info("Cleared previous per-session outputs: %s", sessions_dir)

    for table in AGGREGATED_TABLES:
        stale = output_dir / f"{table}.parquet"
        if stale.exists():
            stale.unlink()
            logger.debug("Removed stale aggregate %s", stale.name)


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------


def aggregate(sessions_dir: Path, output_dir: Path) -> None:
    """Concatenate per-session parquets into experiment-level files.

    Writes one flat ``output_dir/{table}.parquet`` for each name in
    :data:`AGGREGATED_TABLES`, with a ``session_id`` column for joins.

    What gets aggregated is fixed, not configurable: the set is a property of
    the schema — which tables are small enough to scan experiment-wide — rather
    than something a caller should decide per run.

    Per-session files are never deleted. Copying rows into an aggregate is not a
    reason to destroy the source: the per-session files are what
    ``--skip-processing`` re-aggregation reads back, and the only copies carrying
    provenance in their parquet schema.

    Parameters
    ----------
    sessions_dir:
        Directory produced by :func:`process_sessions`
        (i.e. ``output_dir/sessions/``).
    output_dir:
        Root output directory where aggregated files are written.
    """
    sessions_dir = Path(sessions_dir)
    output_dir = Path(output_dir)

    session_dirs = sorted(d for d in sessions_dir.iterdir() if d.is_dir())
    if not session_dirs:
        logger.warning("No session directories found under %s", sessions_dir)
        return

    for table in AGGREGATED_TABLES:
        wrote = _aggregate_table(table, session_dirs, output_dir)
        if table == SESSION_TABLE and not wrote:
            logger.error(
                "No %s.parquet found in any session — the export has no identity table "
                "and nothing to join on; aborting aggregation.",
                SESSION_TABLE,
            )
            return


def _assume_utc(table: "pa.Table") -> "pa.Table":
    """Tag naive timestamp columns as UTC, without shifting the clock.

    Legacy sessions record ``date`` with no offset and current ones record UTC. A column
    holds one type or neither, so the two only merge once the naive half is given a zone.
    """
    import pyarrow as pa

    for index, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type) and field.type.tz is None:
            zoned = pa.timestamp(field.type.unit, tz="UTC")
            table = table.set_column(index, field.with_type(zoned), table.column(index).cast(zoned))
    return table


def _aggregate_table(table: str, session_dirs: list[Path], output_dir: Path) -> bool:
    """Concatenate one table across *session_dirs*; return whether anything was written.

    Stays in Arrow: a pandas round-trip drops the logical types the per-session
    writers set, turning session.parquet's JSON columns back into plain strings.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    tables: list[pa.Table] = []

    for sd in session_dirs:
        p = sd / f"{table}.parquet"
        if not p.exists():
            logger.debug("  %s: no %s.parquet in %s — skipping", table, table, sd.name)
            continue
        t = pq.read_table(p)
        if "session_id" not in t.column_names:
            session_ids = pa.array([sd.name] * t.num_rows, type=pa.large_string())
            t = t.add_column(0, pa.field("session_id", pa.large_string()), session_ids)
        # Metadata is per-session provenance; keeping the first would misattribute the rest.
        tables.append(_assume_utc(t).replace_schema_metadata(None))

    if not tables:
        logger.warning("  %s: no parquet files found across any session — skipped.", table)
        return False

    combined = pa.concat_tables(tables, promote_options="permissive")
    dest = output_dir / f"{table}.parquet"
    pq.write_table(combined, dest)
    logger.info("  %s → %d rows → %s", table, combined.num_rows, dest.name)
    return True
