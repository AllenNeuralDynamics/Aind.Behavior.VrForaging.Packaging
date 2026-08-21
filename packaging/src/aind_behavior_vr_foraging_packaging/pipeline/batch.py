"""Multi-session export: run the session pipeline over many sessions, then aggregate.

Phase 1 — :func:`process_sessions`: iterate raw session directories → per-session parquets.
Phase 2 — :func:`aggregate`: read per-session parquets → experiment-level parquets.

The two phases are independent: Phase 2 reads only what Phase 1 left on disk, so
a slow Phase 1 does not have to be repeated to re-cut the aggregates. The
``vr-foraging-packaging`` CLI in :mod:`.cli` exposes each as its own subcommand.
"""

import io
import logging
import shutil
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from .session import _write_parquet, process_session

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
    definition, so neither a failing session nor a failing batch is caught here.

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


def aggregate_tables(
    session_names: Iterable[str],
    read: Callable[[str, str], bytes | None],
    *,
    max_workers: int = 16,
) -> dict[str, bytes]:
    """Concatenate per-session parquet *bytes* into one parquet per aggregated table.

    The concatenation itself, decoupled from where the bytes came from and where they
    are going: *read* is called as ``read(session_name, table)`` and returns that
    session's parquet bytes, or ``None`` when it has no such table. Returns
    ``{table: parquet_bytes}``.

    Bytes rather than paths because the caller may have no filesystem in the picture at
    all. The server aggregates straight out of the output store — object storage has no
    batch read, so a session's tables are two ordinary GETs and landing them on disk
    first would buy nothing. :func:`aggregate` is the filesystem-shaped caller, so both
    it and the server concatenate through this one implementation rather than two that
    can drift apart.

    Returns an **empty** dict when no session has a ``session.parquet``: without the
    identity table there is nothing to join on, so no table is written at all rather
    than a set of aggregates with no key. Callers decide whether that is an error —
    :func:`aggregate` logs it, the server fails the job.

    *read* is called concurrently. These reads are latency-bound rather than
    bandwidth-bound — one round trip for a small file — so N sessions serially is
    almost pure waiting.
    """
    names = sorted(session_names)
    pairs = [(name, table) for name in names for table in AGGREGATED_TABLES]
    if not pairs:
        logger.warning("No sessions to aggregate.")
        return {}

    def _read_one(pair: tuple[str, str]) -> tuple[tuple[str, str], pd.DataFrame | None]:
        name, table = pair
        buf = read(name, table)
        if buf is None:
            logger.debug("  %s: no %s.parquet — skipping", name, table)
            return pair, None
        # Parsed here, inside the worker thread: parquet decode releases the GIL, so it
        # parallelises, and the compressed bytes are freed as soon as the frame exists
        # instead of every session's buffer being held alive until the pool drains.
        try:
            df = pd.read_parquet(io.BytesIO(buf))
        except Exception as exc:
            # One corrupt session must not wedge the aggregate for every other session:
            # this runs on a schedule against a growing set, so raising here would mean
            # no aggregate at all until someone deletes the bad file. Loud in the log,
            # and visible in the manifest as a row count short of what it should be.
            logger.warning("  %s: %s.parquet is unreadable (%s) — leaving it out", name, table, exc)
            return pair, None
        if "session_id" not in df.columns:
            df.insert(0, "session_id", name)
        return pair, df

    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(pairs)))) as executor:
        parsed = dict(executor.map(_read_one, pairs))

    out: dict[str, bytes] = {}
    for table in AGGREGATED_TABLES:
        # Reassembled in sorted name order, never completion order: an unchanged set of
        # sessions has to produce the same bytes, or no digest over the result means
        # anything.
        frames = [df for name in names if (df := parsed[(name, table)]) is not None]
        if not frames:
            logger.warning("  %s: no parquet files found across any session — skipped.", table)
            continue
        combined = pd.concat(frames, ignore_index=True)
        sink = io.BytesIO()
        _write_parquet(combined, sink)
        out[table] = sink.getvalue()
        logger.info("  %s → %d rows from %d session(s)", table, len(combined), len(frames))

    if SESSION_TABLE not in out:
        logger.error(
            "No %s.parquet found in any session — the export has no identity table "
            "and nothing to join on; aborting aggregation.",
            SESSION_TABLE,
        )
        return {}
    return out


def aggregate(
    sessions_dir: Path,
    output_dir: Path,
    *,
    include: Callable[[Path], bool] | None = None,
) -> None:
    """Concatenate per-session parquets into experiment-level files.

    Writes one flat ``output_dir/{table}.parquet`` for each name in
    :data:`AGGREGATED_TABLES`, with a ``session_id`` column for joins. The
    filesystem-shaped entry point to :func:`aggregate_tables`, which does the work.

    What gets aggregated is fixed, not configurable: the set is a property of
    the schema — which tables are small enough to scan experiment-wide — rather
    than something a caller should decide per run.

    Per-session files are never deleted. Copying rows into an aggregate is not a
    reason to destroy the source: the per-session files are what the ``aggregate``
    subcommand reads back, and the only copies carrying provenance in their
    parquet schema.

    Parameters
    ----------
    sessions_dir:
        Directory produced by :func:`process_sessions`
        (i.e. ``output_dir/sessions/``).
    output_dir:
        Root output directory where aggregated files are written.
    include:
        Optional predicate called with each session directory; return ``False`` to
        leave it out. ``None`` (default) aggregates every subdirectory found.

        Exists because *this* function cannot tell a complete session from one
        abandoned partway through — both are just a directory of parquets. A
        caller that does know can say so. The server layer passes a
        predicate that reads each session's ``output.metadata.json``.
    """
    sessions_dir = Path(sessions_dir)
    output_dir = Path(output_dir)

    session_dirs = sorted(d for d in sessions_dir.iterdir() if d.is_dir())
    if not session_dirs:
        logger.warning("No session directories found under %s", sessions_dir)
        return

    if include is not None:
        kept = [d for d in session_dirs if include(d)]
        for sd in session_dirs:
            if sd not in kept:
                logger.warning("  %s: excluded by the caller's filter — skipping from aggregation", sd.name)
        session_dirs = kept
        if not session_dirs:
            logger.warning("Every session was excluded; nothing to aggregate.")
            return

    def _read(name: str, table: str) -> bytes | None:
        path = sessions_dir / name / f"{table}.parquet"
        return path.read_bytes() if path.exists() else None

    tables = aggregate_tables([d.name for d in session_dirs], _read)
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, payload in tables.items():
        dest = output_dir / f"{table}.parquet"
        dest.write_bytes(payload)
        logger.info("  %s → %s", table, dest.name)
