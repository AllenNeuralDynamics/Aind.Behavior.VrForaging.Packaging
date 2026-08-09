"""Multi-session export pipeline (two phases).

Phase 1 — :func:`process_sessions`: iterate raw session directories → per-session parquets.
Phase 2 — :func:`aggregate`: read per-session parquets → hive-partitioned dataset outputs.
"""

import logging
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .session_pipeline import _write_parquet, create_processors

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass
class AggregationRule:
    """One table to concatenate into a flat parquet file during Phase 2.

    Parameters
    ----------
    table:
        Name of the per-session parquet file (without extension) to aggregate.
    cleanup:
        When ``True`` (default), delete the per-session ``{table}.parquet``
        files after writing the flat aggregate — avoids storing the same data
        in two places. Set to ``False`` to keep the per-session copies.
    """

    table: str
    cleanup: bool = True


@dataclass
class Aggregator:
    """Configuration for Phase 2 aggregation.

    Parameters
    ----------
    rules:
        One :class:`AggregationRule` per table to write as a
        hive-partitioned dataset partitioned by ``session_id``.
    """

    rules: list[AggregationRule]


DEFAULT_AGGREGATOR = Aggregator(
    rules=[
        AggregationRule("sites"),
    ]
)


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------


def _process_one_session(
    raw_path: Path,
    sessions_dir: Path,
    include_set: frozenset[str],
    exclude_set: frozenset[str],
    raise_on_error: bool,
) -> Path | None:
    """Process a single session directory and write per-processor parquets.

    Returns the written session directory on success, ``None`` when the
    dataset fails to load and *raise_on_error* is ``False``.
    """
    from aind_behavior_vr_foraging.data_contract import dataset as load_dataset

    raw_path = Path(raw_path)
    session_id = raw_path.name
    session_out = sessions_dir / session_id
    session_out.mkdir(parents=True, exist_ok=True)
    logger.info("[%s] Processing session", session_id)

    try:
        ds = load_dataset(raw_path)
    except Exception:
        logger.exception("[%s] Failed to load dataset", session_id)
        if raise_on_error:
            raise
        return None

    all_processors = create_processors(
        ds,
        session_path=raw_path,
        raise_on_error=raise_on_error,
    )

    ran = 0
    for proc in all_processors:
        name = proc.output_name
        # session is never filtered out
        if name != "session":
            if include_set and name not in include_set:
                logger.debug("[%s] skip %s (not in include list)", session_id, name)
                continue
            if name in exclude_set:
                logger.debug("[%s] skip %s (excluded)", session_id, name)
                continue

        try:
            df = proc.compute()
            _write_parquet(df, session_out / f"{name}.parquet")
            logger.info("[%s] %s → %d rows", session_id, name, len(df))
            ran += 1
        except Exception as exc:
            logger.warning("[%s] %s FAILED: %s", session_id, name, exc, exc_info=True)
            if raise_on_error:
                raise

    logger.info("[%s] done (%d processors ran)", session_id, ran)
    return session_out


def process_sessions(
    dataset_paths: Iterable[Path],
    output_dir: Path,
    *,
    include_processors: Sequence[str] = (),
    exclude_processors: Sequence[str] = (),
    raise_on_error: bool = False,
    max_workers: int = 1,
) -> list[Path]:
    """Run all processors on every session directory and write per-session parquets.

    Parameters
    ----------
    dataset_paths:
        Iterable of paths, each pointing to the root directory of one raw session.
    output_dir:
        Root of the experiment export. Per-session files go to
        ``output_dir/sessions/{session_id}/``.
    include_processors:
        If non-empty, only processors whose ``output_name`` is in this list run.
        ``session`` is always included regardless of this filter.
    exclude_processors:
        Processors whose ``output_name`` is in this list are skipped.
    raise_on_error:
        Forwarded to every processor. When ``False`` (default) failures are
        logged and the session continues.
    max_workers:
        Number of parallel threads. ``1`` (default) runs sessions sequentially.
        Values ``> 1`` process up to *max_workers* sessions concurrently via
        :class:`~concurrent.futures.ThreadPoolExecutor`.

    Returns
    -------
    list[Path]
        Paths to the written session directories (``output_dir/sessions/{session_id}``).
    """
    paths = [Path(p) for p in dataset_paths]
    sessions_dir = output_dir / "sessions"
    include_set = frozenset(include_processors)
    exclude_set = frozenset(exclude_processors)

    def _submit(raw_path: Path) -> Path | None:
        return _process_one_session(raw_path, sessions_dir, include_set, exclude_set, raise_on_error)

    if max_workers == 1:
        return [r for p in paths if (r := _submit(p)) is not None]

    written: list[Path] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_submit, p): p for p in paths}
        for fut in as_completed(futures):
            result = fut.result()  # re-raises if raise_on_error=True
            if result is not None:
                written.append(result)
    return written


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------


def aggregate(
    sessions_dir: Path,
    output_dir: Path,
    aggregator: Aggregator,
) -> None:
    """Concatenate per-session parquets into flat aggregate files.

    Always writes ``output_dir/session.parquet`` from the per-session
    ``session.parquet`` files, regardless of *aggregator* rules.

    Each rule in *aggregator* writes a single flat ``output_dir/{table}.parquet``
    containing all sessions, with a ``session_id`` column added for joins.
    When ``rule.cleanup`` is ``True`` the per-session source files are removed
    after aggregation to avoid storing the same data twice.

    Parameters
    ----------
    sessions_dir:
        Directory produced by :func:`process_sessions`
        (i.e. ``output_dir/sessions/``).
    output_dir:
        Root output directory where aggregated files are written.
    aggregator:
        Config object listing which tables to aggregate.
    """
    sessions_dir = Path(sessions_dir)
    output_dir = Path(output_dir)

    session_dirs = sorted(d for d in sessions_dir.iterdir() if d.is_dir())
    if not session_dirs:
        logger.warning("No session directories found under %s", sessions_dir)
        return

    # --- Build session.parquet (always) ---
    meta_frames = []
    for sd in session_dirs:
        p = sd / "session.parquet"
        if p.exists():
            meta_frames.append(pd.read_parquet(p))
        else:
            logger.warning("  Missing session.parquet in %s", sd.name)

    if not meta_frames:
        logger.error("No session.parquet files found; session.parquet not written.")
        return

    sessions_df = pd.concat(meta_frames, ignore_index=True)
    _write_parquet(sessions_df, output_dir / "session.parquet")
    logger.info("session.parquet → %d rows", len(sessions_df))

    # --- Apply each rule ---
    for rule in aggregator.rules:
        _apply_rule(rule, session_dirs, output_dir)


def _apply_rule(
    rule: AggregationRule,
    session_dirs: list[Path],
    output_dir: Path,
) -> None:
    frames: list[pd.DataFrame] = []
    source_files: list[Path] = []

    for sd in session_dirs:
        p = sd / f"{rule.table}.parquet"
        if not p.exists():
            logger.debug("  %s: no %s.parquet in %s — skipping", rule.table, rule.table, sd.name)
            continue
        df = pd.read_parquet(p)
        df.insert(0, "session_id", sd.name)
        frames.append(df)
        source_files.append(p)

    if not frames:
        logger.warning("  %s: no parquet files found across any session — skipped.", rule.table)
        return

    combined = pd.concat(frames, ignore_index=True)
    dest = output_dir / f"{rule.table}.parquet"
    combined.to_parquet(dest, index=False)
    logger.info("  %s → %d rows → %s", rule.table, len(combined), dest.name)

    if rule.cleanup:
        for p in source_files:
            p.unlink()
            logger.debug("  removed per-session copy %s", p)
