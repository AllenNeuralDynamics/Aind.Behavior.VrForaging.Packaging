"""Multi-session export pipeline (two phases).

Phase 1 — :func:`process_sessions`: iterate raw session directories → per-session parquets.
Phase 2 — :func:`aggregate`: read per-session parquets → hive-partitioned dataset outputs.
"""

import logging
import shutil
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .session_pipeline import _write_parquet, create_processors

if TYPE_CHECKING:
    import contraqctor.contract

    from ._base import AbstractProcessor

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


def _write_session_nwb(
    raw_path: Path,
    session_out: Path,
    dataset: "contraqctor.contract.Dataset | None",
    processors: Sequence["AbstractProcessor"],
    *,
    raise_on_error: bool,
) -> Path | None:
    """Build and write one NWB-Zarr store for a session.

    Parameters
    ----------
    raw_path:
        Root directory of the raw session (used to initialise :class:`NwbSession`).
    session_out:
        Per-session output directory (``sessions/{session_id}/``); the store is
        written as ``session_out/{session_id}.nwb.zarr``.
    dataset:
        Already-loaded contraqctor Dataset (avoids a second ``load_dataset`` call).
    processors:
        Filtered processor list — the same one used for the parquet step.
    raise_on_error:
        When ``True``, any failure propagates. When ``False`` (default), the
        error is logged and ``None`` is returned so the session is not dropped
        from the result list.

    Returns
    -------
    Path | None
        Path to the written ``.nwb.zarr`` directory, or ``None`` on failure.
    """
    from .nwb_file import NwbSession

    session_id = raw_path.name
    dest = session_out / f"{session_id}.nwb.zarr"

    try:
        session = NwbSession(raw_path, dataset=dataset)
        session.run(*processors)
        # NWBZarrIO("w") does not reliably clear a pre-existing store; remove it
        # first so a re-run with clean=False never mixes old and new objects.
        if dest.exists():
            shutil.rmtree(dest)
        session.write_nwb_zarr(dest)
    except Exception as exc:
        logger.warning("[%s] NWB export FAILED: %s", session_id, exc, exc_info=True)
        if raise_on_error:
            raise
        return None

    logger.info("[%s] NWB → %s", session_id, dest.name)
    return dest


def _process_one_session(
    raw_path: Path,
    sessions_dir: Path,
    include_set: frozenset[str],
    exclude_set: frozenset[str],
    raise_on_error: bool,
    write_nwb: bool,
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

    def _keep(name: str) -> bool:
        if name == "session":  # never filtered; Phase 2 depends on session.parquet
            return True
        if include_set and name not in include_set:
            logger.debug("[%s] skip %s (not in include list)", session_id, name)
            return False
        if name in exclude_set:
            logger.debug("[%s] skip %s (excluded)", session_id, name)
            return False
        return True

    selected = [p for p in all_processors if _keep(p.output_name)]

    ran = 0
    for proc in selected:
        name = proc.output_name
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

    if write_nwb:
        _write_session_nwb(raw_path, session_out, ds, selected, raise_on_error=raise_on_error)

    return session_out


def process_sessions(
    dataset_paths: Iterable[Path],
    output_dir: Path,
    *,
    include_processors: Sequence[str] = (),
    exclude_processors: Sequence[str] = (),
    raise_on_error: bool = False,
    max_workers: int = 1,
    clean: bool = True,
    write_nwb: bool = False,
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
    clean:
        When ``True`` (default), delete *output_dir* entirely before writing
        anything.  This guarantees that a re-run never mixes outputs from two
        different invocations.  Set to ``False`` only when you intentionally
        want to resume a partial run.
    write_nwb:
        When ``True``, also write a NWB-Zarr store for each session alongside
        its parquet files (``sessions/{session_id}/{session_id}.nwb.zarr``).
        The same processor include/exclude filter applies to both outputs.
        Requires the AIND metadata JSON files to be present in each session
        root; sessions missing them will log a warning and skip NWB while
        still writing their parquets (unless *raise_on_error* is ``True``).
        Defaults to ``False``.

    Returns
    -------
    list[Path]
        Paths to the written session directories (``output_dir/sessions/{session_id}``).
    """
    paths = [Path(p) for p in dataset_paths]
    output_dir = Path(output_dir)
    sessions_dir = output_dir / "sessions"

    if clean and output_dir.exists():
        shutil.rmtree(output_dir)
        logger.info("Cleared output directory for clean run: %s", output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    include_set = frozenset(include_processors)
    exclude_set = frozenset(exclude_processors)

    def _submit(raw_path: Path) -> Path | None:
        return _process_one_session(raw_path, sessions_dir, include_set, exclude_set, raise_on_error, write_nwb)

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
