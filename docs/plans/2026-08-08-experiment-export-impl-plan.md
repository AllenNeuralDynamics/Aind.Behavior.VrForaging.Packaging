# Experiment Export Pipeline — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Two-phase lazy export pipeline: `process_sessions()` writes per-session parquets; `aggregate()` concatenates them by subject or dataset; a full `CliApp`-based CLI wires both phases together.

**Architecture:** New `SessionMetadataProcessor` produces a single-row provenance DataFrame per session. `_experiment.py` owns the two public functions and the `Aggregator` config object. `cli.py` is a clean `pydantic-settings` `CliApp` that calls those functions. Integration test exercises the full round-trip on already-cached S3 data.

**Tech Stack:** Python 3.11+, `pydantic-settings>=2.10.1` (`CliApp`), `pandas`, `contraqctor`, existing `AbstractProcessor` / `create_processors()` / `run_session()` infrastructure.

**Key conventions (read before touching any file):**
- All processors live under `src/aind_behavior_vr_foraging_packaging/processing/`.
- `AbstractProcessor._compute()` returns `pd.DataFrame`. `compute()` stamps provenance attrs automatically.
- `run_session()` and `create_processors()` in `pipeline.py` are the per-session building blocks — do not modify them.
- `PackagingProvenance.build(dataset)` → `{packaging_version, data_contract_version, dataset_version}`.
- `_write_parquet(df, path)` (private in `pipeline.py`) — replicate its logic or inline it; do **not** import it (it's private).
- Integration tests live under `tests/integration/`, use `@pytest.mark.integration`, and pull cached data from `tests/integration/.cache/aind-open-data/`.
- The existing `_load_dataset(entry)` pattern in `test_datasets.py` is the canonical way to construct a Dataset from a manifest entry.
- AIND session folder naming: `behavior_{subject_id}_{YYYY-MM-DD}_{HH-MM-SS}` (newer) or `{subject_id}_{YYYY-MM-DD}_{HH-MM-SS}` (legacy).

---

## Task 1: `SessionMetadataProcessor`

**Files:**
- Create: `src/aind_behavior_vr_foraging_packaging/processing/_session_metadata.py`
- Create: `tests/processing/test_session_metadata.py`

---

### Step 1: Write the failing test

```python
# tests/processing/test_session_metadata.py
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from aind_behavior_vr_foraging_packaging.processing._session_metadata import (
    SessionMetadataProcessor,
    _parse_aind_session_name,
)


@pytest.mark.parametrize(
    "folder_name, expected_subject, expected_date",
    [
        ("behavior_815103_2025-11-05_22-52-21", "815103", "2025-11-05"),
        ("716458_2024-05-13_09-03-55", "716458", "2024-05-13"),
        ("behavior_808728_2025-12-10_20-40-41", "808728", "2025-12-10"),
    ],
)
def test_parse_aind_session_name(folder_name, expected_subject, expected_date):
    subject_id, date = _parse_aind_session_name(folder_name)
    assert subject_id == expected_subject
    assert date == expected_date


def test_parse_aind_session_name_invalid():
    with pytest.raises(ValueError, match="Cannot parse"):
        _parse_aind_session_name("not_a_valid_name")


def _make_mock_dataset(version: str = "0.6.1") -> MagicMock:
    ds = MagicMock()
    ds.version = version
    return ds


def test_session_metadata_processor_output_columns():
    ds = _make_mock_dataset()
    session_path = Path("behavior_815103_2025-11-05_22-52-21")
    proc = SessionMetadataProcessor(ds, session_path=session_path)
    # _compute() is called by compute() which stamps provenance attrs
    df = proc._compute()
    assert len(df) == 1
    assert set(df.columns) >= {"session_id", "subject_id", "date"}
    assert df["session_id"].iloc[0] == "behavior_815103_2025-11-05_22-52-21"
    assert df["subject_id"].iloc[0] == "815103"
    assert df["date"].iloc[0] == "2025-11-05"


def test_session_metadata_output_name():
    ds = _make_mock_dataset()
    proc = SessionMetadataProcessor(ds, session_path=Path("behavior_815103_2025-11-05_22-52-21"))
    assert proc.output_name == "session_metadata"
```

### Step 2: Run to confirm failure

```bash
uv run pytest tests/processing/test_session_metadata.py -v
```
Expected: `ImportError` — module doesn't exist yet.

### Step 3: Implement

```python
# src/aind_behavior_vr_foraging_packaging/processing/_session_metadata.py
"""Processor that extracts session-level identity metadata from the AIND folder name."""

import re
from pathlib import Path

import pandas as pd

from .._base import AbstractProcessor

_AIND_NAME_RE = re.compile(
    r"^(?:behavior_)?(\d+)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}-\d{2}$"
)


def _parse_aind_session_name(folder_name: str) -> tuple[str, str]:
    """Return ``(subject_id, date)`` parsed from an AIND session folder name.

    Handles both formats:
    - ``behavior_{subject_id}_{YYYY-MM-DD}_{HH-MM-SS}``
    - ``{subject_id}_{YYYY-MM-DD}_{HH-MM-SS}``

    Raises
    ------
    ValueError
        If *folder_name* does not match either pattern.
    """
    m = _AIND_NAME_RE.match(folder_name)
    if m is None:
        raise ValueError(
            f"Cannot parse AIND session name '{folder_name}'. "
            "Expected format: [behavior_]{subject_id}_{YYYY-MM-DD}_{HH-MM-SS}"
        )
    return m.group(1), m.group(2)


class SessionMetadataProcessor(AbstractProcessor):
    """Produces a single-row DataFrame of session-level metadata.

    Unlike other processors this one needs the *session_path* passed explicitly
    at construction time — the Dataset object itself does not expose a filesystem
    path in contraqctor's public API.
    """

    __output_name__ = "session_metadata"

    def __init__(self, dataset, *, session_path: Path, raise_on_error: bool = False) -> None:
        super().__init__(dataset, raise_on_error=raise_on_error)
        self._session_path = Path(session_path)

    def _compute(self) -> pd.DataFrame:
        folder_name = self._session_path.name
        try:
            subject_id, date = _parse_aind_session_name(folder_name)
        except ValueError:
            if self._raise_on_error:
                raise
            subject_id, date = "unknown", "unknown"

        return pd.DataFrame(
            [
                {
                    "session_id": folder_name,
                    "subject_id": subject_id,
                    "date": date,
                }
            ]
        )
```

### Step 4: Run tests

```bash
uv run pytest tests/processing/test_session_metadata.py -v
```
Expected: all green.

### Step 5: Export from `processing/__init__.py`

Open `src/aind_behavior_vr_foraging_packaging/processing/__init__.py`. Add at the bottom of imports:
```python
from ._session_metadata import SessionMetadataProcessor
```
Add `"SessionMetadataProcessor"` to `__all__`.

### Step 6: Run full unit suite to check no regressions

```bash
uv run pytest tests/ -m "not integration" -v
```
Expected: all green.

### Step 7: Commit

```bash
git add src/aind_behavior_vr_foraging_packaging/processing/_session_metadata.py \
        src/aind_behavior_vr_foraging_packaging/processing/__init__.py \
        tests/processing/test_session_metadata.py
git commit -m "feat: add SessionMetadataProcessor for session-level identity metadata"
```

---

## Task 2: `_experiment.py` — Core Pipeline

**Files:**
- Create: `src/aind_behavior_vr_foraging_packaging/_experiment.py`
- Create: `tests/test_experiment.py`

---

### Step 1: Write failing tests

```python
# tests/test_experiment.py
"""Unit tests for _experiment.py — no real dataset I/O required."""
from pathlib import Path

import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging._experiment import (
    DEFAULT_AGGREGATOR,
    AggregationLevel,
    AggregationRule,
    Aggregator,
    aggregate,
)


# ---------------------------------------------------------------------------
# AggregationRule / Aggregator
# ---------------------------------------------------------------------------


def test_aggregation_rule_fields():
    rule = AggregationRule(table="licks", level=AggregationLevel.SUBJECT)
    assert rule.table == "licks"
    assert rule.level == AggregationLevel.SUBJECT


def test_aggregator_default():
    tables = {r.table for r in DEFAULT_AGGREGATOR.rules}
    assert "trials" in tables
    assert "licks" in tables


# ---------------------------------------------------------------------------
# aggregate() — write fake per-session parquets, verify outputs
# ---------------------------------------------------------------------------


def _write_fake_session(sessions_dir: Path, session_id: str, subject_id: str) -> None:
    """Write minimal parquet files for a fake session."""
    d = sessions_dir / session_id
    d.mkdir(parents=True)

    # session_metadata: 1 row
    pd.DataFrame([{"session_id": session_id, "subject_id": subject_id, "date": "2025-01-01"}]).to_parquet(
        d / "session_metadata.parquet", index=False
    )
    # trials: 3 rows
    pd.DataFrame({"trial": [1, 2, 3]}).to_parquet(d / "trials.parquet", index=False)
    # licks: 5 rows
    pd.DataFrame({"t": range(5)}).to_parquet(d / "licks.parquet", index=False)


def test_aggregate_sessions_parquet(tmp_path):
    """sessions.parquet is always created from session_metadata files."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub1")

    aggregate(sessions_dir, tmp_path, DEFAULT_AGGREGATOR)

    sessions = pd.read_parquet(tmp_path / "sessions.parquet")
    assert len(sessions) == 2
    assert set(sessions.columns) >= {"session_id", "subject_id", "date"}


def test_aggregate_dataset_level(tmp_path):
    """DATASET-level tables are concatenated to output_dir/{table}.parquet with session_id column."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub2")

    agg = Aggregator(rules=[AggregationRule("trials", AggregationLevel.DATASET)])
    aggregate(sessions_dir, tmp_path, agg)

    df = pd.read_parquet(tmp_path / "trials.parquet")
    assert len(df) == 6  # 3 rows * 2 sessions
    assert "session_id" in df.columns
    assert set(df["session_id"].unique()) == {"sess_A", "sess_B"}


def test_aggregate_subject_level(tmp_path):
    """SUBJECT-level tables are written to output_dir/subjects/{subject_id}/{table}.parquet."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    _write_fake_session(sessions_dir, "sess_B", "sub1")
    _write_fake_session(sessions_dir, "sess_C", "sub2")

    agg = Aggregator(rules=[AggregationRule("licks", AggregationLevel.SUBJECT)])
    aggregate(sessions_dir, tmp_path, agg)

    sub1 = pd.read_parquet(tmp_path / "subjects" / "sub1" / "licks.parquet")
    assert len(sub1) == 10  # 5 rows * 2 sessions
    assert "session_id" in sub1.columns

    sub2 = pd.read_parquet(tmp_path / "subjects" / "sub2" / "licks.parquet")
    assert len(sub2) == 5


def test_aggregate_missing_table_is_skipped(tmp_path):
    """A session that lacks a table file does not crash aggregation."""
    sessions_dir = tmp_path / "sessions"
    _write_fake_session(sessions_dir, "sess_A", "sub1")
    # sess_B has no licks.parquet
    d = sessions_dir / "sess_B"
    d.mkdir(parents=True)
    pd.DataFrame([{"session_id": "sess_B", "subject_id": "sub1", "date": "2025-01-02"}]).to_parquet(
        d / "session_metadata.parquet", index=False
    )

    agg = Aggregator(rules=[AggregationRule("licks", AggregationLevel.SUBJECT)])
    aggregate(sessions_dir, tmp_path, agg)  # must not raise

    sub1 = pd.read_parquet(tmp_path / "subjects" / "sub1" / "licks.parquet")
    assert len(sub1) == 5  # only from sess_A
```

### Step 2: Run to confirm failure

```bash
uv run pytest tests/test_experiment.py -v
```
Expected: `ImportError`.

### Step 3: Implement `_experiment.py`

```python
# src/aind_behavior_vr_foraging_packaging/_experiment.py
"""Two-phase lazy experiment export pipeline.

Phase 1 — :func:`process_sessions`: iterate raw session directories → per-session parquets.
Phase 2 — :func:`aggregate`: read per-session parquets → subject- and dataset-level outputs.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class AggregationLevel(str, Enum):
    """Granularity at which a table is concatenated during Phase 2."""

    SUBJECT = "subject"
    DATASET = "dataset"


@dataclass
class AggregationRule:
    """Maps one table name to its aggregation level."""

    table: str
    level: AggregationLevel


@dataclass
class Aggregator:
    """Configuration for Phase 2 aggregation.

    Parameters
    ----------
    rules:
        One :class:`AggregationRule` per table to aggregate.
    group_by:
        Column in ``session_metadata`` used to group sessions for
        :attr:`AggregationLevel.SUBJECT` rules. Defaults to ``"subject_id"``.
    """

    rules: list[AggregationRule]
    group_by: str = "subject_id"


DEFAULT_AGGREGATOR = Aggregator(
    rules=[
        AggregationRule("trials", AggregationLevel.DATASET),
        AggregationRule("licks", AggregationLevel.SUBJECT),
        AggregationRule("position_velocity", AggregationLevel.SUBJECT),
        AggregationRule("sniffing", AggregationLevel.SUBJECT),
        AggregationRule("events", AggregationLevel.SUBJECT),
    ]
)

# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------


def process_sessions(
    dataset_paths: Iterable[Path],
    output_dir: Path,
    *,
    include_processors: Sequence[str] = (),
    exclude_processors: Sequence[str] = (),
    raise_on_error: bool = False,
    sampling_rate_hz: float | None = 250.0,
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
    exclude_processors:
        Processors whose ``output_name`` is in this list are skipped.
    raise_on_error:
        Forwarded to every processor.
    sampling_rate_hz:
        Forwarded to position/velocity processor.

    Returns
    -------
    list[Path]
        Paths to the written session directories (``output_dir/sessions/{session_id}``).
    """
    from aind_behavior_vr_foraging.data_contract import dataset as load_dataset

    from .pipeline import create_processors
    from .processing._session_metadata import SessionMetadataProcessor

    include_set = set(include_processors)
    exclude_set = set(exclude_processors)
    sessions_dir = output_dir / "sessions"
    written: list[Path] = []

    for raw_path in dataset_paths:
        raw_path = Path(raw_path)
        session_id = raw_path.name
        session_out = sessions_dir / session_id
        session_out.mkdir(parents=True, exist_ok=True)
        logger.info("=== Processing session: %s ===", session_id)

        try:
            ds = load_dataset(raw_path)
        except Exception as exc:
            logger.error("  Failed to load dataset %s: %s", session_id, exc)
            if raise_on_error:
                raise
            continue

        all_processors = [
            SessionMetadataProcessor(ds, session_path=raw_path, raise_on_error=raise_on_error),
            *create_processors(ds, raise_on_error=raise_on_error, sampling_rate_hz=sampling_rate_hz),
        ]

        for proc in all_processors:
            name = proc.output_name
            if include_set and name not in include_set:
                logger.debug("  skip %s (not in include list)", name)
                continue
            if name in exclude_set:
                logger.debug("  skip %s (excluded)", name)
                continue

            try:
                df = proc.compute()
                _write_parquet(df, session_out / f"{name}.parquet")
                logger.info("  %s → %d rows", name, len(df))
            except Exception as exc:
                logger.warning("  %s FAILED: %s", name, exc)
                if raise_on_error:
                    raise

        written.append(session_out)
        logger.info("  session %s done (%d processors run)", session_id, len(all_processors))

    return written


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------


def aggregate(
    sessions_dir: Path,
    output_dir: Path,
    aggregator: Aggregator,
) -> None:
    """Concatenate per-session parquets into subject- and dataset-level outputs.

    Always writes ``output_dir/sessions.parquet`` from the per-session
    ``session_metadata.parquet`` files regardless of *aggregator* rules.

    Parameters
    ----------
    sessions_dir:
        Directory produced by :func:`process_sessions`
        (i.e. ``output_dir/sessions/``).
    output_dir:
        Root output directory where aggregated files are written.
    aggregator:
        Config object describing which tables to aggregate and at what level.
    """
    sessions_dir = Path(sessions_dir)
    output_dir = Path(output_dir)

    session_dirs = sorted(d for d in sessions_dir.iterdir() if d.is_dir())
    if not session_dirs:
        logger.warning("No session directories found under %s", sessions_dir)
        return

    # --- Build sessions.parquet (always) ---
    meta_frames = []
    for sd in session_dirs:
        p = sd / "session_metadata.parquet"
        if p.exists():
            meta_frames.append(pd.read_parquet(p))
        else:
            logger.warning("  Missing session_metadata.parquet in %s", sd.name)

    if meta_frames:
        sessions_df = pd.concat(meta_frames, ignore_index=True)
        _write_parquet(sessions_df, output_dir / "sessions.parquet")
        logger.info("sessions.parquet → %d rows", len(sessions_df))
    else:
        logger.error("No session_metadata.parquet files found; sessions.parquet not written.")
        return

    # --- Build subject→session_id mapping ---
    group_col = aggregator.group_by
    if group_col not in sessions_df.columns:
        logger.error(
            "Column '%s' not found in session_metadata (columns: %s); skipping aggregation.",
            group_col,
            list(sessions_df.columns),
        )
        return

    session_to_group: dict[str, str] = dict(
        zip(sessions_df["session_id"], sessions_df[group_col].astype(str))
    )

    # --- Apply each rule ---
    for rule in aggregator.rules:
        _apply_rule(rule, session_dirs, session_to_group, output_dir)


def _apply_rule(
    rule: AggregationRule,
    session_dirs: list[Path],
    session_to_group: dict[str, str],
    output_dir: Path,
) -> None:
    frames = []
    for sd in session_dirs:
        p = sd / f"{rule.table}.parquet"
        if not p.exists():
            logger.debug("  %s: no %s.parquet in %s — skipping", rule.table, rule.table, sd.name)
            continue
        df = pd.read_parquet(p)
        df.insert(0, "session_id", sd.name)
        frames.append((sd.name, df))

    if not frames:
        logger.warning("  %s: no parquet files found across any session — skipped.", rule.table)
        return

    if rule.level == AggregationLevel.DATASET:
        combined = pd.concat([df for _, df in frames], ignore_index=True)
        dest = output_dir / f"{rule.table}.parquet"
        _write_parquet(combined, dest)
        logger.info("  %s (dataset) → %d rows → %s", rule.table, len(combined), dest)

    elif rule.level == AggregationLevel.SUBJECT:
        from collections import defaultdict

        by_group: dict[str, list[pd.DataFrame]] = defaultdict(list)
        for session_id, df in frames:
            group = session_to_group.get(session_id, "unknown")
            by_group[group].append(df)

        for group, dfs in by_group.items():
            combined = pd.concat(dfs, ignore_index=True)
            dest = output_dir / "subjects" / group / f"{rule.table}.parquet"
            dest.parent.mkdir(parents=True, exist_ok=True)
            _write_parquet(combined, dest)
            logger.info(
                "  %s (subject=%s) → %d rows → %s", rule.table, group, len(combined), dest
            )


# ---------------------------------------------------------------------------
# Parquet writer (mirrors pipeline._write_parquet logic)
# ---------------------------------------------------------------------------


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write *df* to *path*, promoting ``df.attrs`` as parquet key-value metadata."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    extra_meta = {k: str(v) for k, v in df.attrs.items()}
    if extra_meta:
        existing = table.schema.metadata or {}
        table = table.replace_schema_metadata({**existing, **extra_meta})
    pq.write_table(table, path)
```

### Step 4: Run tests

```bash
uv run pytest tests/test_experiment.py -v
```
Expected: all green.

### Step 5: Run full unit suite

```bash
uv run pytest tests/ -m "not integration" -v
```
Expected: all green.

### Step 6: Commit

```bash
git add src/aind_behavior_vr_foraging_packaging/_experiment.py \
        tests/test_experiment.py
git commit -m "feat: add _experiment.py with process_sessions/aggregate/Aggregator"
```

---

## Task 3: CLI Full Rewrite

**Files:**
- Rewrite: `src/aind_behavior_vr_foraging_packaging/cli.py`
- Create: `tests/test_cli.py`

---

### Step 1: Write failing tests

```python
# tests/test_cli.py
"""Unit tests for cli.py — no real I/O, patch process_sessions / aggregate."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _run_cli(args: list[str]):
    """Helper: invoke ExportSettings.cli_cmd() with patched sys.argv."""
    from pydantic_settings import CliApp

    from aind_behavior_vr_foraging_packaging.cli import ExportSettings

    return CliApp.run(ExportSettings, cli_args=args)


def test_cli_calls_both_phases(tmp_path):
    """With no skip flags both process_sessions and aggregate are called."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate") as mock_agg,
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={output_dir}",
        ])

    mock_ps.assert_called_once()
    mock_agg.assert_called_once()


def test_cli_skip_processing(tmp_path):
    """--skip-processing skips process_sessions but still calls aggregate."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate") as mock_agg,
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            "--skip-processing",
        ])

    mock_ps.assert_not_called()
    mock_agg.assert_called_once()


def test_cli_skip_aggregation(tmp_path):
    """--skip-aggregation calls process_sessions but skips aggregate."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate") as mock_agg,
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            "--skip-aggregation",
        ])

    mock_ps.assert_called_once()
    mock_agg.assert_not_called()


def test_cli_exclude_processors_forwarded(tmp_path):
    """--exclude-processors values are forwarded to process_sessions."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate"),
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            "--exclude-processors", "sniffing", "software_events",
        ])

    _, kwargs = mock_ps.call_args
    assert set(kwargs.get("exclude_processors", [])) == {"sniffing", "software_events"}


def test_cli_log_file_created(tmp_path):
    """--log-file creates the file when the run completes."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    log_file = tmp_path / "run.log"

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions"),
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate"),
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            f"--log-file={log_file}",
        ])

    assert log_file.exists()
```

### Step 2: Run to confirm failure

```bash
uv run pytest tests/test_cli.py -v
```
Expected: `ImportError` or attribute errors.

### Step 3: Implement `cli.py`

```python
# src/aind_behavior_vr_foraging_packaging/cli.py
"""CLI entry point for the experiment export pipeline.

Provides the ``aind-vr-export`` command, which runs two independent phases:

1. **process_sessions** — run all processors on every raw session directory.
2. **aggregate** — concatenate per-session parquets into subject/dataset outputs.

Usage examples::

    # Full run
    aind-vr-export --input-dir /data/raw --output-dir /data/export

    # Skip sniffing, write a log file
    aind-vr-export --input-dir /data/raw --output-dir /data/export \\
        --exclude-processors sniffing software_events \\
        --log-file /data/export/run.log

    # Re-aggregate only (sessions/ already written)
    aind-vr-export --input-dir /data/raw --output-dir /data/export \\
        --skip-processing
"""

import logging
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, CliApp, SettingsConfigDict

from ._experiment import (
    DEFAULT_AGGREGATOR,
    AggregationLevel,
    AggregationRule,
    Aggregator,
    aggregate,
    process_sessions,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def _setup_logging(log_file: Path | None = None, level: int = logging.INFO) -> None:
    """Configure root logger with console + optional file handler."""
    root = logging.getLogger()
    if root.handlers:
        # Already configured (e.g. in tests); don't add duplicate handlers.
        return
    root.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    if log_file is not None:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
        logger.info("Logging to file: %s", log_file)


# ---------------------------------------------------------------------------
# Settings / CLI
# ---------------------------------------------------------------------------


class ExportSettings(BaseSettings):
    """Pydantic-settings model whose fields map 1-to-1 to CLI flags.

    ``CliApp.run(ExportSettings)`` parses ``sys.argv``, validates the fields,
    and calls :meth:`cli_cmd`.
    """

    model_config = SettingsConfigDict(cli_parse_args=True)

    # ---- Required ----
    input_dir: Path
    """Root folder whose immediate subdirectories are raw session directories."""
    output_dir: Path
    """Destination root for the experiment export."""

    # ---- Logging ----
    log_file: Path | None = None
    """Path to a log file. Appended to if it already exists."""

    # ---- Processor filter ----
    include_processors: list[str] = []
    """Output names of processors to include (empty = all)."""
    exclude_processors: list[str] = []
    """Output names of processors to skip (e.g. ``sniffing``, ``software_events``)."""

    # ---- Aggregation ----
    dataset_tables: list[str] = ["trials"]
    """Tables to concatenate across all sessions (dataset level)."""
    subject_tables: list[str] = ["licks", "position_velocity", "sniffing", "events"]
    """Tables to concatenate per subject (subject level)."""

    # ---- Phase control ----
    skip_processing: bool = False
    """Skip Phase 1; assume sessions/ already populated."""
    skip_aggregation: bool = False
    """Skip Phase 2; write only per-session parquets."""

    raise_on_error: bool = False
    """Raise on the first processor failure (default: log and continue)."""

    def _build_aggregator(self) -> Aggregator:
        rules = [AggregationRule(t, AggregationLevel.DATASET) for t in self.dataset_tables]
        rules += [AggregationRule(t, AggregationLevel.SUBJECT) for t in self.subject_tables]
        return Aggregator(rules=rules)

    def cli_cmd(self) -> None:
        _setup_logging(self.log_file)
        logger.info("=== aind-vr-export started ===")
        logger.info("  input_dir  : %s", self.input_dir)
        logger.info("  output_dir : %s", self.output_dir)

        dataset_paths = sorted(
            p for p in self.input_dir.iterdir() if p.is_dir()
        )
        if not dataset_paths:
            logger.warning("No subdirectories found under %s — nothing to do.", self.input_dir)
            return

        logger.info("  sessions found: %d", len(dataset_paths))
        sessions_dir = self.output_dir / "sessions"

        if not self.skip_processing:
            process_sessions(
                dataset_paths,
                self.output_dir,
                include_processors=self.include_processors,
                exclude_processors=self.exclude_processors,
                raise_on_error=self.raise_on_error,
            )

        if not self.skip_aggregation:
            aggregator = self._build_aggregator()
            aggregate(sessions_dir, self.output_dir, aggregator)

        logger.info("=== aind-vr-export complete ===")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    CliApp.run(ExportSettings)
```

### Step 4: Run tests

```bash
uv run pytest tests/test_cli.py -v
```
Expected: all green.

### Step 5: Run full unit suite

```bash
uv run pytest tests/ -m "not integration" -v
```
Expected: all green.

### Step 6: Commit

```bash
git add src/aind_behavior_vr_foraging_packaging/cli.py tests/test_cli.py
git commit -m "feat: rewrite cli.py with pydantic-settings CliApp (aind-vr-export command)"
```

---

## Task 4: Wire Up Entry Point

**Files:**
- Modify: `pyproject.toml`

---

### Step 1: Add entry point

In `pyproject.toml`, find `[project.scripts]` and add the new entry:

```toml
[project.scripts]
curriculum     = "aind_behavior_vr_foraging_packaging.cli:main"
aind-vr-export = "aind_behavior_vr_foraging_packaging.cli:main"
```

### Step 2: Reinstall to pick up the new script

```bash
uv sync
```

### Step 3: Smoke test the CLI help

```bash
uv run aind-vr-export --help
```
Expected: pydantic-settings auto-generated help with all fields listed.

### Step 4: Commit

```bash
git add pyproject.toml
git commit -m "chore: register aind-vr-export console script entry point"
```

---

## Task 5: Integration Test

**Files:**
- Modify: `tests/integration/conftest.py` — add `all_cached_session_paths` fixture
- Create: `tests/integration/test_experiment_export.py`

---

### Step 1: Add fixture to `tests/integration/conftest.py`

Append to the bottom of the existing `conftest.py`:

```python
@pytest.fixture(scope="session")
def all_cached_session_paths(ensure_datasets_cached) -> list[Path]:  # noqa: ARG001
    """Return all session directories present on disk in the local cache.

    Depends on ``ensure_datasets_cached`` (runs download before listing)
    so every reachable dataset from the manifest is on disk. Sessions that
    failed to download are absent from the cache and automatically skipped.
    """
    cache_dir = CACHE_ROOT / "aind-open-data"
    if not cache_dir.exists():
        return []
    return sorted(p for p in cache_dir.iterdir() if p.is_dir())
```

### Step 2: Write the integration test

```python
# tests/integration/test_experiment_export.py
"""Integration test for the two-phase experiment export pipeline.

Runs against all locally cached sessions from ``datasets.yml``.
"""

from pathlib import Path

import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging._experiment import (
    DEFAULT_AGGREGATOR,
    AggregationLevel,
    AggregationRule,
    Aggregator,
    aggregate,
    process_sessions,
)

pytestmark = pytest.mark.integration


def test_full_export_pipeline(all_cached_session_paths: list[Path], tmp_path: Path) -> None:
    """Full two-phase pipeline: process → aggregate → assert structure."""
    if not all_cached_session_paths:
        pytest.skip("No cached session data available")

    output_dir = tmp_path / "export"
    sessions_dir = output_dir / "sessions"

    # --- Phase 1 ---
    written = process_sessions(all_cached_session_paths, output_dir, raise_on_error=False)
    assert len(written) == len(all_cached_session_paths), (
        f"Expected {len(all_cached_session_paths)} session dirs, got {len(written)}"
    )

    # Every session folder must contain trials.parquet
    for session_path in written:
        assert (session_path / "trials.parquet").exists(), (
            f"Missing trials.parquet in {session_path}"
        )
        assert (session_path / "session_metadata.parquet").exists(), (
            f"Missing session_metadata.parquet in {session_path}"
        )

    # --- Phase 2 ---
    aggregate(sessions_dir, output_dir, DEFAULT_AGGREGATOR)

    # sessions.parquet: one row per session, required columns
    assert (output_dir / "sessions.parquet").exists()
    sessions = pd.read_parquet(output_dir / "sessions.parquet")
    assert len(sessions) == len(all_cached_session_paths), (
        f"sessions.parquet has {len(sessions)} rows; expected {len(all_cached_session_paths)}"
    )
    assert {"session_id", "subject_id", "date"}.issubset(set(sessions.columns))

    # dataset-level: trials.parquet with session_id column
    assert (output_dir / "trials.parquet").exists()
    all_trials = pd.read_parquet(output_dir / "trials.parquet")
    assert "session_id" in all_trials.columns
    assert not all_trials.empty

    # subject-level: at least one subject folder with licks.parquet
    subjects_dir = output_dir / "subjects"
    assert subjects_dir.exists()
    subject_dirs = [d for d in subjects_dir.iterdir() if d.is_dir()]
    assert len(subject_dirs) > 0
    for sub_dir in subject_dirs:
        assert (sub_dir / "licks.parquet").exists(), (
            f"Missing licks.parquet for subject {sub_dir.name}"
        )

    # row-count consistency: sum of per-session trials == dataset-level trials
    per_session_counts = sum(
        len(pd.read_parquet(sessions_dir / sid / "trials.parquet"))
        for sid in sessions["session_id"]
    )
    assert len(all_trials) == per_session_counts


def test_skip_aggregation_writes_only_sessions(
    all_cached_session_paths: list[Path], tmp_path: Path
) -> None:
    """--skip-aggregation equivalent: only sessions/ is written."""
    if not all_cached_session_paths:
        pytest.skip("No cached session data available")

    output_dir = tmp_path / "export"
    process_sessions(all_cached_session_paths, output_dir, raise_on_error=False)

    assert (output_dir / "sessions").exists()
    assert not (output_dir / "sessions.parquet").exists()
    assert not (output_dir / "trials.parquet").exists()


def test_exclude_processor(all_cached_session_paths: list[Path], tmp_path: Path) -> None:
    """Excluding 'sniffing' means no sniffing.parquet in any session dir."""
    if not all_cached_session_paths:
        pytest.skip("No cached session data available")

    output_dir = tmp_path / "export"
    process_sessions(
        all_cached_session_paths,
        output_dir,
        exclude_processors=["sniffing"],
        raise_on_error=False,
    )

    sessions_dir = output_dir / "sessions"
    for session_path in sessions_dir.iterdir():
        assert not (session_path / "sniffing.parquet").exists(), (
            f"sniffing.parquet should not exist in {session_path}"
        )
```

### Step 3: Run the integration test

```bash
uv run pytest tests/integration/test_experiment_export.py -m integration -v -s
```
Expected: all green (downloads happen automatically via `ensure_datasets_cached`).

If you're offline, point at the already-cached data only:
```bash
uv run pytest tests/integration/test_experiment_export.py -m integration -v -s \
  --no-header
```

### Step 4: Run full suite including integration

```bash
uv run pytest tests/ -m integration -v
```
Expected: all integration tests green.

### Step 5: Commit

```bash
git add tests/integration/conftest.py \
        tests/integration/test_experiment_export.py
git commit -m "test: add integration tests for experiment export pipeline"
```

---

## Final Verification

```bash
# All unit tests pass
uv run pytest tests/ -m "not integration" -v

# CLI help works
uv run aind-vr-export --help

# Smoke-run against the cached data (adjust path as needed)
uv run aind-vr-export \
  --input-dir tests/integration/.cache/aind-open-data \
  --output-dir /tmp/vr_export_test \
  --log-file /tmp/vr_export_test/run.log

# Inspect output
ls /tmp/vr_export_test/
ls /tmp/vr_export_test/sessions/
python -c "import pandas as pd; print(pd.read_parquet('/tmp/vr_export_test/sessions.parquet'))"
```

---

## Success Criteria

| # | Check |
|---|---|
| 1 | `uv run pytest tests/ -m "not integration"` is all green |
| 2 | `aind-vr-export --help` lists all expected flags |
| 3 | `sessions.parquet` has one row per session, columns `session_id`, `subject_id`, `date` |
| 4 | `trials.parquet` at root has `session_id` column; row count = sum of per-session rows |
| 5 | `subjects/{id}/licks.parquet` exists for each unique `subject_id` |
| 6 | `--skip-aggregation` → no top-level `sessions.parquet` or `trials.parquet` |
| 7 | `--skip-processing` → reads existing `sessions/`, writes aggregated outputs |
| 8 | `--exclude-processors sniffing` → no `sniffing.parquet` in any session dir |
| 9 | `--log-file run.log` → file exists and contains `INFO` lines |
