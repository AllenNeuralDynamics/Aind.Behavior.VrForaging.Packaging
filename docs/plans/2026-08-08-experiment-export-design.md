# Experiment Dataset Export — Design

**Date:** 2026-08-08  
**Issue:** [#43 — Add support for exporting experiment datasets with hierarchical aggregation](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging/issues/43)  
**Branch:** `feat-export`

---

## Goals

Accept a folder of raw session directories, run all processors on every session,
write per-session parquet tables, then optionally aggregate selected tables across
sessions at the **subject** or **dataset** level. A final `sessions.parquet`
summarises one row per session for downstream filtering.

---

## Architecture: Two-Phase Lazy Pipeline

Phases are **independent** — Phase 2 reads only files written by Phase 1 and can
be re-run without re-processing. Both are orchestrated by the CLI but callable
independently from Python.

```
Phase 1 — process_sessions(dataset_paths, output_dir, ...)
  For each session directory:
    1. Load Dataset via contraqctor
    2. create_processors(dataset) + SessionMetadataProcessor(dataset, session_path=...)
    3. Apply include/exclude filter by processor.output_name
    4. Compute each processor → save to output_dir/sessions/{session_id}/*.parquet
    5. Log per-session outcome (success / skipped / error)

Phase 2 — aggregate(sessions_dir, output_dir, aggregator)
  Reads all output_dir/sessions/{session_id}/ folders.
  For each AggregationRule:
    SUBJECT  → group by subject_id → output_dir/subjects/{subject_id}/{table}.parquet
    DATASET  → concat all sessions  → output_dir/{table}.parquet
  Always:
    concat all session_metadata.parquet → output_dir/sessions.parquet
  Traceability: prepend session_id column to every concatenated table.
```

---

## Output Structure

```
<output_dir>/
├── sessions/                                         # Phase 1 output
│   ├── behavior_815103_2025-11-05_22-52-21/
│   │   ├── trials.parquet
│   │   ├── licks.parquet
│   │   ├── position_velocity.parquet
│   │   ├── sniffing.parquet
│   │   ├── software_events.parquet
│   │   ├── events.parquet
│   │   └── session_metadata.parquet                 # 1 row
│   └── behavior_815103_2025-11-24_22-54-13/
│       └── ...
├── subjects/                                         # Phase 2, SUBJECT-level
│   ├── 815103/
│   │   ├── licks.parquet
│   │   └── position_velocity.parquet
│   └── 808728/
│       └── ...
├── trials.parquet                                    # Phase 2, DATASET-level
└── sessions.parquet                                  # Phase 2, always generated
```

---

## New Modules

### `src/.../processing/_session_metadata.py`

`SessionMetadataProcessor` extends `AbstractProcessor` normally.  
Constructor signature: `__init__(self, dataset, *, session_path: Path, raise_on_error=False)`.  
`_compute()` returns a **single-row** DataFrame:

| Column | Source |
|---|---|
| `session_id` | `session_path.name` |
| `subject_id` | parsed from AIND name convention (`behavior_{id}_*` or `{id}_*`) |
| `date` | parsed from AIND name convention (second token) |
| `dataset_version` | `PackagingProvenance` (already on attrs via `compute()`) |
| `packaging_version` | idem |
| `data_contract_version` | idem |

AIND name parser handles both formats:
- `behavior_815103_2025-11-05_22-52-21` → subject `815103`, date `2025-11-05`
- `716458_2024-05-13_09-03-55` → subject `716458`, date `2024-05-13`

### `src/.../_experiment.py`

```python
class AggregationLevel(str, Enum):
    SUBJECT = "subject"   # concat per subject_id column
    DATASET = "dataset"   # concat all sessions flat

@dataclass
class AggregationRule:
    table: str
    level: AggregationLevel

@dataclass
class Aggregator:
    rules: list[AggregationRule]
    group_by: str = "subject_id"       # column from session_metadata

def process_sessions(
    dataset_paths: Iterable[Path],
    output_dir: Path,
    *,
    include_processors: Sequence[str] = (),   # empty = all
    exclude_processors: Sequence[str] = (),
    raise_on_error: bool = False,
    sampling_rate_hz: float | None = 250.0,
) -> list[Path]: ...   # returns list of written session dirs

def aggregate(
    sessions_dir: Path,
    output_dir: Path,
    aggregator: Aggregator,
) -> None: ...
```

Default `Aggregator` (used by CLI when no overrides given):
```python
DEFAULT_AGGREGATOR = Aggregator(rules=[
    AggregationRule("trials",             AggregationLevel.DATASET),
    AggregationRule("licks",              AggregationLevel.SUBJECT),
    AggregationRule("position_velocity",  AggregationLevel.SUBJECT),
    AggregationRule("sniffing",           AggregationLevel.SUBJECT),
    AggregationRule("events",             AggregationLevel.SUBJECT),
])
```

---

## CLI — `cli.py` (full rewrite)

Uses `pydantic-settings` `CliApp` (already a dependency at `>=2.10.1`).

```python
class ExportSettings(BaseSettings):
    model_config = SettingsConfigDict(cli_parse_args=True)

    # Required
    input_dir:  Path   # folder whose subdirectories are raw session roots
    output_dir: Path   # destination root

    # Logging
    log_file: Path | None = None   # appended to if it already exists

    # Processor filter (by output_name, e.g. "sniffing", "licks")
    include_processors: list[str] = []   # empty → include all
    exclude_processors: list[str] = []

    # Aggregation (table names → level)
    dataset_tables: list[str] = ["trials"]
    subject_tables: list[str] = ["licks", "position_velocity", "sniffing", "events"]

    # Phase control
    skip_processing:  bool = False
    skip_aggregation: bool = False

    raise_on_error: bool = False

    def cli_cmd(self) -> None: ...

def main() -> None:
    CliApp.run(ExportSettings)
```

Entry point added to `pyproject.toml`:
```toml
[project.scripts]
curriculum     = "aind_behavior_vr_foraging_packaging.cli:main"
aind-vr-export = "aind_behavior_vr_foraging_packaging.cli:main"
```

Logging: a shared `_setup_logging(log_file)` helper wires both `StreamHandler`
and an optional `FileHandler` onto the root logger at `INFO` level.

### Example invocations

```bash
# Full run
aind-vr-export --input-dir /data/raw --output-dir /data/export \
               --log-file /data/export/run.log

# Session-only (no aggregation), skip expensive processors
aind-vr-export --input-dir /data/raw --output-dir /data/export \
               --exclude-processors sniffing software_events \
               --skip-aggregation

# Re-aggregate with custom tables
aind-vr-export --input-dir /data/raw --output-dir /data/export \
               --skip-processing \
               --subject-tables licks \
               --dataset-tables trials
```

---

## Integration Test

File: `tests/integration/test_experiment_export.py`  
Marker: `@pytest.mark.integration`

```python
def test_full_export_pipeline(all_cached_session_paths, tmp_path):
    output_dir = tmp_path / "export"
    sessions_dir = output_dir / "sessions"

    process_sessions(all_cached_session_paths, output_dir)
    aggregate(sessions_dir, output_dir, DEFAULT_AGGREGATOR)

    # sessions.parquet: one row per session, required columns present
    sessions = pd.read_parquet(output_dir / "sessions.parquet")
    assert len(sessions) == len(all_cached_session_paths)
    assert {"session_id", "subject_id", "date"}.issubset(sessions.columns)

    # per-session tables written
    for sid in sessions["session_id"]:
        assert (sessions_dir / sid / "trials.parquet").exists()

    # dataset-level aggregation: session_id column injected for traceability
    trials = pd.read_parquet(output_dir / "trials.parquet")
    assert "session_id" in trials.columns

    # subject-level aggregation
    for sub in sessions["subject_id"].unique():
        assert (output_dir / "subjects" / str(sub) / "licks.parquet").exists()
```

`all_cached_session_paths` — new `scope="session"` fixture in
`tests/integration/conftest.py` that lists `.cache/aind-open-data/` subdirs
already present on disk (no S3 download; only cached data used).

---

## Files Changed

| Action | Path |
|---|---|
| **new** | `src/.../processing/_session_metadata.py` |
| **new** | `src/.../_experiment.py` |
| **rewrite** | `src/.../cli.py` |
| **edit** | `src/.../processing/__init__.py` — export `SessionMetadataProcessor` |
| **edit** | `pyproject.toml` — add `aind-vr-export` entry point |
| **new** | `tests/integration/test_experiment_export.py` |
| **edit** | `tests/integration/conftest.py` — add `all_cached_session_paths` fixture |

---

## Success Criteria

1. `aind-vr-export --input-dir <cache> --output-dir <tmp>` exits 0 against integration test data.
2. `sessions.parquet` has one row per session with `session_id`, `subject_id`, `date`.
3. `trials.parquet` at root has a `session_id` column and row count = sum of per-session trial counts.
4. Per-subject `licks.parquet` exists for each unique `subject_id`.
5. `--skip-aggregation` writes only `sessions/` and no top-level parquets.
6. `--skip-processing` reads existing `sessions/` and writes aggregated outputs.
7. `--exclude-processors sniffing` produces no `sniffing.parquet` in any session dir.
8. `--log-file run.log` writes all `INFO` lines to the file.
