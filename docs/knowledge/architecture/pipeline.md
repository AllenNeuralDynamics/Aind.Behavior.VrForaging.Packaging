---
type: Component
title: Pipeline — version dispatch, fan-out, and parquet output
description: pipeline.py selects the correct processor set for a dataset version, runs them, and writes provenance-stamped parquet files.
resource: src/aind_behavior_vr_foraging_packaging/pipeline.py
tags: [architecture, pipeline, parquet, version-dispatch]
timestamp: 2026-07-03T00:00:00Z
---

`pipeline.py` is the thin orchestration layer. It answers two questions:
*which* processors apply to a given dataset, and *how* to run them to parquet.

# Version dispatch

The single source of truth for legacy behavior is:

```python
_LEGACY_VERSION_CUTOFF = semver.Version(major=0, minor=6, patch=0)
```

`create_processors(dataset, *, raise_on_error=False, sampling_rate_hz=250.0)`
parses `dataset.version` and, if it is `< 0.6.0`, swaps in the legacy variants:

| Concern | Current (`>= 0.6.0`) | Legacy (`< 0.6.0`) |
|---------|----------------------|--------------------|
| Trial table | `TrialTableProcessor` | `LegacyTrialTableProcessor` |
| Position/velocity | `PositionAndVelocityProcessor` | `LegacyPositionAndVelocityProcessor` |
| Licks / Sniffing / Software events | (shared — no legacy variant) | (same) |

The returned list is **ordered**: trial table first, then position/velocity,
then licks, sniffing, and software events.

Two convenience getters return a single version-correct processor without
building the whole list:

- `get_trial_table_processor(dataset, *, raise_on_error=False)`
- `get_position_velocity_processor(dataset, *, sampling_rate_hz=250.0, raise_on_error=False)`

# Running to parquet

`run_session(dataset, output_dir, *, raise_on_error=False, sampling_rate_hz=250.0)`:

1. Creates `output_dir` if absent.
2. For each processor from `create_processors`, calls `compute()` and writes
   `output_dir/<output_name>.parquet`.
3. Returns `dict[str, pd.DataFrame]` keyed by `output_name`.

Output filenames come straight from each processor's `output_name`:
`trials`, `position_velocity`, `licks`, `sniffing`, `software_events`.

# Provenance in parquet

`_write_parquet` promotes every key in `df.attrs` (see
[processor-abstraction.md](processor-abstraction.md)) into the parquet schema
metadata **twice**: inside the pandas metadata blob (for pandas round-trips)
and as top-level key/value entries (readable from DuckDB, Polars, R arrow,
Spark). This is why provenance is not lost when a downstream tool reads the
file without pandas.

# Examples

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.pipeline import run_session

ds = dataset("/path/to/session")
data = run_session(ds, "/path/to/out")   # writes 5 parquet files
trials = data["trials"]                    # also returned in-memory
```

See `scripts/example_parquet_pipeline.py` for all-at-once, single-stream, and
load-from-disk patterns.
