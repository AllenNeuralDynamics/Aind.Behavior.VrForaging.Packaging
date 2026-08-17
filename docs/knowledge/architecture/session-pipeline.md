---
type: Component
title: Session pipeline — version dispatch, fan-out, and parquet output
description: session_pipeline.py selects the correct processor set for a dataset version, runs them over one session via process_session, and writes provenance-stamped parquet files.
resource: src/aind_behavior_vr_foraging_packaging/session_pipeline.py
tags: [architecture, pipeline, parquet, version-dispatch]
timestamp: 2026-08-16T00:00:00Z
---

`session_pipeline.py` is the thin orchestration layer for **one** session. It
answers two questions: *which* processors apply to a given dataset, and *how* to
run them to parquet. Running many sessions and combining their outputs is a
separate layer — see [export-pipeline.md](export-pipeline.md).

# Version dispatch

The single source of truth for legacy behavior is:

```python
_LEGACY_VERSION_CUTOFF = semver.Version(major=0, minor=6, patch=0)
```

`create_processors(dataset, *, strict_parsing=False)` parses
`dataset.version` and, if it is `< 0.6.0`, swaps in the legacy variants:

| Concern | Current (`>= 0.6.0`) | Legacy (`< 0.6.0`) |
|---------|----------------------|--------------------|
| Site table | `SiteTableProcessor` | `LegacySiteTableProcessor` |
| Position/velocity | `PositionAndVelocityProcessor` | `LegacyPositionAndVelocityProcessor` |
| Licks / Sniffing / Software events / Events | (shared — no legacy variant) | (same) |

The returned list is **ordered**: session metadata first, then
position/velocity, site table, licks, sniffing, software events, and events.

`SessionMetadataProcessor` (`output_name` `session`) is always first and takes
no extra arguments. It emits the one-row-per-session table used for
dataset-level filtering: `session_id` is the session directory's name, while
`subject` and `date` come from the contraqctor `Behavior/InputSchemas/Session`
stream. The stream's own `session_name` field is ignored — it is `null` on
pre-0.6 datasets and, where populated, formatted differently from the
directory that every other layer keys on.

Two convenience getters return a single version-correct processor without
building the whole list:

- `resolve_site_table_processor(dataset, *, strict_parsing=False)`
- `resolve_position_velocity_processor(dataset, *, sampling_rate_hz=250.0, strict_parsing=False)`

# Running to parquet

```python
process_session(
    dataset, output_dir=".", *,
    strict_parsing=False, processors=None, on_error=None,
    write_parquet=True, write_nwb=False,
) -> dict[str, pd.DataFrame]
```

`output_dir` accepts a `str` or `Path` and defaults to the current working
directory, so `process_session(ds)` is a complete call.

Log lines are prefixed with `[{session_id}]`, taken from the dataset via
`_base.session_root`, so per-processor progress stays grep-able by session in a
batch run without the caller having to pass a label down.

1. Creates `output_dir` if either writer is enabled.
2. For each processor, calls `compute()` and, when `write_parquet`, writes
   `output_dir/<output_name>.parquet`.
3. When `write_nwb`, writes `output_dir/<session_id>.nwb.zarr`.
4. Returns `dict[str, pd.DataFrame]` keyed by `output_name`.

Output filenames come straight from each processor's `output_name`: `session`,
`sites`, `position_velocity`, `licks`, `sniffing`, `software_events`, `events`.

# Both output formats, one function

`write_parquet` (default `True`) and `write_nwb` (default `False`) are
independent switches over the *same* computed frames. Every processor runs
either way — the flags choose what reaches disk, not what is computed — so the
returned dict is identical whichever combination is set, and
`write_parquet=False` is a legitimate way to compute in memory without touching
disk. Turning both off writes nothing and creates no directory.

This is the reason `session_pipeline` owns the NWB write rather than
`export_pipeline`. The NWB step needs exactly what the parquet step needs — a
loaded dataset and a processor list — so a second copy of the fan-out one layer
up bought nothing but drift. The session root is recovered from the dataset via
`_base.session_root`, the same helper `SessionMetadataProcessor` uses for
`session_id`, so the store's name and the table's join key cannot disagree.

`export_pipeline._process_one_session` now forwards `write_nwb` and owns only
what is genuinely multi-session: resolving the per-session output directory and
applying the include/exclude filter.

`processors` lets a caller supply an already-filtered list instead of having
one built — this is how `export_pipeline` applies `--include-processors` /
`--exclude-processors`. When passed, `strict_parsing` no longer affects
processor construction (they are already constructed).

`on_error` is the only tolerance mechanism anywhere in the package, and it is
opt-in. Called as `on_error(processor, exception)` when `compute()` raises;
returning normally skips that processor, re-raising aborts. It defaults to
`None` — propagate — and nothing in the package passes anything else. See
[error-policy.md](../conventions/error-policy.md).

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
from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

ds = dataset("/path/to/session")
data = process_session(ds, "/path/to/out")  # writes 7 parquet files
sites = data["sites"]  # also returned in-memory
```

See [session-from-disk.md](../../guides/session-from-disk.md) for
all-at-once, single-stream, and load-from-disk patterns.
