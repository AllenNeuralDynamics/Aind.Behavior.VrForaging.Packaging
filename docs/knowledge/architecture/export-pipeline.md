---
type: Component
title: Export pipeline — many sessions to a queryable dataset
description: export_pipeline.py runs the session pipeline over many session directories, writes per-session parquets, optionally writes one NWB-Zarr store per session, and concatenates chosen tables into flat per-experiment parquet files; driven by the aind-vr-export CLI.
resource: src/aind_behavior_vr_foraging_packaging/export_pipeline.py
tags: [architecture, pipeline, parquet, nwb, export, cli, aggregation]
timestamp: 2026-08-16T00:00:00Z
---

`export_pipeline.py` is the multi-session layer above
[session-pipeline.md](session-pipeline.md). Where `process_session` handles one
session, this turns a folder of raw sessions into a single queryable export.
It runs in **two independent phases**, each skippable, so a slow Phase 1 does
not have to be repeated to re-cut the aggregates.

# Phase 1 — `process_sessions`

```python
process_sessions(
    dataset_paths, output_dir, *,
    include_processors=(), exclude_processors=(),
    strict_parsing=False,
    max_workers=1, clean=True, write_nwb=False,
) -> list[Path]
```

For each raw session directory it loads the dataset, builds the processor list
via `create_processors(ds, strict_parsing=...)` — `SessionMetadataProcessor` is
unconditional, so every session gets a `session.parquet` — computes each
processor and writes `output_dir/sessions/{session_id}/{output_name}.parquet`.
`session_id` is the raw directory's name, which is also what
`SessionMetadataProcessor` writes into `session.parquet`.

- `include_processors` / `exclude_processors` filter by `output_name`. The
  `session` processor is **never** filtered out, since Phase 2 depends on it.
- **Everything propagates.** There is no `except` anywhere in this module. A
  dataset that will not load, a processor that raises, a failed NWB write — any
  of them aborts the whole run. The reasoning is the error policy's: anything
  escaping `compute()` is *unexpected by definition*, so the session is not a
  usable partial result and there is nothing safe to salvage. The only knob is
  `strict_parsing`, forwarded to the processors, and it decides solely whether a
  *known, anticipated* anomaly is fatal — never whether a general exception is
  caught. See [error-policy.md](../conventions/error-policy.md).
- `max_workers > 1` fans sessions out over a `ThreadPoolExecutor`. Only
  worthwhile because the work is largely IO and pandas/pyarrow calls that drop
  the GIL. `fut.result()` re-raises, so a failing session still aborts the batch.
- `write_nwb=True` additionally writes a NWB-Zarr store per session; see
  [NWB output](#nwb-output-write_nwb) below.

# NWB output (`write_nwb`)

`write_nwb` is forwarded straight to
[`process_session`](session-pipeline.md), which owns both output formats — this
layer adds nothing to the NWB path. The **same filtered processor list** drives
both, so one `--exclude-processors sniffing` removes sniffing from parquet and
NWB alike.

The store is written to:

```text
output_dir/sessions/{session_id}/{session_id}.nwb.zarr
```

collocated with the parquets for that session, because `process_session`'s
`output_dir` *is* the per-session directory.

**Failure.** `create_base_nwb_file` (called inside `NwbSession`) requires five
AIND metadata JSON files in the session root. A session lacking them fails the
NWB step, and that failure propagates like any other — it is not swallowed, and
the session's parquets do not make it a success.

**Stale stores.** If `clean=False` and a store already exists from a prior run,
`process_session` removes it with `shutil.rmtree` before writing, because
`NWBZarrIO("w")` does not guarantee a full overwrite.

**Runtime cost.** `compute()` (parquet) and `nwbize()` (NWB) share no state by
design, so `nwbize()` re-enters `compute()`. The five processors where that was
expensive now memoize via `cached_frame` (see
[processor-abstraction.md](processor-abstraction.md)), so `write_nwb=True` costs
the NWB assembly and write rather than a second full parse of every stream.

# Phase 2 — `aggregate`

```python
aggregate(sessions_dir, output_dir, aggregator: Aggregator) -> None
```

Two `@dataclass` config types drive it: an `Aggregator` holds a list of
`AggregationRule`, and each rule names one `table` plus a `cleanup` flag.
`DEFAULT_AGGREGATOR` aggregates only `sites`.

- `output_dir/session.parquet` is **always** written, by concatenating every
  per-session `session.parquet`. This is the dataset-level filtering table —
  one row per session, carrying date, subject, versions — that you join back
  to the big tables on `session_id`.
- Each rule concatenates its `{table}.parquet` across sessions into one flat
  `output_dir/{table}.parquet`, inserting a `session_id` column at position 0
  for the join — the session directory's name, which is the same value
  `SessionMetadataProcessor` wrote to `session.parquet`. Sessions missing that
  table are skipped with a debug log.
- `cleanup=True` (the default) deletes the per-session source files after a
  successful aggregate, so the same rows are not stored twice.

Tables *not* named by a rule simply stay as per-session files under
`sessions/{session_id}/` — that is the intended home for the large streams
(`position_velocity`, `licks`, `sniffing`), which are read per-session rather
than scanned across the experiment.

# CLI: `aind-vr-export`

`cli.py` exposes the two phases through a `pydantic-settings` `BaseSettings`
whose fields map 1-to-1 onto kebab-case flags (`CliApp.run(ExportSettings)`
parses, validates, and dispatches to `cli_cmd`). The entry point is declared in
`pyproject.toml` as `aind-vr-export`.

```bash
# Full run
aind-vr-export --input-dir /data/raw --output-dir /data/export

# Also write NWB-Zarr per session
aind-vr-export --input-dir /data/raw --output-dir /data/export --write-nwb

# Skip slow streams, tee to a log file
aind-vr-export --input-dir /data/raw --output-dir /data/export \
    --exclude-processors sniffing software_events \
    --log-file /data/export/run.log

# Re-aggregate only; sessions/ already written by a previous run
aind-vr-export --input-dir /data/raw --output-dir /data/export --skip-processing
```

`--input-dir` is scanned one level deep: every immediate subdirectory is taken
to be one raw session. `--dataset-tables` chooses which tables Phase 2 flattens
(default `sites`); `--skip-processing` / `--skip-aggregation` / `--write-nwb`
are bare flags (no value needed); `--workers` sets Phase 1 concurrency.

All boolean flags use `cli_implicit_flags=True`, so the negation form is
`--no-<flag>` (e.g. `--no-skip-processing`).

# Reading an export back

The flat tables are ordinary parquet, so any engine can read them without
pandas. `docs/examples/query_export.py` and `docs/examples/query_export_s3.py`
show DuckDB queries over a local and an S3-hosted export respectively.

## Provenance does not survive Phase 2

The per-session files carry the packaging/parser/dataset versions in their
parquet schema metadata, because Phase 1 writes them with `_write_parquet`
(see [session-pipeline.md](session-pipeline.md)). The **aggregated** files do
not, for two independent reasons:

- `_apply_rule` writes with plain `DataFrame.to_parquet`, which does not promote
  `df.attrs` to schema metadata the way `_write_parquet` does.
- `pd.concat` keeps `df.attrs` only when every input frame agrees, and
  `dataset_version` differs across sessions by design — so the concatenated
  frame's attrs are empty before the write even happens.

`session.parquet` is the exception, and deliberately so: `SessionMetadata`
carries `dataset_version`, `data_contract_version` and `packaging_version` as
ordinary **columns**, not attrs, so they survive `pd.concat` and the write like
any other data. That is the whole point — it is the one aggregate where
per-session provenance is recoverable, and it is joinable to every other table
on `session_id`.

Beyond that, provenance survives only in per-session files left on disk —
which, with the default `cleanup=True`, excludes exactly the tables you
aggregated. Under `DEFAULT_AGGREGATOR` that means `sites`' attrs are deleted
with its per-session copies, while `position_velocity`, `licks`, `sniffing` and
`events` keep theirs. Pass `cleanup=False` if an export needs to stay auditable
at the row level.

```sql
-- sites for one subject, filtered via the session table
SELECT s.* FROM 'sites.parquet' s
JOIN 'session.parquet' m USING (session_id)
WHERE m.subject_id = '123456';
```
