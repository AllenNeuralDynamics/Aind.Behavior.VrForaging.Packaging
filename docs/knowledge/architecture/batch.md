---
type: Component
title: Batch pipeline — many sessions to a queryable export
description: pipeline/batch.py runs the session pipeline over many session directories, writes per-session parquets, optionally writes one NWB-Zarr store per session, and concatenates chosen tables into flat per-experiment parquet files; driven by the vr-foraging-packaging CLI.
resource: src/aind_behavior_vr_foraging_packaging/pipeline/batch.py
tags: [architecture, pipeline, parquet, nwb, export, cli, aggregation]
timestamp: 2026-08-16T00:00:00Z
---

`pipeline/batch.py` is the multi-session layer above [session.md](session.md).
Where `process_session` handles one session, this runs the same pipeline over a
folder of them and concatenates the results into a single queryable export.
It runs in **two independent phases**, each skippable, so a slow Phase 1 does
not have to be repeated to re-cut the aggregates.

# Phase 1 — `process_sessions`

```python
process_sessions(
    dataset_paths, output_dir, *,
    include_processors=(), exclude_processors=(),
    strict_parsing=False, max_workers=1, clean=True,
    write_parquet=True, write_nwb=False,
) -> list[Path]
```

For each raw session directory it calls
[`process_session`](session.md) with
`output_dir/sessions/{session_id}/`. `session_id` is the raw directory's name,
which is also what `SessionMetadataProcessor` writes into `session.parquet`.

This layer is deliberately thin. Everything per-session — loading the dataset,
building and filtering the processor list, writing parquet and NWB — belongs to
`process_session`, so `include_processors`, `exclude_processors`,
`strict_parsing`, `write_parquet` and `write_nwb` are pass-throughs with no
behaviour added here. What is genuinely multi-session and lives here:

- resolving each session's output directory under `sessions/`,
- `clean`, which removes the previous run's outputs (see below),
- `max_workers`, the thread pool,
- Phase 2 aggregation.

Notes:

- `include_processors` / `exclude_processors` filter by `output_name`. The
  `session` processor is **never** filtered out — see
  [`filter_processors`](session.md). Phase 2 depends on it, but so does
  every join, which is why the rule lives in the session layer.
- `write_parquet=False` leaves Phase 2 nothing to aggregate. The CLI rejects
  `--no-write-parquet` unless `--skip-aggregation` is also set.
- `clean` (default `True`) removes only what a previous run of *this* function
  wrote — the `sessions/` tree and the aggregated `{table}.parquet` files. It
  deliberately does **not** `rmtree(output_dir)`, which it used to: that took
  anything else living there with it, and `--log-file <output-dir>/run.log` is
  both the obvious case and what the docs suggest. On Windows the delete failed
  outright, because the log handler holds the file open; on POSIX the log would
  have silently vanished mid-run while the handler kept writing to a deleted
  inode. Scoping it also means `--output-dir ~/data` no longer erases `~/data`.
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
[`process_session`](session.md), which owns both output formats — this
layer adds nothing to the NWB path. The **same filtered processor list** drives
both, so one `--exclude-processors sniffing` removes sniffing from parquet and
NWB alike.

The store is written to:

```text
output_dir/sessions/{session_id}/behavior.nwb.zarr
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
aggregate(sessions_dir, output_dir) -> None
```

Two positional arguments and no options. **What gets aggregated is fixed, not
configurable:** the module constant

```python
SESSION_TABLE = "session"
AGGREGATED_TABLES = (SESSION_TABLE, "sites")
```

names every table flattened across the experiment. That set is a property of the
schema — which tables are small enough to scan experiment-wide — rather than a
per-run decision. The only control is whether Phase 2 runs at all, which is
`--skip-aggregation`.

This replaced an `Aggregator` / `AggregationRule` dataclass pair, a
`DEFAULT_AGGREGATOR` singleton, a `cleanup` flag, and the CLI's
`--dataset-tables`, all of which existed to parameterise something that never
varied.

- Each name is concatenated across sessions into one flat
  `output_dir/{table}.parquet`, inserting a `session_id` column at position 0
  when the table does not already carry one — the session directory's name,
  which is the same value `SessionMetadataProcessor` writes into
  `session.parquet`. Sessions missing a table are skipped with a debug log.
- `session` goes through the same path as every other table; it used to have a
  bespoke concat block above the loop. It is listed first because a missing
  identity table aborts the rest: without it the export has nothing to join on.
- **Per-session files are never deleted.** Copying rows into an aggregate is not
  a reason to destroy the source. They are what `--skip-processing`
  re-aggregation reads back, and the only copies carrying provenance in their
  parquet schema.

Tables *not* in `AGGREGATED_TABLES` simply stay as per-session files under
`sessions/{session_id}/` — that is the intended home for the large streams
(`position_velocity`, `licks`, `sniffing`), which are read per-session rather
than scanned across the experiment.

# CLI: `vr-foraging-packaging`

See [cli.md](cli.md). One subcommand per public function — `session`, `batch`,
`aggregate` — so the command surface and the module surface are the same shape.

# Reading an export back

The flat tables are ordinary parquet, so any engine can read them without
pandas. `docs/examples/query_export.py` and `docs/examples/query_export_s3.py`
show DuckDB queries over a local and an S3-hosted export respectively.

## Provenance does not survive Phase 2

The per-session files carry the packaging/parser/dataset versions in their
parquet schema metadata, because Phase 1 writes them with `_write_parquet`
(see [session.md](session.md)). Phase 2 uses the same writer,
but the **aggregated** files still carry nothing: `pd.concat` keeps `df.attrs`
only when every input frame agrees, and `dataset_version` differs across
sessions by design, so the concatenated frame's attrs are empty before the write
even happens.

`session.parquet` is the exception, and deliberately so: `SessionMetadata`
carries `dataset_version`, `data_contract_version` and `packaging_version` as
ordinary **columns**, not attrs, so they survive `pd.concat` and the write like
any other data. That is the whole point — it is the one aggregate where
per-session provenance is recoverable, and it is joinable to every other table
on `session_id`.

Everywhere else, provenance lives in the per-session files, which Phase 2 never
deletes. An export therefore stays auditable at the row level without anyone
having to ask for it.

```sql
-- sites for one subject, filtered via the session table
SELECT s.* FROM 'sites.parquet' s
JOIN 'session.parquet' m USING (session_id)
WHERE m.subject_id = '123456';
```
