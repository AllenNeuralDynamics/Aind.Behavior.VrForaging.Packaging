---
type: Component
title: CLI — vr-foraging-packaging
description: One subcommand per pipeline function (session / batch / aggregate), built on pydantic-settings CliSubCommand; adds no behaviour beyond dispatch and two guards.
resource: packaging/src/aind_behavior_vr_foraging_packaging/pipeline/cli.py
tags: [architecture, cli, pipeline, pydantic-settings]
timestamp: 2026-08-16T00:00:00Z
---

`pipeline/cli.py` is the single console entry point, declared in
`pyproject.toml` as
`vr-foraging-packaging = "aind_behavior_vr_foraging_packaging.pipeline.cli:main"`.

The organising rule: **one subcommand per public pipeline function.** The
command surface and the module surface are the same shape, so there is no
mapping to learn and nowhere for the two to drift.

| Subcommand | Calls | `--input-dir` is |
|---|---|---|
| `session` | [`process_session`](session.md) | one raw session directory |
| `batch` | [`process_sessions`](batch.md) then `aggregate` | a folder of raw session directories |
| `aggregate` | [`aggregate`](batch.md) | a `sessions/` tree from an earlier run |

`--input-dir` and `--output-dir` mean the same thing in all three. Only what
*counts* as an input differs, and that difference is exactly what distinguishes
the subcommands — which is why the flags are not renamed per command.

# Schema

Three model layers, mirroring how much each subcommand needs:

```text
_Command            input_dir, output_dir, log_file      → cli_cmd() sets up logging, calls run()
└─ _ProcessingCommand  + include/exclude_processors,
                         strict_parsing, write_parquet, write_nwb
   ├─ SessionCommand    (nothing more)
   └─ BatchCommand     + workers, clean, skip_aggregation
└─ AggregateCommand  (nothing more — it reads parquet, it does not process)
```

`_ProcessingCommand` exists because `session` and `batch` both bottom out in
`process_session`, so they take the same processor-selection and output-format
flags. `aggregate` skips that layer entirely: it never constructs a processor,
so offering `--strict-parsing` or `--write-nwb` there would be a lie.

`cli_cmd` is the hook pydantic-settings dispatches to. The base class implements
it — configure logging, log the paths, call `run()` — so subclasses implement
only `run()` and logging is set up exactly once, in one place.

# Guards

The CLI owns no behaviour beyond dispatch, with one exception: combinations that
cannot work are rejected rather than run.

- `batch --no-write-parquet` without `--skip-aggregation` raises. Aggregation
  reads the per-session parquets back off disk, so the run would report success
  having produced an empty aggregate.
- The same combination on `session` is fine and has no guard: that subcommand
  never aggregates, so "NWB only" is unambiguous.

# Examples

```bash
# One session, parquet + NWB, into the current directory
vr-foraging-packaging session --input-dir /data/raw/behavior_123_2025-01-01 --write-nwb

# A folder of sessions, skipping the slow streams, no aggregation yet
vr-foraging-packaging batch --input-dir /data/raw --output-dir /data/export \
    --exclude-processors sniffing software_events --skip-aggregation

# Aggregate later, re-processing nothing
vr-foraging-packaging aggregate --input-dir /data/export/sessions --output-dir /data/export

# Parallel run, tee to a log file
vr-foraging-packaging batch --input-dir /data/raw --output-dir /data/export \
    --workers 8 --log-file /data/export/run.log
```

All boolean flags use `cli_implicit_flags=True`, so every one has a `--no-`
form (`--no-write-parquet`, `--no-clean`, `--no-skip-aggregation`).

# History

This replaced a single flag-driven `aind-vr-export` command whose phase
selection was expressed as *negations* — `--skip-processing` to re-aggregate,
`--skip-aggregation` to process only. Two booleans encoded three intentions, one
combination (`--skip-processing --skip-aggregation`) did nothing at all, and
exporting a single session was not expressible without pointing the tool at a
parent directory. Subcommands say the intention directly, and
`--skip-processing` disappears because "aggregate only" is now its own verb.
