# Getting Started

## Installation

=== "uv (recommended)"

    Add to a uv project:

    ```bash
    uv add aind-behavior-vr-foraging-packaging
    ```

    Or install into the active environment:

    ```bash
    uv pip install aind-behavior-vr-foraging-packaging
    ```

=== "pip"

    ```bash
    pip install aind-behavior-vr-foraging-packaging
    ```

=== "from GitHub (latest)"

    ```bash
    uv pip install "git+https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging.git"
    ```

## Quick start — sites table

Load a raw session directory and compute the sites table (one row per *site*):

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.pipeline.session import resolve_site_table_processor

ds = dataset("path/to/session")                        # load the raw session
sites_df = resolve_site_table_processor(ds).compute()  # version-dispatch automatic

print(f"{len(sites_df)} sites, {sites_df['has_reward'].sum()} rewarded")
sites_df.to_parquet("sites.parquet")                   # optional: persist to disk
```

`resolve_site_table_processor` automatically picks the current or legacy
`SiteTableProcessor` based on the dataset's schema version.

## Quick start — all tables at once

To produce every table in a single call, use `process_session`. It writes one
parquet per processor and returns them keyed by name:

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.pipeline.session import process_session

ds = dataset("path/to/session")
results = process_session(ds, output_dir="output/")
# → output/sites.parquet, output/position_velocity.parquet, …

print(results.keys())
# dict_keys(['session', 'sites', 'position_velocity', 'licks', 'sniffing',
#            'software_events', 'events'])
```

## Quick start — CLI export

Install the CLI tool with [uvx](https://docs.astral.sh/uv/guides/tools/):

```bash
uvx install aind-behavior-vr-foraging-packaging
```

Then export a folder of raw session directories
(`--input-dir` must contain one subdirectory per session):

```bash
vr-foraging-packaging batch --input-dir /data/raw --output-dir /data/export
```

The export directory receives:

```text
/data/export/
├── session.parquet          # session catalogue (one row per session)
├── sites.parquet            # aggregated sites table (all sessions)
└── sessions/
    └── <session_id>/
        ├── sites.parquet
        ├── position_velocity.parquet
        └── …
```

### Subcommands

| Command | `--input-dir` is | What it does |
|------|---------|-------------|
| `session` | one raw session directory | Export that session's tables (and optionally NWB) |
| `batch` | a folder of raw session directories | Export every session, then aggregate |
| `aggregate` | a `sessions/` tree from an earlier run | Rebuild the experiment-level tables only |

```bash
# One session
vr-foraging-packaging session --input-dir /data/raw/behavior_123_2025-01-01 --write-nwb

# Aggregate later, re-processing nothing
vr-foraging-packaging aggregate --input-dir /data/export/sessions --output-dir /data/export
```

### Common CLI flags

| Flag | Default | Description | Commands |
|------|---------|-------------|----------|
| `--include-processors a b` | *(all)* | Run only the listed processors | `session`, `batch` |
| `--exclude-processors a b` | *(none)* | Skip named processors | `session`, `batch` |
| `--strict-parsing` | `false` | Treat a known data anomaly as fatal | `session`, `batch` |
| `--write-nwb` | `false` | Also write an NWB-Zarr store per session | `session`, `batch` |
| `--no-write-parquet` | *(parquet on)* | Skip the parquet tables | `session`, `batch` |
| `--workers N` | `1` | Parallel threads for the per-session phase | `batch` |
| `--skip-aggregation` | `false` | Write only per-session outputs | `batch` |
| `--no-clean` | *(clean on)* | Keep `--output-dir` instead of wiping it | `batch` |
| `--log-file path` | *(none)* | Append a structured log to this path | all |

Run `vr-foraging-packaging --help`, or `vr-foraging-packaging <command> --help`,
for the full flag reference.

## Next steps

<div class="grid cards" markdown>

-   :material-folder-open:{ .lg .middle } **Session from disk**

    ---

    Individual processors, selective computation, and the processor lifecycle.

    [:octicons-arrow-right-24: Guide](guides/session-from-disk.md)

-   :material-table:{ .lg .middle } **Parquet files**

    ---

    Query the export output with pandas, DuckDB, or Polars.

    [:octicons-arrow-right-24: Guide](guides/parquet-files.md)

-   :material-aws:{ .lg .middle } **AWS S3**

    ---

    Query data directly from S3 without downloading it.

    [:octicons-arrow-right-24: Guide](guides/aws-s3.md)

-   :material-api:{ .lg .middle } **API reference**

    ---

    Full API documentation for all public functions and classes.

    [:octicons-arrow-right-24: Reference](api/index.md)

</div>
