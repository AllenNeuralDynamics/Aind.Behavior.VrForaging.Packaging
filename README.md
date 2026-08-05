# Aind.Behavior.VrForaging.Packaging

![CI](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging/actions/workflows/aind-behavior-vr-foraging-packaging.yml/badge.svg)
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

Parses raw AIND VR-foraging behavioral sessions into analysis-ready **parquet** tables and an **NWB** file.

## Architecture

A session is loaded once (via `contraqctor`), then a set of independent
**processors** fan out over it. Each processor owns one output and knows how to
express it in two targets:

```text
raw session dir
      │
      ▼
  Dataset  ◄── aind_behavior_vr_foraging.data_contract.dataset(path)
      │
      ▼
  create_processors(dataset)          # picks processor variants by dataset version
      │   [SiteTable, PositionAndVelocity, Licks, Sniffing, SoftwareEvents, Events]
      │
      ├─► proc.compute()  ──► pandas DataFrame  ──► one <name>.parquet   (run_session)
      │                        (provenance stamped into df.attrs / parquet schema)
      │
      └─► proc.nwbize(nwb) ──► populates an NWBFile ──► .nwb.zarr (NwbSession)
```

- **Processor** — every processor subclasses `AbstractProcessor`, implementing
  `_compute()` and (optionally) `nwbize()`. `compute()` wraps `_compute()` and
  stamps provenance (`packaging_version`, `data_contract_version`,
  `dataset_version`, `processor`) into the DataFrame's `attrs`.
- **DataFrame** — the common in-memory representation. One row per unit of the
  output (e.g. one site-table row = one *site*).
- **Parquet** — `pipeline.run_session()` calls `compute()` on each processor and
  writes a parquet per processor, promoting `df.attrs` to first-class parquet
  metadata (readable from DuckDB, Polars, R arrow, Spark, …).
- **NWB** — `NwbSession` builds a single `NWBFile` from AIND metadata,
  then calls each processor's `nwbize()` to fill it, and writes NWB-Zarr.

Version dispatch is automatic: datasets with schema version `< 0.6.0` receive
legacy processor variants.

## Examples

- Runnable script covering the parquet workflows (all-at-once, single stream,
  load-back): [scripts/example_parquet_pipeline.py](scripts/example_parquet_pipeline.py)
- The NWB workflow: [docs/knowledge/architecture/nwb-packaging.md](docs/knowledge/architecture/nwb-packaging.md)
- Full architecture docs: [docs/knowledge/](docs/knowledge/) (start at [overview.md](docs/knowledge/overview.md))

### Get a sites table

Install straight from GitHub with [uv](https://docs.astral.sh/uv/):

```bash
# into a uv project
uv add "git+https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging.git"

# or into the current environment
uv pip install "git+https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging.git"
```

Then load a session and compute the sites table (one row per *site*):

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.session_pipeline import get_site_table_processor

ds = dataset("path/to/session")  # load the raw session
sites_df = get_site_table_processor(ds).compute()

sites_df.to_parquet("sites.parquet")  # optional: persist to disk
print(f"{len(sites_df)} sites, {sites_df['has_reward'].sum()} rewarded")
```

`get_site_table_processor` automatically picks the current or legacy variant
based on the dataset's schema version. To produce every table at once, use
`run_session(ds, "output_dir")` instead — it writes `sites.parquet`,
`position_velocity.parquet`, and the rest, and returns them keyed by name.

## Exporting a dataset collection

Install the CLI with [uvx](https://docs.astral.sh/uv/guides/tools/):

```bash
uvx install "git+https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging.git"
```

Then run the export pipeline across a folder of raw session directories
(`--input-dir` must contain one subdirectory per session):

```bash
uvx run aind-vr-export --input-dir /data/raw --output-dir /data/export
```

`--output-dir` receives the results:

```text
/data/export/
├── session.parquet          # session catalogue (one row per session)
├── sites.parquet            # aggregated sites table (all sessions)
└── sessions/
    └── <session_id>/
        ├── sites.parquet
        ├── position_velocity.parquet
        └── ...
```

### Common flags

| Flag | Default | Description |
| --- | --- | --- |
| `--workers N` | `1` | Parallel threads for Phase 1 (per-session processing) |
| `--exclude-processors a b` | *(none)* | Skip named processors, e.g. `sniffing software_events` |
| `--include-processors a b` | *(all)* | Run only the listed processors |
| `--dataset-tables a b` | `sites` | Tables to flatten across sessions in Phase 2 |
| `--skip-processing` | `false` | Jump straight to Phase 2 (sessions/ already written) |
| `--skip-aggregation` | `false` | Write only per-session parquets |
| `--log-file path` | *(none)* | Append a structured log to this path |
| `--raise-on-error` | `false` | Abort on the first failure instead of logging and continuing |

### Example: fast parallel run, skip sniffing

```bash
uvx run aind-vr-export \
    --input-dir /data/raw \
    --output-dir /data/export \
    --workers 8 \
    --exclude-processors sniffing software_events \
    --log-file /data/export/run.log
```

### Example: re-aggregate only

Per-session parquets already written in `sessions/`:

```bash
uvx run aind-vr-export \
    --input-dir /data/raw \
    --output-dir /data/export \
    --skip-processing
```

See `uvx run aind-vr-export --help` for the full flag reference.

## Contributors

Contributions to this repository are welcome! However, please ensure that your code adheres to the recommended DevOps practices below:

### Linting

We use [ruff](https://docs.astral.sh/ruff/) as our primary linting tool.

### Testing

Attempt to add tests when new features are added.
To run the currently available tests, run `uv run pytest` from the root of the repository.

## Integration tests

Integration tests run the parser end-to-end against real datasets stored in a public S3 bucket. They are gated by a pytest marker so they don't run by default.

**Run locally:**

```bash
uv run pytest -m integration
```

The first run downloads datasets (~100 MB per dataset) to `tests/integration/.cache/`. Subsequent runs reuse the cache when the S3 ETag matches. The cache directory is gitignored.

**Trigger on a PR:**

Integration tests do not run on every PR. To run them for a specific PR, add the `run-integration` label via the GitHub UI (open the PR, click **Labels** in the right-hand sidebar, and select `run-integration`) or with:

```bash
gh pr edit <PR_NUMBER> --add-label run-integration
```

The integration job runs automatically on push to `main` and on `release: published`. A release cannot ship without the integration suite passing.

**Adding a dataset:**

Add an entry to `tests/integration/datasets.yml`. The manifest schema and full field documentation are in `tests/integration/model.py` (Pydantic model). The `rationale` field is required and is printed alongside any test failure to make triage fast.

### Lock files

We use [uv](https://docs.astral.sh/uv/) to manage our lock files and therefore encourage everyone to use uv as a package manager as well.
