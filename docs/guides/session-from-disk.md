# Run a session from disk

This guide walks through loading a raw session directory and running
processors individually or all at once.

## Concepts

A **session directory** is the root folder of one raw behavioral session,
as written by the VR-foraging acquisition software. It contains stream
files in a layout defined by the
[aind-behavior-vr-foraging data contract](https://github.com/AllenNeuralDynamics/aind-behavior-vr-foraging).

The package uses [contraqctor](https://github.com/AllenNeuralDynamics/contraqctor)
to load this directory into a typed `Dataset` object, which processors then
fan out over. You never have to parse individual files by hand.

## Load a session

```python
from aind_behavior_vr_foraging.data_contract import dataset

ds = dataset("path/to/behavior_<subject>_<date>")
print(ds.version)   # e.g. "0.7.0"
```

`dataset()` validates the directory layout and returns a `Dataset`. The
`version` attribute (from the data contract schema) determines which
processor variants are selected automatically.

## Run a single processor

```python
from aind_behavior_vr_foraging_packaging.session_pipeline import (
    get_site_table_processor,
    get_position_velocity_processor,
)

# Sites table — one row per site (the fundamental trial unit)
sites_df = get_site_table_processor(ds).compute()
print(sites_df.head())

# Position and velocity — one row per sample
pos_vel_df = get_position_velocity_processor(ds).compute()
print(pos_vel_df.dtypes)
```

!!! note "Version dispatch"
    `get_site_table_processor` and `get_position_velocity_processor` pick
    `LegacySiteTableProcessor` / `LegacyPositionAndVelocityProcessor` when
    the dataset version is `< 0.6.0`. You never need to select the class
    manually.

## Run all processors

`create_processors` returns the full ordered list; call `compute()` on each:

```python
from aind_behavior_vr_foraging_packaging.session_pipeline import create_processors

processors = create_processors(ds, session_path="path/to/behavior_<subject>_<date>")

for proc in processors:
    df = proc.compute()
    print(f"{proc.output_name}: {len(df)} rows")
    # → session: 1 rows
    # → sites: 123 rows
    # → position_velocity: 450000 rows
    # → licks: 780 rows
    # → sniffing: 450000 rows
    # → software_events: 45 rows
    # → events: 230 rows
```

Pass `session_path` to include a `SessionMetadataProcessor` (writes
`session.parquet` — the session catalogue row).

## Run all processors and save parquets

`run_session` calls `compute()` on every processor and writes one parquet
file per processor to an output directory:

```python
from aind_behavior_vr_foraging_packaging.session_pipeline import run_session

results = run_session(ds, output_dir="output/my_session/",
                      session_path="path/to/behavior_<subject>_<date>")
# → output/my_session/session.parquet
# → output/my_session/sites.parquet
# → output/my_session/position_velocity.parquet
# → …

sites_df = results["sites"]   # already in memory; no disk read needed
```

## Inspect provenance

Every DataFrame carries provenance metadata in `df.attrs`, written to the
parquet schema so downstream tools (DuckDB, Polars, R arrow, Spark) can
read it without loading the full file:

```python
print(sites_df.attrs)
# {
#   'packaging_version': '1.2.3',
#   'data_contract_version': '0.7.0',
#   'dataset_version': '0.7.0',
#   'processor': 'SiteTableProcessor',
# }
```

## Error handling

By default, processor failures are logged as warnings and the session
continues. Pass `raise_on_error=True` to abort on the first anomaly:

```python
processors = create_processors(ds, raise_on_error=True)
```

## Also write NWB

Add `write_nwb=True` to `run_session` to also produce an NWB-Zarr store
alongside the parquets. The session directory must contain the standard
AIND metadata JSON files:

```python
from aind_behavior_vr_foraging_packaging.session_pipeline import run_session
from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession

# Option A: via run_session (only parquets — NWB not available here)
# For NWB, use NwbSession directly:

from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.session_pipeline import create_processors

raw = "path/to/behavior_<subject>_<date>"
ds = dataset(raw)
processors = create_processors(ds)

session = NwbSession(raw)
session.run(*processors)
session.write_nwb_zarr("output/my_session/my_session.nwb.zarr")
```

See the [NWB API reference](../api/nwb.md) for the full `NwbSession` interface.
