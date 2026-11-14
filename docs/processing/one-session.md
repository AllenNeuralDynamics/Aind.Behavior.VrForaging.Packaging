# One session in Python

!!! package "Uses this package"
    Everything on this page runs `aind-behavior-vr-foraging-packaging` on a raw session folder.

## Load the session

```python
from aind_behavior_vr_foraging.data_contract import dataset

ds = dataset("path/to/session")
ds.version  # the session's data schema version, e.g. "0.6.4"
```

`dataset()` reads the folder through the [raw data contract](../reference/raw-data-contract.md).
The version decides which variant of each processor is used, so older sessions are handled automatically.

## Compute one table

To get a single table without writing anything, pass `include` and turn the parquet output off:

```python
from aind_behavior_vr_foraging_packaging.pipeline.session import process_session

tables = process_session(ds, include=["sites"], write_parquet=False)
```

=== "Polars"

    ```python
    import polars as pl  # pip install polars

    sites = pl.from_pandas(tables["sites"])
    sites.group_by("site_label").agg(pl.len(), pl.col("has_reward").sum())
    ```

=== "Pandas"

    ```python
    sites = tables["sites"]
    sites.groupby("site_label").agg(n=("site_label", "size"), rewards=("has_reward", "sum"))
    ```

The `session` table is always computed too, so `tables` also has a `session` entry.
The table names are `session`, `sites`, `position_velocity`, `licks`, `sniffing`, `software_events` and `events`.
The stream tables are indexed by `timestamp`; use `.reset_index()` before converting to Polars to keep it as a column.

## Choose what is written

`write_parquet` (on by default) and `write_nwb` (off by default) choose what reaches disk; every requested table is computed either way:

```python
process_session(ds, "output/my_session")  # parquet only
process_session(ds, "output/my_session", write_nwb=True)  # parquet and NWB
process_session(ds, "output/my_session", write_parquet=False, write_nwb=True)  # NWB only
tables = process_session(ds, write_parquet=False)  # nothing, in memory
```

The NWB file is written as `output/my_session/behavior.nwb.zarr`.
It needs the standard AIND metadata JSON files in the session folder; a session without them fails the NWB step.

For more control over the NWB file, such as a custom base file, use `NwbSession` directly:

```python
from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession
from aind_behavior_vr_foraging_packaging.pipeline.session import create_processors

nwb = NwbSession("path/to/session", dataset=ds)
nwb.run(*create_processors(ds))
nwb.write_nwb_zarr("output/my_session/my_session.nwb.zarr")
```

## When something fails

Errors are not swallowed: if a table fails, `process_session` raises and nothing partial is returned.
To skip failing tables instead, pass `on_error`:

```python
tables = process_session(
    ds,
    "output/my_session",
    on_error=lambda proc, exc: print(f"skipping {proc.output_name}: {exc}"),
)
```

`strict_parsing=True` makes known data anomalies fatal; by default they log a warning and use a documented fallback.
It does not change how other errors are handled.

## Provenance

Each table carries the versions that produced it, in `df.attrs` and in the parquet file's schema metadata:

```python
tables["sites"].attrs
# {'packaging_version': ..., 'data_contract_version': ..., 'dataset_version': '0.6.4', 'processor': 'SiteTableProcessor'}
```
