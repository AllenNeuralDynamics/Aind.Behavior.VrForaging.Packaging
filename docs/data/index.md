# Use existing data

!!! allen "Shared Allen Institute data: no install of this package"
    Everything in this section reads data and services the Allen Institute already runs.
    The sessions were processed for you; you only read the results.

## 1. Install a dataframe library

=== "Polars"

    ```bash
    pip install polars
    ```

=== "Pandas"

    ```bash
    pip install pandas pyarrow s3fs
    ```

To [find sessions](find-sessions.md) by subject or date you also need `pip install aind-data-access-api`.

No AWS account or credentials are needed: the bucket is public and every read is anonymous.

## 2. Know where the data is

All processed sessions live in one shared dataset:

```text
s3://aind-scratch-data/vr-foraging/vr-foraging-dataset/
├── session.parquet    # one row per session: subject, date, curriculum, ... and where its asset is
└── sites.parquet      # one row per site, every session
```

The large tables (position, licks, sniffing, events) are stored per session, in that session's own asset.
Its location is the `source_s3_location` column of `session.parquet`.
[Load tables](load-tables.md) shows how to read both kinds.

`session_id` (e.g. `841312_2026-06-04_20-19-36`) is the key that joins everything: the tables, the per-session assets and the DocDB metadata.

## 3. Load a session

=== "Polars"

    ```python
    import polars as pl

    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"skip_signature": "true", "aws_region": "us-west-2"}
    SESSION_ID = "841312_2026-06-04_20-19-36"

    sites = (
        pl.scan_parquet(f"{ROOT}/sites.parquet", storage_options=OPTS).filter(pl.col("session_id") == SESSION_ID).collect()
    )
    sites.group_by("site_label").agg(pl.len(), pl.col("has_reward").sum())
    ```

=== "Pandas"

    ```python
    import pandas as pd

    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"anon": True}
    SESSION_ID = "841312_2026-06-04_20-19-36"

    sites = pd.read_parquet(f"{ROOT}/sites.parquet", storage_options=OPTS, filters=[("session_id", "==", SESSION_ID)])
    sites.groupby("site_label").agg(n=("site_label", "size"), rewards=("has_reward", "sum"))
    ```

Each row is one site the animal passed in the corridor. Every column is described in [Processed tables](../reference/processed-tables.md#sites).

## Next

<div class="grid cards" markdown>

-   :material-magnify:{ .lg .middle } **Find sessions**

    ---

    Look up sessions by subject or date, and get their metadata.

    [:octicons-arrow-right-24: Find sessions](find-sessions.md)

-   :material-table:{ .lg .middle } **Load tables**

    ---

    The session catalogue, sites, and the large per-session streams.

    [:octicons-arrow-right-24: Load tables](load-tables.md)

-   :material-notebook:{ .lg .middle } **Example: one session**

    ---

    A notebook that plots a session's sites with velocity.

    [:octicons-arrow-right-24: Open notebook](example-session.md)

-   :material-chart-line:{ .lg .middle } **Example: one subject**

    ---

    A notebook that follows one subject across sessions.

    [:octicons-arrow-right-24: Open notebook](example-subject.md)

</div>
