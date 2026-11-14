# VR Foraging data

Behavior from the AIND VR foraging task, as analysis-ready tables: one row per session, one row per site the animal visited, and the position, licking and sniffing streams behind them.

## Pick your starting point

<div class="grid cards" markdown>

-   :material-database:{ .lg .middle } **Use existing data**

    ---

    The sessions are already processed and sitting on S3.
    You only need Polars or pandas; **no install of this package**.

    *Most people want this.*

    [:octicons-arrow-right-24: Start here](data/index.md)

-   :material-package-variant-closed:{ .lg .middle } **Process raw sessions**

    ---

    You have raw session folders (new, local, or not processed yet) and want the tables or an NWB file.
    **Uses this package.**

    [:octicons-arrow-right-24: Start here](processing/index.md)

</div>

## Try it in 30 seconds

Load the sites of one session straight from the shared dataset:

=== "Polars"

    ```python
    import polars as pl  # pip install polars

    sites = (
        pl.scan_parquet(
            "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset/sites.parquet",
            storage_options={"skip_signature": "true", "aws_region": "us-west-2"},
        )
        .filter(pl.col("session_id") == "841312_2026-06-04_20-19-36")
        .collect()
    )
    ```

=== "Pandas"

    ```python
    import pandas as pd  # pip install pandas pyarrow s3fs

    sites = pd.read_parquet(
        "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset/sites.parquet",
        storage_options={"anon": True},
        filters=[("session_id", "==", "841312_2026-06-04_20-19-36")],
    )
    ```

## How to read these docs

Each page is marked with where its data or code comes from:

!!! allen "Shared Allen Institute data"
    Data and services the Allen Institute already runs: the processed dataset on S3 and the metadata database (DocDB).
    You read them; you do not need this package.

!!! package "Uses this package"
    Steps where you run `aind-behavior-vr-foraging-packaging` yourself, to turn raw sessions into tables.

Code examples that work with dataframes have **Polars** and **Pandas** tabs. Pick one and every example on the site switches with it.

| Section | For |
| --- | --- |
| [Use existing data](data/index.md) | Finding sessions and loading the shared tables |
| [Process raw sessions](processing/index.md) | Running this package on raw session folders |
| [Reference](reference/index.md) | Every table column, the raw data streams, and the Python API |
