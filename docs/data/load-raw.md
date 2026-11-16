# Load raw streams

!!! allen "Shared Allen Institute data: the raw asset is public"
    Each session's raw asset lives in the public `aind-open-data` bucket, so every read is anonymous.
    Reading a file needs only Polars or pandas.
    The [contract path](#without-hard-coding-the-path) needs `pip install "aind-behavior-vr-foraging[data]"` to find the file for you.

The [processed tables](load-tables.md) cover what most analyses need.
Read a raw stream when you need something they do not extract, or to check how a table was made.
A raw stream is one file in the session's asset, for example:

```text
s3://aind-open-data/841312_2026-06-04_20-19-36/
└── behavior/
    └── OperationControl/
        └── IsStopped.csv
```

Every stream and its file is listed in [Raw data contract](../reference/raw-data-contract.md).

## Find the asset

The raw asset's location is the `raw_s3_location` column of `session.parquet`, read the same way as in [Load tables](load-tables.md#session-catalogue-and-sites).
Read `data_contract_version` with it: it names the data version the session was acquired with, which the [contract path](#without-hard-coding-the-path) below needs.
(`source_s3_location` points at the *processed* asset, which holds the tables, not the raw files.)

=== "Polars"

    ```python
    import polars as pl

    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"skip_signature": "true", "aws_region": "us-west-2", "max_retries": 10}
    SESSION_ID = "841312_2026-06-04_20-19-36"

    row = (
        pl.scan_parquet(f"{ROOT}/session.parquet", storage_options=OPTS)
        .filter(pl.col("session_id") == SESSION_ID)
        .select("raw_s3_location", "data_contract_version")
        .collect()
        .row(0, named=True)
    )
    asset = row["raw_s3_location"]  # s3://aind-open-data/841312_2026-06-04_20-19-36
    version = row["data_contract_version"]
    ```

=== "Pandas"

    ```python
    import pandas as pd

    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"anon": True}
    SESSION_ID = "841312_2026-06-04_20-19-36"

    row = pd.read_parquet(
        f"{ROOT}/session.parquet",
        storage_options=OPTS,
        columns=["raw_s3_location", "data_contract_version"],
        filters=[("session_id", "==", SESSION_ID)],
    ).iloc[0]
    asset = row["raw_s3_location"]  # s3://aind-open-data/841312_2026-06-04_20-19-36
    version = row["data_contract_version"]
    ```

## Hard-code a stream

The simplest read: append the stream's file path to the asset and load the file.
`behavior/OperationControl/IsStopped.csv` is a two-column CSV, harp time in `Seconds` and a boolean `IsStopped`:

=== "Polars"

    ```python
    stream = "behavior/OperationControl/IsStopped.csv"

    is_stopped = pl.read_csv(f"{asset}/{stream}", storage_options=OPTS)
    is_stopped.head(3)
    # Seconds        IsStopped
    # 14397.911392   false
    # 14401.807392   true
    # 14402.075392   false
    ```

=== "Pandas"

    ```python
    stream = "behavior/OperationControl/IsStopped.csv"

    is_stopped = pd.read_csv(f"{asset}/{stream}", storage_options=OPTS, index_col="Seconds")
    is_stopped.head(3)  # indexed by harp time
    ```

The file is downloaded and parsed into memory; nothing is written to disk.
Times are harp time, in seconds, so they line up with the [processed tables](load-tables.md).

This is fine for one stream you already know.
Its weakness is that the path is a string you typed: if a stream is renamed or moved in a later data version, the read fails (or reads the wrong file) and nothing tells you why.

## Without hard-coding the path

The [data contract](../reference/raw-data-contract.md) already knows where every stream's file is.
Open the dataset with [contraqctor](../processing/raw-streams.md), navigate to the stream by name, and ask it for its path.
Nothing is read at this point, and no local files are needed:

```python
from aind_behavior_vr_foraging.data_contract import dataset

ds = dataset(".", version=version)  # a placeholder root: we only want the paths inside it
node = ds.at("Behavior").at("OperationControl").at("IsStopped")

stream = node.reader_params.path.as_posix()
# 'behavior/OperationControl/IsStopped.csv'
```

`stream` is relative to the session root, so it is the same string as the hard-coded one, and you read it the same way:

=== "Polars"

    ```python
    is_stopped = pl.read_csv(f"{asset}/{stream}", storage_options=OPTS)
    ```

=== "Pandas"

    ```python
    is_stopped = pd.read_csv(f"{asset}/{stream}", storage_options=OPTS, index_col="Seconds")
    ```

Now the stream is chosen by its contract name (`Behavior` / `OperationControl` / `IsStopped`), not by a file path.
The same lookup works for any stream; for example, change the last line of the navigation to `.at("HarpLickometer").at("LickState")` and the path follows.
To browse the names, see [Raw streams](../processing/raw-streams.md#find-a-stream).

!!! note "Why pass the version"
    The contract is versioned: sessions acquired with an older data version can have different file names.
    A placeholder root has no version to infer, so `dataset(".")` would silently use the installed package's contract.
    Passing the session's own `data_contract_version` makes the path match the files that session has.

!!! tip "Not every stream is a CSV"
    The contract also records how each file is read (`node.reader_params`): CSV, binary harp registers, JSON and so on.
    Only the path is used here, so this pattern is simplest for CSV streams like the one above.
    Other file formats may not be easily streamed this way and may require copying the asset locally before reading.

## See it run

The [raw stream example](example-raw-stream.md) is a notebook that runs both versions end to end, checks that they return the same data, and plots the result.
