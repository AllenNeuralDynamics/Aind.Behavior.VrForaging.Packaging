# Load tables

!!! allen "Shared Allen Institute data: no install of this package"
    These tables were produced by this package and published to a public S3 bucket.
    You read them with Polars or pandas; every read is anonymous.

## Where each table lives

| Table | Where | Size |
| --- | --- | --- |
| `session` | `<root>/session.parquet` | small: one row per session |
| `sites` | `<root>/sites.parquet` | medium: one row per site, every session |
| `position_velocity`, `licks`, `sniffing`, `software_events`, `events` | `<source_s3_location>/<table>.parquet` | large: stored once per session, not aggregated |

`<root>` is `s3://aind-scratch-data/vr-foraging/vr-foraging-dataset`.
`<source_s3_location>` is a column of `session.parquet` that points at the session's own processed asset; always look it up there rather than building the path yourself.
Every column is described in [Processed tables](../reference/processed-tables.md).

!!! warning "Scratch means scratch"
    `aind-scratch-data` is a scratch bucket and the dataset can be rebuilt in place.
    If you need to know later exactly which build you read, [record what you read](#record-what-you-read).

## Setup

=== "Polars"

    ```python
    import polars as pl

    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"skip_signature": "true", "aws_region": "us-west-2", "max_retries": 10}

    session_ids = ["841312_2026-06-04_20-19-36"]  # e.g. from Find sessions
    ```

=== "Pandas"

    ```python
    import pandas as pd

    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"anon": True}

    session_ids = ["841312_2026-06-04_20-19-36"]  # e.g. from Find sessions
    ```

`max_retries` helps when you read many large files: the default of 2 turns one dropped connection into a failed run.

## Session catalogue and sites

Filter on `session_id` in the read, so only the matching parts of the file are downloaded.
`session.parquet` also has large serialized config columns (`session`, `rig`, `task_logic`, `trainer_state`, ...) that make up nearly all of its bytes, so read only the columns you need.

=== "Polars"

    ```python
    in_ids = pl.col("session_id").is_in(session_ids)

    session = (
        pl.scan_parquet(f"{ROOT}/session.parquet", storage_options=OPTS)
        .select("session_id", "subject_id", "date", "curriculum_stage_name", "source_s3_location")
        .filter(in_ids)
        .collect()
    )
    sites = pl.scan_parquet(f"{ROOT}/sites.parquet", storage_options=OPTS).filter(in_ids).collect()
    ```

=== "Pandas"

    ```python
    in_ids = [("session_id", "in", session_ids)]

    session = pd.read_parquet(
        f"{ROOT}/session.parquet",
        storage_options=OPTS,
        columns=["session_id", "subject_id", "date", "curriculum_stage_name", "source_s3_location"],
        filters=in_ids,
    )
    sites = pd.read_parquet(f"{ROOT}/sites.parquet", storage_options=OPTS, filters=in_ids)
    ```

## Per-session tables

Read one file per session from its `source_s3_location`, tag it with its `session_id`, and stack them:

=== "Polars"

    ```python
    def load_table(table: str, session: pl.DataFrame) -> pl.DataFrame:
        dtype = pl.Enum(sorted(session["session_id"]))  # cheap for ~1M-row tables
        frames = [
            pl.scan_parquet(f"{location}/{table}.parquet", storage_options=OPTS)
            .with_columns(pl.lit(sid, dtype=dtype).alias("session_id"))
            for sid, location in zip(session["session_id"], session["source_s3_location"])
        ]
        return pl.concat(frames).collect()

    licks = load_table("licks", session)
    ```

=== "Pandas"

    ```python
    def load_table(table: str, session: pd.DataFrame) -> pd.DataFrame:
        frames = [
            pd.read_parquet(f"{location}/{table}.parquet", storage_options=OPTS).assign(session_id=sid)
            for sid, location in zip(session["session_id"], session["source_s3_location"])
        ]
        return pd.concat(frames).astype({"session_id": "category"})

    licks = load_table("licks", session)  # timestamp is the index
    ```

## Read only a time window

Position and sniffing are about 1M rows per session. Filter on `timestamp` in the read to fetch just a slice:

=== "Polars"

    ```python
    location = session["source_s3_location"][0]
    t0 = sites["start_time"].min() + 600  # 10 minutes in

    window = (
        pl.scan_parquet(f"{location}/position_velocity.parquet", storage_options=OPTS)
        .filter(pl.col("timestamp").is_between(t0, t0 + 60))
        .collect()
    )
    ```

=== "Pandas"

    ```python
    location = session["source_s3_location"].iloc[0]
    t0 = sites["start_time"].min() + 600  # 10 minutes in

    window = pd.read_parquet(
        f"{location}/position_velocity.parquet",
        storage_options=OPTS,
        filters=[("timestamp", ">=", t0), ("timestamp", "<=", t0 + 60)],
    )
    ```

All times are harp time, in seconds, so sites and streams line up directly.
The [one-session example](example-session.md) plots exactly this.

## Record what you read

Next to the tables, `data_description.json` and `processing.json` record what the dataset is and what built it.
Save them with your results, along with the ETag of every file you read: since the dataset can be rebuilt in place, the ETags tell you later which build a result came from.

```python
import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
bucket, prefix = "aind-scratch-data", "vr-foraging/vr-foraging-dataset"

for name in ("session.parquet", "sites.parquet", "data_description.json", "processing.json"):
    head = s3.head_object(Bucket=bucket, Key=f"{prefix}/{name}")
    print(name, head["ContentLength"], head["ETag"].strip('"'))
```

Each parquet file also carries the versions that produced it (`packaging_version`, `data_contract_version`, `dataset_version`) in its schema metadata.
