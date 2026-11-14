# Find sessions

!!! allen "Shared Allen Institute data: no install of this package"
    Session metadata lives in the AIND metadata database (DocDB), as `aind-data-schema` records.
    You query it through the public API gateway; no credentials are needed.

```bash
pip install aind-data-access-api
```

## Connect

```python
from aind_data_access_api.document_db import MetadataDbClient

client = MetadataDbClient(
    host="api.allenneuraldynamics.org",
    database="metadata_index",
    collection="data_assets",
    version="v2",
)
```

!!! warning "Use `version=\"v2\"`"
    VR foraging sessions use the newer `aind-data-schema` layout.
    With the default `"v1"` the queries below return nothing.

## Find sessions by subject and date

```python
records = client.retrieve_docdb_records(
    filter_query={
        "subject.subject_id": "841312",
        "acquisition.acquisition_start_time": {"$gte": "2026-06-01"},
        "data_description.data_level": "raw",
    },
    projection={"name": 1, "subject.subject_id": 1, "acquisition.acquisition_start_time": 1},
)
session_ids = sorted(r["name"] for r in records)
```

- `data_description.data_level: "raw"` keeps one record per acquisition (processed assets are separate records).
- A record's `name` is the `session_id` used by every table.
- Use `{"$in": [...]}` to match several subjects at once, and pass `paginate=True` for large results.

To look at the results as a table:

=== "Polars"

    ```python
    import polars as pl

    found = pl.json_normalize(records)  # columns: name, subject.subject_id, acquisition.acquisition_start_time
    ```

=== "Pandas"

    ```python
    import pandas as pd

    found = pd.json_normalize(records)  # columns: name, subject.subject_id, acquisition.acquisition_start_time
    ```

## Check they are in the processed dataset

DocDB lists every acquired session; the processed dataset may not have all of them yet.
Check before you analyze, so you never silently work on a subset:

=== "Polars"

    ```python
    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"skip_signature": "true", "aws_region": "us-west-2"}

    available = (
        pl.scan_parquet(f"{ROOT}/session.parquet", storage_options=OPTS)
        .select("session_id")
        .filter(pl.col("session_id").is_in(session_ids))
        .collect()["session_id"]
    )
    missing = set(session_ids) - set(available)
    ```

=== "Pandas"

    ```python
    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"anon": True}

    available = pd.read_parquet(
        f"{ROOT}/session.parquet",
        storage_options=OPTS,
        columns=["session_id"],
        filters=[("session_id", "in", session_ids)],
    )["session_id"]
    missing = set(session_ids) - set(available)
    ```

## Get a session's metadata

The processed tables hold what the pipeline extracted.
Everything else (subject details, acquisition, data description) is in the DocDB record.
Filter by `name` and project the parts you want; drop the projection to get the whole record:

```python
(record,) = client.retrieve_docdb_records(
    filter_query={"name": "841312_2026-06-04_20-19-36"},
    projection={"subject": 1, "acquisition": 1, "data_description": 1},
)
record["subject"]["subject_id"]
record["acquisition"]["acquisition_start_time"]
```

Next: [load the tables](load-tables.md) for the sessions you found.
