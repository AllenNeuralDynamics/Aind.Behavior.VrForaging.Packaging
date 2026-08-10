# Query from AWS S3

The export layout works identically whether it lives on local disk or in S3.
DuckDB and Polars can read parquet files directly from S3 using HTTP range
requests — no full download needed.

Runnable examples for both libraries are in the **Examples** section:

- [Query from S3 (DuckDB)](../examples/query-export-s3-duckdb.md)
- [Query from S3 (Polars)](../examples/query-export-s3-polars.md)

## DuckDB + httpfs

DuckDB's built-in `httpfs` extension handles all S3 I/O.

```bash
uv pip install duckdb
```

```python
import duckdb

# ── Configure this path to point at your export root ─────────────────────────
S3_ROOT = "s3://aind-scratch-data/vr-foraging/demo"
# ─────────────────────────────────────────────────────────────────────────────

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# For a public bucket no secret is needed.
# For a private bucket, uncomment and fill in the block below:
#
# import os, boto3
# AWS_PROFILE = "aind-scientist"  # your SSO profile name
# _session = boto3.Session(profile_name=AWS_PROFILE)
# _creds = _session.get_credentials().get_frozen_credentials()
# _region = _session.region_name or "us-west-2"
# os.environ["AWS_ACCESS_KEY_ID"] = _creds.access_key
# os.environ["AWS_SECRET_ACCESS_KEY"] = _creds.secret_key
# os.environ["AWS_SESSION_TOKEN"] = _creds.token or ""
# os.environ["AWS_DEFAULT_REGION"] = _region
# con.execute("""
#     CREATE SECRET s3_creds (
#         TYPE S3,
#         PROVIDER CREDENTIAL_CHAIN,
#         CHAIN 'env'
#     )
# """)
```

### Session catalogue

```python
con.execute(f"CREATE VIEW session AS SELECT * FROM read_parquet('{S3_ROOT}/session.parquet')")

sessions = con.execute("SELECT session_id, subject_id, date FROM session ORDER BY date").df()
print(sessions)

first_animal = sessions["subject_id"].iloc[0]
```

### Cross-session flat table

```python
con.execute(f"CREATE VIEW sites AS SELECT * FROM read_parquet('{S3_ROOT}/sites.parquet')")

# Predicate is pushed into the S3 scan — only matching row groups are downloaded
result = con.execute(f"""
    SELECT t.*, s.date
    FROM sites t
    JOIN session s USING (session_id)
    WHERE s.subject_id = '{first_animal}'
    ORDER BY s.date
""").df()
print(f"{len(result)} rows for {first_animal!r}")
```

### Per-session large tables via S3 glob

DuckDB resolves the glob against S3 and reads only matching files. Extract
the session ID from the file path with `regexp_extract`:

```python
POS_VEL_GLOB = f"{S3_ROOT}/sessions/*/position_velocity.parquet"

result = con.execute(f"""
    WITH pv AS (
        SELECT
            regexp_extract(filename, '/sessions/([^/]+)/', 1) AS session_id,
            * EXCLUDE (filename)
        FROM read_parquet('{POS_VEL_GLOB}', filename=true)
    )
    SELECT pv.*
    FROM pv
    JOIN session s USING (session_id)
    WHERE s.subject_id = '{first_animal}'
""").df()
print(f"{len(result)} rows")

con.close()
```

## Polars + S3

```bash
uv pip install polars
```

```python
import polars as pl

S3_ROOT = "s3://aind-scratch-data/vr-foraging/demo"
STORAGE_OPTIONS = {"skip_signature": "true"}  # public bucket — no credentials needed

# For a private bucket, replace STORAGE_OPTIONS with SSO credentials:
# import boto3
# AWS_PROFILE = "aind-scientist"
# _session = boto3.Session(profile_name=AWS_PROFILE)
# _creds = _session.get_credentials().get_frozen_credentials()
# STORAGE_OPTIONS = {
#     "aws_access_key_id": _creds.access_key,
#     "aws_secret_access_key": _creds.secret_key,
#     "aws_session_token": _creds.token or "",
#     "aws_region": _session.region_name or "us-west-2",
# }
```

### Polars: session catalogue

```python
session = pl.scan_parquet(f"{S3_ROOT}/session.parquet", storage_options=STORAGE_OPTIONS)
sessions = session.select("session_id", "subject_id", "date").sort("date").collect()
print(sessions)

first_animal = sessions["subject_id"][0]
```

### Polars: cross-session flat table

```python
sites = pl.scan_parquet(f"{S3_ROOT}/sites.parquet", storage_options=STORAGE_OPTIONS)

result = (
    sites
    .join(
        session.filter(pl.col("subject_id") == first_animal).select("session_id"),
        on="session_id",
        how="inner",
    )
    .collect()
)
print(f"{len(result)} rows, {result['session_id'].n_unique()} session(s)")
```

### Polars: multi-session glob scan

Use `include_file_paths` to extract the session ID from the path without
a separate catalogue join:

```python
POS_VEL_GLOB = f"{S3_ROOT}/sessions/*/position_velocity.parquet"

result = (
    pl.scan_parquet(POS_VEL_GLOB, storage_options=STORAGE_OPTIONS, include_file_paths="source_path")
    .with_columns(
        pl.col("source_path").str.extract(r"/sessions/([^/]+)/", 1).alias("session_id")
    )
    .drop("source_path")
    .join(
        session.filter(pl.col("subject_id") == first_animal).select("session_id"),
        on="session_id",
        how="inner",
    )
    .collect()
)
print(f"{len(result)} rows, {result['session_id'].n_unique()} sessions")
```

!!! tip "Private bucket credentials"
    SSO tokens expire after a few hours. Re-run `aws sso login --profile aind-scientist`
    when you see `ExpiredTokenException`.
