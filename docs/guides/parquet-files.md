# Query parquet files

The export pipeline writes two types of parquet output:

| Output | Location | Description |
|--------|----------|-------------|
| `session.parquet` | `export/` | Session catalogue — one row per session |
| `{table}.parquet` | `export/` | Flat cross-session table (small tables, e.g. `sites`) |
| `{table}.parquet` | `export/sessions/{session_id}/` | Per-session table (large tables) |

This guide covers how to generate the export and then query it with
pandas, DuckDB, and Polars.

## Generate the export

=== "CLI"

    ```bash
    vr-foraging-packaging --input-dir /data/raw --output-dir /data/export --workers 4
    ```

=== "Python"

    ```python
    from pathlib import Path
    from aind_behavior_vr_foraging_packaging.pipeline.batch import (
        process_sessions, aggregate
    )

    raw_sessions = sorted(Path("/data/raw").iterdir())  # one dir per session
    written = process_sessions(raw_sessions, "/data/export", max_workers=4)
    aggregate("/data/export/sessions", "/data/export")
    ```

## Runnable example

The [query-export example](../examples/query-export.md) covers the full
workflow end-to-end — session catalogue, cross-session flat tables, per-session
large tables, and DuckDB — as a standalone script you can run with `uv`:

```bash
uv run docs/examples/query_export.py
```

## Key patterns

### Session catalogue

```python
import pandas as pd

sessions = pd.read_parquet("/data/export/session.parquet")
print(sessions[["session_id", "subject_id", "date"]])
```

### Cross-session flat table (pandas)

`sites.parquet` is a single file — all sessions concatenated. Filter by
`session_id` to get a subset:

```python
all_sites = pd.read_parquet("/data/export/sites.parquet")
first_animal = sessions["subject_id"].iloc[0]
animal_ids = sessions.loc[sessions["subject_id"] == first_animal, "session_id"].tolist()
animal_sites = all_sites[all_sites["session_id"].isin(animal_ids)]
```

### DuckDB — join catalogue + flat table

DuckDB treats parquet files as first-class SQL sources with full predicate
pushdown and column pruning:

```python
import duckdb

con = duckdb.connect()
con.execute("CREATE VIEW session AS SELECT * FROM read_parquet('/data/export/session.parquet')")
con.execute("CREATE VIEW sites  AS SELECT * FROM read_parquet('/data/export/sites.parquet')")

result = con.execute("""
    SELECT t.*, s.date
    FROM sites t
    JOIN session s USING (session_id)
    WHERE s.subject_id = 'my_animal'
    ORDER BY s.date
""").df()
con.close()
```

### Read parquet metadata (provenance)

Every parquet written by this package carries provenance metadata in the
schema, readable without loading the data:

```python
import pyarrow.parquet as pq

meta = pq.read_metadata("/data/export/sessions/my_session/sites.parquet")
for k, v in meta.metadata.items():
    if any(t in k for t in [b"packaging", b"version", b"processor"]):
        print(f"{k.decode()}: {v.decode()}")
# packaging_version: 1.2.3
# data_contract_version: 0.7.0
# processor: SiteTableProcessor
```
