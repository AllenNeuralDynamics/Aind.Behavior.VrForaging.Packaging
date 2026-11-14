# Many sessions

!!! package "Uses this package"
    This page runs `aind-behavior-vr-foraging-packaging` over a folder of raw sessions and reads back what it wrote.
    The result has the same layout as the [shared dataset](../data/index.md), on your own disk.

## Run the export

=== "Command line"

    ```bash
    uv tool install aind-behavior-vr-foraging-packaging
    vr-foraging-packaging batch --input-dir /data/raw --output-dir /data/export --workers 4
    ```

=== "Python"

    ```python
    from pathlib import Path
    from aind_behavior_vr_foraging_packaging.pipeline.batch import aggregate, process_sessions

    raw_sessions = sorted(Path("/data/raw").iterdir())  # one folder per session
    process_sessions(raw_sessions, Path("/data/export"), max_workers=4)
    aggregate(Path("/data/export/sessions"), Path("/data/export"))
    ```

`--input-dir` holds one raw session folder per session.
Add `--write-nwb` to also write an NWB file per session.
Run `vr-foraging-packaging batch --help` for every option, or see the [CLI reference](../api/cli.md).

The CLI has two more commands:

| Command | `--input-dir` is | What it does |
| --- | --- | --- |
| `batch` | a folder of raw session folders | Process every session, then aggregate |
| `session` | one raw session folder | Process just that session |
| `aggregate` | the `sessions/` folder of an earlier export | Rebuild the aggregated tables only, without reprocessing |

## What it writes

```text
/data/export/
├── session.parquet          # one row per session
├── sites.parquet            # one row per site, every session
└── sessions/
    └── <session_id>/
        ├── session.parquet
        ├── sites.parquet
        ├── position_velocity.parquet
        ├── licks.parquet
        └── ...
```

`session.parquet` and `sites.parquet` at the top are the per-session files stacked together, with a `session_id` column.
The large tables stay per session, under `sessions/<session_id>/`.

!!! note "Local layout vs the shared dataset"
    In the shared dataset each session's large tables live in that session's own asset, found through `source_s3_location`.
    In a local export they are under `sessions/<session_id>/`.

## Read it back

=== "Polars"

    ```python
    import polars as pl

    EXPORT = "/data/export"

    session = pl.read_parquet(f"{EXPORT}/session.parquet", columns=["session_id", "subject_id", "date"])
    sites = pl.read_parquet(f"{EXPORT}/sites.parquet")

    # All sessions' position at once; the session id comes from the folder name
    position = (
        pl.scan_parquet(f"{EXPORT}/sessions/*/position_velocity.parquet", include_file_paths="path")
        .with_columns(pl.col("path").str.extract(r"sessions[/\\]([^/\\]+)[/\\]").alias("session_id"))
        .drop("path")
        .collect()
    )
    ```

=== "Pandas"

    ```python
    import pandas as pd
    from pathlib import Path

    EXPORT = Path("/data/export")

    session = pd.read_parquet(EXPORT / "session.parquet", columns=["session_id", "subject_id", "date"])
    sites = pd.read_parquet(EXPORT / "sites.parquet")

    # All sessions' position at once; the session id comes from the folder name
    position = pd.concat(
        pd.read_parquet(path).assign(session_id=path.parent.name)
        for path in sorted(EXPORT.glob("sessions/*/position_velocity.parquet"))
    )
    ```

To filter sites by subject, join them to the session table:

=== "Polars"

    ```python
    subject_sites = sites.join(session.filter(pl.col("subject_id") == "841312"), on="session_id")
    ```

=== "Pandas"

    ```python
    subject_sites = sites.merge(session[session["subject_id"] == "841312"], on="session_id")
    ```

DuckDB reads the same files with SQL, e.g. `SELECT * FROM '/data/export/sites.parquet'`.
