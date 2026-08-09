# /// script
# dependencies = [
#     "polars==1.43.2",
# ]
# requires-python = ">=3.11"
# ///
"""Querying the experiment export directly from S3 with Polars.

All reads hit S3 using Polars' lazy Parquet scanner — no local copies needed.
Predicate pushdown and Parquet column pruning keep network I/O minimal.

Remote layout (mirrors the local export structure)::

    s3://aind-scratch-data/vr-foraging/demo/
    ├── session.parquet            # flat catalogue, one row per session
    ├── sites.parquet              # flat sites table, all sessions
    └── sessions/
        └── {session_id}/
            ├── position_velocity.parquet
            └── ...

Prerequisites
-------------
Install Polars::

    pip install polars

Run from the project root::

    uv run --with polars python examples/query_export_s3_polars.py
"""

import polars as pl

# ── Configure these ───────────────────────────────────────────────────────────
S3_ROOT = "s3://aind-scratch-data/vr-foraging/demo"
STORAGE_OPTIONS = {"skip_signature": "true"}  # no credentials required for public bucket
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# 1. Session catalogue
#    Single small file — Polars reads only the columns selected below.
# ─────────────────────────────────────────────────────────────────────────────

session = pl.scan_parquet(f"{S3_ROOT}/session.parquet", storage_options=STORAGE_OPTIONS)
print("\n=== session catalogue columns ===")
print(session.collect_schema())  # schema is stored as metadata: doesn't require full read

sessions = (
    session
    # selecting only necessary if you want to use subset of columns, or transform data/change names
    .select("session_id", "subject_id", "date")
    .sort("date")
    .collect()
)
print("=== session catalogue ===")
print(sessions)

first_animal = sessions["subject_id"][0]
animal_session_ids = sessions.filter(pl.col("subject_id") == first_animal)["session_id"].to_list()
print(f"\nAnimal '{first_animal}' has {len(animal_session_ids)} session(s)")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Cross-session flat table  (sites)
#    Single file, full predicate pushdown — Polars streams only matching rows.
# ─────────────────────────────────────────────────────────────────────────────

sites = pl.scan_parquet(f"{S3_ROOT}/sites.parquet", storage_options=STORAGE_OPTIONS)
total_sites = sites.select(pl.len()).collect().item()
total_sessions = sites.select(pl.col("session_id").n_unique()).collect().item()
print(f"\n=== all sites: {total_sites} rows across {total_sessions} sessions ===")

animal_sites = sites.filter(pl.col("session_id").is_in(animal_session_ids)).select(pl.len()).collect().item()
print(f"=== sites for '{first_animal}': {animal_sites} rows ===")

print("\n=== Polars: site counts per session ===")
counts = sites.group_by("session_id").agg(pl.len().alias("n_sites")).sort("session_id").collect()
print(counts)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Join catalogue + flat table
#    The filter and projection are pushed into both lazy S3 scans.
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n=== sites for '{first_animal}' joined with session catalogue ===")
result = (
    sites.join(
        other=(
            session.select("session_id", "subject_id", "date")
            # joins can be expensive for big tables, so filter first if poss
            .filter(pl.col("subject_id") == first_animal)
        ),
        on="session_id",
        how="inner",
    )
    .sort("date")
    .collect()
)
print(f"  {len(result)} rows, {result['session_id'].n_unique()} session(s)")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Per-session large tables via S3 glob
#    Polars resolves the glob against S3 and reads only matching files.
# ─────────────────────────────────────────────────────────────────────────────

POS_VEL_GLOB = f"{S3_ROOT}/sessions/*/position_velocity.parquet"

try:
    pos_vel = pl.scan_parquet(
        POS_VEL_GLOB,
        storage_options=STORAGE_OPTIONS,
        include_file_paths="source_path",
    )
except pl.exceptions.ComputeError as exc:
    if "expanded paths were empty" not in str(exc):
        raise
    result = None
else:
    # session_id is not a column inside the parquet — extract it from the file path.
    # include_file_paths adds the source path to the lazy scan.
    print(len(pos_vel.collect()))
    print()
    result = (
        pos_vel.with_columns(pl.col("source_path").str.extract(r"/sessions/([^/]+)/", 1).alias("session_id"))
        .drop("source_path")
        .join(
            session.select("session_id", "subject_id"),
            on="session_id",
            how="inner",
        )
        .filter(pl.col("subject_id") == first_animal)
        .drop("subject_id")
        .collect()
    )

if result is not None:
    print(f"\n=== position_velocity for '{first_animal}' (glob scan across sessions) ===")
    print(f"  {len(result)} rows, {result['session_id'].n_unique()} session(s)")
else:
    print(f"\n(no position_velocity files found under {S3_ROOT}/sessions/ — skipping)")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Download the entire sites table into memory
# ─────────────────────────────────────────────────────────────────────────────

print("\n=== downloading full sites table into memory ===")
sites_df = sites.collect()
print(f"{sites_df.shape[0]:,} rows × {sites_df.shape[1]} columns")
print(sites_df.dtypes)

print("\nDone.")
