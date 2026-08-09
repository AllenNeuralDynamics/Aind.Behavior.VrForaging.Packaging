"""Querying the experiment export with pandas and DuckDB.

The export pipeline writes two kinds of output:

  session.parquet          — flat catalogue, one row per session
  {table}.parquet           — flat file, all sessions concatenated (small cross-session tables)
  sessions/{session_id}/    — per-session directory (large per-session tables)
      position_position_velocity.parquet
      position.parquet
      ...

Small tables (e.g. ``sites``) are aggregated into a single flat parquet at the
export root. Large tables (e.g. ``position_velocity``, ``position``) stay per-session —
they are typically accessed one session at a time and are too large to concatenate.

Generating the export
---------------------
Run the CLI against a directory of raw session folders::

    aind-vr-export --input-dir /data/raw --output-dir /data/export

Or with the helper script (dev / scratch)::

    .\\scratch\\run_export.ps1

Dependencies
------------
Core examples use only ``pandas`` (a runtime dependency).
DuckDB examples require the optional ``db`` extra::

    pip install "aind-behavior-vr-foraging-packaging[db]"

Run from the project root::

    uv run python examples/query_export.py
"""

from pathlib import Path

import pandas as pd

# ── Configure this path to point at your export root ─────────────────────────
EXPORT_DIR = Path("scratch/export")

if not EXPORT_DIR.exists():
    raise SystemExit(
        f"Export directory not found: {EXPORT_DIR.resolve()}\n"
        "Generate it first with: aind-vr-export --input-dir <raw> --output-dir scratch/export"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. Session catalogue
# ─────────────────────────────────────────────────────────────────────────────

sessions = pd.read_parquet(EXPORT_DIR / "session.parquet")
print("=== sessions catalogue ===")
print(sessions[["session_id", "subject_id", "date"]].to_string(index=False))

first_animal = sessions["subject_id"].iloc[0]
animal_session_ids = sessions[sessions["subject_id"] == first_animal]["session_id"].tolist()
print(f"\nAnimal '{first_animal}' has {len(animal_session_ids)} session(s)")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Cross-session flat table  (pandas)
#
# sites.parquet is a single file — all sessions in one place.
# Filter by session_id to get one animal's sites.
# ─────────────────────────────────────────────────────────────────────────────

sites_path = EXPORT_DIR / "sites.parquet"
if not sites_path.exists():
    raise SystemExit(
        f"sites.parquet not found under {EXPORT_DIR}.\nRe-run the export (aggregation phase) to generate it."
    )

all_sites = pd.read_parquet(sites_path)
print(f"\n=== all sites: {len(all_sites)} rows across {all_sites['session_id'].nunique()} sessions ===")

animal_sites = all_sites[all_sites["session_id"].isin(animal_session_ids)]
print(f"=== sites for '{first_animal}': {len(animal_sites)} rows ===")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Per-session large table  (pandas)
#
# Large tables (position_velocity, position, licks, …) are never aggregated cross-session.
# Access them directly by session path — fast, no unnecessary data loaded.
# ─────────────────────────────────────────────────────────────────────────────

first_session = animal_session_ids[0]
pos_vel_path = EXPORT_DIR / "sessions" / first_session / "position_position_velocity.parquet"

if pos_vel_path.exists():
    position_velocity = pd.read_parquet(pos_vel_path)
    print(f"\n=== position_velocity [{first_session}]: {len(position_velocity)} rows ===")
else:
    print(f"\n(position_velocity not found for {first_session} — skipping)")


# ─────────────────────────────────────────────────────────────────────────────
# 4. DuckDB  (optional — install with: pip install "package[db]")
#
# Flat parquet files work as first-class DuckDB sources.
# For large per-session tables, build a file list from the catalogue and pass
# it to read_parquet() — DuckDB reads only those files, nothing else.
# ─────────────────────────────────────────────────────────────────────────────

try:
    import duckdb
except ImportError:
    print(
        "\nDuckDB not installed — skipping section 4.\n"
        'Install with: pip install "aind-behavior-vr-foraging-packaging[db]"'
    )
    raise SystemExit(0)

con = duckdb.connect()

# Register flat tables as views — single-file scan, full predicate pushdown
con.execute(f"CREATE VIEW session AS SELECT * FROM read_parquet('{EXPORT_DIR / 'session.parquet'}')")
con.execute(f"CREATE VIEW sites  AS SELECT * FROM read_parquet('{EXPORT_DIR / 'sites.parquet'}')")

print("\n=== DuckDB: site counts per session ===")
counts = con.execute("""
    SELECT session_id, COUNT(*) AS n_sites
    FROM sites
    GROUP BY session_id
    ORDER BY session_id
""").df()
print(counts.to_string(index=False))

print(f"\n=== DuckDB: sites for '{first_animal}' (join with catalogue) ===")
result = con.execute(f"""
    SELECT t.*, s.date
    FROM sites t
    JOIN session s USING (session_id)
    WHERE s.subject_id = '{first_animal}'
    ORDER BY s.date
""").df()
print(f"  {len(result)} rows, {result['session_id'].nunique()} session(s)")

# Large per-session table: build file list from catalogue, pass to DuckDB
pos_vel_files = [
    str(EXPORT_DIR / "sessions" / sid / "position_position_velocity.parquet")
    for sid in animal_session_ids
    if (EXPORT_DIR / "sessions" / sid / "position_position_velocity.parquet").exists()
]
if pos_vel_files:
    print(f"\n=== DuckDB: position_velocity for '{first_animal}' across {len(pos_vel_files)} session(s) ===")
    result = con.execute(f"SELECT * FROM read_parquet({pos_vel_files})").df()
    print(f"  {len(result)} rows")
else:
    print(f"\n(no position_velocity files found for '{first_animal}' — skipping)")

con.close()
print("\nDone.")
