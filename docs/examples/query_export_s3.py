# /// script
# dependencies = [
#     "duckdb>=1.0",
# ]
# requires-python = ">=3.11"
# ///
"""Querying the experiment export directly from S3 with DuckDB.

All reads hit S3 using DuckDB's native httpfs extension — no local copies needed.
Predicate pushdown and Parquet column pruning keep network I/O minimal.

Remote layout (mirrors the local export structure)::

    s3://aind-scratch-data/vr-foraging/demo/
    ├── session.parquet            # flat catalogue, one row per session
    ├── sites.parquet              # flat sites table, all sessions
    └── sessions/
        └── {session_id}/
            ├── position_velocity.parquet
            └── ...

Dependencies are declared inline (PEP 723) — ``uv`` resolves them per-run, so this
script needs no project install::

    uv run docs/examples/query_export_s3.py
"""

import duckdb

# ── Configure this path to point at your export root ─────────────────────────
S3_ROOT = "s3://aind-scratch-data/vr-foraging/demo"
# ─────────────────────────────────────────────────────────────────────────────

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# For a public bucket no credentials are needed.
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


# ─────────────────────────────────────────────────────────────────────────────
# 1. Session catalogue
#    Single small file — DuckDB reads only the columns you SELECT.
# ─────────────────────────────────────────────────────────────────────────────

con.execute(f"CREATE VIEW session AS SELECT * FROM read_parquet('{S3_ROOT}/session.parquet')")

sessions = con.execute("SELECT session_id, subject_id, date FROM session ORDER BY date").df()
print("=== session catalogue ===")
print(sessions.to_string(index=False))

first_animal = sessions["subject_id"].iloc[0]
animal_session_ids = sessions[sessions["subject_id"] == first_animal]["session_id"].tolist()
print(f"\nAnimal '{first_animal}' has {len(animal_session_ids)} session(s)")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Cross-session flat table  (sites)
#    Single file, full predicate pushdown — DuckDB streams only matching rows.
# ─────────────────────────────────────────────────────────────────────────────

con.execute(f"CREATE VIEW sites AS SELECT * FROM read_parquet('{S3_ROOT}/sites.parquet')")

total_sites = con.execute("SELECT COUNT(*) AS n FROM sites").fetchone()[0]
total_sessions = con.execute("SELECT COUNT(DISTINCT session_id) AS n FROM sites").fetchone()[0]
print(f"\n=== all sites: {total_sites} rows across {total_sessions} sessions ===")

animal_sites = con.execute(f"""
    SELECT COUNT(*) AS n
    FROM sites
    WHERE session_id IN ({", ".join(f"'{s}'" for s in animal_session_ids)})
""").fetchone()[0]
print(f"=== sites for '{first_animal}': {animal_sites} rows ===")

print("\n=== DuckDB: site counts per session ===")
counts = con.execute("""
    SELECT session_id, COUNT(*) AS n_sites
    FROM sites
    GROUP BY session_id
    ORDER BY session_id
""").df()
print(counts.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# 3. Join catalogue + flat table
#    DuckDB pushes the WHERE into the S3 scan — reads only matching row groups.
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n=== sites for '{first_animal}' joined with session catalogue ===")
result = con.execute(f"""
    SELECT t.*, s.date
    FROM sites t
    JOIN session s USING (session_id)
    WHERE s.subject_id = '{first_animal}'
    ORDER BY s.date
""").df()
print(f"  {len(result)} rows, {result['session_id'].nunique()} session(s)")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Per-session large tables via S3 glob
#    DuckDB resolves the glob against S3 and reads only the matching files —
#    no directory listing round-trip per session.
# ─────────────────────────────────────────────────────────────────────────────

POS_VEL_GLOB = f"{S3_ROOT}/sessions/*/position_velocity.parquet"

try:
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{POS_VEL_GLOB}')").fetchone()[0]
except duckdb.IOException:
    n = 0

if n:
    print(f"\n=== position_velocity for '{first_animal}' (glob scan across sessions) ===")
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
    print(f"  {len(result)} rows, {result['session_id'].nunique()} session(s)")
else:
    print(f"\n(no position_velocity files found under {S3_ROOT}/sessions/ — skipping)")

con.close()
print("\nDone.")
