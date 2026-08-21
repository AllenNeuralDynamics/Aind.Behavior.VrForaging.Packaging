# Guides

Step-by-step walkthroughs for the most common tasks.

<div class="grid cards" markdown>

-   :material-folder-open:{ .lg .middle } **Run a session from disk**

    ---

    Load a raw session directory, run individual processors, and access
    the output DataFrames.

    [:octicons-arrow-right-24: Open guide](session-from-disk.md)

-   :material-table:{ .lg .middle } **Query parquet files**

    ---

    Work with the export output using pandas, DuckDB, and Polars.
    Covers the session catalogue, flat cross-session tables, and
    per-session large tables.

    [:octicons-arrow-right-24: Open guide](parquet-files.md)

-   :material-aws:{ .lg .middle } **Query from AWS S3**

    ---

    Point DuckDB or Polars directly at S3 — no download needed. Includes
    SSO credential setup and glob-based multi-session queries.

    [:octicons-arrow-right-24: Open guide](aws-s3.md)

-   :material-stream:{ .lg .middle } **Raw streams with contraqctor**

    ---

    Access the underlying behavioral streams directly via contraqctor.
    Useful for custom analysis beyond the built-in processors.

    [:octicons-arrow-right-24: Open guide](contraqctor-streams.md)

-   :material-docker:{ .lg .middle } **Run a batch campaign**

    ---

    Process a fixed session list end-to-end with the Docker-based pipeline:
    stage from S3, run processors in containers, write parquet and NWB
    locally, and aggregate. Includes a no-credentials smoke test.

    [:octicons-arrow-right-24: Open guide](batch-campaign.md)

</div>
