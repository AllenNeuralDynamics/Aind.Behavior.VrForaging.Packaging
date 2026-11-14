# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo>=0.25.2",
#     "polars>=1.0",
#     "matplotlib>=3.8",
#     "aind-data-access-api>=1.9.2",
# ]
# ///

import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    mo.md(
        r"""
        # One subject across sessions

        Find a subject's sessions in DocDB, load their sites from the shared dataset, and summarize each session.
        Reads are anonymous; this package is not used.
        """
    )
    return (mo,)


@app.cell
def _():
    import matplotlib.pyplot as plt
    import polars as pl
    from aind_data_access_api.document_db import MetadataDbClient

    SUBJECT_ID = "841312"
    SINCE = "2026-06-01"
    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"skip_signature": "true", "aws_region": "us-west-2", "max_retries": 10}
    return MetadataDbClient, OPTS, ROOT, SINCE, SUBJECT_ID, pl, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Find sessions (DocDB)")
    return


@app.cell
def _(MetadataDbClient, SINCE, SUBJECT_ID):
    client = MetadataDbClient(
        host="api.allenneuraldynamics.org",
        database="metadata_index",
        collection="data_assets",
        version="v2",
    )
    records = client.retrieve_docdb_records(
        filter_query={
            "subject.subject_id": SUBJECT_ID,
            "acquisition.acquisition_start_time": {"$gte": SINCE},
            "data_description.data_level": "raw",
        },
        projection={"name": 1},
    )
    session_ids = sorted(r["name"] for r in records)
    len(session_ids)
    return (session_ids,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Sites (S3) and a per-session summary")
    return


@app.cell
def _(OPTS, ROOT, pl, session_ids):
    sites = (
        pl.scan_parquet(f"{ROOT}/sites.parquet", storage_options=OPTS)
        .filter(pl.col("session_id").is_in(session_ids), pl.col("site_label") == "RewardSite")
        .collect()
    )
    summary = (
        sites.group_by("session_id")
        .agg(
            pl.len().alias("reward_sites"),
            pl.col("has_choice").mean().alias("stop_rate"),
            pl.col("has_reward").sum().alias("rewards"),
        )
        .sort("session_id")
    )
    summary
    return (summary,)


@app.cell
def _(SUBJECT_ID, plt, summary):
    fig, (ax_stop, ax_rew) = plt.subplots(2, 1, figsize=(10, 5), sharex=True)
    x = range(summary.height)

    ax_stop.plot(x, summary["stop_rate"], marker="o")
    ax_stop.set(ylabel="stop rate at\nreward sites", ylim=(0, 1), title=f"Subject {SUBJECT_ID}")

    ax_rew.bar(x, summary["rewards"])
    ax_rew.set(ylabel="rewards", xlabel="session")
    ax_rew.set_xticks(list(x), [s.split("_")[1] for s in summary["session_id"]], rotation=45, ha="right")
    fig.tight_layout()
    fig
    return


if __name__ == "__main__":
    app.run()
