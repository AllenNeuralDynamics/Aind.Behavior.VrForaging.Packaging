# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo>=0.25.2",
#     "polars>=1.0",
#     "matplotlib>=3.8",
#     "aind-data-access-api>=1.9.2",
#     "boto3>=1.35",  # keeps the resolver off ancient boto3 when python-dateutil 4 is picked
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
        # One session: sites and velocity

        The [data access pattern](https://allenneuraldynamics.github.io/Aind.Behavior.VrForaging.Packaging/data/load-tables/) end to end:
        DocDB metadata, the session catalogue, the sites table, and the position and velocity stream from the session's own asset.
        Reads are anonymous.
        """
    )
    return


@app.cell
def _():
    import matplotlib.pyplot as plt
    import polars as pl
    from aind_data_access_api.document_db import MetadataDbClient

    SESSION_ID = "841312_2026-06-04_20-19-36"
    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"skip_signature": "true", "aws_region": "us-west-2", "max_retries": 10}
    return MetadataDbClient, OPTS, ROOT, SESSION_ID, pl, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Metadata (DocDB)")
    return


@app.cell
def _(MetadataDbClient, SESSION_ID):
    client = MetadataDbClient(
        host="api.allenneuraldynamics.org",
        database="metadata_index",
        collection="data_assets",
        version="v2",
    )
    (record,) = client.retrieve_docdb_records(
        filter_query={"name": SESSION_ID},
        projection={"subject": 1, "acquisition": 1},
    )
    record["subject"]["subject_id"], record["acquisition"]["acquisition_start_time"]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Catalogue and sites (S3)")
    return


@app.cell
def _(OPTS, ROOT, SESSION_ID, pl):
    in_session = pl.col("session_id") == SESSION_ID

    session = (
        pl.scan_parquet(f"{ROOT}/session.parquet", storage_options=OPTS)
        .select("session_id", "subject_id", "source_s3_location")
        .filter(in_session)
        .collect()
    )
    sites = (
        pl.scan_parquet(f"{ROOT}/sites.parquet", storage_options=OPTS).filter(in_session).sort("start_time").collect()
    )
    asset = session["source_s3_location"][0]
    session
    return asset, sites


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Position and velocity (session asset)")
    return


@app.cell
def _(OPTS, asset, pl, sites):
    t0 = sites["start_time"].min() + 600
    t1 = t0 + 60

    pos_vel = (
        pl.scan_parquet(f"{asset}/position_velocity.parquet", storage_options=OPTS)
        .filter(pl.col("timestamp").is_between(t0, t1))
        .sort("timestamp")
        .collect()
    )
    return pos_vel, t0, t1


@app.cell
def _(SESSION_ID, pl, plt, pos_vel, sites, t0, t1):
    colors = {"InterPatch": "#d9d9d9", "InterSite": "#eeeeee", "RewardSite": "#a6cee3"}

    fig, ax = plt.subplots(figsize=(11, 4))
    for site in sites.filter(pl.col("stop_time") >= t0, pl.col("start_time") <= t1).iter_rows(named=True):
        ax.axvspan(site["start_time"], site["stop_time"], color=colors[site["site_label"]], lw=0)

    ax.plot(pos_vel["timestamp"], pos_vel["position"], color="0.25")
    ax.set(xlim=(t0, t1), xlabel="time (s)", ylabel="position", title=SESSION_ID)

    ax_vel = ax.twinx()
    ax_vel.plot(pos_vel["timestamp"], pos_vel["velocity"], color="tab:red")
    ax_vel.set_ylabel("velocity", color="tab:red")
    fig
    return


if __name__ == "__main__":
    app.run()
