# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo>=0.25.2",
#     "polars>=1.0",
#     "matplotlib>=3.8",
#     "aind-behavior-vr-foraging[data]>=1.2.5",
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
        # One raw stream

        Read one file straight from a session's raw asset, two ways:
        with the path typed by hand, and with the path taken from the data contract.
        Reads are anonymous.
        """
    )
    return (mo,)


@app.cell
def _():
    import matplotlib.pyplot as plt
    import polars as pl
    from aind_behavior_vr_foraging.data_contract import dataset

    SESSION_ID = "841312_2026-06-04_20-19-36"
    ROOT = "s3://aind-scratch-data/vr-foraging/vr-foraging-dataset"
    OPTS = {"skip_signature": "true", "aws_region": "us-west-2", "max_retries": 10}
    return OPTS, ROOT, SESSION_ID, dataset, pl, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Find the raw asset (catalogue)")
    return


@app.cell
def _(OPTS, ROOT, SESSION_ID, pl):
    row = (
        pl.scan_parquet(f"{ROOT}/session.parquet", storage_options=OPTS)
        .filter(pl.col("session_id") == SESSION_ID)
        .select("raw_s3_location", "data_contract_version")
        .collect()
        .row(0, named=True)
    )
    asset = row["raw_s3_location"]
    version = row["data_contract_version"]
    asset, version
    return asset, version


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Hard-coded path")
    return


@app.cell
def _(OPTS, asset, pl):
    stream_hardcoded = "behavior/OperationControl/IsStopped.csv"

    is_stopped = pl.read_csv(f"{asset}/{stream_hardcoded}", storage_options=OPTS)
    is_stopped.head()
    return (is_stopped,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Path from the data contract

        The contract knows where each stream's file is, so name the stream instead of typing the path.
        Nothing is read here: `dataset(".")` is a placeholder root and only the relative path is used.
        The session's own `data_contract_version` selects the matching contract.
        """
    )
    return


@app.cell
def _(OPTS, asset, dataset, is_stopped, pl, version):
    ds = dataset(".", version=version)
    node = ds.at("Behavior").at("OperationControl").at("IsStopped")

    stream = node.reader_params.path.as_posix()

    is_stopped_contract = pl.read_csv(f"{asset}/{stream}", storage_options=OPTS)
    assert is_stopped_contract.equals(is_stopped), "contract path read a different file"
    stream
    return (is_stopped_contract,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("## Plot")
    return


@app.cell
def _(SESSION_ID, is_stopped_contract, plt):
    t0 = is_stopped_contract["Seconds"].min()

    fig, ax = plt.subplots(figsize=(11, 2.5))
    ax.step(
        is_stopped_contract["Seconds"] - t0,
        is_stopped_contract["IsStopped"].cast(int),
        where="post",
        color="0.25",
    )
    ax.set(xlabel="time since first event (s)", ylabel="IsStopped", yticks=[0, 1], title=SESSION_ID)
    fig
    return


if __name__ == "__main__":
    app.run()
