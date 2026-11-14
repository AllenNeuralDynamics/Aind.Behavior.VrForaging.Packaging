# Process raw sessions

!!! package "Uses this package"
    This section runs `aind-behavior-vr-foraging-packaging` on raw session folders, to produce the same tables that are in the [shared dataset](../data/index.md).
    If the sessions you need are already in the shared dataset, you do not need any of this.

You need this when a session is not processed yet, when you have the raw data locally, or when you are changing how the tables are made.

## 1. Install

=== "uv"

    ```bash
    uv add aind-behavior-vr-foraging-packaging
    ```

=== "pip"

    ```bash
    pip install aind-behavior-vr-foraging-packaging
    ```

=== "latest from GitHub"

    ```bash
    uv pip install "git+https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging.git"
    ```

## 2. Process one session

A raw session folder is what the rig wrote: `behavior/`, the AIND metadata JSONs, and so on.
`process_session` computes every table and writes one parquet per table:

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.pipeline.session import process_session

ds = dataset("path/to/session")  # the raw session folder
tables = process_session(ds, output_dir="output/my_session")
# output/my_session/session.parquet, sites.parquet, position_velocity.parquet, ...
```

`tables` maps each table name to its dataframe (pandas), already in memory:

=== "Polars"

    ```python
    import polars as pl  # pip install polars

    sites = pl.from_pandas(tables["sites"])
    position = pl.from_pandas(tables["position_velocity"].reset_index())  # timestamp as a column
    ```

=== "Pandas"

    ```python
    sites = tables["sites"]
    position = tables["position_velocity"]  # indexed by timestamp
    ```

The tables are the same as in the shared dataset; every column is described in [Processed tables](../reference/processed-tables.md).

## 3. Or many sessions from the command line

```bash
uv tool install aind-behavior-vr-foraging-packaging
vr-foraging-packaging batch --input-dir /data/raw --output-dir /data/export
```

`--input-dir` holds one raw session folder per session. See [Many sessions](many-sessions.md) for the output layout and how to read it back.

## Next

<div class="grid cards" markdown>

-   :material-folder-open:{ .lg .middle } **One session in Python**

    ---

    Single tables, output formats (parquet, NWB), errors and provenance.

    [:octicons-arrow-right-24: One session](one-session.md)

-   :material-folder-multiple:{ .lg .middle } **Many sessions**

    ---

    The `batch` command, its output layout, and reading the export.

    [:octicons-arrow-right-24: Many sessions](many-sessions.md)

-   :material-sine-wave:{ .lg .middle } **Raw streams**

    ---

    Read the raw files of a session directly, for anything the tables do not cover.

    [:octicons-arrow-right-24: Raw streams](raw-streams.md)

</div>
