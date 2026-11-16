# Raw streams

!!! package "Uses this package"
    This page reads the raw files of a session folder.
    The reader comes from the [`aind-behavior-vr-foraging`](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging) data contract and [contraqctor](https://github.com/AllenNeuralDynamics/contraqctor), which are installed with this package.

The processed tables cover what most analyses need.
Read the raw streams when you need something they do not extract, or to check how a table was made.
Every stream is listed, with its file and description, in [Raw data contract](../reference/raw-data-contract.md).
To read one stream straight from a session's public raw asset, without downloading the session, see [Load raw streams](../data/load-raw.md).

## Load a session

```python
from aind_behavior_vr_foraging.data_contract import dataset

ds = dataset("path/to/session")
```

Nothing is read yet; each stream is loaded the first time you ask for its data.

## Find a stream

Streams are nested: a collection (such as `Behavior`) holds devices and logs, which hold streams.
Walk down with `.at(...)`:

```python
[stream.name for stream in ds.at("Behavior").data]
# ['HarpBehavior', 'HarpManipulator', 'HarpTreadmill', ..., 'SoftwareEvents', 'Logs', 'InputSchemas']
```

To see a whole branch at once:

```python
from contraqctor.contract.utils import print_data_stream_tree

print(print_data_stream_tree(ds.at("Behavior").at("SoftwareEvents")))
```

## Read a stream

`.data` loads the stream and returns it, usually as a pandas DataFrame indexed by harp time:

```python
# Raw treadmill encoder (what position and velocity are computed from)
encoder = ds.at("Behavior").at("HarpTreadmill").at("SensorData").data

# Lickometer state
lick_state = ds.at("Behavior").at("HarpLickometer").at("LickState").data

# One software event stream
active_site = ds.at("Behavior").at("SoftwareEvents").at("ActiveSite").data

# The task configuration used for the session
task_logic = ds.at("Behavior").at("InputSchemas").at("TaskLogic").data
```

=== "Polars"

    ```python
    import polars as pl  # pip install polars

    encoder = pl.from_pandas(encoder.reset_index())  # keep the harp time as a column
    ```

=== "Pandas"

    ```python
    encoder.head()  # indexed by harp time
    ```

Which streams exist depends on the session's data version (`ds.version`); older sessions may be missing some or name them differently.

## Turn it into a table

To add your own table to the pipeline, subclass `AbstractProcessor` and pass it to `process_session` with the standard ones:

```python
import pandas as pd
from aind_behavior_vr_foraging_packaging._base import AbstractProcessor
from aind_behavior_vr_foraging_packaging.pipeline.session import create_processors, process_session


class EncoderProcessor(AbstractProcessor):
    """Raw treadmill encoder counts, one row per sample."""

    __output_name__ = "encoder"  # the table name and parquet file name

    def _compute(self) -> pd.DataFrame:
        data = self.dataset.at("Behavior").at("HarpTreadmill").at("SensorData").data
        return data[["Encoder"]]


tables = process_session(ds, "output/my_session", processors=[*create_processors(ds), EncoderProcessor(ds)])
# output/my_session/encoder.parquet, alongside the standard tables
```

Your table gets the same provenance metadata as the built-in ones.
See [Processors](../api/processors.md) for the full interface.
