# Raw streams with contraqctor

The built-in processors cover the most common outputs (sites, position/velocity,
licks, sniffing, software events, hardware events). For custom analysis —
novel derived signals, debugging, or exploring streams not surfaced by any
processor — you can access the raw behavioral streams directly through
[contraqctor](https://github.com/AllenNeuralDynamics/contraqctor).

## What is contraqctor?

contraqctor is a data-loading library that understands the layout of AIND
behavioral data directories. It validates the directory against a typed
schema (the *data contract*), then gives you typed, lazy accessors to every
stream inside it.

The aind-behavior-vr-foraging data contract defines every stream the
acquisition software writes. contraqctor is the layer that maps those schema
definitions to Python objects.

## Load a Dataset

```python
from aind_behavior_vr_foraging.data_contract import dataset

ds = dataset("path/to/behavior_<subject>_<date>")
print(type(ds))   # contraqctor.contract.Dataset
print(ds.version) # e.g. '0.7.0'
```

## Explore available streams

A `Dataset` exposes its streams as typed attributes. Use tab-completion in
an interactive shell, or inspect the fields:

```python
import dataclasses

# List all stream fields defined by the data contract
fields = [f.name for f in dataclasses.fields(ds)]
print(fields)
# ['encoder', 'lick_sensor', 'sniff_sensor', 'software_events',
#  'hardware_events', 'task_logic', 'rig', ...]
```

## Read a stream

Each stream attribute is a contraqctor *Reader*. Call `.load()` to read it:

```python
# Hardware events stream
events = ds.hardware_events.load()
print(type(events))   # typically a pandas DataFrame or a pydantic model

# Software events
sw_events = ds.software_events.load()
print(sw_events.head())
```

## Encoder (position/velocity raw data)

The raw encoder stream contains position samples at the acquisition rate
(typically 250 Hz). The `PositionAndVelocityProcessor` derives velocity from
this, but you can access the raw samples directly:

```python
encoder_df = ds.encoder.load()
print(encoder_df.columns.tolist())
# e.g. ['timestamp', 'position', ...]
print(encoder_df.head())
```

## Lick sensor

```python
lick_df = ds.lick_sensor.load()
print(lick_df.head())
```

## Task logic and rig configuration

Static configuration objects (not time series) are also accessible:

```python
task_logic = ds.task_logic.load()  # task parameters (pydantic model)
print(task_logic)

rig = ds.rig.load()  # rig hardware definition
print(rig)
```

## Build a custom processor

The cleanest way to add a new derived output is to subclass
`AbstractProcessor`:

```python
import pandas as pd
from contraqctor.contract import Dataset
from aind_behavior_vr_foraging_packaging._base import AbstractProcessor


class MyCustomProcessor(AbstractProcessor):
    """Extract my custom signal."""

    output_name = "my_signal"

    def _compute(self) -> pd.DataFrame:
        raw = self.dataset.encoder.load()
        # … your logic here …
        return raw[["timestamp", "position"]].rename(columns={"position": "my_signal"})
```

Then slot it into the standard pipeline:

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.pipeline.session import create_processors

ds = dataset("path/to/session")
processors = create_processors(ds)
processors.append(MyCustomProcessor(ds))

for proc in processors:
    df = proc.compute()
    print(f"{proc.output_name}: {len(df)} rows, provenance={df.attrs}")
```

`compute()` stamps `packaging_version`, `data_contract_version`,
`dataset_version`, and `processor` into `df.attrs` automatically — your
custom processor inherits provenance tracking for free.

See the [AbstractProcessor API reference](../api/processors.md) for the full
interface.

## Version-specific streams

Some streams were introduced or changed in particular schema versions. The
built-in processors handle this through `LegacySiteTableProcessor` /
`LegacySiteTableProcessor` pattern (version `< 0.6.0`). For custom
processors, gate on the dataset version the same way:

```python
import semver

version = semver.Version.parse(str(ds.version))
if version < semver.Version(0, 6, 0):
    raw = ds.some_legacy_stream.load()
else:
    raw = ds.some_current_stream.load()
```
