# NWB export

`NwbSession` builds a single `NWBFile` from AIND metadata, then calls each
processor's `nwbize()` to fill it, and writes the result as an NWB-Zarr store.

## Lifecycle

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.session_pipeline import create_processors
from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession

raw = "path/to/behavior_<subject>_<date>"
ds = dataset(raw)
processors = create_processors(ds)

session = NwbSession(raw)      # reads AIND metadata JSONs from raw/
session.run(*processors)       # calls nwbize() on each processor
session.write_nwb_zarr(        # writes the NWBFile as NWB-Zarr
    "output/my_session.nwb.zarr"
)
```

## NwbSession

::: aind_behavior_vr_foraging_packaging.nwb_file.NwbSession
