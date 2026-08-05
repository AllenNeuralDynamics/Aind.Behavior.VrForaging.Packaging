---
type: Component
title: NwbSession — building and writing the NWB file
description: NwbSession builds a base NWBFile via aind-nwb-utils and drives each processor's nwbize(), then writes NWB-Zarr.
resource: src/aind_behavior_vr_foraging_packaging/nwb_file/__init__.py
tags: [architecture, nwb, zarr, metadata]
timestamp: 2026-08-04T00:00:00Z
---

`NwbSession` (`nwb_file/__init__.py`) is the NWB counterpart to
[`run_session`](pipeline.md). Where the pipeline writes parquet, `NwbSession`
builds a single `NWBFile` and lets each processor contribute.

# Lifecycle

```python
from pathlib import Path
from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession
from aind_behavior_vr_foraging_packaging.pipeline import create_processors

session = NwbSession(Path("/path/to/session"))
nwb = session.run(*create_processors(session.dataset))  # process() + nwbize() loop
session.write_nwb_zarr(Path("/path/to/out.nwb.zarr"))
```

- `__init__(root_path, *, dataset=None)` — loads the dataset (via
  `aind_behavior_vr_foraging.data_contract.dataset` if not given).
- `process()` — lazily creates the `NWBFile` (idempotent) by delegating to
  `aind_nwb_utils.utils.create_base_nwb_file(root_path)`.
- `run(*processors)` — calls `process()`, then `processor.nwbize(nwb)` for each
  processor in order.
- `write_nwb_zarr(output)` — writes with `hdmf_zarr.NWBZarrIO`.

# Metadata source

File identity is not assembled here. `create_base_nwb_file` reads the AIND
metadata jsons sitting in the session root — `data_description.json`,
`subject.json`, `procedures.json`, `processing.json`, and one of
`acquisition.json` / `session.json` — and derives `session_id`,
`session_description`, `session_start_time`, `institution`, `lab`, and the
`Subject` object from them. All five files must be present locally; there is no
network/DocDB lookup.

# Relationship to processors

Each processor's `nwbize()` (see
[processor-abstraction.md](processor-abstraction.md) and
[continuous-and-event-streams.md](continuous-and-event-streams.md)) is
responsible for its own NWB structure — sites table, `behavior` processing
module `TimeSeries`, or per-event `DynamicTable` acquisitions. `NwbSession`
owns only the file skeleton and the write; it does not know processor
internals.
