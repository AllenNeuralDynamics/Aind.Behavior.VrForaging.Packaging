---
type: Component
title: NwbSession — building and writing the NWB file
description: NwbSession constructs an NdxEventsNWBFile from AIND metadata and drives each processor's nwbize(), then writes NWB-Zarr.
resource: src/aind_behavior_vr_foraging_packaging/nwb_file/__init__.py
tags: [architecture, nwb, zarr, aind-data-schema, metadata]
timestamp: 2026-07-03T00:00:00Z
---

`NwbSession` (`nwb_file/__init__.py`) is the NWB counterpart to
[`run_session`](pipeline.md). Where the pipeline writes parquet, `NwbSession`
builds a single `NdxEventsNWBFile` and lets each processor contribute.

# Lifecycle

```python
from pathlib import Path
from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession
from aind_behavior_vr_foraging_packaging.pipeline import create_processors

session = NwbSession(Path("/path/to/session"))
nwb = session.run(*create_processors(session.dataset))   # process() + nwbize() loop
session.write_nwb_zarr(Path("/path/to/out.nwb.zarr"))
```

- `__init__(root_path, *, dataset=None, use_local_schema=False)` — loads the
  dataset (via `aind_behavior_vr_foraging.data_contract.dataset` if not given)
  and the AIND metadata (see below).
- `process()` — lazily creates the `NdxEventsNWBFile` (idempotent) from AIND
  metadata: `session_id`, `session_description` (dataset version),
  `session_start_time` (acquisition start), `identifier` (subject id), and a
  subject object via `aind_nwb_utils.get_subject_nwb_object`.
- `run(*processors)` — calls `process()`, then `processor.nwbize(nwb)` for each
  processor in order.
- `write_nwb_zarr(output)` — writes with `hdmf_zarr.NWBZarrIO`.

# Metadata source: `_AindDataSchemaJson`

The NWB file's identity comes from four AIND metadata records —
`acquisition`, `instrument`, `subject`, `data_description` — bundled in the
`_AindDataSchemaJson` dataclass. Two loaders:

- `from_root_path(root_path)` — reads local `*acquisition*.json`,
  `*data_description*.json`, `*subject*.json`, `*instrument*.json` (asserts
  exactly one of each). Selected when `use_local_schema=True`.
- `from_doc_db(session_id)` — queries the AIND DocumentDB
  (`api.allenneuraldynamics.org`, v2) by `name`. The default.

Integration tests force the local loader via a `monkeypatch` fixture so they
run without DocDB access — see
[testing/integration-tests.md](../testing/integration-tests.md).

# Relationship to processors

Each processor's `nwbize()` (see
[processor-abstraction.md](processor-abstraction.md) and
[continuous-and-event-streams.md](continuous-and-event-streams.md)) is
responsible for its own NWB structure — trials table, `behavior` processing
module `TimeSeries`, or per-event `DynamicTable` acquisitions. `NwbSession`
owns only the file skeleton and the write; it does not know processor
internals.
