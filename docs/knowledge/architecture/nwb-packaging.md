---
type: Component
title: NwbSession — building and writing the NWB file
description: NwbSession builds a base NWBFile via aind-nwb-utils, stamps provenance into was_generated_by, drives each processor's nwbize(), then writes NWB-Zarr.
resource: src/aind_behavior_vr_foraging_packaging/nwb_file/__init__.py
tags: [architecture, nwb, zarr, metadata, provenance]
timestamp: 2026-08-09T00:00:00Z
---

`NwbSession` (`nwb_file/__init__.py`) is the NWB counterpart to
[`run_session`](session-pipeline.md). Where the session pipeline writes parquet,
`NwbSession` builds a single `NWBFile` and lets each processor contribute.

It is used in two contexts: directly (scripts, notebooks), and automatically
by the [export pipeline](export-pipeline.md) when `write_nwb=True` is passed
to `process_sessions`.

# Lifecycle

```python
from pathlib import Path
from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession
from aind_behavior_vr_foraging_packaging.session_pipeline import create_processors

session = NwbSession(Path("/path/to/session"))
nwb = session.run(*create_processors(session.dataset))  # process() + nwbize() loop
session.write_nwb_zarr(Path("/path/to/out.nwb.zarr"))
```

- `__init__(root_path, *, dataset=None, base_nwb_file=None)` — loads the dataset
  (via `aind_behavior_vr_foraging.data_contract.dataset` if not given).
  `base_nwb_file` supplies a ready-made `NWBFile` to decorate instead of deriving
  one from the metadata jsons, for callers that already hold one or source their
  metadata elsewhere; it is also what lets the unit tests exercise the session
  without maintaining a synthetic copy of the AIND schema.
- `process()` — lazily creates the `NWBFile` (idempotent) by delegating to
  `aind_nwb_utils.utils.create_base_nwb_file(root_path)`, then stamping
  provenance onto it.
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

# Provenance

`_create_nwb_file` appends the session's versions to `NWBFile.was_generated_by`,
the core NWB field for recording what produced a file. `create_base_nwb_file`
has already put its own entry there, so ours extend it:

```python
dict(nwb.was_generated_by[:])
# {'aind-nwb-utils': '0.2.8',
#  'packaging_version': '0.0.4',
#  'data_contract_version': '1.2.1',
#  'dataset_version': '0.6.1'}
```

`NwbSession.provenance` is just `PackagingProvenance.build(dataset).model_dump()`
— the same object `AbstractProcessor.compute` stamps into `df.attrs`, so the key
names cannot drift between the parquet and NWB outputs of a session and the two
can be compared directly. `_provenance.py` is the only place those names are
defined; see [data-contract-and-versioning.md](data-contract-and-versioning.md).

The field is nominally software name/version pairs — `aind-nwb-utils`'s entry
uses that convention — but nothing constrains the first element, so
`dataset_version` (a data schema version, not a package) rides along as a plain
key. That mild stretch buys a lot: no extension, no namespace to name or version,
and no dependency for readers.

Two constraints to respect when changing this:

- `was_generated_by` is **write-once**. Reassigning it after
  `create_base_nwb_file` raises `AttributeError`; only `extend`/`append` on the
  existing list works, and only before the file is written. Once read back it is a
  read-only array.
- Nothing can be added after the write, so the integration suite re-reads
  provenance from disk rather than trusting the in-memory object.

`notes` is not an alternative home either: pynwb fields are write-once, so a
version string recorded there cannot be appended to.

# Relationship to processors

Each processor's `nwbize()` (see
[processor-abstraction.md](processor-abstraction.md) and
[continuous-and-event-streams.md](continuous-and-event-streams.md)) is
responsible for its own NWB structure — sites table, `behavior` processing
module `TimeSeries`, or per-event `DynamicTable` acquisitions. `NwbSession`
owns only the file skeleton and the write; it does not know processor
internals.
