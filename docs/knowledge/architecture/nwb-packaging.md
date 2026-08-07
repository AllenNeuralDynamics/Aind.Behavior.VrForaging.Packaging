---
type: Component
title: NwbSession — building and writing the NWB file
description: NwbSession builds a base NWBFile via aind-nwb-utils, stamps provenance into lab_meta_data, drives each processor's nwbize(), then writes NWB-Zarr.
resource: src/aind_behavior_vr_foraging_packaging/nwb_file/__init__.py
tags: [architecture, nwb, zarr, metadata, provenance]
timestamp: 2026-08-07T00:00:00Z
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

`_create_nwb_file` attaches a `PackagingProvenance` container
(`nwb_file/_provenance.py`) carrying `dataset_version`, `packaging_version`, and
`data_contract_version` — the same keys the parquet outputs carry in `df.attrs`,
so the two can be compared (see
[data-contract-and-versioning.md](data-contract-and-versioning.md)):

```python
nwb.lab_meta_data["provenance"].dataset_version   # '0.6.1'
nwb.lab_meta_data["provenance"].fields            # plain dict of str
```

## Why it needs a spec

`add_lab_meta_data` accepts a plain `LabMetaData` subclass carrying extra
attributes and writes it **without error** — but hdmf drops any attribute it has
no spec for, so the values vanish on read-back with nothing signalling the loss.
An in-memory assertion cannot catch this; only re-reading the written file can.
`provenance_type()` therefore builds a `GroupSpec` at runtime and registers it as
the `vr-foraging-packaging` namespace.

No `ndx-*` package is involved, and no namespace yamls are shipped: they are
exported to a temp dir purely because `NamespaceBuilder` only writes to disk and
`load_namespaces` only reads from disk, then discarded once the spec is parsed
into the type map.

pynwb caches the spec into every file written — visible as
`specifications/vr-foraging-packaging` inside the zarr, alongside `core` and
`hdmf-common` — so a reader with only `pynwb` and `hdmf-zarr` installed gets the
attributes back. The container class is generated from the cached spec, so it
supports attribute access but cannot be `isinstance`-checked against ours.

Two constraints to respect when changing this:

- It depends on `cache_spec` staying enabled at write time (pynwb's default, not
  overridden by `write_nwb_zarr`). The integration suite re-reads provenance from
  disk rather than trusting the in-memory object, so a regression fails a test
  instead of silently shipping stripped files.
- `NAMESPACE` is kept short deliberately. The cached-spec path embeds it twice and
  zarr's atomic writes append a ~40-char `.partial` suffix; the full distribution
  name pushed that past Windows' 260-char `MAX_PATH`.

`notes` is not an alternative home for any of this: pynwb fields are write-once,
so a version string recorded there cannot be appended to.

# Relationship to processors

Each processor's `nwbize()` (see
[processor-abstraction.md](processor-abstraction.md) and
[continuous-and-event-streams.md](continuous-and-event-streams.md)) is
responsible for its own NWB structure — sites table, `behavior` processing
module `TimeSeries`, or per-event `DynamicTable` acquisitions. `NwbSession`
owns only the file skeleton and the write; it does not know processor
internals.
