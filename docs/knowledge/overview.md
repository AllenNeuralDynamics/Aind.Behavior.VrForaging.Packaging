---
type: System Overview
title: aind-behavior-vr-foraging-packaging — Overview
description: A parser/packager that turns raw AIND VR-foraging behavioral sessions into tabular (parquet) and NWB outputs.
resource: https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging
tags: [overview, architecture, vr-foraging, nwb, parquet]
timestamp: 2026-07-03T00:00:00Z
---

`aind-behavior-vr-foraging-packaging` reads a raw VR-foraging **session**
(Harp device streams + software events + AIND metadata) and produces
analysis-ready artifacts: a set of **parquet** tables and an **NWB** file. It
is the "packaging" layer that sits downstream of acquisition and upstream of
analysis.

# Dataflow

The whole system is a fan-out of independent **processors** over one loaded
dataset:

```
raw session dir
      │
      ▼
contraqctor Dataset  ◄── aind_behavior_vr_foraging.data_contract.dataset(path)
      │
      ├─► create_processors(dataset)         # version-dispatched processor list
      │        │
      │        ▼
      │   [TrialTable, PositionAndVelocity, Licks, Sniffing, SoftwareEvents]
      │        │
      │        ├─► proc.compute()  ──► pandas DataFrame (+ provenance in df.attrs)
      │        │        └─► run_session(...) writes one <output_name>.parquet each
      │        │
      │        └─► proc.nwbize(nwb) ─► writes into an NdxEventsNWBFile
      │
      └─► NwbSession(path).run(*processors) ─► NWB (Zarr) file
```

Two output targets share the same processors:

- **Parquet** — [pipeline.run_session](architecture/pipeline.md) calls
  `compute()` on each processor and writes a parquet per processor, stamping
  provenance metadata into the parquet schema.
- **NWB** — [NwbSession](architecture/nwb-packaging.md) builds an
  `NdxEventsNWBFile` from AIND metadata, then calls each processor's
  `nwbize()` to populate it.

# Core vocabulary

The behavioral task is organized hierarchically. Understanding these terms is
prerequisite to reading [trial-table.md](architecture/trial-table.md):

- **Site** — the atomic unit; a stretch of the virtual corridor the animal
  runs through (e.g. a `RewardSite` or an inter-site gap). One row of the
  trial table = one site.
- **Patch** — a contiguous group of sites that share a patch type/label
  (odor identity, reward statistics).
- **Block** — a group of patches sharing a task regime.

Sites, patches, and blocks each get several index columns (global, within
parent, and "by type") — see the [trial table](architecture/trial-table.md).

# Key dependencies

- `contraqctor` — the data-contract layer that lazily loads Harp device
  streams and software events from a session directory.
- `aind-behavior-vr-foraging` — defines the behavioral **data contract**
  (schema) and provides `data_contract.dataset(...)`; its version is the
  "parser version".
- `aind-data-schema` / `aind-nwb-utils` / `pynwb` / `hdmf-zarr` / `ndx-events`
  — metadata models and the NWB/Zarr writing stack.
- `semver` — every version comparison (legacy dispatch, provenance) is semver.

# Where to go next

- Code structure and contracts → [architecture/index.md](architecture/index.md)
- How correctness is guarded → [testing/index.md](testing/index.md)
- How to contribute without breaking CI → [conventions/index.md](conventions/index.md)
