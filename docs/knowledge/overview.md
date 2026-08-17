---
type: System Overview
title: aind-behavior-vr-foraging-packaging — Overview
description: A parser/packager that turns raw AIND VR-foraging behavioral sessions into tabular (parquet) and NWB outputs.
resource: https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging
tags: [overview, architecture, vr-foraging, nwb, parquet, export]
timestamp: 2026-08-09T00:00:00Z
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
      │   [SessionMetadata, PositionAndVelocity, SiteTable,
      │    Licks, Sniffing, SoftwareEvents, Events]
      │        │
      │        ├─► proc.compute()  ──► pandas DataFrame (+ provenance in df.attrs)
      │        │        └─► process_session(...) writes one <output_name>.parquet each
      │        │
      │        └─► proc.nwbize(nwb) ─► writes into the NWBFile
      │
      └─► NwbSession(path).run(*processors) ─► NWB (Zarr) file
```

Two output targets share the same processors:

- **Parquet** — [pipeline.session.process_session](architecture/session.md)
  calls `compute()` on each processor and writes a parquet per processor,
  stamping provenance metadata into the parquet schema.
- **NWB** — [NwbSession](architecture/nwb-packaging.md) builds a base `NWBFile`
  via `aind_nwb_utils.utils.create_base_nwb_file`, then calls each processor's
  `nwbize()` to populate it.

Above the single session sits the
[export pipeline](architecture/batch.md): the `vr-foraging-packaging` CLI
runs the parquet path over a folder of sessions, then concatenates the chosen
tables into flat experiment-level files joinable on `session_id`.

# Core vocabulary

The behavioral task is organized hierarchically. Understanding these terms is
prerequisite to reading [site-table.md](architecture/site-table.md):

- **Site** — the atomic unit; a stretch of the virtual corridor the animal
  runs through (e.g. a `RewardSite` or an inter-site gap). One row of the
  sites table = one site.
- **Patch** — a contiguous group of sites that share a patch type/label
  (odor identity, reward statistics).
- **Block** — a group of patches sharing a task regime.

Sites, patches, and blocks each get several index columns (global, within
parent, and "by type") — see the [site table](architecture/site-table.md).

# Key dependencies

- `contraqctor` — the data-contract layer that lazily loads Harp device
  streams and software events from a session directory.
- `aind-behavior-vr-foraging` — defines the behavioral **data contract**
  (schema) and provides `data_contract.dataset(...)`; its version is the
  "parser version".
- `aind-nwb-utils` / `pynwb` / `hdmf-zarr` — base NWB file construction from the
  session's metadata jsons (`create_base_nwb_file`), and the NWB/Zarr writing
  stack. Events use the `EventsTable` merged into core `pynwb`.
- `semver` — every version comparison (legacy dispatch, provenance) is semver.

# Where to go next

- Code structure and contracts → [architecture/index.md](architecture/index.md)
- How correctness is guarded → [testing/index.md](testing/index.md)
- How to contribute without breaking CI → [conventions/index.md](conventions/index.md)
