---
type: Reference
title: Data contract, Harp streams, and the three versions
description: How the library reads sessions via contraqctor/aind-behavior-vr-foraging, and the three semver versions it tracks and dispatches on.
resource: src/aind_behavior_vr_foraging_packaging/_base.py
tags: [architecture, data-contract, contraqctor, harp, semver, versioning]
timestamp: 2026-07-03T00:00:00Z
---

Processors never touch raw files directly. They read through a **data
contract** and reason about **three distinct versions**.

# The data contract

A session is loaded as a `contraqctor.contract.Dataset`, obtained from
`aind_behavior_vr_foraging.data_contract.dataset(path[, version=...])`. The
dataset lazily exposes a tree of streams accessed with `.at(...)` / `[...]`
and materialized with `.load()`:

```
Dataset
└── "Behavior"
    ├── "SoftwareEvents"  → ActiveSite, ActivePatch, Block, GiveReward,
    │                        PatchStateAtReward, WaitRewardOutcome, PatchState, ...
    ├── "HarpBehavior"    → PwmStart (choice tone), OutputSet (water)
    ├── "HarpOlfactometer"→ EndValveState (odor)
    ├── "HarpTreadmill"   → SensorData (encoder), BrakeCurrentSetPoint (friction)
    ├── "HarpLickometer"  → LickState
    ├── "HarpSniffDetector"→ RawVoltage
    ├── "OperationControl"→ IsStopped
    └── "InputSchemas"    → Rig (calibration & configuration)
```

Harp streams carry a `MessageType` column (`WRITE` / `EVENT`); processors
filter on it. Software-event payloads live in a `data` column (dicts, often
`pd.json_normalize`d). All streams are indexed by **harp time in seconds**.

# The three versions

Tracked on [`AbstractProcessor`](processor-abstraction.md) and used for
dispatch and provenance:

| Name | Property / source | What it means |
|------|-------------------|---------------|
| **Packaging version** | `aind_behavior_vr_foraging_packaging.__version__` / `__semver__` | This library's version. |
| **Parser / data-contract version** | `parser_version` ← `aind_behavior_vr_foraging.__semver__` | Version of the schema library used to interpret the session. |
| **Dataset version** | `dataset_version` ← the session's `tasklogic_input.json` | The schema version the data was actually recorded with. |

A mismatch between `dataset_version` and `parser_version` is logged as a
warning (`TrialTableProcessor.__init__`), not an error.

## PEP 440 → SemVer

`__init__.py::pep440_to_semver` converts the package's PEP 440 version to a
SemVer-compatible string so `semver.Version.parse` accepts it (e.g.
`1.2.3rc2 → 1.2.3-rc2`, `1.2.3.post1 → 1.2.3+post1`). All version comparisons
in the codebase go through `semver`.

## Legacy cutoff

`pipeline._LEGACY_VERSION_CUTOFF = 0.6.0`. Datasets below it use the
`Legacy*` processors. Concretely, `< 0.6.0` datasets differ in odor
specification format, block-stream availability, `PatchStateAtReward`
presence, and `IsStopped`/velocity streams. The integration suite covers a
`0.3.0` legacy dataset explicitly (see
[testing/integration-tests.md](../testing/integration-tests.md)); test code
also normalizes `schema_version` `< 0.4.0` sessions onto the `0.4.0` loader.

# Why this matters

When a new schema version ships, the decision tree is: does existing parsing
still hold? If yes, bump nothing. If no, either branch inside a processor on
`self.dataset_version`, or (for a clean break) add a `Legacy*` variant and
move the cutoff. Keep the cutoff constant in one place.
