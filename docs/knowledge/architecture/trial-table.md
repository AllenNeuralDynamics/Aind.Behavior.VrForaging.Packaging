---
type: Component
title: TrialTableProcessor and the Site model
description: The core processor that reconstructs one row per site (trial) by aligning software events, hardware events, and continuous streams on the harp timeline.
resource: src/aind_behavior_vr_foraging_packaging/processing/_trial_table.py
tags: [architecture, processor, trial-table, site, alignment]
timestamp: 2026-07-03T00:00:00Z
---

`TrialTableProcessor` (`processing/_trial_table.py`, `output_name = "trials"`)
is the most complex processor and produces the primary scientific output: a
table with **one row per site**. Each row is validated through the
[`Site`](#schema) pydantic model in `models.py`.

# How it works

`process_to_sites() -> list[Site]` is the heart of it:

1. **Load the hierarchy.** `ActiveSite`, `ActivePatch`, and `Block` software
   events are loaded; each site is assigned to its most-recent patch and block
   via `pd.merge_asof(..., direction="backward")`.
2. **Precompute indices vectorized.** All site- and patch-level index columns
   are computed with `groupby(...).cumcount()` (global, within-parent, and
   "by type"). This vectorized approach replaced an imperative loop that had
   an off-by-one bug when a block and patch boundary coincided — see the
   regression tests in [testing/unit-tests.md](../testing/unit-tests.md).
3. **Slice per-site event windows.** For each site interval
   `[this_timestamp, next_timestamp)` the processor slices choice feedback,
   odor onset, water delivery, reward metadata, friction, `IsStopped`, and
   velocity using the `_helper` utilities, then derives the timing/outcome
   fields (choice time, reward onset, last stop, velocity at last stop, …).
4. **Build & validate** a `Site` for every interval **except the last** (the
   final site may be incomplete and is dropped).

`_compute()` wraps this into a DataFrame (`pd.DataFrame([s.model_dump() ...])`).
`nwbize()` adds each non-time column as a trial column and appends one NWB
trial per row (`start_time`/`stop_time` are NWB-reserved and handled specially).

# Key inputs (data streams)

Sourced through the [contraqctor dataset](data-contract-and-versioning.md):

- `Behavior/SoftwareEvents/{ActiveSite, ActivePatch, Block, GiveReward, PatchStateAtReward, WaitRewardOutcome}`
- `Behavior/HarpBehavior/{PwmStart (choice tone), OutputSet (water)}`
- `Behavior/HarpOlfactometer/EndValveState` (odor onset)
- `Behavior/HarpTreadmill/BrakeCurrentSetPoint` (friction) and encoder (velocity, via `PositionAndVelocityProcessor`)
- `Behavior/OperationControl/IsStopped` (stops before choice)
- `Behavior/InputSchemas/Rig` (rig configuration; olfactometer channel count)

# Alignment assumptions & error policy

The parser assumes each site's relevant events fall within its software-event
timestamp interval. Documented edge cases it handles:

- **Odor onset slightly before the site** (< 2 ms): a warning is logged and
  the site onset is used.
- **Choice with no in-window `IsStopped=True`**: falls back to a global search
  before the choice time (or raises if `raise_on_error`).
- **Reward metadata present but no hardware water delivery**: raises or logs
  depending on `raise_on_error`.

`DatasetProcessorError` is raised for hard failures (and for
`PatchStateAtReward` on datasets `< 0.6.0`, which is why those go through the
[legacy processor](data-contract-and-versioning.md)).

# Schema

The `Site` model (`models.py`) — one instance per row. Selected fields (all
times are harp/software seconds unless noted):

| Field | Type | Meaning |
|-------|------|---------|
| `start_time` / `stop_time` | float | Site interval bounds. |
| `start_position` / `length` | float (cm) | Position and length in the VR corridor. |
| `site_label` / `patch_label` | str | Site and patch type names. |
| `friction` | float (%) | Last-known brake/friction setpoint. |
| `odor_concentration` | list[float] (%) | Per-channel odor concentration. |
| `odor_onset_time` | float? | Odor valve onset; null if none. |
| `reward_onset_time` / `reward_amount` / `reward_probability` / `reward_available` | float? | Reward timing and patch-state-at-reward values. |
| `has_reward` / `has_choice` / `has_waited_reward_delay` | bool? | Outcome flags. |
| `choice_cue_time` | float? | Choice/stop tone time. |
| `reward_delay_duration` | float? | `reward_onset_time - choice_cue_time`. |
| `last_stop_time` / `last_stop_duration` / `velocity_at_last_stop` | float? | Last stop before choice, and speed there (cm/s). |
| `site_index`, `patch_index`, `block_index` | int | Global indices. |
| `site_index_in_patch`, `site_index_in_block`, `site_index_by_type`, `site_index_in_patch_by_type`, `site_index_in_block_by_type` | int | Site sub-indices. |
| `patch_index_by_type`, `patch_index_in_block`, `patch_index_in_block_by_type` | int | Patch sub-indices. |

See [overview.md](../overview.md#core-vocabulary) for site/patch/block.

# Examples

```python
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging_packaging.processing import TrialTableProcessor
import pandas as pd

ds = dataset("session_path")
sites = TrialTableProcessor(ds).process_to_sites()   # list[Site]
df = pd.DataFrame([s.model_dump() for s in sites])
```
