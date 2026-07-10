---
type: Component
title: Continuous and event stream processors
description: Position/velocity, licks, sniffing, software-events, and events processors — the non-trial outputs and their NWB representations.
resource: src/aind_behavior_vr_foraging_packaging/processing/
tags: [architecture, processor, position, velocity, licks, sniffing, software-events, events]
timestamp: 2026-07-08T00:00:00Z
---

Beyond the [trial table](trial-table.md), five processors produce continuous
or event-level outputs. All subclass [`AbstractProcessor`](processor-abstraction.md)
and follow the same `_compute`/`nwbize` contract.

# PositionAndVelocityProcessor

`output_name = "position_velocity"`. Computes `position` (cm) and `velocity`
(cm/s) from the treadmill encoder, indexed by harp time.

- Reads `Behavior/HarpTreadmill/SensorData` (`Encoder`) and the rig calibration
  (`harp_treadmill.calibration`: `wheel_diameter`, `pulses_per_revolution`,
  `invert_direction`). Missing calibration raises `KeyError` with a note.
- `sampling_rate_hz` (default 250 Hz via the pipeline; `None` = native): when
  set, the series is resampled with `resample(...).mean()` and the index is
  converted back to harp seconds.
- `compute_position_and_velocity_from_treadmill(dataset, rig_config)` is a
  `@staticmethod` reused by the trial table to derive velocity.
- `nwbize()` adds a `Position`/`SpatialSeries` and a velocity `TimeSeries` to
  the `behavior` processing module.

# LicksProcessor

`output_name = "licks"`. Produces a boolean `is_lick_onset` series indexed by
harp time.

- Reads `Behavior/HarpLickometer/LickState` (`Channel0`), keeps distinct
  state transitions (alternating onset/offset starting on the first onset),
  mirroring `contraqctor.qc.harp.lickety_split`.
- `refractory_period_s` (default 0.01): onsets closer than this to the prior
  onset are treated as spurious double-detections and removed with their
  paired offset.
- `nwbize()` adds a `licks` `TimeSeries` to the `behavior` module.

# SniffingProcessor

`output_name = "sniffing"`. Produces a filtered breathing signal (`voltage`,
V) indexed by harp time; the sampling rate is stored in
`df.attrs["sampling_rate_hz"]`.

- Reads `Behavior/HarpSniffDetector/RawVoltage`, resamples onto a uniform grid
  (default: median sample rate, or `resampling_frequency_hz`), then applies a
  0.2–20 Hz Butterworth band-pass (`scipy.signal.butter` + `filtfilt`),
  mirroring `contraqctor.qc.harp.sniff_detector`.
- `nwbize()` adds a `sniffing` `TimeSeries` to the `behavior` module.

# SoftwareEventsProcessor

`output_name = "software_events"`. Collects **all** `Behavior/SoftwareEvents`
streams into a single tall table: columns `event_name` (str) and `data`
(JSON-serialized payload), indexed by `timestamp` (harp seconds).

- Payloads are polymorphic; parse back with `df["data"].apply(json.loads)`,
  or flatten one type with `pd.json_normalize`.
- Streams with errors are skipped (or raise if `raise_on_error`).
- `nwbize()` differs from `compute()`: it writes **one `DynamicTable`
  acquisition per event type** (preserving the original stream structure)
  rather than the single tall table, using
  `acquisition.helper.clean_dataframe_for_nwb` to coerce NWB-safe dtypes.

# EventsProcessor

`output_name = "events"`. Same tall shape as `SoftwareEventsProcessor`
(`event_name`, `data`, indexed by `timestamp`), but for events that are
**derived** from one or more underlying streams rather than a straight
passthrough of a single raw stream.

- Sources are registered in the `_EVENT_SOURCES` class-level list as
  `(event_name, callable)` pairs; each takes `dataset` and returns a
  `data`-column `DataFrame` indexed by timestamp. `_compute()` iterates the
  list, tags each source's rows with its `event_name`, and concatenates.
  Adding a new derived event is a self-contained addition: write one function,
  append one tuple.
- A source that raises is skipped and logged (or raises, if
  `raise_on_error`) — one broken source cannot take down the others.
- Current source: `ManualWaterDelivery` — `ForceGiveReward` (forced/manual
  reward) events, re-timestamped to their recovered hardware valve-open time by
  `_helper.parse_manual_water_delivery`. `GiveReward` deliveries claim their
  nearest valve open first; each `ForceGiveReward` is then matched to its
  nearest remaining one via `_helper.nearest_positions`.
  `_helper.parse_manual_water_delivery` is the **single source of truth** for
  forced rewards: `TrialTableProcessor.Site.has_forced_rewards` (see
  [trial-table.md](trial-table.md)) derives its per-site boolean by
  `slice_by_index`-ing these same de-conflicted hardware times, so the trial
  table and the events table can never disagree.
- `nwbize()` writes the tall `compute()` frame into an ndx-events `EventsTable`
  (named `"events"`) on the `NdxEventsNWBFile`: the index becomes the required
  `timestamp` column, and `event_name`/`data` become columns (`data`
  JSON-serialized). No table is added when there are no derived events. This is
  the NWB home for the exact forced-reward times, so the trial table needs only
  a boolean and stays free of ragged columns.

# Legacy variants

`LegacyPositionAndVelocityProcessor` and `LegacyTrialTableProcessor` handle
datasets with schema version `< 0.6.0` (different odor-specification format,
block-stream fallback, optional `PatchStateAtReward`, absent
`IsStopped`/velocity). The [pipeline](pipeline.md) selects them automatically;
licks, sniffing, software events, and events have no legacy variant.
