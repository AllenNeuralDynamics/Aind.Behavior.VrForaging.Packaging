# API Reference

Auto-generated reference for every public function and class in the package.

## Modules

| Module | Description |
|--------|-------------|
| [`pipeline.session`](session.md) | Per-session pipeline factory and `process_session` |
| [`pipeline.batch`](batch.md) | Multi-session export: `process_sessions`, `aggregate`, and the `vr-foraging-packaging` CLI |
| [`processors`](processors.md) | `AbstractProcessor` base class and all processor implementations |
| [`nwb`](nwb.md) | `NwbSession` — build and write NWB-Zarr files |

## Quick navigation

### pipeline.session

- [`create_processors`](session.md#aind_behavior_vr_foraging_packaging.pipeline.session.create_processors) — return the ordered processor list for a dataset
- [`resolve_site_table_processor`](session.md#aind_behavior_vr_foraging_packaging.pipeline.session.resolve_site_table_processor) — return the correct site-table processor
- [`resolve_position_velocity_processor`](session.md#aind_behavior_vr_foraging_packaging.pipeline.session.resolve_position_velocity_processor) — return the correct position/velocity processor
- [`process_session`](session.md#aind_behavior_vr_foraging_packaging.pipeline.session.process_session) — run all processors and write parquets

### pipeline.batch

- [`process_sessions`](batch.md#aind_behavior_vr_foraging_packaging.pipeline.batch.process_sessions) — Phase 1: per-session parquets
- [`aggregate`](batch.md#aind_behavior_vr_foraging_packaging.pipeline.batch.aggregate) — Phase 2: flat cross-session parquets
- [`AGGREGATED_TABLES`](batch.md#aind_behavior_vr_foraging_packaging.pipeline.batch.AGGREGATED_TABLES) — the fixed set of tables Phase 2 flattens

### pipeline.cli

- [`Cli`](cli.md#aind_behavior_vr_foraging_packaging.pipeline.cli.Cli) — root parser
- [`SessionCommand`](cli.md#aind_behavior_vr_foraging_packaging.pipeline.cli.SessionCommand) — `session`
- [`BatchCommand`](cli.md#aind_behavior_vr_foraging_packaging.pipeline.cli.BatchCommand) — `batch`
- [`AggregateCommand`](cli.md#aind_behavior_vr_foraging_packaging.pipeline.cli.AggregateCommand) — `aggregate`

### processors

- [`AbstractProcessor`](processors.md#aind_behavior_vr_foraging_packaging._base.AbstractProcessor) — base class for all processors
- [`SessionMetadataProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.SessionMetadataProcessor)
- [`SiteTableProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.SiteTableProcessor)
- [`PositionAndVelocityProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.PositionAndVelocityProcessor)
- [`LicksProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.LicksProcessor)
- [`SniffingProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.SniffingProcessor)
- [`SoftwareEventsProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.SoftwareEventsProcessor)
- [`EventsProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.EventsProcessor)

### nwb

- [`NwbSession`](nwb.md#aind_behavior_vr_foraging_packaging.nwb_file.NwbSession)
