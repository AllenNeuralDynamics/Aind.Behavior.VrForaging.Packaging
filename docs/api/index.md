# API Reference

Auto-generated reference for every public function and class in the package.

## Modules

| Module | Description |
|--------|-------------|
| [`session_pipeline`](session-pipeline.md) | Per-session pipeline factory and `run_session` |
| [`export_pipeline`](export-pipeline.md) | Multi-session export: `process_sessions` and `aggregate` |
| [`processors`](processors.md) | `AbstractProcessor` base class and all processor implementations |
| [`nwb`](nwb.md) | `NwbSession` — build and write NWB-Zarr files |

## Quick navigation

### session_pipeline

- [`create_processors`](session-pipeline.md#aind_behavior_vr_foraging_packaging.session_pipeline.create_processors) — return the ordered processor list for a dataset
- [`get_site_table_processor`](session-pipeline.md#aind_behavior_vr_foraging_packaging.session_pipeline.get_site_table_processor) — return the correct site-table processor
- [`get_position_velocity_processor`](session-pipeline.md#aind_behavior_vr_foraging_packaging.session_pipeline.get_position_velocity_processor) — return the correct position/velocity processor
- [`run_session`](session-pipeline.md#aind_behavior_vr_foraging_packaging.session_pipeline.run_session) — run all processors and write parquets

### export_pipeline

- [`process_sessions`](export-pipeline.md#aind_behavior_vr_foraging_packaging.export_pipeline.process_sessions) — Phase 1: per-session parquets
- [`aggregate`](export-pipeline.md#aind_behavior_vr_foraging_packaging.export_pipeline.aggregate) — Phase 2: flat cross-session parquets
- [`AggregationRule`](export-pipeline.md#aind_behavior_vr_foraging_packaging.export_pipeline.AggregationRule)
- [`Aggregator`](export-pipeline.md#aind_behavior_vr_foraging_packaging.export_pipeline.Aggregator)
- [`DEFAULT_AGGREGATOR`](export-pipeline.md#aind_behavior_vr_foraging_packaging.export_pipeline.DEFAULT_AGGREGATOR)

### processors

- [`AbstractProcessor`](processors.md#aind_behavior_vr_foraging_packaging._base.AbstractProcessor) — base class for all processors
- [`SiteTableProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.SiteTableProcessor)
- [`PositionAndVelocityProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.PositionAndVelocityProcessor)
- [`LicksProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.LicksProcessor)
- [`SniffingProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.SniffingProcessor)
- [`SoftwareEventsProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.SoftwareEventsProcessor)
- [`EventsProcessor`](processors.md#aind_behavior_vr_foraging_packaging.processing.EventsProcessor)

### nwb

- [`NwbSession`](nwb.md#aind_behavior_vr_foraging_packaging.nwb_file.NwbSession)
