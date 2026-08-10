# export_pipeline

Multi-session export pipeline. Runs in two phases:

- **Phase 1** — [`process_sessions`][aind_behavior_vr_foraging_packaging.export_pipeline.process_sessions]:
  iterate raw session directories → per-session parquets (optionally also NWB-Zarr).
- **Phase 2** — [`aggregate`][aind_behavior_vr_foraging_packaging.export_pipeline.aggregate]:
  read per-session parquets → flat cross-session parquets.

---

::: aind_behavior_vr_foraging_packaging.export_pipeline
    options:
      members:
        - AggregationRule
        - Aggregator
        - DEFAULT_AGGREGATOR
        - process_sessions
        - aggregate
