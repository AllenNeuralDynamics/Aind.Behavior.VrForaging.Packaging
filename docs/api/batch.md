# export_pipeline

Multi-session export pipeline, and the `vr-foraging-packaging` command that drives it.
Runs in two phases:

- **Phase 1** — [`process_sessions`][aind_behavior_vr_foraging_packaging.pipeline.batch.process_sessions]:
  iterate raw session directories → per-session parquets (optionally also NWB-Zarr).
- **Phase 2** — [`aggregate`][aind_behavior_vr_foraging_packaging.pipeline.batch.aggregate]:
  read per-session parquets → flat cross-session parquets.

The [`vr-foraging-packaging`](cli.md) CLI exposes each phase as a subcommand.

---

::: aind_behavior_vr_foraging_packaging.pipeline.batch
    options:
      members:
        - AGGREGATED_TABLES
        - process_sessions
        - aggregate
