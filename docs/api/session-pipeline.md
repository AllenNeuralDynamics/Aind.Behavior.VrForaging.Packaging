# session_pipeline

Per-session pipeline factory. Selects the correct processor set for a dataset
version and provides helpers to run processors and write parquet outputs.

Version dispatch is automatic: datasets with schema version `< 0.6.0` receive
legacy processor variants; all others use the current implementations.

---

::: aind_behavior_vr_foraging_packaging.session_pipeline
