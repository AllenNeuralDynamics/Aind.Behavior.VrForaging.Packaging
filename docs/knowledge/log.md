# Change Log

Chronological history of changes to this knowledge bundle, newest first.
Add an entry here whenever you add, remove, or materially revise a concept.

## 2026-08-09

* **Architecture**: Renamed `pipeline.md` → [session-pipeline.md](architecture/session-pipeline.md)
  to track the `pipeline.py` → `session_pipeline.py` rename and to disambiguate
  it from the new export layer. Corrected the `create_processors` /
  `run_session` signatures (no `sampling_rate_hz`; added `session_path` and the
  `session` output) and updated all inbound cross-links.
* **Architecture**: Added [export-pipeline.md](architecture/export-pipeline.md)
  covering `export_pipeline.py` (`process_sessions`, `aggregate`, `Aggregator`)
  and the `aind-vr-export` CLI. This code shipped to main undocumented.
* **Architecture**: Rewrote the three-versions section of
  [data-contract-and-versioning.md](architecture/data-contract-and-versioning.md)
  around `PackagingProvenance`, now the single definition of the provenance key
  names. The former `parser_version` property no longer exists.
* **Architecture**: Documented (in both
  [export-pipeline.md](architecture/export-pipeline.md) and
  [data-contract-and-versioning.md](architecture/data-contract-and-versioning.md))
  that provenance does **not** survive Phase 2 aggregation — verified against
  pandas 3.0 behaviour, not assumed.
* **Architecture**: Fixed [nwb-packaging.md](architecture/nwb-packaging.md) —
  its frontmatter claimed provenance went to `lab_meta_data` while the body
  correctly documented `was_generated_by`, a leftover from an abandoned
  approach.
* **Architecture**: Corrected the package layout tree in
  [architecture/index.md](architecture/index.md) — it still listed the removed
  `pipeline.py` and described `cli.py` as a stub.
* **Conventions**: Updated the dependency lists in
  [tooling-and-style.md](conventions/tooling-and-style.md) — the `[db]` extra,
  the `pynwb>=4.1` floor (`EventsTable` moved into core pynwb, replacing
  `ndx-events`), the dropped `aind-data-schema` / `aind-data-access-api`, the
  `ty` source excludes, and the codespell ignore list.
* **Testing**: Documented `test_full_pipeline`'s NWB coverage and
  `test_experiment_export.py` in
  [integration-tests.md](testing/integration-tests.md).

## 2026-07-03

* **Initialization**: Created the OKF knowledge bundle. Added the root
  [index](index.md) and [overview](overview.md).
* **Architecture**: Documented the [processor abstraction](architecture/processor-abstraction.md),
  [session pipeline](architecture/session-pipeline.md), [site table](architecture/site-table.md),
  [continuous & event streams](architecture/continuous-and-event-streams.md),
  [NWB packaging](architecture/nwb-packaging.md), and
  [data contract & versioning](architecture/data-contract-and-versioning.md).
* **Testing**: Documented the [unit](testing/unit-tests.md) and
  [integration](testing/integration-tests.md) test tiers.
* **Conventions**: Documented [tooling & style](conventions/tooling-and-style.md)
  and [CI/CD & release](conventions/ci-cd-and-release.md).
