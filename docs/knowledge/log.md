# Change Log

Chronological history of changes to this knowledge bundle, newest first.
Add an entry here whenever you add, remove, or materially revise a concept.

## 2026-08-14 (error-policy audit)

* **Conventions**: Added [error-policy.md](conventions/error-policy.md) — a new
  `Convention` concept splitting failures into known anomaly / expected absence /
  general failure, documenting the `except Exception` + `raise_on_error`
  anti-pattern that an audit found in six processor sites (all now fixed), when
  raising unconditionally is correct, why isolation belongs to the driver, and
  two unfixed legacy hard-raises under **Known gaps**. Listed it in
  [conventions/index.md](conventions/index.md).
* **Architecture**: Corrected [continuous-and-event-streams.md](architecture/continuous-and-event-streams.md)
  — the claim that a failing `EventsProcessor` source "is skipped and logged …
  one broken source cannot take down the others" no longer held; sources now
  propagate and own their own expected-absence handling. Also tightened the
  `SoftwareEventsProcessor` bullet to separate `has_error` (flag-governed) from
  malformed streams (propagate).
* **Architecture**: Tightened the `raise_on_error` row in
  [processor-abstraction.md](architecture/processor-abstraction.md), noted in
  [export-pipeline.md](architecture/export-pipeline.md) that its two broad
  catches are the package's only legitimate ones, and added a
  legacy-reachability note to [site-table.md](architecture/site-table.md)
  (`LegacySiteTableProcessor` inherits `_compute`, so the flag *is* honoured
  pre-0.6.0 — but the two `IsStopped` branches are unreachable there).
* **Conventions**: Replaced the three-line error-policy summary in
  [tooling-and-style.md](conventions/tooling-and-style.md) with a pointer to the
  new concept.
* **Testing**: Expanded [unit-tests.md](testing/unit-tests.md) beyond its
  original site-table-only scope — added the `# Pinning the error policy`
  section (both test shapes, parametrized over the flag) and refreshed its
  `description`/`resource`/`timestamp`.

## 2026-08-09 (NWB export feature)

* **Architecture**: Updated [export-pipeline.md](architecture/export-pipeline.md) —
  added `write_nwb` parameter to the `process_sessions` signature, documented
  the new `# NWB output` section (collocated path, failure isolation, stale-store
  handling, runtime cost), updated the CLI example to show `--write-nwb`, and
  noted that all boolean flags are now bare (`cli_implicit_flags=True`).
* **Architecture**: Updated [nwb-packaging.md](architecture/nwb-packaging.md) —
  noted that `NwbSession` is now called from two contexts: directly and via
  the export pipeline.

## 2026-08-09

* **Conventions**: Removed the `[db]` extra from
  [tooling-and-style.md](conventions/tooling-and-style.md) and added an
  "Examples: PEP 723 inline scripts" section. Query backends (`duckdb`,
  `polars`) are no longer distribution dependencies at all — each script in
  `examples/` declares its own via an inline `# /// script` block. `boto3`
  moved into the `dev` group because `tests/integration/conftest.py` imports it
  at collection time. Supersedes the `[db]` entry further down this section.
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
