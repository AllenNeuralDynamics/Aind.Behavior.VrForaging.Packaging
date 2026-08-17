# Change Log

Chronological history of changes to this knowledge bundle, newest first.
Add an entry here whenever you add, remove, or materially revise a concept.

## 2026-08-16 (one session, two output formats)

* **Architecture**: `_write_session_nwb` moved out of `export_pipeline` and into
  `process_session` as a `write_nwb: bool = False` option, joined by
  `write_parquet: bool = True`. The two formats are now independent switches
  over the same computed frames — every processor runs either way, so the
  returned dict does not depend on them, and both off writes nothing and creates
  no directory. The NWB step needed exactly what the parquet step needed (a
  loaded dataset and a processor list), so a second fan-out one layer up bought
  nothing but drift; `export_pipeline._process_one_session` now forwards the flag
  and owns only what is genuinely multi-session. See
  [session-pipeline.md](architecture/session-pipeline.md).
* **Architecture**: `process_session` no longer takes a `log_prefix`. It derives
  `[{session_id}]` from the dataset itself, so batch runs are grep-able by
  session without a caller threading a label down.
* **Architecture**: `process_session`'s `output_dir` became optional, accepting
  a `str` or `Path` and defaulting to the current working directory, so
  `process_session(ds)` is a complete call.
* **Architecture**: the session-root derivation moved from
  `SessionMetadataProcessor._session_root` to a shared `_base.session_root`,
  since `process_session` needs the same directory to name the NWB store that
  the processor uses for `session_id`. One derivation means the store's name and
  the table's join key cannot disagree.

## 2026-08-16 (documentation sweep)

An audit against the source found the bundle had drifted on every rename and
policy change below. Corrected across
[export-pipeline.md](architecture/export-pipeline.md),
[session-pipeline.md](architecture/session-pipeline.md),
[processor-abstraction.md](architecture/processor-abstraction.md),
[nwb-packaging.md](architecture/nwb-packaging.md),
[architecture/index.md](architecture/index.md), [overview.md](overview.md),
[error-policy.md](conventions/error-policy.md),
[tooling-and-style.md](conventions/tooling-and-style.md),
[unit-tests.md](testing/unit-tests.md),
[integration-tests.md](testing/integration-tests.md), plus `README.md` and the
user-facing pages under `docs/guides/`, `docs/api/` and `docs/getting-started.md`:

* `run_session` → `process_session`; `get_*_processor` → `resolve_*_processor`.
* The `session_path` argument, gone from `create_processors` and
  `process_session`, was still documented as required for `session.parquet`.
* **`raise_on_error` no longer exists anywhere in the codebase.** Several pages
  still documented it as a live orchestration flag, including a signature and a
  "two flags, deliberately separate" table. `strict_parsing` is now the only
  flag; [error-policy.md](conventions/error-policy.md) says so explicitly so the
  next reader who greps for the old name gets an answer.
* **Nothing is isolated any more.** The claim that "a processor that raises is
  logged while the rest of the session continues" and that `export_pipeline`
  holds "the package's only legitimate broad catches" was false in both
  directions — that module now contains no `except` at all. The one remaining
  tolerance mechanism, `process_session`'s opt-in `on_error` callback, was
  undocumented; it now is.
* `SessionMetadata` gained version *columns*, so the claim that the
  experiment-level export "carries no provenance at all" was wrong:
  `session.parquet` is precisely where per-session provenance survives Phase 2.
* `cached_frame` was undocumented; added to
  [processor-abstraction.md](architecture/processor-abstraction.md), and the
  "expect Phase 1 to take roughly twice as long with `write_nwb=True`" estimate
  it invalidated was corrected.
* Dead paths: `scripts/example_parquet_pipeline.py` and the top-level
  `examples/` directory were removed in `fe30e8e`; scripts now live in
  `docs/examples/`.

## 2026-08-16 (strict_parsing rename)

* **Conventions**: `raise_on_error` renamed to `strict_parsing` on the
  processor layer. The old name promised general exception gating and did the
  opposite. A separate orchestration-level `raise_on_error` was briefly added to
  `export_pipeline` and then removed once the pipeline settled on propagating
  every failure, which left it with one caller and no meaningful `False` case.
  Only `--strict-parsing` is exposed on the CLI. See
  [error-policy.md](conventions/error-policy.md).
* **Processors**: Added the opt-in `cached_frame` decorator on `_compute`, used
  by the five processors whose `nwbize()` re-enters `compute()` and therefore
  built every frame twice under `--write-nwb`. Returns a copy per call, so the
  no-shared-state guarantee documented on `nwbize` still holds.
* **Processors**: `SessionMetadataProcessor` lost its `session_path` argument
  and its `session_output.json` fallback, and is now unconditionally included
  by `create_processors`. `session_id` is the session directory's name;
  `subject` and `date` come from the contraqctor Session stream. The stream's
  own `session_name` is ignored — it is `null` on pre-0.6 datasets (verified on
  0.3.0 and 0.4.0) and, where populated, formatted differently from the
  directory (`815103_2025-11-05T225221Z` vs `behavior_815103_2025-11-05_22-52-21`),
  which split the `session_id` join key between `session.parquet` and every
  aggregated table. See [session-pipeline.md](architecture/session-pipeline.md).
* **Architecture**: `DatasetProcessorError` moved from `processing/_site_table.py`
  to `_base.py`, next to the `strict_parsing` flag that governs it. It is still
  re-exported from `processing/__init__.py`.

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
