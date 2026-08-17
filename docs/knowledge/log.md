# Change Log

Chronological history of changes to this knowledge bundle, newest first.
Add an entry here whenever you add, remove, or materially revise a concept.

## 2026-08-17 (orchestration → server)

* **Naming**: the second distribution is now **`server`** throughout — directory,
  module, distribution and command:

  | was | now |
  |---|---|
  | `orchestration/` | `server/` |
  | `aind_behavior_vr_foraging_orchestration` | `aind_behavior_vr_foraging_server` |
  | `aind-behavior-vr-foraging-orchestration` | `aind-behavior-vr-foraging-server` |
  | `vr-foraging-orchestrator` | `vr-foraging-server` |
  | `architecture/orchestration.md` | [architecture/server.md](architecture/server.md) |
  | `diagrams/orchestration.drawio` | `diagrams/server.drawio` |

  Nothing about the design changed: the dependency still runs one way, server →
  packaging, still enforced by `tests/test_package_boundary.py`, and the layer still
  owns the ledger, discovery, stores, the worker and the dashboard.

  Note that the entries **below this one describe the same package under its old
  name**. Their identifiers and paths have been updated so links and code references
  still resolve — a change log that points at files nobody can open is worse than one
  whose prose is a little anachronistic — but the history itself is untouched.

## 2026-08-17 (work-volume lifetime, log uniformity, worker provenance)

* **Architecture**: documented and fixed what happens to a session's bytes on the
  work volume after publishing, in
  [server.md](architecture/server.md#what-lives-on-the-work-volume-and-for-how-long).
  Cleanup now runs on **entry** to `process_job` as well as in a `finally`. The entry
  half is the load-bearing one and the only correctness fix here: `job_id` is stable
  across attempts, so an attempt killed mid-write left partial output at exactly the
  path the retry uses, and since `publish` ships `out/` wholesale while the sidecar is
  rewritten, the orphan reached the output store inside a session the ledger recorded
  as a clean success. Nothing can `finally` its way out of SIGKILL; only entry-side
  cleanup survives one.

  Reclaiming a stranded directory now has exactly one owner per job state —
  `running` nobody, `pending`/`retrying` the next attempt, terminal states the new
  `Worker.sweep_work_dir` running beside `reap_expired_leases`. The ledger decides,
  never the filesystem: workers share the volume, and a directory whose name is not a
  job id it knows is reported and left alone.

* **Convention**: `jobs.log_uri` now means one kind of thing. Every job's log is
  published under `output.log_prefix` and the local copy deleted; it previously held a
  worker-local path on success and an output-store URI on failure, so nothing could
  follow the column without first guessing which it had.

* **Architecture**: closed the worker-provenance gap the previous entry's diagram
  recorded as open. `workers.worker_image` (plus the first additive column migration)
  records which image the worker itself ran, `docker/compose.yaml` pins both services
  by digest via `${VRF_IMAGE:?}`, and `doctor` refuses a campaign whose worker cannot
  say what it is unless `processor.allow_unpinned` relaxes both halves.

* **Config**: `worker.max_disk_bytes` → `worker.min_free_disk_bytes`, now actually
  enforced, and as a pre-claim guard rather than a mid-job check — a job claimed onto a
  full volume dies on ENOSPC and burns one of `max_attempts`. New
  `worker.keep_work_dir` / `work --keep-work` for reading a failed job's directory
  before it disappears.

## 2026-08-17 (server layer)

* **Architecture**: the repo is now a **`uv` workspace with two distributions**.
  `aind-behavior-vr-foraging-server` lives in `server/`, is never
  published (`Private :: Do Not Upload`, so an accidental `uv publish` fails rather
  than succeeds), and holds the SQLite job ledger, DocDB/local discovery, input
  staging and output stores, the `docker run` worker, and the dashboard. New concept:
  [server.md](architecture/server.md), with a diagram and a local-run
  guide.

  The dependency runs **one way** — server → packaging — and
  `tests/test_package_boundary.py` enforces it by walking the AST of every published
  module, including function bodies where a lazy import would hide. Two packages
  rather than an extra: an extra would put boto3 and DocDB in the published wheel's
  metadata and leave the import direction to convention.

* **Architecture**: the `output.metadata.json` sidecar belongs to the server
  package, not to `pipeline/`. `process_session` instead grew a generic
  `on_output(processor, frame, path)` hook beside the existing `on_error`, and knows
  nothing about sidecars, JSON, or containers. `SidecarRecorder` plugs into both.

  Recording is not tolerance: `SidecarRecorder.on_error` appends the failure and
  then re-raises, so a failing processor still propagates and the container still
  exits nonzero — the sidecar just names the culprit on the way past.

* **Architecture**: `aggregate()` takes an `include: Callable[[Path], bool]`
  predicate instead of reading sidecars itself. It cannot tell a complete session
  from one abandoned partway through — both are a directory of parquets — so a
  caller that can, says so. Server passes `session_completed_ok`.

* **Architecture**: the container runs `vr-foraging-server process`, and that
  is the image's `ENTRYPOINT`. `vr-foraging-packaging` is still on PATH inside the
  image for a human poking around, but nothing launches it.

* **Convention**: a session's identity is its **input directory's name**, and the
  server layer upholds that by mounting each session at its true name
  (`/mnt/{session_name}`, or `in/{session_name}` when staged). There is no
  `--session-name` override. Getting it wrong raises nothing — every table is
  stamped with the wrong session — so it is tested as an invariant.

* **Convention**: list-valued CLI flags must be **repeated per value**
  (`--exclude-processors a --exclude-processors b`). pydantic-settings gives a list
  field an `append` action; the space-separated form exits 2. Previously documented
  the wrong way round in `pipeline/cli.py`.

* **CI/CD**: the processor image is built and pushed to GHCR on **releases only**
  (plus manual dispatch), not on pushes to `main`. A worker config pins
  `processor.digest`, so every image reaching the registry is something an operator
  may run for a whole campaign; tying that to a release keeps the runnable digests
  equal to the released versions, and keeps `setuptools_scm` from stamping images
  with `.dev` versions nobody can reproduce. `latest` now follows release events
  rather than `is_default_branch`, which would otherwise never match again.

* **Testing**: the post-build image check moved out of the workflow into
  [`docker/smoke-test.sh`](../../docker/smoke-test.sh), so the same assertions can be
  pointed at a local build or a released digest without a runner.

## 2026-08-16 (scoped `clean`)

* **Architecture**: `process_sessions(clean=True)` no longer does
  `shutil.rmtree(output_dir)`. It removes only what a previous run of the same
  function wrote — `sessions/` and the aggregated `{table}.parquet` files.

  Found by running `batch --log-file <output-dir>/run.log`, which is the
  invocation the docs suggest: it crashed with `PermissionError [WinError 32]`
  because `clean` tried to delete the log file the handler had just opened. On
  POSIX it would not have crashed — the log would simply have disappeared
  mid-run while the handler wrote on to a deleted inode. Scoping the delete also
  means `--output-dir ~/data` no longer erases unrelated contents of `~/data`.
  Regression tests in `tests/pipeline/test_batch.py`.

## 2026-08-16 (pipeline package + subcommand CLI)

* **Architecture**: `session_pipeline.py` and `export_pipeline.py` moved into a
  `pipeline/` package as `session.py` and `batch.py`, joined by `cli.py`. Tests
  mirror it under `tests/pipeline/`, and the bundle pages were renamed to match
  (`session-pipeline.md` → [session.md](architecture/session.md),
  `export-pipeline.md` → [batch.md](architecture/batch.md), plus a new
  [cli.md](architecture/cli.md)).
* **Naming**: the many-sessions layer is **`batch`**, not `export`. `session` vs
  `export` did not name the actual distinction — both export; one does it for a
  single session and the other for many. `batch` says that, and unlike
  `sessions.py` it is not one letter away from `session.py` at an import site.
  "Export" survives only as the name of the *output* ("the export directory").
* **Architecture**: the CLI became **subcommands** under a single
  `vr-foraging-packaging` entry point, replacing `aind-vr-export`. One
  subcommand per public pipeline function — `session`, `batch`, `aggregate` —
  so the command surface and the module surface are the same shape.

  The old design expressed phase selection as negations: `--skip-processing` to
  re-aggregate, `--skip-aggregation` to process only. Two booleans encoded three
  intentions, `--skip-processing --skip-aggregation` did nothing at all, and
  exporting a *single* session was not expressible without pointing the tool at
  a parent directory. `--skip-processing` is gone: "aggregate only" is now its
  own verb.
* **Architecture**: subcommand options are layered to match what each one
  actually needs — `_Command` (I/O + logging) → `_ProcessingCommand` (processor
  filters, output formats) → `SessionCommand` / `BatchCommand`, with
  `AggregateCommand` skipping the middle layer entirely because it never
  constructs a processor. Offering `--strict-parsing` on `aggregate` would have
  been a lie.
* **Architecture**: `batch` also gained `--clean/--no-clean`, which was already
  a `process_sessions` parameter but had never been exposed.

## 2026-08-16 (aggregation config removed)

* **Architecture**: `Aggregator`, `AggregationRule`, `DEFAULT_AGGREGATOR`, the
  `cleanup` flag and the CLI's `--dataset-tables` were all deleted. They existed
  to parameterise something that never varied; what Phase 2 writes is a property
  of the schema, not a per-run decision. `aggregate(sessions_dir, output_dir)`
  now takes two positional arguments and no options, driven by
  `AGGREGATED_TABLES = ("session", "sites")`. The only control is whether Phase 2
  runs at all — `--skip-aggregation`. See
  [batch.md](architecture/batch.md).
* **Architecture**: `session` is aggregated through the same code path as every
  other table, replacing a bespoke concat block above the loop. It is listed
  first because a missing identity table aborts the rest — the export would have
  nothing to join on.
* **Architecture**: per-session parquets are **never deleted** now that `cleanup`
  is gone. They are what `--skip-processing` re-aggregation reads back, and the
  only copies carrying provenance in their parquet schema, so an export stays
  auditable at the row level by default. Phase 2 also writes through
  `_write_parquet` uniformly, which corrects the two-reasons explanation in
  *Provenance does not survive Phase 2* — only the `pd.concat` reason still
  applies.
* **Docs**: `--raise-on-error` was still listed in the CLI flag tables in
  `README.md` and `docs/getting-started.md`. The August audit grepped
  `raise_on_error` and missed the hyphenated form. Both tables now list
  `--strict-parsing`, `--write-nwb` and `--no-write-parquet` instead.

## 2026-08-16 (export pipeline derives from the session pipeline)

* **Architecture**: processor selection moved into the session layer as
  `filter_processors(processors, *, include, exclude)`, called by
  `create_processors`. `pipeline.batch` had been reimplementing it as a local
  `_keep` closure — a second copy of the "never filter `session`" rule with
  nothing keeping the two in step.
* **Architecture**: `process_session` now accepts a raw session **path** as well
  as a loaded `Dataset`, and takes `include`/`exclude`. With those two changes
  `pipeline.batch._process_one_session` had nothing left but
  `sessions_dir / raw_path.name` and two log lines duplicating what
  `process_session` already emits, so it was **deleted** and inlined into
  `process_sessions`. `pipeline.batch` no longer has a per-session function at
  all, and every per-session option on `process_sessions` is a pass-through
  documented once, in the session layer.
* **Architecture**: `write_parquet` reached `process_sessions` and the CLI. The
  CLI rejects `--no-write-parquet` unless `--skip-aggregation` is also set, since
  Phase 2 reads the per-session parquets back off disk and would otherwise
  produce an empty aggregate from a run that reported success.

## 2026-08-16 (one session, two output formats)

* **Architecture**: `_write_session_nwb` moved out of `pipeline.batch` and into
  `process_session` as a `write_nwb: bool = False` option, joined by
  `write_parquet: bool = True`. The two formats are now independent switches
  over the same computed frames — every processor runs either way, so the
  returned dict does not depend on them, and both off writes nothing and creates
  no directory. The NWB step needed exactly what the parquet step needed (a
  loaded dataset and a processor list), so a second fan-out one layer up bought
  nothing but drift. `pipeline.batch._process_one_session` was reduced to
  forwarding the flag at this point, and removed entirely in the entry above.
  See [session.md](architecture/session.md).
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
[batch.md](architecture/batch.md),
[session.md](architecture/session.md),
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
  still documented it as a live server flag, including a signature and a
  "two flags, deliberately separate" table. `strict_parsing` is now the only
  flag; [error-policy.md](conventions/error-policy.md) says so explicitly so the
  next reader who greps for the old name gets an answer.
* **Nothing is isolated any more.** The claim that "a processor that raises is
  logged while the rest of the session continues" and that `pipeline.batch`
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
  opposite. A separate server-level `raise_on_error` was briefly added to
  `pipeline.batch` and then removed once the pipeline settled on propagating
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
  aggregated table. See [session.md](architecture/session.md).
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
  [batch.md](architecture/batch.md) that its two broad
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

* **Architecture**: Updated [batch.md](architecture/batch.md) —
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
* **Architecture**: Renamed `pipeline.md` → [session.md](architecture/session.md)
  to track the `pipeline.py` → `pipeline/session.py` rename and to disambiguate
  it from the new export layer. Corrected the `create_processors` /
  `run_session` signatures (no `sampling_rate_hz`; added `session_path` and the
  `session` output) and updated all inbound cross-links.
* **Architecture**: Added [batch.md](architecture/batch.md)
  covering `pipeline/batch.py` (`process_sessions`, `aggregate`, `Aggregator`)
  and the `vr-foraging-packaging` CLI. This code shipped to main undocumented.
* **Architecture**: Rewrote the three-versions section of
  [data-contract-and-versioning.md](architecture/data-contract-and-versioning.md)
  around `PackagingProvenance`, now the single definition of the provenance key
  names. The former `parser_version` property no longer exists.
* **Architecture**: Documented (in both
  [batch.md](architecture/batch.md) and
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
  [session pipeline](architecture/session.md), [site table](architecture/site-table.md),
  [continuous & event streams](architecture/continuous-and-event-streams.md),
  [NWB packaging](architecture/nwb-packaging.md), and
  [data contract & versioning](architecture/data-contract-and-versioning.md).
* **Testing**: Documented the [unit](testing/unit-tests.md) and
  [integration](testing/integration-tests.md) test tiers.
* **Conventions**: Documented [tooling & style](conventions/tooling-and-style.md)
  and [CI/CD & release](conventions/ci-cd-and-release.md).
