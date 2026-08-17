---
type: Test Suite
title: Integration tests — end-to-end parsing against real S3 datasets
description: Marker-gated pytest suite that downloads real sessions from public S3, runs the parquet/NWB/export pipelines end-to-end, and asserts scalar invariants declared in a validated YAML manifest.
resource: tests/integration/
tags: [testing, integration, s3, manifest, pydantic, caching, nwb, export]
timestamp: 2026-08-16T00:00:00Z
---

The integration tier (`tests/integration/`) runs the parser end-to-end against
real datasets in the public `aind-open-data` S3 bucket. It is gated behind the
`integration` marker (`pytestmark = pytest.mark.integration`) so the default
suite is unaffected.

# Layout

| File | Role |
|------|------|
| `datasets.yml` | The **manifest**: one entry per dataset (see [schema](#schema)). |
| `model.py` | Pydantic models (`DatasetManifest`, `DatasetEntry`, `ExpectedInvariants`) with `extra="forbid"` so typos in the YAML fail loudly. |
| `conftest.py` | S3 download + ETag caching; the `all_cached_session_paths` fixture that hands the whole cache to the export test. |
| `test_datasets.py` | Two parametrized tests per manifest entry: site table, and the full parquet + NWB pipeline. |
| `test_experiment_export.py` | Four tests running the [export pipeline](../architecture/batch.md) across *all* cached sessions at once: `test_full_export_pipeline` (both phases, asserts the output tree and that `session_id` joins from `session.parquet` to `sites.parquet`), plus `test_skip_aggregation_writes_only_sessions`, `test_exclude_processor`, and `test_rerun_aggregation_only`. Skip when the cache is empty. |

# The tests

Both tests in `test_datasets.py` are parametrized over `_manifest.datasets`
(test id = entry `id`). `entry.xfail` marks known-broken datasets
`pytest.xfail(strict=True)` — an unexpected pass forces removal of the marker.
Every failure message includes the entry's `rationale` to speed triage.

`test_sites_table` resolves the cached path, builds the version-correct
processor via `resolve_site_table_processor` (honouring the entry's
`strict_parsing`), computes the sites DataFrame, and asserts the declared
invariants.

`test_full_pipeline` runs both output targets over **one** loaded dataset, so
the expensive site-table computation is not paid for twice. It calls
`process_session` for parquet, then `NwbSession.run(*create_processors(ds))` for
NWB, and checks:

- Identity fields (`session_id`, `identifier`, timezone-aware
  `session_start_time`, `subject.subject_id`) are populated. These come from the
  session's real metadata jsons via `create_base_nwb_file`, which is why this
  tier — not the synthetic unit fixtures — is what catches an upstream metadata
  key rename.
- `nwb.trials` has the same row count as the parquet `sites` table, and
  satisfies the same manifest invariants. The two are computed independently
  (`nwbize` calls `compute()` itself), so agreement is the signal that the
  outputs have not drifted apart.
- After `write_nwb_zarr` and a re-read, the invariants still hold and
  `was_generated_by` still contains every key `NwbSession.provenance` recorded.
  The check is a **superset** (`>=`), because `create_base_nwb_file` adds its own
  entry that is not ours to assert on. Re-reading from disk matters here:
  `was_generated_by` is write-once, so a missing entry cannot be corrected after
  the fact (see [nwb-packaging.md](../architecture/nwb-packaging.md)).
- Optionally `pynwb.validate`, when the entry sets `expected.nwb_validates: true`.

It deliberately uses `process_session`'s default `strict_parsing=False` rather
than the entry's own value: some sessions legitimately lack optional
SoftwareEvents streams (e.g. `ForceGiveReward`, `PatchRewardAmount`), and an
absent optional stream should not fail a smoke test. `test_sites_table` is the
one that honours `entry.strict_parsing`.

# Caching (why re-runs are cheap)

`conftest.py` downloads each dataset once into `tests/integration/.cache/`
(gitignored). A warm cache is validated with **1 HEAD request**: it compares
local total bytes and the ETag of a sentinel file (`data_description.json`)
stored in `.cache/_etags.json`. Only a mismatch triggers a re-list/re-download.
Video files (`**/*.mp4`, `**/*.avi`, `**/*.mkv`) are excluded by default, plus
any per-entry `exclude` globs.

# Schema

`datasets.yml` entries (validated by `model.py`; unknown keys rejected):

| Field | Required | Meaning |
|-------|----------|---------|
| `id` | yes | Stable unique handle; used as the pytest test id (kebab-case). |
| `uri` | yes | `s3://bucket/prefix/` (trailing slash); listed/downloaded recursively. |
| `rationale` | yes | Why this dataset is in the suite; printed on failure. |
| `exclude` | no | Glob patterns (case-insensitive) excluded from download. |
| `expected` | no | Scalar invariants: `n_sites`, `n_choices`, `n_rewards`, `n_blocks`, `n_patches`, `nwb_validates`. Omit → smoke test only (must not crash / be empty). |
| `strict_parsing` | no (default **true**) | Make *known* data anomalies fatal in `test_sites_table` (see [error-policy.md](../conventions/error-policy.md)). The two legacy entries set it `false`. |
| `xfail` / `xfail_reason` | no | Keep a known-broken dataset in the suite without blocking CI. |

# Adding a dataset

1. Add an entry to `tests/integration/datasets.yml` — `rationale` is required
   and should say what edge case or bug the dataset exercises.
2. Fill `expected` invariants where known (compute them once from a trusted
   run). Leave off for a smoke-only entry.
3. Run `uv run pytest -m integration` locally; the first run downloads (~100 MB
   per dataset) and caches.

# Running & CI

```bash
uv run pytest -m integration          # local; downloads on first run
gh pr edit <PR> --add-label run-integration   # opt a PR into CI integration
```

In CI the integration job runs on `workflow_dispatch`, on push to `main`, on
PRs labelled `run-integration`, and on published releases — a release cannot
ship without it passing. See
[conventions/ci-cd-and-release.md](../conventions/ci-cd-and-release.md).
