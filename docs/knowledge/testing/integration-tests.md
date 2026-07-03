---
type: Test Suite
title: Integration tests — end-to-end parsing against real S3 datasets
description: Marker-gated pytest suite that downloads real sessions from public S3, parses them, and asserts scalar invariants declared in a validated YAML manifest.
resource: tests/integration/
tags: [testing, integration, s3, manifest, pydantic, caching]
timestamp: 2026-07-03T00:00:00Z
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
| `conftest.py` | S3 download + ETag caching; a fixture that patches `NwbSession` to use local metadata JSON (no DocDB). |
| `test_datasets.py` | One parametrized test per manifest entry; parses and asserts invariants. |

# The test

`test_trials_table` is parametrized over `_manifest.datasets` (test id =
entry `id`). For each dataset it: resolves the cached path, reads
`tasklogic_input.json` to pick the loader version (normalizing `< 0.4.0`
sessions to `0.4.0`), builds the version-correct processor via
`get_trial_table_processor`, computes the sites DataFrame, and asserts the
declared invariants. `entry.xfail` marks known-broken datasets
`pytest.xfail(strict=True)` — an unexpected pass forces removal of the marker.
Every failure message includes the entry's `rationale` to speed triage.

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
| `raise_on_error` | no (default true) | Turn parser warnings into hard errors. |
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
