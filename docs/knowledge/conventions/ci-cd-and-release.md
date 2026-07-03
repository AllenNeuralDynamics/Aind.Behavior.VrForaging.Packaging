---
type: Convention
title: CI/CD and release mechanics
description: The GitHub Actions workflow — test matrix, label-gated integration job, and the release-tag-driven version bump.
resource: .github/workflows/aind-behavior-vr-foraging-packaging.yml
tags: [conventions, ci, cd, github-actions, release, versioning]
timestamp: 2026-07-03T00:00:00Z
---

CI is a single workflow:
`.github/workflows/aind-behavior-vr-foraging-packaging.yml`. It has three
active jobs (plus two disabled ones).

# Job: `tests` (always)

Runs on every PR and on push to `main`/`dev*`/`release*`. Matrix of
`{ubuntu, windows, macos} × {3.11, 3.12, 3.13}`, `fail-fast: false`. Steps:
`uv sync` → `ruff format --check` → `ruff check` → `codespell` → `ty check`
→ `pytest` (unit only) → `uv build`. This is the gate every change must pass;
the local equivalent is in [index.md](index.md).

# Job: `integration-tests` (conditional)

Runs `uv run pytest -m integration` on ubuntu, only when:

- `workflow_dispatch` (manual), or
- push to `main`, or
- a PR carries the `run-integration` label, or
- a release is published.

It restores/saves `tests/integration/.cache` keyed on
`hashFiles('tests/integration/datasets.yml')`, so the cache invalidates when
the manifest changes. See
[testing/integration-tests.md](../testing/integration-tests.md).

# Job: `prepare-release` (on published release)

`needs: [tests, integration-tests]` — a release therefore cannot ship unless
both the full matrix and the integration suite pass. It:

1. Extracts the version from the release tag (`vX.Y.Z` → `X.Y.Z`).
2. Validates and sets it with `uv version`.
3. Builds and uploads the wheels artifact.
4. Commits `Set version X.Y.Z [skip ci]` and force-pushes the tag back to
   `main`, using a `RELEASE_PAT` admin token to bypass the branch ruleset.

# Disabled jobs

`publish-to-pypi` and `build-docs` (mkdocs → GitHub Pages) are present but
commented out. If docs publishing is re-enabled, this OKF bundle under
`docs/knowledge/` is a natural source to wire in.

# Practical contributor flow

1. Branch, implement, keep the six local checks green
   ([tooling-and-style.md](tooling-and-style.md)).
2. If the change touches parsing correctness, add/adjust an integration
   dataset and add the `run-integration` label to your PR so CI exercises it.
3. Merge to `main`; releases are cut by publishing a GitHub release tagged
   `vX.Y.Z`.
