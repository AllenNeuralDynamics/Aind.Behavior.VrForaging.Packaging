---
type: Test Suite
title: Unit tests — data-free alignment logic
description: Fast pytest tests that replicate the trial-table merge/index logic on synthetic frames, catching regressions without any real dataset.
resource: tests/processing/test_trial_table.py
tags: [testing, unit, pytest, trial-table, regression]
timestamp: 2026-07-03T00:00:00Z
---

The unit tier lives under `tests/` (currently `tests/processing/`) and runs on
every `uv run pytest`. It contains **no dataset dependency** — tests build
small synthetic `sites`/`patches`/`blocks` DataFrames and exercise the exact
pandas operations used by the production code.

# Approach

`tests/processing/test_trial_table.py` defines `_build_merged(...)`, which
**mirrors** the merge + `groupby().cumcount()` index precomputation from
`TrialTableProcessor.process_to_sites`. Tests then assert the resulting index
columns against hand-computed expectations for several session layouts:

- `TestMergeAssignment` — `merge_asof` assigns each site to the correct
  backward patch/block and preserves the time index.
- `TestSiteLevelIndices` / `TestPatchLevelIndices` — the global, within-parent,
  and "by type" index columns.
- `TestSingleBlockSinglePatch` — minimal edge case (all indices sequential / 0).
- `TestSimultaneousBlockAndPatchChange` — the **regression test** for the bug
  the vectorized rewrite fixed: when a block and patch boundary coincide, the
  old imperative counter produced `-1`. See
  [architecture/trial-table.md](../architecture/trial-table.md).
- `TestManyPatchTypesInBlock` — alternating patch types within one block.

# Conventions

- **pytest-style**, not `unittest.TestCase`. Use fixtures and plain `assert`.
  (Test classes are named `Test*` and functions `test_*` per
  `pyproject.toml`.)
- Tests that replicate production logic should keep the replicated code in
  lockstep with the source. If you change `process_to_sites`' merge/index
  sequence, update `_build_merged` too — otherwise the tests pass while
  testing stale logic.
- Prefer adding a focused synthetic scenario for each new invariant or bug.

# Running

```bash
uv run pytest                      # all unit tests
uv run pytest tests/processing     # a subset
uv run pytest -k SimultaneousBlock # by name
```
