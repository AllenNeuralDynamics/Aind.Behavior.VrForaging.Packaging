---
type: Test Suite
title: Unit tests — data-free alignment logic
description: Fast pytest tests over synthetic frames and mocked streams — site-table merge/index logic, per-processor behaviour, and the error-policy contract — with no dataset dependency.
resource: tests/processing/
tags: [testing, unit, pytest, site-table, error-policy, regression]
timestamp: 2026-08-14T00:00:00Z
---

The unit tier lives under `tests/` (currently `tests/processing/`) and runs on
every `uv run pytest`. It contains **no dataset dependency** — tests build
small synthetic `sites`/`patches`/`blocks` DataFrames and exercise the exact
pandas operations used by the production code.

# Approach

`tests/processing/test_site_table.py` defines `_build_merged(...)`, which
**mirrors** the merge + `groupby().cumcount()` index precomputation from
`SiteTableProcessor.process_to_sites`. Tests then assert the resulting index
columns against hand-computed expectations for several session layouts:

- `TestMergeAssignment` — `merge_asof` assigns each site to the correct
  backward patch/block and preserves the time index.
- `TestSiteLevelIndices` / `TestPatchLevelIndices` — the global, within-parent,
  and "by type" index columns.
- `TestSingleBlockSinglePatch` — minimal edge case (all indices sequential / 0).
- `TestSimultaneousBlockAndPatchChange` — the **regression test** for the bug
  the vectorized rewrite fixed: when a block and patch boundary coincide, the
  old imperative counter produced `-1`. See
  [architecture/site-table.md](../architecture/site-table.md).
- `TestManyPatchTypesInBlock` — alternating patch types within one block.

Other processors are covered the same way, substituting `MagicMock` streams for
synthetic frames where the input is a contraqctor stream rather than a DataFrame
(`test_software_events.py`, `test_events.py`, `test_session_metadata.py`,
`test_licks.py`, `test_sniffing.py`, `test_legacy_site_table.py`).

# Pinning the error policy

The known-anomaly vs. general-failure split
([conventions/error-policy.md](../conventions/error-policy.md)) is only real if
tests enforce it, since the difference is invisible on well-formed data — every
one of these tests passes both before and after a regression *on real sessions*.
Two shapes, both parametrized over `raise_on_error` in `[False, True]`:

- **A general failure must propagate under both values.** A source that raises
  (`test_events.py::test_failing_source_propagates_regardless_of_flag`); a
  stream that loads but lacks its `data` column
  (`test_software_events.py::test_malformed_stream_propagates`).
- **A known anomaly must stay flag-governed.** `has_error` streams
  (`test_software_events.py::test_has_error_stream_is_governed_by_flag`) — raise
  when `True`, skip when `False`.

Fallbacks driven by *expected absence* get the same treatment: in
`test_session_metadata.py`, `KeyError` and `FileNotFoundError` fall back to
`session_output.json` while `RuntimeError` propagates **even when a valid JSON
file is present** — the fallback must not mask a real stream failure.

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
