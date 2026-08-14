---
type: Convention
title: Error policy — known anomalies vs. general failures
description: What raise_on_error does and does not cover; why processors must never gate a bare `except Exception` on it, and where failure isolation actually belongs.
resource: src/aind_behavior_vr_foraging_packaging/_base.py
tags: [conventions, error-handling, raise_on_error, processor, robustness]
timestamp: 2026-08-14T00:00:00Z
---

Raw behavioural sessions are irregular: streams appear and disappear across
schema versions, rigs change wiring mid-project, and experimenters intervene
during acquisition. A parser that stopped at the first oddity would process
almost nothing, so `raise_on_error` exists to let one flag choose between
*strict* and *best-effort* parsing.

The distinction that makes it work — and the one that is easy to lose — is
**which** failures it covers.

# Schema

Three categories, three different behaviours. Only the first consults the flag.

| Category | Meaning | Behaviour |
|---|---|---|
| **Known anomaly** | A condition a processor explicitly checks for and can name, where a degraded-but-meaningful output still exists. | Governed by `raise_on_error`: raise `DatasetProcessorError` when `True`, else `logger.warning` + documented fallback. |
| **Expected absence** | A stream this schema version simply does not declare, or a file that is not there. | Narrow `except (KeyError, FileNotFoundError)` → documented fallback. Not the flag's business; it is not an anomaly at all. |
| **General failure** | Anything else: a typo, API drift, a corrupt file, a violated internal assumption. | Propagates. Always. |

The canonical shape for a known anomaly:

```python
if <specific condition detected>:
    msg = "<what was violated>"
    if self.raise_on_error:
        raise DatasetProcessorError(msg)
    logger.warning("%s; <what is used instead>.", msg)
```

# The anti-pattern

```python
# WRONG — do not write this in a processor.
try:
    df = stream.data
    frames.append(transform(df))
except Exception as exc:
    if self._raise_on_error:
        raise
    logger.debug("Could not load %s: %s", stream.name, exc)
```

This reads as careful error handling and is the opposite. Because
`raise_on_error=False` is the default everywhere (the CLI, `run_session`,
`process_sessions`), the effective behaviour is: **swallow every exception and
carry on**. A genuine bug — a renamed pandas method, a corrupt payload, a typo
in the transform — is absorbed exactly like a legacy dataset quirk. The
processor emits a silently short or empty table, `compute()` returns normally,
the pipeline writes the parquet, and the run reports success.

The failure is invisible twice over: once because the exception never
surfaces, and again because these handlers were logging at `debug`, below the
`INFO` default. Six such sites were removed in August 2026 (see
[log.md](../log.md)); the audit that found them is the reason this concept file
exists.

Catch only the exception types that signal an *expected* condition, and let
everything else propagate.

# When raising unconditionally is right

Some failures leave nothing meaningful to emit. Those should raise regardless
of the flag, because there is no degraded output to fall back to:

- Missing treadmill calibration (`wheel_diameter`, `pulses_per_revolution`) in
  `_extract_legacy_treadmill_calibration` — without it there is no position to
  compute. See [continuous-and-event-streams.md](../architecture/continuous-and-event-streams.md).
- A processor constructed against a dataset version it does not support
  (`LegacySiteTableProcessor` on `>= 0.6.0`).

"Degrade to what?" is the test. If the answer is "an all-`NaN` column that
downstream code can filter", the flag applies. If it is "nothing", raise.

# Where isolation belongs

`raise_on_error` is not an isolation mechanism, and processors should not try
to be one. Keeping one failure from taking down a batch is the **driver's**
job:

- `export_pipeline._process_one_session` catches per-processor, so one bad
  processor does not lose the session's other tables.
- `process_sessions` catches per-session, so one bad session does not abort
  the run.

Both log at `warning`/`exception` with `exc_info=True`, so nothing disappears.
These are the only places in the package where a broad `except Exception` is
correct — a supervisor cannot know what its children raise. See
[export-pipeline.md](../architecture/export-pipeline.md).

The consequence of routing general failures there rather than swallowing them
in-processor: a processor that hits a real bug now loses its *whole* table for
that session instead of writing a partial one. That is the intended trade —
a missing table is visible, a quietly truncated one is not.

# Known gaps

Unfixed as of 2026-08-14, both in the pre-0.6.0 path
([legacy processors](../architecture/data-contract-and-versioning.md)):

- **`_legacy_site_table._process_odor_concentration`** raises `TypeError`
  unconditionally when `odor_specification["index"]` is not an `int`, even
  though the `odor_specification is None` branch immediately above degrades to
  zero concentrations. A session merely missing that key loses its entire
  sites table. Two edges on the check itself: `isinstance(np.int64(1), int)` is
  `False` (a numpy int spuriously raises) and `isinstance(True, int)` is `True`
  (a bool indexes `concentration[1]`).
- **`_legacy_site_table._parse_patch_state_at_reward`** combines the three
  split `PatchReward*` streams assuming they are equal-length and co-indexed.
  The assumption is stated in a comment but never checked; a violation raises
  `ValueError: Length of values (N) does not match length of index (M)`, which
  names none of the three streams. This sits in the fallback path that exists
  precisely because legacy data is irregular.

# Verifying the convention holds

```bash
# Should return nothing in processing/ or acquisition/ —
# a hit is either a new violation or a deliberate, commented exception.
rg -n "except Exception" src/aind_behavior_vr_foraging_packaging/processing \
                         src/aind_behavior_vr_foraging_packaging/acquisition

# Broad catches in the drivers are expected; confirm they still log with exc_info.
rg -n -A3 "except Exception" src/aind_behavior_vr_foraging_packaging/export_pipeline.py
```

Unit tests pin the distinction per processor: a general exception must
propagate under **both** flag values, while the `has_error` path stays
flag-governed. See [unit-tests.md](../testing/unit-tests.md).
