---
type: Convention
title: Error policy — known anomalies vs. general failures
description: What strict_parsing does and does not cover; why processors must never gate a bare `except Exception` on it, and why the pipeline propagates every failure rather than isolating it.
resource: src/aind_behavior_vr_foraging_packaging/_base.py
tags: [conventions, error-handling, strict_parsing, processor, robustness]
timestamp: 2026-08-16T00:00:00Z
---

Raw behavioural sessions are irregular: streams appear and disappear across
schema versions, rigs change wiring mid-project, and experimenters intervene
during acquisition. A parser that stopped at the first oddity would process
almost nothing, so `strict_parsing` exists to let one flag choose between
*strict* and *best-effort* parsing.

The distinction that makes it work — and the one that is easy to lose — is
**which** failures it covers.

# Schema

Three categories, three different behaviours. Only the first consults the flag.

| Category | Meaning | Behaviour |
|---|---|---|
| **Known anomaly** | A condition a processor explicitly checks for and can name, where a degraded-but-meaningful output still exists. | Governed by `strict_parsing`: raise `DatasetProcessorError` when `True`, else `logger.warning` + documented fallback. |
| **Expected absence** | A stream this schema version simply does not declare, or a file that is not there. | Narrow `except (KeyError, FileNotFoundError)` → documented fallback. Not the flag's business; it is not an anomaly at all. |
| **General failure** | Anything else: a typo, API drift, a corrupt file, a violated internal assumption. | Propagates. Always. |

The canonical shape for a known anomaly:

```python
if <specific condition detected>:
    msg = "<what was violated>"
    if self.strict_parsing:
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
    if self._strict_parsing:
        raise
    logger.debug("Could not load %s: %s", stream.name, exc)
```

This reads as careful error handling and is the opposite. Because
`strict_parsing=False` is the default everywhere (the CLI, `process_session`,
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
- A session whose root directory cannot be recovered from the Session stream's
  path in `SessionMetadataProcessor` — the directory name *is* the session's
  `session_id`, so there is no identity to degrade to.

"Degrade to what?" is the test. If the answer is "an all-`NaN` column that
downstream code can filter", the flag applies. If it is "nothing", raise.

# One flag, and only one

The processor flag was called `raise_on_error` until August 2026. The name
promised general exception gating and delivered the opposite of it, which is
precisely the confusion the table above exists to prevent, so it was renamed to
`strict_parsing`.

There is now exactly one flag in the package, and its scope is narrow:

| Flag | Layer | Question it answers |
|---|---|---|
| `strict_parsing` | `AbstractProcessor` and every processor; `create_processors`, `process_session`, `process_sessions`, and the CLI's `--strict-parsing` | Is a *known, anticipated* anomaly fatal, or do we degrade past it? |

A second, orchestration-level `raise_on_error` existed briefly on
`pipeline.batch` to choose between propagating and carrying on. It was removed
once the pipeline settled on propagating unconditionally (below), which left it
with a single caller and no meaningful `False` case. **It exists nowhere in the
codebase** — if you find it referenced, that reference is stale.

# Nothing is isolated

`strict_parsing` is not an isolation mechanism, and neither is anything else:
the pipeline no longer isolates failures at all. `pipeline/batch.py` contains
no `except` statement. A processor that raises aborts the session; a session
that raises aborts the batch, including under `max_workers > 1`, where
`fut.result()` re-raises.

The single deliberate exception is `pipeline.session.process_session`'s
optional `on_error` callback. It defaults to `None`, meaning propagate, and no
caller in the package passes anything else. It exists so an external driver can
opt into per-processor tolerance without the pipeline assuming tolerance is
wanted — the callback itself decides whether to skip that processor or re-raise.

The reasoning is the table above, applied one level up. Anything escaping
`compute()` is a *general failure* by definition — the processor already
handled everything it anticipated. A session that hit one is not a usable
partial result, so writing its remaining tables and reporting success would
publish data whose gaps are invisible to whoever reads it later. A loud abort is
recoverable; a silently short export is not.

The cost is real and accepted: one corrupt session in a thousand-session batch
stops the batch. Re-running with that session excluded is the intended remedy,
because it makes the exclusion explicit and recorded rather than buried in a
log line.

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

# Should return nothing at all: pipeline/batch.py isolates nothing.
rg -n "except" src/aind_behavior_vr_foraging_packaging/pipeline/batch.py

# Should return exactly one hit — process_session's opt-in `on_error` callback,
# which defaults to None (propagate) and has no in-package caller.
rg -n -B2 -A5 "except Exception" src/aind_behavior_vr_foraging_packaging/pipeline/session.py

# Should return nothing anywhere: the flag was removed in August 2026.
rg -n "raise_on_error" src/ tests/
```

Unit tests pin the distinction per processor: a general exception must
propagate under **both** flag values, while the `has_error` path stays
flag-governed. See [unit-tests.md](../testing/unit-tests.md).
