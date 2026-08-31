---
type: Component
title: AbstractProcessor — the processor contract
description: The abstract base class every processor implements; defines compute()/_compute(), nwbize(), write_parquet(), output_name, provenance stamping, and the opt-in cached_frame decorator.
resource: src/aind_behavior_vr_foraging_packaging/_base.py
tags: [architecture, processor, base-class, contract, provenance, caching, parquet]
timestamp: 2026-08-30T00:00:00Z
---

Every unit of parsing logic is a subclass of `AbstractProcessor`
(`src/aind_behavior_vr_foraging_packaging/_base.py`). A processor wraps one
loaded dataset and knows how to turn it into exactly one tabular output and,
optionally, one NWB representation.

# Schema

The contract a subclass must satisfy and may extend:

| Member | Kind | Responsibility |
|--------|------|----------------|
| `_compute(self) -> pd.DataFrame` | **abstract** | The real work. Return the output DataFrame. Never call directly from outside. |
| `compute(self) -> pd.DataFrame` | concrete | Calls `_compute`, then stamps provenance into `df.attrs`. This is the public entry point. |
| `nwbize(self, nwb_file) -> nwb_file` | concrete (no-op default) | Write this processor's data into an NWB file. Override where an NWB representation exists. |
| `write_parquet(self, output_dir, filename=None)` | concrete | Calls `compute()` internally and writes `output_dir/(filename or f"{output_name}.parquet")`, promoting `df.attrs` into parquet schema metadata. Override wholesale to customize the arrow table before it's written — `SessionMetadataProcessor` is the one processor that does, tagging its `Json[...]` fields with Parquet's native `JSON` logical type (see [session.md](session.md)). |
| `__output_name__: ClassVar[str \| None]` | class attr | Canonical parquet filename stem (e.g. `"sites"`). |
| `output_name` | property | `__output_name__` if set, else snake_case of the class name. |
| `dataset` | property | The loaded contraqctor Dataset. |
| `provenance` | `cached_property` | A `PackagingProvenance` carrying all three versions; cached so `compute()` and any version checks share one instance (see [versioning](data-contract-and-versioning.md)). |
| `strict_parsing` / `with_strict_parsing(...)` | error policy | Governs **known** data anomalies only — those a processor names explicitly and can degrade past. `True` raises `DatasetProcessorError`; `False` (default) logs a warning and falls back. It does *not* gate general exceptions, which always propagate. See [error-policy.md](../conventions/error-policy.md). |

Construction is uniform: `Processor(dataset, *, strict_parsing=False)`.
Subclasses add their own keyword-only options (e.g. `sampling_rate_hz`,
`refractory_period_s`, `resampling_frequency_hz`).

# Provenance stamping

`compute()` is the reason `_compute()` exists separately. After computing, it
sets (via `setdefault`, so a processor that already set a key wins) four keys
in `df.attrs` — the three fields of `self.provenance.model_dump()` plus one of
its own:

- `packaging_version` — version of this package.
- `data_contract_version` — version of `aind-behavior-vr-foraging` (the schema).
- `dataset_version` — the version recorded in the session's `tasklogic_input.json`.
- `processor` — the processor's class name.

Only the last is defined here; the other three come from `_provenance.py`, so
adding a provenance field never means editing this class.

`process_session` later promotes `df.attrs` to first-class parquet metadata, so
provenance survives a round-trip to disk and is readable from DuckDB, Polars,
R arrow, Spark, etc. See [session.md](session.md).

# `cached_frame` — opt-in memoization

`compute()`, `nwbize()` and `write_parquet()` share no state, and both
`nwbize()` and the default `write_parquet()` re-enter `compute()` independently
of whatever the caller already computed. `pipeline.session.process_session`
already calls `compute()` once for the frame it returns, so `write_parquet()`
re-entering it means every processor risks a second (or third, under
`--write-nwb`) full `_compute()` per session. `cached_frame` is a decorator
applied to `_compute` to remove the rebuild:

```python
from .._base import AbstractProcessor, cached_frame


class LicksProcessor(AbstractProcessor):
    @cached_frame
    def _compute(self) -> pd.DataFrame: ...
```

Still opt-in per processor rather than built into `AbstractProcessor` — but
since `write_parquet()` re-entering `compute()` now applies to *every*
processor's default parquet-writing path (not only `nwbize()` under
`--write-nwb`), all seven currently decorate `_compute` with it, including
`SessionMetadataProcessor` and `SoftwareEventsProcessor`, which used to be the
two holdouts (`SoftwareEventsProcessor` builds its NWB tables straight from the
raw streams, bypassing `compute()`, and `SessionMetadataProcessor` had no
`nwbize` at all — neither gained anything until `write_parquet()` started
re-entering `compute()` too).

Two properties keep it from weakening the contract above:

- **Every call returns a copy**, so the no-shared-state guarantee still holds
  exactly and callers can mutate what they get back. Copying a frame is far
  cheaper than re-parsing the streams.
- **Failures are not cached.** An exception leaves the cache empty and the next
  call retries.

The cache lives on the instance, and `create_processors` builds a fresh
processor per session, so it dies with the session. There is no cross-session
staleness and nothing to invalidate.

# Examples

Adding a new processor is intentionally small:

```python
from ._base import AbstractProcessor
import pandas as pd


class RewardRateProcessor(AbstractProcessor):
    __output_name__ = "reward_rate"

    def _compute(self) -> pd.DataFrame:
        # ...read streams via self.dataset, build a DataFrame...
        return df

    def nwbize(self, nwb_file):  # optional
        # ...add a TimeSeries / table...
        return nwb_file
```

Then register it in [session_pipeline.create_processors](session.md) and export it
from `processing/__init__.py`.

# Design notes

- `compute()`, `nwbize()` and `write_parquet()` are intentionally independent
  of one another and of whatever the caller already computed. `nwbize()` and
  the default `write_parquet()` may call `compute()` internally, but none of
  the three depends on another having run. `cached_frame` preserves this,
  since it hands back a copy on every call.
- Keeping one output per processor is what makes the fan-out in
  [session.md](session.md) trivial and makes each output independently
  testable.
