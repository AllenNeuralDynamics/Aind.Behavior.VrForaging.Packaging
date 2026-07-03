---
type: Component
title: AbstractProcessor — the processor contract
description: The abstract base class every processor implements; defines compute()/_compute(), nwbize(), output_name, and provenance stamping.
resource: src/aind_behavior_vr_foraging_packaging/_base.py
tags: [architecture, processor, base-class, contract, provenance]
timestamp: 2026-07-03T00:00:00Z
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
| `__output_name__: ClassVar[str \| None]` | class attr | Canonical parquet filename stem (e.g. `"trials"`). |
| `output_name` | property | `__output_name__` if set, else snake_case of the class name. |
| `dataset` / `dataset_version` / `parser_version` | properties | The loaded dataset and the two semver versions (see [versioning](data-contract-and-versioning.md)). |
| `raise_on_error` / `with_raise_errors(...)` | error policy | When `True`, parsing anomalies raise; when `False` (default), they log and continue. |

Construction is uniform: `Processor(dataset, *, raise_on_error=False)`.
Subclasses add their own keyword-only options (e.g. `sampling_rate_hz`,
`refractory_period_s`, `resampling_frequency_hz`).

# Provenance stamping

`compute()` is the reason `_compute()` exists separately. After computing, it
sets (via `setdefault`, so a processor that already set a key wins) four keys
in `df.attrs`:

- `packaging_version` — version of this package.
- `data_contract_version` — version of `aind-behavior-vr-foraging` (the schema).
- `dataset_version` — the version recorded in the session's `tasklogic_input.json`.
- `processor` — the processor's class name.

`run_session` later promotes `df.attrs` to first-class parquet metadata, so
provenance survives a round-trip to disk and is readable from DuckDB, Polars,
R arrow, Spark, etc. See [pipeline.md](pipeline.md).

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

    def nwbize(self, nwb_file):          # optional
        # ...add a TimeSeries / table...
        return nwb_file
```

Then register it in [pipeline.create_processors](pipeline.md) and export it
from `processing/__init__.py`.

# Design notes

- `compute()` and `nwbize()` are intentionally independent — no shared state.
  `nwbize()` may call `compute()` internally, but neither depends on the other
  having run.
- Keeping one output per processor is what makes the fan-out in
  [pipeline.md](pipeline.md) trivial and makes each output independently
  testable.
