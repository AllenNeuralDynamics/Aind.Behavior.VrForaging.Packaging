# Aind.Behavior.VrForaging.Nwb

![CI](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Nwb/actions/workflows/vr-foraging-nwb.yml/badge.svg)
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

## Getting Started

A quick example of how to use the TrialTableProcessor to extract site-level metadata from a dataset:

```python
from aind_behavior_vr_foraging.data_contract import dataset
import pandas as pd

from aind_behavior_vr_foraging_nwb.processing import (
    TrialTableProcessor,
)

ds = dataset("session_path")
ttp = TrialTableProcessor(ds)
sites = ttp.process_to_sites()
sites_df = pd.DataFrame([s.model_dump() for s in sites])
```

## Contributors

Contributions to this repository are welcome! However, please ensure that your code adheres to the recommended DevOps practices below:

### Linting

We use [ruff](https://docs.astral.sh/ruff/) as our primary linting tool.

### Testing

Attempt to add tests when new features are added.
To run the currently available tests, run `uv run pytest` from the root of the repository.

## Integration tests

Integration tests run the parser end-to-end against real datasets stored in a public S3 bucket. They are gated by a pytest marker so they don't run by default.

**Run locally:**

```bash
uv run pytest -m integration
```

The first run downloads datasets (~100 MB per dataset) to `tests/integration/.cache/`. Subsequent runs reuse the cache when the S3 ETag matches. The cache directory is gitignored.

**Trigger on a PR:**

Integration tests do not run on every PR. To run them for a specific PR, add the `run-integration` label via the GitHub UI (open the PR, click **Labels** in the right-hand sidebar, and select `run-integration`) or with:

```bash
gh pr edit <PR_NUMBER> --add-label run-integration
```

The integration job runs automatically on push to `main` and on `release: published`. A release cannot ship without the integration suite passing.

**Adding a dataset:**

Add an entry to `tests/integration/datasets.yml`. The manifest schema and full field documentation are in `tests/integration/model.py` (Pydantic model). The `rationale` field is required and is printed alongside any test failure to make triage fast.

### Lock files

We use [uv](https://docs.astral.sh/uv/) to manage our lock files and therefore encourage everyone to use uv as a package manager as well.