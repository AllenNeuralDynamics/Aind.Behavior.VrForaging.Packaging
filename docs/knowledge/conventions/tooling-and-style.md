---
type: Convention
title: Tooling and code style
description: The uv-managed toolchain (ruff, ty, codespell, pytest), the runtime/dev dependency split, the PEP 723 convention for examples, and the style rules enforced in CI.
resource: pyproject.toml
tags: [conventions, uv, ruff, ty, codespell, pytest, style, dependencies, pep723, examples]
timestamp: 2026-08-09T00:00:00Z
---

# Package management: uv

`uv` is the only supported package manager. The lockfile (`uv.lock`) is
authoritative; do not hand-edit it. Common commands:

```bash
uv sync                 # install deps + dev group into .venv
uv run <cmd>            # run a command in the environment
uv build                # build the wheel/sdist
uv version <x.y.z>      # set the project version (used by release)
```

Dev dependencies live in the `dev` dependency group (default group): `ruff`,
`pytest`, `pytest-cov`, `codespell`, `ty`, and `boto3`. `boto3` is there because
`tests/integration/conftest.py` imports it at module scope — conftest files are
imported at *collection* time, so deselecting integration tests with
`-m 'not integration'` does not make it optional.

Runtime deps are pinned in `[project.dependencies]` (notably
`aind-behavior-vr-foraging[data] >= 1.2.1`, `aind-nwb-utils`, `pynwb>=4.1`,
`hdmf-zarr`, `pandas`, `pyarrow`, `numpy>=2`, `scipy`, `pydantic`,
`pydantic-settings`). There are **no** optional-dependency extras: query
backends (`duckdb`, `polars`) are needed only to *read* an export, never to
produce one, so they are declared per-script with PEP 723 inline metadata
instead of in the distribution — see "Examples" below.

`pynwb>=4.1` is a hard floor, not a preference: `EventsTable` was merged into
core pynwb at that version, replacing the former `ndx-events` extension.

# Examples: PEP 723 inline scripts

Everything in `docs/examples/` is a self-contained script carrying its own
dependency block, so it runs without a project install:

```python
# /// script
# dependencies = ["polars==1.43.2"]
# requires-python = ">=3.11"
# ///
```

```bash
uv run docs/examples/query_export_s3_polars.py    # uv resolves the block per-run
```

This is what keeps query backends out of the published distribution. Two backends
are demonstrated over the same export layout — DuckDB (`query_export.py`,
`query_export_s3.py`) and Polars (`query_export_s3_polars.py`) — pinned
independently of the package's own dependency set.

# Linting & formatting: ruff

Configured in `pyproject.toml`:

- `line-length = 120`, `target-version = "py311"`.
- Lint rule sets: `Q` (quotes), `RUF100`, `C90` (mccabe, `max-complexity = 14`),
  `I` (isort).
- Docstring convention: **Google** (`pydocstyle` convention `google`).

CI runs both `ruff format --check` and `ruff check`; run `uv run ruff format`
to fix formatting before committing.

# Type checking: ty

CI runs `uv run ty check` (Astral's type checker). Keep annotations accurate;
new public functions should be fully typed. Python target is 3.11+ (CI matrix:
3.11, 3.12, 3.13 on Ubuntu, Windows, macOS). `[tool.ty.src]` excludes
`examples/**`, `scripts/**` and `docs/**` — those are illustrative snippets, so
keep type-checked code out of them.

# Spelling: codespell

`uv run codespell --check-filenames`. Config skips `.git`, `*.pdf`, `*.svg`,
`uv.lock`; `ignore-words-list = "nd,setuptools"`.

# Tests: pytest

See [testing/index.md](../testing/index.md) for the full harness. Style rules:

- **pytest-style only** — no `unittest.TestCase`. Files `test_*.py`, classes
  `Test*`, functions `test_*`.
- `--strict-markers`: register any new marker in `[tool.pytest.ini_options]`.
- Add tests when adding features (README expectation).

# Style habits observed in the code

- Processors are private modules (`_name.py`) re-exported from the package
  `__init__`. Follow this when adding one.
- Google-style docstrings with explicit units in field/param descriptions
  (e.g. "(unit: cm/s)").
- Error policy is explicit and threaded via `strict_parsing`, which covers
  **named data anomalies only** — never a bare `except Exception`. Catch narrowly,
  let general failures propagate, and raise `DatasetProcessorError` for hard
  parsing failures. The rules and the anti-pattern are in
  [error-policy.md](error-policy.md).
