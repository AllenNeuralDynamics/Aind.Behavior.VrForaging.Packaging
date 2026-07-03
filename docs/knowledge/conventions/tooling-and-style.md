---
type: Convention
title: Tooling and code style
description: The uv-managed toolchain (ruff, ty, codespell, pytest) and the style rules enforced in CI.
resource: pyproject.toml
tags: [conventions, uv, ruff, ty, codespell, pytest, style]
timestamp: 2026-07-03T00:00:00Z
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
`pytest`, `pytest-cov`, `codespell`, `ty`, `pyarrow`. Runtime deps are pinned
in `[project.dependencies]` (notably `aind-behavior-vr-foraging[data] >= 1`,
`aind-data-schema`, `pynwb`, `hdmf-zarr`, `ndx-events`, `pandas`, `numpy>=2`,
`scipy`, `semver`, `aind-data-access-api`).

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
3.11, 3.12, 3.13 on Ubuntu, Windows, macOS).

# Spelling: codespell

`uv run codespell --check-filenames`. Config skips `.git`, `*.pdf`, `*.svg`,
`uv.lock`; `ignore-words-list = "nd"`.

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
- Error policy is explicit and threaded via `raise_on_error`; prefer logging
  a warning + graceful fallback over silent failure, and raise
  `DatasetProcessorError` for hard parsing failures.
