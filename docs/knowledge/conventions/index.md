# Conventions

The rules a contribution must respect to pass CI and keep the codebase
coherent. None of this is derivable from a single file — it's spread across
`pyproject.toml`, the CI workflow, and the README.

- [tooling-and-style.md](tooling-and-style.md) — uv, ruff, ty, codespell, pytest: the exact commands CI runs and the local equivalents.
- [ci-cd-and-release.md](ci-cd-and-release.md) — the GitHub Actions pipeline, the label-gated integration job, and the release/version-bump mechanics.

## The one-line summary

Use **uv** for everything. Before pushing, the local equivalent of CI is:

```bash
uv run ruff format --check
uv run ruff check
uv run codespell --check-filenames
uv run ty check
uv run pytest
uv build
```

If all six pass, the `tests` CI job will pass.
