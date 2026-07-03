# Testing

The suite has two tiers with very different cost profiles. Keeping them
separate is deliberate: the default `uv run pytest` must stay fast and
network-free, while heavy end-to-end validation is opt-in.

- [unit-tests.md](unit-tests.md) — Fast, data-free tests that replicate the pandas alignment logic in isolation. Run by default.
- [integration-tests.md](integration-tests.md) — End-to-end parsing against real datasets downloaded from a public S3 bucket. Gated behind the `integration` marker.

## The default gate

`pyproject.toml` configures pytest to **exclude** integration tests by
default:

```toml
[tool.pytest.ini_options]
addopts = "--strict-markers --tb=short --cov=src --cov-report=term-missing --cov-fail-under=0 -m 'not integration'"
markers = ["integration: integration tests that download data from S3 (run with `-m integration`)"]
testpaths = ["tests"]
```

So:

- `uv run pytest` → unit tests only (fast, no network).
- `uv run pytest -m integration` → integration tests only.

`--strict-markers` means an unregistered marker is an error — register new
markers in `pyproject.toml`. Coverage is measured (`--cov=src`) but not
enforced (`--cov-fail-under=0`).
