# Example: query from S3 (Polars)

Query the export directly from S3 using Polars' lazy parquet scanner.
Predicate pushdown and column pruning keep network I/O minimal.

Run with uv:

```bash
uv run --with polars docs/examples/query_export_s3_polars.py
```

---

<!-- The snippet below is an mkdocs-include-markdown directive, not real Python.
     ruff's markdown formatter would otherwise mangle `--8<--` into `--8 < --`;
     keep the fmt:off/on pair below it verbatim (extra text breaks ruff's match). -->
<!-- fmt: off -->
```python
--8<-- "docs/examples/query_export_s3_polars.py"
```
<!-- fmt: on -->
