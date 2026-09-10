# Example: query from S3 (DuckDB)

Query the export directly from S3 using DuckDB's `httpfs` extension.
No local download needed — DuckDB streams only the row groups that match
your query.

Run with uv:

```bash
uv run docs/examples/query_export_s3.py
```

---

<!-- The snippet below is an mkdocs-include-markdown directive, not real Python.
     ruff's markdown formatter would otherwise mangle `--8<--` into `--8 < --`;
     keep the fmt:off/on pair below it verbatim (extra text breaks ruff's match). -->
<!-- fmt: off -->
```python
--8<-- "docs/examples/query_export_s3.py"
```
<!-- fmt: on -->
