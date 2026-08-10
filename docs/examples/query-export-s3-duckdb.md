# Example: query from S3 (DuckDB)

Query the export directly from S3 using DuckDB's `httpfs` extension.
No local download needed — DuckDB streams only the row groups that match
your query.

Run with uv:

```bash
uv run docs/examples/query_export_s3.py
```

---

```python
--8<-- "docs/examples/query_export_s3.py"
```
