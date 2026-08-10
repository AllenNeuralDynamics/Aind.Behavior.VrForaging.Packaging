# Example: query from S3 (Polars)

Query the export directly from S3 using Polars' lazy parquet scanner.
Predicate pushdown and column pruning keep network I/O minimal.

Run with uv:

```bash
uv run --with polars docs/examples/query_export_s3_polars.py
```

---

```python
--8<-- "docs/examples/query_export_s3_polars.py"
```
