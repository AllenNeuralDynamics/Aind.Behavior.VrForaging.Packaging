"""Historical schema migrations and corpus test harness."""

from .harness import (
    DocumentValidationFailure,
    HarmonizedRow,
    HarmonizedSchemas,
    HarnessFailure,
    HarnessResult,
    SchemaHarmonizationError,
    current_model_harmonizer,
    run_parquet_harness,
    schema_harmonizer,
)
from .migrations import migrate_documents
from .table import (
    MIGRATED_SCHEMA_COLUMNS,
    SOURCE_SCHEMA_COLUMNS,
    SchemaMigrationMode,
    append_migrated_schema_columns,
)

__all__ = [
    "MIGRATED_SCHEMA_COLUMNS",
    "SOURCE_SCHEMA_COLUMNS",
    "DocumentValidationFailure",
    "HarmonizedRow",
    "HarmonizedSchemas",
    "HarnessFailure",
    "HarnessResult",
    "SchemaHarmonizationError",
    "SchemaMigrationMode",
    "append_migrated_schema_columns",
    "current_model_harmonizer",
    "migrate_documents",
    "run_parquet_harness",
    "schema_harmonizer",
]
