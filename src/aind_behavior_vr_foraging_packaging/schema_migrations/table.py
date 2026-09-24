"""Apply schema migrations to Arrow session tables without replacing raw data."""

from enum import StrEnum

import pyarrow as pa

from .harness import schema_harmonizer

SOURCE_SCHEMA_COLUMNS = ("dataset_version", "session", "rig", "task_logic")
MIGRATED_SCHEMA_COLUMNS = (
    "session_migrated",
    "rig_migrated",
    "task_logic_migrated",
)


class SchemaMigrationMode(StrEnum):
    """How migrated schema columns are handled."""

    DISABLED = "disabled"
    FILL_MISSING = "fill-missing"
    FORCE = "force"


def append_migrated_schema_columns(
    table: pa.Table,
    *,
    mode: SchemaMigrationMode = SchemaMigrationMode.FILL_MISSING,
) -> pa.Table:
    """Return *table* with current-schema JSON appended where it is missing.

    Raw ``session``, ``rig``, and ``task_logic`` values are never replaced.
    Existing non-null migrated values are also preserved, including when a
    table contains a mixture of migrated and unmigrated rows. Only absent or
    null migrated cells are computed from that row's raw documents.

    Parameters
    ----------
    table:
        An Arrow session table containing ``dataset_version`` and the three raw
        schema columns.
    mode:
        ``FILL_MISSING`` preserves non-null migrated values and fills absent or
        null cells. ``FORCE`` recomputes every migrated cell. ``DISABLED``
        returns the input table unchanged.
    """
    if mode is SchemaMigrationMode.DISABLED:
        return table

    missing = [name for name in SOURCE_SCHEMA_COLUMNS if name not in table.column_names]
    if missing:
        raise ValueError(f"Session table cannot be schema-migrated; missing columns: {', '.join(missing)}")

    migrated_values = {
        name: table.column(name).to_pylist() if name in table.column_names else [None] * table.num_rows
        for name in MIGRATED_SCHEMA_COLUMNS
    }
    source_values = {name: table.column(name).to_pylist() for name in SOURCE_SCHEMA_COLUMNS}
    session_ids = (
        table.column("session_id").to_pylist() if "session_id" in table.column_names else [None] * table.num_rows
    )

    for row_index in range(table.num_rows):
        needed = [
            name
            for name in MIGRATED_SCHEMA_COLUMNS
            if mode is SchemaMigrationMode.FORCE or migrated_values[name][row_index] is None
        ]
        if not needed:
            continue

        source_version = source_values["dataset_version"][row_index]
        if source_version is None or not str(source_version).strip():
            identity = session_ids[row_index] or f"row {row_index}"
            raise ValueError(f"Could not migrate schemas for {identity!r}: dataset_version is missing")

        try:
            schemas = schema_harmonizer(
                source_version=str(source_version),
                session=source_values["session"][row_index],
                rig=source_values["rig"][row_index],
                task_logic=source_values["task_logic"][row_index],
            )
        except Exception as exc:
            identity = session_ids[row_index] or f"row {row_index}"
            raise ValueError(f"Could not migrate schemas for {identity!r} from version {source_version!r}") from exc

        serialized = {
            "session_migrated": schemas.session.model_dump_json(),
            "rig_migrated": schemas.rig.model_dump_json(),
            "task_logic_migrated": schemas.task_logic.model_dump_json(),
        }
        for name in needed:
            migrated_values[name][row_index] = serialized[name]

    json_type_factory = getattr(pa, "json_", None)
    json_type = json_type_factory(pa.utf8()) if json_type_factory is not None else pa.large_string()
    for name in MIGRATED_SCHEMA_COLUMNS:
        values = pa.array(migrated_values[name], type=json_type)
        if name in table.column_names:
            index = table.schema.get_field_index(name)
            table = table.set_column(index, pa.field(name, json_type), values)
        else:
            table = table.append_column(pa.field(name, json_type), values)

    return table
