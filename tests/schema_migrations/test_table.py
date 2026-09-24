from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from aind_behavior_vr_foraging_packaging.schema_migrations import (
    MIGRATED_SCHEMA_COLUMNS,
    SchemaMigrationMode,
    append_migrated_schema_columns,
)


def test_mixed_migrated_and_unmigrated_rows_are_filled_non_destructively():
    asset = Path(__file__).parent / "assets" / "session_schema_transitions.parquet"
    raw = pq.read_table(asset).slice(0, 2)
    migrated_first = append_migrated_schema_columns(raw.slice(0, 1))
    mixed = pa.concat_tables(
        [migrated_first, raw.slice(1, 1)],
        promote_options="permissive",
    )
    preserved = {name: mixed[name][0].as_py() for name in MIGRATED_SCHEMA_COLUMNS}

    result = append_migrated_schema_columns(mixed)

    for name in ("session", "rig", "task_logic"):
        assert result[name].to_pylist() == mixed[name].to_pylist()
    for name in MIGRATED_SCHEMA_COLUMNS:
        assert result[name][0].as_py() == preserved[name]
        assert result[name][1].as_py() is not None
        assert result.schema.field(name).type == pa.json_(pa.utf8())


def test_only_missing_migrated_cells_are_filled():
    asset = Path(__file__).parent / "assets" / "session_schema_transitions.parquet"
    migrated = append_migrated_schema_columns(pq.read_table(asset).slice(0, 1))
    sentinel = '{"already":"present"}'
    partial = migrated.set_column(
        migrated.schema.get_field_index("session_migrated"),
        "session_migrated",
        pa.array([sentinel], type=pa.string()),
    ).set_column(
        migrated.schema.get_field_index("rig_migrated"),
        "rig_migrated",
        pa.nulls(1),
    )

    result = append_migrated_schema_columns(partial)

    assert result["session_migrated"][0].as_py() == sentinel
    assert result["rig_migrated"][0].as_py() is not None
    assert result["task_logic_migrated"][0].as_py() == migrated["task_logic_migrated"][0].as_py()


def test_force_recomputes_all_existing_migrated_cells():
    asset = Path(__file__).parent / "assets" / "session_schema_transitions.parquet"
    migrated = append_migrated_schema_columns(pq.read_table(asset).slice(0, 1))
    sentinel = '{"stale":"value"}'
    stale = migrated
    for name in MIGRATED_SCHEMA_COLUMNS:
        stale = stale.set_column(
            stale.schema.get_field_index(name),
            name,
            pa.array([sentinel], type=pa.string()),
        )

    result = append_migrated_schema_columns(stale, mode=SchemaMigrationMode.FORCE)

    for name in MIGRATED_SCHEMA_COLUMNS:
        assert result[name][0].as_py() == migrated[name][0].as_py()
        assert result[name][0].as_py() != sentinel


def test_disabled_mode_returns_the_original_table():
    table = pa.table({"unrelated": [1]})

    result = append_migrated_schema_columns(table, mode=SchemaMigrationMode.DISABLED)

    assert result is table
