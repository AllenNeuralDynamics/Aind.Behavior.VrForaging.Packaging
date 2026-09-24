"""CLI option forwarding tests."""

from unittest.mock import patch

from aind_behavior_vr_foraging_packaging.pipeline.cli import AggregateCommand, BatchCommand, SessionCommand
from aind_behavior_vr_foraging_packaging.schema_migrations import SchemaMigrationMode


def test_session_command_forwards_schema_migration_mode(tmp_path):
    with patch("aind_behavior_vr_foraging_packaging.pipeline.cli.process_session") as process:
        SessionCommand(
            input_dir=tmp_path / "raw",
            output_dir=tmp_path / "out",
            schema_migration_mode=SchemaMigrationMode.FILL_MISSING,
        ).run()

    assert process.call_args.kwargs["schema_migration_mode"] is SchemaMigrationMode.FILL_MISSING


def test_batch_command_forwards_schema_migration_mode_to_both_phases(tmp_path):
    raw = tmp_path / "raw" / "session"
    raw.mkdir(parents=True)
    output = tmp_path / "out"
    with (
        patch("aind_behavior_vr_foraging_packaging.pipeline.cli.process_sessions") as process,
        patch("aind_behavior_vr_foraging_packaging.pipeline.cli.aggregate") as aggregate,
    ):
        BatchCommand(
            input_dir=raw.parent,
            output_dir=output,
            schema_migration_mode=SchemaMigrationMode.FILL_MISSING,
        ).run()

    assert process.call_args.kwargs["schema_migration_mode"] is SchemaMigrationMode.FILL_MISSING
    assert aggregate.call_args.kwargs["schema_migration_mode"] is SchemaMigrationMode.FILL_MISSING


def test_aggregate_command_forwards_schema_migration_mode(tmp_path):
    with patch("aind_behavior_vr_foraging_packaging.pipeline.cli.aggregate") as aggregate:
        AggregateCommand(
            input_dir=tmp_path / "sessions",
            output_dir=tmp_path / "out",
            schema_migration_mode=SchemaMigrationMode.FILL_MISSING,
        ).run()

    assert aggregate.call_args.kwargs["schema_migration_mode"] is SchemaMigrationMode.FILL_MISSING


def test_aggregate_command_forwards_force_mode(tmp_path):
    with patch("aind_behavior_vr_foraging_packaging.pipeline.cli.aggregate") as aggregate:
        AggregateCommand(
            input_dir=tmp_path / "sessions",
            output_dir=tmp_path / "out",
            schema_migration_mode=SchemaMigrationMode.FORCE,
        ).run()

    assert aggregate.call_args.kwargs["schema_migration_mode"] is SchemaMigrationMode.FORCE
