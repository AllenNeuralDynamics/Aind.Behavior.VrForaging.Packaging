"""Tests for the parquet schema-migration corpus harness."""

import json

import pandas as pd
import pytest
from aind_behavior_services.session import Session
from aind_behavior_vr_foraging.rig import AindVrForagingRig
from aind_behavior_vr_foraging.task_logic import AindVrForagingTaskLogic

from aind_behavior_vr_foraging_packaging.schema_migrations.harness import (
    HarmonizedSchemas,
    SchemaHarmonizationError,
    current_model_harmonizer,
    run_parquet_harness,
)


def _write_parquet(tmp_path, rows):
    path = tmp_path / "schemas.parquet"
    pd.DataFrame(rows).to_parquet(path)
    return path


def _constructed_schemas() -> HarmonizedSchemas:
    """Typed sentinels for tests that exercise only parquet orchestration."""
    return HarmonizedSchemas(
        session=Session.model_construct(),
        rig=AindVrForagingRig.model_construct(),
        task_logic=AindVrForagingTaskLogic.model_construct(),
    )


def test_harness_uses_default_columns_and_returns_all_three_outputs(tmp_path):
    path = _write_parquet(
        tmp_path,
        [
            {
                "dataset_version": "0.6.3",
                "session": json.dumps({"subject": "1"}),
                "rig": json.dumps({"rig_name": "rig-a"}),
                "task_logic": json.dumps({"task_parameters": {}}),
                "ignored": "not loaded",
            }
        ],
    )

    def harmonizer(**kwargs):
        assert kwargs["source_version"] == "0.6.3"
        return _constructed_schemas()

    result = run_parquet_harness(path, harmonizer=harmonizer)

    assert result.passed
    assert result.total_rows == 1
    assert isinstance(result.rows[0].session, Session)
    assert isinstance(result.rows[0].rig, AindVrForagingRig)
    assert isinstance(result.rows[0].task_logic, AindVrForagingTaskLogic)


def test_harness_supports_custom_column_names(tmp_path):
    path = _write_parquet(
        tmp_path,
        [{"release": "1.2.5", "raw_session": "{}", "raw_rig": "{}", "raw_task": "{}"}],
    )

    def harmonizer(**_kwargs):
        return _constructed_schemas()

    result = run_parquet_harness(
        path,
        version_column="release",
        session_column="raw_session",
        rig_column="raw_rig",
        task_logic_column="raw_task",
        harmonizer=harmonizer,
    )

    assert result.passed


def test_harness_can_validate_without_retaining_models(tmp_path):
    path = _write_parquet(
        tmp_path,
        [{"dataset_version": "1.2.5", "session": "{}", "rig": "{}", "task_logic": "{}"}],
    )

    result = run_parquet_harness(path, harmonizer=lambda **_kwargs: _constructed_schemas(), retain_rows=False)

    assert result.passed
    assert result.success_count == 1
    assert result.rows == ()


def test_current_models_report_all_three_document_failures():
    with pytest.raises(SchemaHarmonizationError) as exc_info:
        current_model_harmonizer(source_version="0.3.0", session="{}", rig="{}", task_logic="not-json")

    assert {failure.document for failure in exc_info.value.failures} == {"session", "rig", "task_logic"}
    assert all(failure.errors for failure in exc_info.value.failures)


def test_harness_collects_failures_and_continues(tmp_path):
    path = _write_parquet(
        tmp_path,
        [
            {"dataset_version": "0.3.0", "session": "{}", "rig": "{}", "task_logic": "not-json"},
            {"dataset_version": "1.2.5", "session": "{}", "rig": "{}", "task_logic": "{}"},
        ],
    )

    def harmonizer(**kwargs):
        if kwargs["source_version"] == "0.3.0":
            return current_model_harmonizer(**kwargs)
        return _constructed_schemas()

    result = run_parquet_harness(path, harmonizer=harmonizer)

    assert not result.passed
    assert len(result.rows) == 1
    assert result.rows[0].row_number == 1
    assert {failure.document for failure in result.failures} == {"session", "rig", "task_logic"}
    assert {failure.row_number for failure in result.failures} == {0}

    with pytest.raises(AssertionError, match="0.3.0/rig=1"):
        result.raise_for_failures()


def test_missing_version_is_a_row_failure(tmp_path):
    path = _write_parquet(
        tmp_path,
        [{"dataset_version": None, "session": "{}", "rig": "{}", "task_logic": "{}"}],
    )

    result = run_parquet_harness(path)

    assert len(result.failures) == 1
    assert result.failures[0].document == "bundle"
    assert "Missing source version" in result.failures[0].message
