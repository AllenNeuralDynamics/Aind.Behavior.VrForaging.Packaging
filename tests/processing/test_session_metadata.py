import datetime
import json
import logging
import os
from unittest.mock import MagicMock

import pytest
from aind_behavior_services.session import Session

from aind_behavior_vr_foraging_packaging._base import DatasetProcessorError
from aind_behavior_vr_foraging_packaging.processing._session_metadata import (
    SessionMetadataProcessor,
)

_VALID = {
    # Present but deliberately ignored — session_id comes from the directory.
    "session_name": "815103_2025-11-05T225221Z",
    "subject": "815103",
    "date": "2025-11-05T22:52:21Z",
}

_ROOT_NAME = "behavior_815103_2025-11-05_22-52-21"

#: Mirrors the real on-disk layout: <root>/behavior/Logs/session_input.json.
#: Forward slashes: pathlib reads those as separators on every platform, while a
#: ``C:\...`` literal is one opaque component off Windows.
_STREAM_PATH = f"/data/{_ROOT_NAME}/behavior/Logs/session_input.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dataset(
    stream_data: dict | Session | None = None,
    *,
    error: Exception | None = None,
    stream_path: str | None = _STREAM_PATH,
) -> MagicMock:
    """Return a mock dataset whose Behavior/InputSchemas/Session stream returns *stream_data*.

    Pass *error* to make ``load()`` raise instead. *stream_path* is what
    ``session_id`` is derived from; pass ``None`` to simulate a stream with no
    on-disk source.
    """
    ds = MagicMock()
    stream = ds.at.return_value.at.return_value.at.return_value
    stream.reader_params.path = stream_path
    if error is not None:
        stream.load.side_effect = error
    else:
        stream.load.return_value.data = _VALID if stream_data is None else stream_data
    return ds


def _make_dataset_with_schemas(
    *,
    session: dict | Session | None = None,
    rig: dict | Session | None = None,
    task_logic: dict | Session | None = None,
    stream_path: str | None = _STREAM_PATH,
) -> MagicMock:
    """Return a mock dataset with distinct payloads for Session, Rig and TaskLogic.

    Unlike :func:`_make_dataset`, each ``InputSchemas`` node gets its own payload,
    so ``session``/``rig``/``task_logic`` can be asserted independently.
    """
    payloads = {
        "Session": _VALID if session is None else session,
        "Rig": rig,
        "TaskLogic": task_logic,
    }

    def _at(name: str) -> MagicMock:
        node = MagicMock()
        node.reader_params.path = stream_path
        node.load.return_value.data = payloads[name]
        return node

    ds = MagicMock()
    ds.at.return_value.at.return_value.at.side_effect = _at
    return ds


# ---------------------------------------------------------------------------
# session_id is the directory name, always
# ---------------------------------------------------------------------------


def test_session_id_is_the_directory_name():
    df = SessionMetadataProcessor(_make_dataset())._compute()
    assert len(df) == 1
    assert set(df.columns) >= {"session_id", "subject_id", "date"}
    assert df["session_id"].iloc[0] == _ROOT_NAME
    assert df["subject_id"].iloc[0] == "815103"
    assert df["date"].iloc[0] == datetime.datetime(2025, 11, 5, 22, 52, 21, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize(
    "payload",
    [{}, {"session_name": None}, {"session_name": "a_completely_different_name"}],
    ids=["absent", "null", "disagrees"],
)
def test_stream_session_name_is_ignored_whatever_it_says(payload):
    raw = {k: v for k, v in _VALID.items() if k != "session_name"} | payload
    df = SessionMetadataProcessor(_make_dataset(raw))._compute()
    assert df["session_id"].iloc[0] == _ROOT_NAME


def test_session_root_anchors_on_behavior_dir_not_depth():
    """A log nested deeper under ``behavior/`` still resolves to the session root."""
    df = SessionMetadataProcessor(
        _make_dataset(stream_path="/data/my_session/behavior/Logs/nested/session_input.json")
    )._compute()
    assert df["session_id"].iloc[0] == "my_session"


def test_pydantic_model_normalised_to_dict():
    """Regression: a BaseModel payload must be model_dump()ed before the dict lookups."""
    df = SessionMetadataProcessor(_make_dataset(Session(subject="841299", session_name="841299_x")))._compute()
    assert df["subject_id"].iloc[0] == "841299"
    assert df["session_id"].iloc[0] == _ROOT_NAME


# ---------------------------------------------------------------------------
# An unrecoverable directory is fatal — there is no identity to degrade to
# ---------------------------------------------------------------------------


def test_unrecoverable_root_raises():
    proc = SessionMetadataProcessor(_make_dataset(stream_path="/somewhere/else/session_input.json"))
    with pytest.raises(DatasetProcessorError, match="session root"):
        proc._compute()


def test_absent_stream_path_raises():
    proc = SessionMetadataProcessor(_make_dataset(stream_path=None))
    with pytest.raises(DatasetProcessorError, match="source path"):
        proc._compute()


def test_unrecoverable_root_is_fatal_regardless_of_strict_parsing():
    proc = SessionMetadataProcessor(_make_dataset(stream_path=None), strict_parsing=False)
    with pytest.raises(DatasetProcessorError):
        proc._compute()


# ---------------------------------------------------------------------------
# subject / date come from the stream, with no fallback
# ---------------------------------------------------------------------------


def test_integer_subject_coerced_to_str():
    df = SessionMetadataProcessor(_make_dataset({**_VALID, "subject": 815103}))._compute()
    assert df["subject_id"].iloc[0] == "815103"


@pytest.mark.parametrize("field", ["subject", "date"])
def test_missing_required_field_raises(field):
    proc = SessionMetadataProcessor(_make_dataset({k: v for k, v in _VALID.items() if k != field}))
    with pytest.raises(KeyError, match=field):
        proc._compute()


@pytest.mark.parametrize("field", ["subject", "date"])
def test_empty_required_field_raises(field):
    """A present-but-null field is just as fatal as an absent one."""
    proc = SessionMetadataProcessor(_make_dataset({**_VALID, field: None}))
    with pytest.raises(KeyError, match=field):
        proc._compute()


# ---------------------------------------------------------------------------
# No second file: an unloadable stream is a crash, not a legacy dataset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError("session_input.json not found"),
        KeyError("Session"),
        RuntimeError("corrupt stream"),
    ],
    ids=["missing-file", "undeclared-node", "corrupt"],
)
def test_unloadable_stream_propagates(error):
    """Every stream failure propagates — there is no second source for subject/date."""
    proc = SessionMetadataProcessor(_make_dataset(error=error))
    with pytest.raises(type(error)):
        proc._compute()


# ---------------------------------------------------------------------------
# session / rig / task_logic are carried verbatim, as JSON strings
# ---------------------------------------------------------------------------


def test_dict_payloads_are_carried_verbatim():
    """Legacy (plain-``Json``) streams are already JSON-safe dicts; just carry them."""
    rig = {"rig_name": "vr-foraging-1", "calibration": {"gain": 1.5}}
    task_logic = {"task_parameters": {"n_patches": 4}}
    ds = _make_dataset_with_schemas(rig=rig, task_logic=task_logic)

    df = SessionMetadataProcessor(ds)._compute()

    assert df["session"].iloc[0] == _VALID
    assert df["rig"].iloc[0] == rig
    assert df["task_logic"].iloc[0] == task_logic


def test_pydantic_payloads_are_model_dumped():
    """A BaseModel payload is normalized with model_dump(mode='json') first."""
    rig = Session(subject="rig-subject", session_name="n/a")
    task_logic = Session(subject="task-logic-subject", session_name="n/a")
    ds = _make_dataset_with_schemas(rig=rig, task_logic=task_logic)

    df = SessionMetadataProcessor(ds)._compute()

    assert df["rig"].iloc[0] == rig.model_dump(mode="json")
    assert df["task_logic"].iloc[0] == task_logic.model_dump(mode="json")


def test_session_column_matches_the_metadata_fields():
    """The `session` column is the same Session stream subject/date were read from."""
    ds = _make_dataset_with_schemas(rig={"a": 1}, task_logic={"b": 2})

    df = SessionMetadataProcessor(ds)._compute()

    assert df["session"].iloc[0] == _VALID


def test_write_parquet_tags_json_fields_with_the_json_logical_type(tmp_path):
    """SessionMetadataProcessor overrides write_parquet to tag session/rig/task_logic/trainer_state."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    ds = _make_dataset_with_schemas(rig={"a": 1}, task_logic={"b": 2})
    proc = SessionMetadataProcessor(ds)

    proc.write_parquet(tmp_path)

    table = pq.read_table(tmp_path / "session.parquet")
    assert table.schema.field("session").type == pa.json_(pa.utf8())
    assert table.schema.field("rig").type == pa.json_(pa.utf8())
    assert table.schema.field("task_logic").type == pa.json_(pa.utf8())
    assert table.schema.field("trainer_state").type == pa.json_(pa.utf8())
    assert table.schema.field("session_id").type not in (pa.json_(pa.utf8()), pa.json_(pa.large_utf8()))


# ---------------------------------------------------------------------------
# Curriculum state: behavior/trainer_state.json (optional, not a contraqctor stream)
# ---------------------------------------------------------------------------


def _session_dir(tmp_path, root_name: str = _ROOT_NAME):
    """Create <tmp_path>/<root_name>/behavior/ on disk and return the session dir."""
    d = tmp_path / root_name
    (d / "behavior").mkdir(parents=True)
    return d


def _stream_path_under(session_dir) -> str:
    """Mirror the real on-disk layout for a session dir created by :func:`_session_dir`."""
    return str(session_dir / "behavior" / "Logs" / "session_input.json")


def test_no_trainer_state_file_leaves_curriculum_fields_null(tmp_path):
    """The file is optional; its absence is not an error."""
    session_dir = _session_dir(tmp_path)
    ds = _make_dataset(stream_path=_stream_path_under(session_dir))

    df = SessionMetadataProcessor(ds)._compute()

    assert df["curriculum_enabled"].iloc[0] is None
    assert df["curriculum_name"].iloc[0] is None
    assert df["curriculum_stage_name"].iloc[0] is None
    assert df["trainer_state"].iloc[0] is None


def _valid_trainer_state(*, curriculum_name: str = "vr-foraging-curriculum", stage_name: str = "stage_2") -> dict:
    """A minimal payload that actually validates against the real, bare
    ``aind_behavior_curriculum.trainer.TrainerState`` — as opposed to the shorthand
    dicts used elsewhere in this module, which are deliberately missing required
    fields (``curriculum.version``, ``stage.task``) to exercise the fallback path.
    """
    return {
        "curriculum": {"name": curriculum_name, "version": "0.1.0"},
        "stage": {"name": stage_name, "task": {"name": "dummy_task", "task_parameters": {}}},
        "is_on_curriculum": True,
        "active_policies": ["my_project.policies.policy_a", "my_project.policies.policy_b"],
    }


def test_trainer_state_fields_are_surfaced(tmp_path, caplog):
    """A well-formed payload validates cleanly against TrainerState — no fallback warning."""
    session_dir = _session_dir(tmp_path)
    payload = _valid_trainer_state()
    (session_dir / "behavior" / "trainer_state.json").write_text(json.dumps(payload))
    ds = _make_dataset(stream_path=_stream_path_under(session_dir))

    with caplog.at_level(logging.WARNING):
        df = SessionMetadataProcessor(ds)._compute()

    assert bool(df["curriculum_enabled"].iloc[0]) is True
    assert df["curriculum_name"].iloc[0] == "vr-foraging-curriculum"
    assert df["curriculum_stage_name"].iloc[0] == "stage_2"
    # active_policies isn't broken out as its own column; it's still queryable from the raw payload.
    assert df["trainer_state"].iloc[0]["active_policies"] == payload["active_policies"]
    assert "does not validate" not in caplog.text


def test_trainer_state_off_curriculum_has_null_curriculum_and_stage(tmp_path):
    """A subject ejected from curriculum: curriculum/stage are null but the file still exists.

    This is the exact shape ``TrainerState.default()`` produces, and it validates
    cleanly (``curriculum``/``stage`` are ``Optional``).
    """
    payload = {"curriculum": None, "stage": None, "is_on_curriculum": False, "active_policies": None}
    session_dir = _session_dir(tmp_path)
    (session_dir / "behavior" / "trainer_state.json").write_text(json.dumps(payload))
    ds = _make_dataset(stream_path=_stream_path_under(session_dir))

    df = SessionMetadataProcessor(ds)._compute()

    assert bool(df["curriculum_enabled"].iloc[0]) is False
    assert df["curriculum_name"].iloc[0] is None
    assert df["curriculum_stage_name"].iloc[0] is None


def test_multiple_trainer_state_files_uses_the_most_recently_modified(tmp_path, caplog):
    """A datetime-suffixed trainer_state may be duplicated; the newest on disk wins."""
    session_dir = _session_dir(tmp_path)
    behavior_dir = session_dir / "behavior"
    older = behavior_dir / "trainer_state_2025-01-01T000000Z.json"
    newer = behavior_dir / "trainer_state_2025-06-01T000000Z.json"
    older.write_text(json.dumps(_valid_trainer_state(curriculum_name="old")))
    newer.write_text(json.dumps(_valid_trainer_state(curriculum_name="new")))
    os.utime(older, (1_000_000, 1_000_000))
    os.utime(newer, (2_000_000, 2_000_000))
    ds = _make_dataset(stream_path=_stream_path_under(session_dir))

    with caplog.at_level(logging.WARNING):
        df = SessionMetadataProcessor(ds)._compute()

    assert df["curriculum_name"].iloc[0] == "new"
    assert "Multiple trainer_state files" in caplog.text


def test_trainer_state_schema_mismatch_falls_back_with_a_warning(tmp_path, caplog):
    """A trainer_state.json that doesn't validate (e.g. a version-drifted curriculum
    library) degrades to the raw, unvalidated dict rather than crashing the session.
    """
    session_dir = _session_dir(tmp_path)
    payload = {"unexpected": "shape", "from": "some other schema version"}
    (session_dir / "behavior" / "trainer_state.json").write_text(json.dumps(payload))
    ds = _make_dataset(stream_path=_stream_path_under(session_dir))

    with caplog.at_level(logging.WARNING):
        df = SessionMetadataProcessor(ds)._compute()

    assert df["curriculum_enabled"].iloc[0] is None
    assert df["curriculum_name"].iloc[0] is None
    assert df["curriculum_stage_name"].iloc[0] is None
    assert df["trainer_state"].iloc[0] == payload
    assert "does not validate" in caplog.text


def test_trainer_state_schema_mismatch_raises_under_strict_parsing(tmp_path):
    session_dir = _session_dir(tmp_path)
    (session_dir / "behavior" / "trainer_state.json").write_text(json.dumps({"unexpected": "shape"}))
    ds = _make_dataset(stream_path=_stream_path_under(session_dir))

    proc = SessionMetadataProcessor(ds, strict_parsing=True)
    with pytest.raises(DatasetProcessorError, match="does not validate"):
        proc._compute()


def test_write_parquet_forces_a_stable_type_regardless_of_curriculum_nullness(tmp_path):
    """A session with no trainer_state.json (all-null columns) must still get the same
    arrow type as a session with real curriculum data — otherwise pyarrow infers a bare
    ``null`` type for the former, and scanning many sessions' session.parquet files as
    one schema-unified dataset (DuckDB glob reads, pyarrow.dataset) breaks on the drift.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    without = _session_dir(tmp_path, "without_curriculum")
    with_curriculum = _session_dir(tmp_path, "with_curriculum")
    (with_curriculum / "behavior" / "trainer_state.json").write_text(json.dumps(_valid_trainer_state()))

    without_out, with_out = tmp_path / "without_out", tmp_path / "with_out"
    without_out.mkdir()
    with_out.mkdir()
    SessionMetadataProcessor(_make_dataset(stream_path=_stream_path_under(without))).write_parquet(without_out)
    SessionMetadataProcessor(_make_dataset(stream_path=_stream_path_under(with_curriculum))).write_parquet(with_out)

    without_schema = pq.read_table(without_out / "session.parquet").schema
    with_schema = pq.read_table(with_out / "session.parquet").schema

    for name, expected in (("curriculum_enabled", pa.bool_()), ("curriculum_name", pa.large_string())):
        assert without_schema.field(name).type == expected, f"{name}: all-null column should still be {expected}"
        assert with_schema.field(name).type == expected
    assert without_schema.field("curriculum_enabled").type == with_schema.field("curriculum_enabled").type
    assert without_schema.field("curriculum_name").type == with_schema.field("curriculum_name").type


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_constructor_matches_every_other_processor():
    """No bespoke session_path kwarg — plain (dataset, strict_parsing)."""
    proc = SessionMetadataProcessor(_make_dataset(), strict_parsing=True)
    assert proc.output_name == "session"
    assert proc.strict_parsing is True
