import datetime
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
    """SessionMetadataProcessor overrides write_parquet to tag session/rig/task_logic."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    ds = _make_dataset_with_schemas(rig={"a": 1}, task_logic={"b": 2})
    proc = SessionMetadataProcessor(ds)

    proc.write_parquet(tmp_path)

    table = pq.read_table(tmp_path / "session.parquet")
    assert table.schema.field("session").type == pa.json_(pa.utf8())
    assert table.schema.field("rig").type == pa.json_(pa.utf8())
    assert table.schema.field("task_logic").type == pa.json_(pa.utf8())
    assert table.schema.field("session_id").type not in (pa.json_(pa.utf8()), pa.json_(pa.large_utf8()))


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_constructor_matches_every_other_processor():
    """No bespoke session_path kwarg — plain (dataset, strict_parsing)."""
    proc = SessionMetadataProcessor(_make_dataset(), strict_parsing=True)
    assert proc.output_name == "session"
    assert proc.strict_parsing is True
