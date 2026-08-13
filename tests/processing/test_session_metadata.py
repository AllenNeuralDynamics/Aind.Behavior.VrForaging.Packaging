import datetime
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from aind_behavior_services.session import Session

from aind_behavior_vr_foraging_packaging.processing._session_metadata import (
    SessionMetadataProcessor,
)

_SESSION_PATH = Path("behavior_815103_2025-11-05_22-52-21")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dataset(stream_data: dict | None = None, fail: bool = False) -> MagicMock:
    """Return a mock dataset whose Behavior/InputSchemas/Session stream returns *stream_data*.

    If *fail* is True the stream raises RuntimeError.
    """
    ds = MagicMock()
    loader = ds.at.return_value.at.return_value.at.return_value.load.return_value
    if fail:
        ds.at.return_value.at.return_value.at.return_value.load.side_effect = RuntimeError("stream unavailable")
    else:
        loader.data = stream_data if stream_data is not None else {"subject": "815103", "date": "2025-11-05T22:52:21Z"}
    return ds


def _write_session_json(
    directory: Path,
    subject: str | int = "815103",
    date: str = "2025-11-05T22:52:21+00:00",
) -> None:
    logs_dir = directory / "behavior" / "Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (logs_dir / "session_output.json").write_text(json.dumps({"subject": subject, "date": date}), encoding="utf-8")


# ---------------------------------------------------------------------------
# Stream loader (primary path)
# ---------------------------------------------------------------------------


def test_reads_from_stream(tmp_path):
    """Primary path: reads subject and date from the contraqctor stream."""
    ds = _make_dataset({"subject": 815103, "date": "2025-11-05T22:52:21Z"})
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    df = proc._compute()
    assert len(df) == 1
    assert set(df.columns) >= {"session_id", "subject_id", "date"}
    assert df["subject_id"].iloc[0] == "815103"
    assert df["date"].iloc[0] == datetime.datetime(2025, 11, 5, 22, 52, 21, tzinfo=datetime.timezone.utc)


def test_stream_integer_subject_coerced_to_str(tmp_path):
    """Integer subject in stream dict must be coerced to str."""
    ds = _make_dataset({"subject": 815103, "date": "2025-11-05T22:52:21Z"})
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    df = proc._compute()
    assert df["subject_id"].iloc[0] == "815103"


def test_session_id_is_folder_name(tmp_path):
    ds = _make_dataset()
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    df = proc._compute()
    assert df["session_id"].iloc[0] == tmp_path.name


def test_stream_pydantic_model_normalised_to_dict(tmp_path):
    """Regression: when .data is a Pydantic BaseModel (not a dict), _fetch_stream_raw
    must call model_dump() so that _build_metadata can do plain dict lookups.

    Before the fix, ``"subject" not in raw`` evaluated True on a real
    ``aind_behavior_services.session.Session`` instance even when
    ``raw.subject`` held a real value, causing a spurious KeyError on 100 %
    of sessions where the contraqctor stream was available.
    """
    session = Session(subject="841299")

    ds = MagicMock()
    loader = ds.at.return_value.at.return_value.at.return_value.load.return_value
    loader.data = session

    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    df = proc._compute()
    assert df["subject_id"].iloc[0] == "841299"
    # date is auto-populated by the Session model; just verify it's a real datetime


def test_stream_missing_subject_raises(tmp_path):
    """Stream dict that lacks 'subject' raises KeyError."""
    ds = _make_dataset({"date": "2025-11-05T22:52:21Z"})
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    with pytest.raises(KeyError, match="subject"):
        proc._compute()


def test_stream_missing_date_raises(tmp_path):
    """Stream dict that lacks 'date' raises KeyError."""
    ds = _make_dataset({"subject": "815103"})
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    with pytest.raises(KeyError, match="date"):
        proc._compute()


# ---------------------------------------------------------------------------
# JSON fallback (stream fails → read session_output.json)
# ---------------------------------------------------------------------------


def test_falls_back_to_json_when_stream_fails(tmp_path):
    """When the contraqctor stream raises, fall back to session_output.json."""
    ds = _make_dataset(fail=True)
    _write_session_json(tmp_path, subject="815103")
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    df = proc._compute()
    assert df["subject_id"].iloc[0] == "815103"
    assert df["date"].iloc[0] == datetime.datetime(2025, 11, 5, 22, 52, 21, tzinfo=datetime.timezone.utc)


def test_json_integer_subject_coerced(tmp_path):
    """Integer subject in JSON must be coerced to str (fallback path)."""
    ds = _make_dataset(fail=True)
    _write_session_json(tmp_path, subject=815103)
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    df = proc._compute()
    assert df["subject_id"].iloc[0] == "815103"


def test_legacy_version_fallback_reads_json(tmp_path):
    """Legacy datasets (stream fails) fall back to raw JSON correctly."""
    ds = _make_dataset(fail=True)
    _write_session_json(tmp_path, subject="716458", date="2024-05-13T09:03:55+00:00")
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    df = proc._compute()
    assert df["subject_id"].iloc[0] == "716458"
    assert df["date"].iloc[0] == datetime.datetime(2024, 5, 13, 9, 3, 55, tzinfo=datetime.timezone.utc)


def test_json_missing_subject_raises(tmp_path):
    """JSON that lacks 'subject' raises KeyError — not swallowed by fallback logic."""
    ds = _make_dataset(fail=True)
    logs_dir = tmp_path / "behavior" / "Logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "session_output.json").write_text(json.dumps({"date": "2025-11-05T22:52:21Z"}), encoding="utf-8")
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    with pytest.raises(KeyError, match="subject"):
        proc._compute()


def test_stream_unavailable_and_json_missing_raises(tmp_path):
    """Stream unavailable and JSON absent → FileNotFoundError (no silent fallback)."""
    ds = _make_dataset(fail=True)
    proc = SessionMetadataProcessor(ds, session_path=tmp_path)
    with pytest.raises(FileNotFoundError):
        proc._compute()


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_output_name():
    proc = SessionMetadataProcessor(_make_dataset(), session_path=_SESSION_PATH)
    assert proc.output_name == "session"
