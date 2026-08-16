import datetime
from unittest.mock import MagicMock

import pytest
from aind_behavior_services.session import Session

from aind_behavior_vr_foraging_packaging.processing._session_metadata import (
    SessionMetadataProcessor,
)

_VALID = {
    "session_name": "815103_2025-11-05T225221Z",
    "subject": "815103",
    "date": "2025-11-05T22:52:21Z",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dataset(stream_data: dict | Session | None = None, *, error: Exception | None = None) -> MagicMock:
    """Return a mock dataset whose Behavior/InputSchemas/Session stream returns *stream_data*.

    Pass *error* to make ``load()`` raise instead.
    """
    ds = MagicMock()
    stream = ds.at.return_value.at.return_value.at.return_value
    if error is not None:
        stream.load.side_effect = error
    else:
        stream.load.return_value.data = _VALID if stream_data is None else stream_data
    return ds


# ---------------------------------------------------------------------------
# Stream loader — the only source
# ---------------------------------------------------------------------------


def test_reads_all_identity_fields_from_stream():
    df = SessionMetadataProcessor(_make_dataset())._compute()
    assert len(df) == 1
    assert set(df.columns) >= {"session_id", "subject_id", "date"}
    assert df["session_id"].iloc[0] == "815103_2025-11-05T225221Z"
    assert df["subject_id"].iloc[0] == "815103"
    assert df["date"].iloc[0] == datetime.datetime(2025, 11, 5, 22, 52, 21, tzinfo=datetime.timezone.utc)


def test_integer_subject_coerced_to_str():
    df = SessionMetadataProcessor(_make_dataset({**_VALID, "subject": 815103}))._compute()
    assert df["subject_id"].iloc[0] == "815103"


def test_pydantic_model_normalised_to_dict():
    """Regression: when .data is a Pydantic BaseModel (not a dict), the payload must
    be model_dump()ed so the required-field checks can do plain dict lookups.

    Before the fix, ``"subject" not in raw`` evaluated True on a real
    ``aind_behavior_services.session.Session`` instance even when ``raw.subject``
    held a real value, causing a spurious KeyError on 100 % of sessions where the
    contraqctor stream was available.
    """
    df = SessionMetadataProcessor(_make_dataset(Session(subject="841299", session_name="841299_x")))._compute()
    assert df["subject_id"].iloc[0] == "841299"
    assert df["session_id"].iloc[0] == "841299_x"


# ---------------------------------------------------------------------------
# Every required field is required — no defaults, no derivation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", ["session_name", "subject", "date"])
def test_missing_required_field_raises(field):
    proc = SessionMetadataProcessor(_make_dataset({k: v for k, v in _VALID.items() if k != field}))
    with pytest.raises(KeyError, match=field):
        proc._compute()


@pytest.mark.parametrize("field", ["session_name", "subject", "date"])
def test_empty_required_field_raises(field):
    """A present-but-null field (e.g. session_name=None on older launchers) is
    just as fatal as an absent one — identity is never guessed."""
    proc = SessionMetadataProcessor(_make_dataset({**_VALID, field: None}))
    with pytest.raises(KeyError, match=field):
        proc._compute()


# ---------------------------------------------------------------------------
# No fallback: an unloadable stream is a crash, not a legacy dataset
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
    """Every stream failure propagates — there is deliberately no second source.

    Reading a different file instead would report success off a source that may
    disagree with the stream.
    """
    proc = SessionMetadataProcessor(_make_dataset(error=error))
    with pytest.raises(type(error)):
        proc._compute()


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_constructor_matches_every_other_processor():
    """No bespoke session_path kwarg — plain (dataset, raise_on_error)."""
    proc = SessionMetadataProcessor(_make_dataset(), raise_on_error=True)
    assert proc.output_name == "session"
    assert proc.raise_on_error is True
