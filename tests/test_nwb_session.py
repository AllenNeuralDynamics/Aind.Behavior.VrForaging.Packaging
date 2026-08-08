"""Tests for ``NwbSession``.

These cover what the session itself owns — provenance and caching — over a
base file handed in directly. Deriving that base file from the AIND metadata jsons
belongs to ``aind_nwb_utils.create_base_nwb_file``; a synthetic copy of those jsons
here could only drift from the schema, so the real ones are exercised against real
sessions in ``tests/integration/test_datasets.py``.
"""

from datetime import datetime, timezone
from pathlib import Path

from contraqctor.contract import Dataset
from hdmf_zarr import NWBZarrIO
from pynwb import NWBFile
from pynwb.file import Subject

from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession

SESSION_NAME = "vrforaging_123456_2026-01-15T103000"
SUBJECT_ID = "123456"
CREATION_TIME = datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc)


def _base_nwb_file() -> NWBFile:
    """A minimal base file, standing in for whatever create_base_nwb_file would return."""
    return NWBFile(
        session_description="A VR Foraging experiment.",
        identifier="test-identifier",
        session_start_time=CREATION_TIME,
        session_id=SESSION_NAME,
        subject=Subject(subject_id=SUBJECT_ID, species="Mus musculus"),
    )


def _session(root: Path) -> NwbSession:
    """A session over an empty dataset (no raw streams needed) and a hand-built base file."""
    return NwbSession(
        root,
        dataset=Dataset("EmptyDataset", version="0.0.0", data_streams=[]),
        base_nwb_file=_base_nwb_file(),
    )


def test_nwb_session(tmp_path: Path) -> None:
    """The session passes the base file through and stamps the dataset version onto it."""
    session = _session(tmp_path)

    nwb = session.process()

    assert isinstance(nwb, NWBFile)
    assert nwb.session_id == SESSION_NAME
    assert nwb.session_start_time is not None
    assert nwb.session_start_time.tzinfo is not None
    assert nwb.subject is not None
    assert nwb.subject.subject_id == SUBJECT_ID
    # the file is built once and cached, not rebuilt per call
    assert session.process() is nwb
    assert session.nwb_file is nwb


def test_provenance_survives_round_trip(tmp_path: Path) -> None:
    """Provenance lands in was_generated_by and is still there after a write/read.

    Asserting on the read-back file matters because ``was_generated_by`` is
    write-once: the entries have to be appended before the write, and there is no
    second chance to correct them afterwards.
    """
    session = _session(tmp_path)
    nwb = session.process()

    assert dict(nwb.was_generated_by) == {
        "packaging_version": session.packaging_version,
        "data_contract_version": str(session.parser_version),
        "dataset_version": "0.0.0",
    }

    out = tmp_path / "session.nwb.zarr"
    session.write_nwb_zarr(out)

    with NWBZarrIO(out.as_posix(), "r") as io:
        round_tripped = io.read().was_generated_by
        assert {key: value for key, value in round_tripped[:]} == session.provenance
