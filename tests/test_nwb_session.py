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
from aind_behavior_vr_foraging_packaging.nwb_file._provenance import LAB_META_DATA_KEY

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
    """Provenance lands as spec'd lab_meta_data, and is still there after a write/read.

    Asserting on the read-back file is the point: hdmf writes a LabMetaData subclass
    whose attributes have no spec and drops them silently, which an in-memory-only
    assertion would sail straight past.
    """
    session = _session(tmp_path)
    nwb = session.process()

    provenance = nwb.lab_meta_data[LAB_META_DATA_KEY]
    assert provenance.dataset_version == "0.0.0"
    assert provenance.packaging_version == session.packaging_version
    assert provenance.parser_version == str(session.parser_version)

    out = tmp_path / "session.nwb.zarr"
    session.write_nwb_zarr(out)

    with NWBZarrIO(out.as_posix(), "r") as io:
        round_tripped = io.read().lab_meta_data[LAB_META_DATA_KEY]
        assert round_tripped.fields == provenance.fields
