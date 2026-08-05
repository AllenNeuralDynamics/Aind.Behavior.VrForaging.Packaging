"""Tests for NwbSession's NWB file creation.

The base file is built by ``aind_nwb_utils.utils.create_base_nwb_file``, which
reads the AIND metadata jsons sitting in the session root. These tests lay down
a minimal set of those jsons and assert the resulting NWBFile carries the
identity fields downstream processors and the zarr writer rely on.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from pynwb import NWBFile

SESSION_NAME = "vr-foraging_123456_2026-01-15_10-30-00"
SUBJECT_ID = "123456"
CREATION_TIME = "2026-01-15T10:30:00"


def _write_metadata(root):
    """Write the metadata jsons create_base_nwb_file requires.

    Uses AIND data schema v2 key names (``acquisition.json`` present, so
    ``acquisition_start_time`` / ``acquisition_type``).
    """
    files = {
        "data_description.json": {
            "name": SESSION_NAME,
            "subject_id": SUBJECT_ID,
            "creation_time": CREATION_TIME,
            "institution": {"name": "Allen Institute for Neural Dynamics"},
            "project_name": "VR Foraging",
            "group": "behavior",
            "modality": [{"name": "Behavior"}],
        },
        "subject.json": {
            "schema_version": "2.0.0",
            "subject_id": SUBJECT_ID,
            "subject_details": {
                "date_of_birth": "2025-06-01",
                "sex": "Male",
                "species": {"name": "Mus musculus"},
                "strain": {"name": "C57BL/6J"},
                "genotype": "wt/wt",
            },
        },
        "acquisition.json": {
            "acquisition_type": "VR Foraging",
            "acquisition_start_time": CREATION_TIME,
        },
        "procedures.json": {},
        "processing.json": {"processing_pipeline": {"data_processes": []}},
    }
    for name, payload in files.items():
        (root / name).write_text(json.dumps(payload))


@pytest.fixture
def session_root(tmp_path):
    _write_metadata(tmp_path)
    return tmp_path


@pytest.fixture
def session(session_root):
    """An NwbSession over a session root holding metadata, data contract stubbed."""
    from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession

    with patch(
        "aind_behavior_vr_foraging_packaging.nwb_file.aind_behavior_vr_foraging.data_contract.dataset",
        return_value=MagicMock(),
    ):
        yield NwbSession(session_root)


def test_process_creates_nwb_file_with_expected_fields(session):
    nwb = session.process()

    assert isinstance(nwb, NWBFile)
    assert nwb.session_id == SESSION_NAME
    assert nwb.identifier  # uuid assigned by create_base_nwb_file
    assert nwb.session_start_time is not None
    assert nwb.session_start_time.tzinfo is not None
    assert nwb.institution == "Allen Institute for Neural Dynamics"
    assert nwb.lab == "behavior"
    assert "VR Foraging" in nwb.session_description
    assert "Behavior" in nwb.session_description


def test_process_populates_subject(session):
    subject = session.process().subject

    assert subject is not None
    assert subject.subject_id == SUBJECT_ID
    assert subject.species == "Mus musculus"
    assert subject.sex == "M"
    assert subject.strain == "C57BL/6J"
    assert subject.genotype == "wt/wt"
    assert subject.age.startswith("P") and subject.age.endswith("D")


def test_process_is_idempotent(session):
    assert session.process() is session.process()


def test_nwb_file_raises_before_process(session):
    with pytest.raises(ValueError, match="has not been created yet"):
        _ = session.nwb_file


def test_nwb_file_returns_created_file_after_process(session):
    created = session.process()
    assert session.nwb_file is created


def test_run_applies_each_processor_in_order(session):
    """run() threads the base file through every processor's nwbize()."""
    intermediate = MagicMock(spec=NWBFile)
    final = MagicMock(spec=NWBFile)

    first = MagicMock()
    first.nwbize.return_value = intermediate
    second = MagicMock()
    second.nwbize.return_value = final

    assert session.run(first, second) is final
    first.nwbize.assert_called_once_with(session.nwb_file)
    second.nwbize.assert_called_once_with(intermediate)


def test_write_nwb_zarr_raises_before_process(session, tmp_path):
    with pytest.raises(ValueError, match="has not been created yet"):
        session.write_nwb_zarr(tmp_path / "out.nwb.zarr")


def test_write_nwb_zarr_writes_readable_file(session, tmp_path):
    from hdmf_zarr import NWBZarrIO

    session.process()
    out = tmp_path / "out.nwb.zarr"
    session.write_nwb_zarr(out)

    assert out.exists()
    with NWBZarrIO(out.as_posix(), "r") as io:
        assert io.read().session_id == SESSION_NAME
