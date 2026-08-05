"""Tests for ``NwbSession``.

The base file is built by ``aind_nwb_utils.utils.create_base_nwb_file``, which
reads the AIND metadata jsons sitting in the session root. These tests lay down
a minimal set of those jsons in ``tmp_path`` and drive the real session, with no
part of the creation path stubbed.
"""

import json
from pathlib import Path

from contraqctor.contract import Dataset
from pynwb import NWBFile

from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession

SESSION_NAME = "vr-foraging_123456_2026-01-15_10-30-00"
SUBJECT_ID = "123456"
CREATION_TIME = "2026-01-15T10:30:00"


def _write_metadata(root: Path) -> None:
    """Write the metadata jsons create_base_nwb_file requires (AIND data schema v2 key names)."""
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


def _session(root: Path) -> NwbSession:
    """A session over a root holding metadata, with an empty dataset (no raw streams needed)."""
    _write_metadata(root)
    return NwbSession(root, dataset=Dataset("EmptyDataset", version="0.0.0", data_streams=[]))


def test_nwb_session(tmp_path: Path) -> None:
    """The session builds a base file carrying the identity fields read from the metadata jsons."""
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
