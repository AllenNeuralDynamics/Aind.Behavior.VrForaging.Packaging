"""Fixtures shared across the test suite."""

from datetime import datetime, timezone

import pytest
from pynwb import NWBFile


@pytest.fixture
def nwb_file() -> NWBFile:
    """A minimal, empty NWBFile for processors to write into."""
    return NWBFile(
        session_description="test",
        identifier="test",
        session_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
