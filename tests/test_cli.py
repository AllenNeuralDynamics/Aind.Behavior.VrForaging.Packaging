"""Unit tests for cli.py — no real I/O; process_sessions and aggregate are patched."""
import logging
from pathlib import Path
from unittest.mock import patch


def _run_cli(args: list[str]):
    """Invoke ExportSettings.cli_cmd() with the given arg list."""
    from pydantic_settings import CliApp

    from aind_behavior_vr_foraging_packaging.cli import ExportSettings

    return CliApp.run(ExportSettings, cli_args=args)


def _make_input_dir(tmp_path: Path) -> Path:
    """Create an input dir with one fake session subdir so cli_cmd proceeds past the guard."""
    d = tmp_path / "input"
    (d / "fake_session").mkdir(parents=True)
    return d


def test_cli_calls_both_phases(tmp_path):
    """With no skip flags both process_sessions and aggregate are called."""
    input_dir = _make_input_dir(tmp_path)

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate") as mock_agg,
    ):
        _run_cli([f"--input-dir={input_dir}", f"--output-dir={tmp_path / 'out'}"])

    mock_ps.assert_called_once()
    mock_agg.assert_called_once()


def test_cli_skip_processing(tmp_path):
    """--skip-processing suppresses process_sessions but still calls aggregate."""
    input_dir = _make_input_dir(tmp_path)

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate") as mock_agg,
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            "--skip-processing=true",
        ])

    mock_ps.assert_not_called()
    mock_agg.assert_called_once()


def test_cli_skip_aggregation(tmp_path):
    """--skip-aggregation calls process_sessions but suppresses aggregate."""
    input_dir = _make_input_dir(tmp_path)

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate") as mock_agg,
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            "--skip-aggregation=true",
        ])

    mock_ps.assert_called_once()
    mock_agg.assert_not_called()


def test_cli_exclude_processors_forwarded(tmp_path):
    """--exclude-processors comma-separated values reach process_sessions."""
    input_dir = _make_input_dir(tmp_path)

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate"),
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            "--exclude-processors=sniffing,software_events",
        ])

    _, kwargs = mock_ps.call_args
    assert set(kwargs.get("exclude_processors", [])) == {"sniffing", "software_events"}


def test_cli_include_processors_forwarded(tmp_path):
    """--include-processors values reach process_sessions as keyword arg."""
    input_dir = _make_input_dir(tmp_path)

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate"),
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            "--include-processors=trials",
        ])

    _, kwargs = mock_ps.call_args
    assert "trials" in kwargs.get("include_processors", [])


def test_cli_log_file_created(tmp_path):
    """--log-file creates the file even when the root logger already has handlers."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    log_file = tmp_path / "run.log"

    # Ensure root logger has no pre-existing file handlers for this path
    root = logging.getLogger()
    initial_handlers = list(root.handlers)

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions"),
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate"),
    ):
        _run_cli([
            f"--input-dir={input_dir}",
            f"--output-dir={tmp_path / 'out'}",
            f"--log-file={log_file}",
        ])

    assert log_file.exists(), "log file was not created"

    # Clean up the file handler added during this test
    for h in root.handlers:
        if h not in initial_handlers:
            h.close()
            root.removeHandler(h)


def test_cli_no_input_subdirs_does_nothing(tmp_path):
    """Empty input_dir logs a warning and returns without calling either phase."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()  # empty — no session subdirs

    with (
        patch("aind_behavior_vr_foraging_packaging.cli.process_sessions") as mock_ps,
        patch("aind_behavior_vr_foraging_packaging.cli.aggregate") as mock_agg,
    ):
        _run_cli([f"--input-dir={input_dir}", f"--output-dir={tmp_path / 'out'}"])

    mock_ps.assert_not_called()
    mock_agg.assert_not_called()
