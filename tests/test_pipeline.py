from unittest.mock import MagicMock, patch

import pandas as pd


def _make_mock_proc(name: str) -> MagicMock:
    m = MagicMock()
    m.output_name = name
    m.compute.return_value = pd.DataFrame({"x": [1]})
    return m


def test_run_session_saves_parquet_per_processor(tmp_path):
    """run_session() saves one parquet per processor using proc.effective_output_name."""
    from aind_behavior_vr_foraging_packaging.pipeline import run_session

    mock_dataset = MagicMock()
    mock_dataset.version = "0.6.1"

    with patch(
        "aind_behavior_vr_foraging_packaging.pipeline.create_processors", return_value=[_make_mock_proc("trials")]
    ):
        data = run_session(mock_dataset, tmp_path)

    assert "trials" in data
    assert (tmp_path / "trials.parquet").exists()


def test_run_session_returns_all_dataframes(tmp_path):
    from aind_behavior_vr_foraging_packaging.pipeline import run_session

    mock_dataset = MagicMock()
    mock_dataset.version = "0.6.1"

    procs = [_make_mock_proc("trials"), _make_mock_proc("position_velocity")]

    with patch("aind_behavior_vr_foraging_packaging.pipeline.create_processors", return_value=procs):
        data = run_session(mock_dataset, tmp_path)

    assert set(data.keys()) == {"trials", "position_velocity"}
    assert all((tmp_path / f"{k}.parquet").exists() for k in data)
