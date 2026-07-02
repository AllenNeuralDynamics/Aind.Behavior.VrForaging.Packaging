from unittest.mock import MagicMock, patch

import pandas as pd


def _make_mock_proc(name: str) -> MagicMock:
    df = pd.DataFrame({"x": [1]})
    df.attrs.update(
        {
            "packaging_version": "test",
            "data_contract_version": "1.0.0",
            "dataset_version": "0.6.1",
            "processor": name,
        }
    )
    m = MagicMock()
    m.output_name = name
    m.compute.return_value = df
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


def test_parquet_metadata_written_to_schema(tmp_path):
    """run_session() embeds provenance in parquet schema metadata (not just pandas attrs)."""
    import pyarrow.parquet as pq

    from aind_behavior_vr_foraging_packaging.pipeline import run_session

    mock_dataset = MagicMock()
    mock_dataset.version = "0.6.1"

    with patch(
        "aind_behavior_vr_foraging_packaging.pipeline.create_processors", return_value=[_make_mock_proc("trials")]
    ):
        run_session(mock_dataset, tmp_path)

    meta = pq.read_metadata(tmp_path / "trials.parquet").metadata
    assert b"dataset_version" in meta
    assert meta[b"dataset_version"] == b"0.6.1"
    assert b"packaging_version" in meta
    assert b"data_contract_version" in meta
    assert b"processor" in meta
