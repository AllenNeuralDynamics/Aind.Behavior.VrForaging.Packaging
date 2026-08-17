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


def _dataset_rooted_at(root) -> MagicMock:
    """Mock dataset whose Session stream path makes session_root() resolve to *root*."""
    ds = MagicMock()
    ds.version = "0.6.1"
    stream = ds.at.return_value.at.return_value.at.return_value
    stream.reader_params.path = str(root / "behavior" / "Logs" / "session_input.json")
    return ds


def test_process_session_saves_parquet_per_processor(tmp_path):
    """process_session() saves one parquet per processor, named by proc.output_name."""
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    mock_dataset = _dataset_rooted_at(tmp_path / "raw" / "sess_A")

    with patch(
        "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
        return_value=[_make_mock_proc("sites")],
    ):
        data = process_session(mock_dataset, tmp_path)

    assert "sites" in data
    assert (tmp_path / "sites.parquet").exists()


def test_process_session_returns_all_dataframes(tmp_path):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    mock_dataset = _dataset_rooted_at(tmp_path / "raw" / "sess_A")

    procs = [_make_mock_proc("sites"), _make_mock_proc("position_velocity")]

    with patch("aind_behavior_vr_foraging_packaging.session_pipeline.create_processors", return_value=procs):
        data = process_session(mock_dataset, tmp_path)

    assert set(data.keys()) == {"sites", "position_velocity"}
    assert all((tmp_path / f"{k}.parquet").exists() for k in data)


def test_parquet_metadata_written_to_schema(tmp_path):
    """process_session() embeds provenance in parquet schema metadata (not just pandas attrs)."""
    import pyarrow.parquet as pq

    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    mock_dataset = _dataset_rooted_at(tmp_path / "raw" / "sess_A")

    with patch(
        "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
        return_value=[_make_mock_proc("sites")],
    ):
        process_session(mock_dataset, tmp_path)

    meta = pq.read_metadata(tmp_path / "sites.parquet").metadata
    assert b"dataset_version" in meta
    assert meta[b"dataset_version"] == b"0.6.1"
    assert b"packaging_version" in meta
    assert b"data_contract_version" in meta
    assert b"processor" in meta


# ---------------------------------------------------------------------------
# write_nwb — both output formats come out of process_session
# ---------------------------------------------------------------------------


def test_write_nwb_false_by_default(tmp_path):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")

    with (
        patch(
            "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
            return_value=[_make_mock_proc("sites")],
        ),
        patch("aind_behavior_vr_foraging_packaging.nwb_file.NwbSession") as nwb_cls,
    ):
        process_session(ds, tmp_path / "out")

    nwb_cls.assert_not_called()


def test_write_nwb_writes_store_named_for_the_session_dir(tmp_path):
    """The store is <output_dir>/<session dir name>.nwb.zarr, built from the same
    processor list as the parquets — one filtered selection, two formats."""
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    root = tmp_path / "raw" / "behavior_815103_2025-11-05_22-52-21"
    ds = _dataset_rooted_at(root)
    procs = [_make_mock_proc("sites"), _make_mock_proc("licks")]
    out = tmp_path / "out"

    with (
        patch(
            "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
            return_value=procs,
        ),
        patch("aind_behavior_vr_foraging_packaging.nwb_file.NwbSession") as nwb_cls,
    ):
        process_session(ds, out, write_nwb=True)

    nwb_cls.assert_called_once_with(root, dataset=ds)
    session = nwb_cls.return_value
    session.run.assert_called_once_with(*procs)
    session.write_nwb_zarr.assert_called_once_with(out / "behavior_815103_2025-11-05_22-52-21.nwb.zarr")


def test_write_nwb_uses_the_filtered_processor_list(tmp_path):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")
    selected = [_make_mock_proc("sites")]

    with patch("aind_behavior_vr_foraging_packaging.nwb_file.NwbSession") as nwb_cls:
        process_session(ds, tmp_path / "out", processors=selected, write_nwb=True)

    nwb_cls.return_value.run.assert_called_once_with(*selected)


def test_write_nwb_failure_propagates(tmp_path):
    """A session whose NWB step failed is not a usable partial result."""
    import pytest

    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")

    with (
        patch(
            "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
            return_value=[_make_mock_proc("sites")],
        ),
        patch("aind_behavior_vr_foraging_packaging.nwb_file.NwbSession") as nwb_cls,
    ):
        nwb_cls.return_value.run.side_effect = RuntimeError("missing metadata jsons")
        with pytest.raises(RuntimeError, match="missing metadata jsons"):
            process_session(ds, tmp_path / "out", write_nwb=True)


# ---------------------------------------------------------------------------
# write_parquet — the formats are independent switches over the same frames
# ---------------------------------------------------------------------------


def test_write_parquet_true_by_default(tmp_path):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")

    with patch(
        "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
        return_value=[_make_mock_proc("sites")],
    ):
        process_session(ds, tmp_path / "out")

    assert (tmp_path / "out" / "sites.parquet").exists()


def test_write_parquet_false_skips_files_but_still_returns_frames(tmp_path):
    """The flags choose what reaches disk, not what is computed."""
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")
    procs = [_make_mock_proc("sites"), _make_mock_proc("licks")]

    with patch(
        "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
        return_value=procs,
    ):
        data = process_session(ds, tmp_path / "out", write_parquet=False)

    assert set(data.keys()) == {"sites", "licks"}
    assert not (tmp_path / "out" / "sites.parquet").exists()
    for proc in procs:
        proc.compute.assert_called_once()


def test_nwb_only_writes_no_parquet(tmp_path):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    root = tmp_path / "raw" / "sess_A"
    ds = _dataset_rooted_at(root)
    out = tmp_path / "out"

    with (
        patch(
            "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
            return_value=[_make_mock_proc("sites")],
        ),
        patch("aind_behavior_vr_foraging_packaging.nwb_file.NwbSession") as nwb_cls,
    ):
        process_session(ds, out, write_parquet=False, write_nwb=True)

    assert not (out / "sites.parquet").exists()
    nwb_cls.return_value.write_nwb_zarr.assert_called_once_with(out / "sess_A.nwb.zarr")


def test_both_writers_off_creates_no_output_dir(tmp_path):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")
    out = tmp_path / "out"

    with patch(
        "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
        return_value=[_make_mock_proc("sites")],
    ):
        data = process_session(ds, out, write_parquet=False)

    assert data["sites"] is not None
    assert not out.exists()


# ---------------------------------------------------------------------------
# output_dir defaults to the current working directory
# ---------------------------------------------------------------------------


def test_output_dir_defaults_to_cwd(tmp_path, monkeypatch):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")
    workdir = tmp_path / "cwd"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with patch(
        "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
        return_value=[_make_mock_proc("sites")],
    ):
        process_session(ds)

    assert (workdir / "sites.parquet").exists()


def test_output_dir_accepts_a_string(tmp_path):
    from aind_behavior_vr_foraging_packaging.session_pipeline import process_session

    ds = _dataset_rooted_at(tmp_path / "raw" / "sess_A")

    with patch(
        "aind_behavior_vr_foraging_packaging.session_pipeline.create_processors",
        return_value=[_make_mock_proc("sites")],
    ):
        process_session(ds, str(tmp_path / "out"))

    assert (tmp_path / "out" / "sites.parquet").exists()
