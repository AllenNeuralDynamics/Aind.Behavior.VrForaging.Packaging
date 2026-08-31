from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from aind_behavior_vr_foraging_packaging._base import AbstractProcessor


class _Minimal(AbstractProcessor):
    def _compute(self) -> pd.DataFrame:
        return pd.DataFrame({"x": [1, 2, 3]})


def test_compute_returns_dataframe():
    from unittest.mock import MagicMock as _MM

    proc = _Minimal.__new__(_Minimal)
    proc._dataset = _MM()
    proc._dataset.version = "0.6.0"
    result = proc.compute()
    assert isinstance(result, pd.DataFrame)
    # provenance attrs are stamped automatically
    assert "packaging_version" in result.attrs
    assert result.attrs["dataset_version"] == "0.6.0"
    assert result.attrs["processor"] == "_Minimal"
    assert "data_contract_version" in result.attrs


def test_nwbize_is_noop_by_default():
    proc = _Minimal.__new__(_Minimal)
    nwb = MagicMock()
    result = proc.nwbize(nwb)
    assert result is nwb


def test_process_no_longer_exists():
    proc = _Minimal.__new__(_Minimal)
    assert not hasattr(proc, "process")


def test_output_name_defaults_to_snake_case():
    proc = _Minimal.__new__(_Minimal)
    assert proc.output_name == "__minimal"  # _Minimal → insert _ before M → __minimal


class _Named(AbstractProcessor):
    __output_name__ = "my_stream"

    def _compute(self) -> pd.DataFrame:
        return pd.DataFrame()


def test_output_name_uses_class_override():
    proc = _Named.__new__(_Named)
    assert proc.output_name == "my_stream"


# ---------------------------------------------------------------------------
# write_parquet — default implementation, overridable per processor
# ---------------------------------------------------------------------------


def test_write_parquet_writes_a_readable_file(tmp_path):
    import pyarrow.parquet as pq

    proc = _Minimal.__new__(_Minimal)
    proc._dataset = MagicMock()
    proc._dataset.version = "0.6.0"

    proc.write_parquet(tmp_path)

    assert pq.read_table(tmp_path / "__minimal.parquet").to_pandas()["x"].tolist() == [1, 2, 3]


def test_write_parquet_promotes_attrs_to_schema_metadata(tmp_path):
    import pyarrow.parquet as pq

    proc = _Minimal.__new__(_Minimal)
    proc._dataset = MagicMock()
    proc._dataset.version = "0.6.0"

    proc.write_parquet(tmp_path)

    assert pq.read_metadata(tmp_path / "__minimal.parquet").metadata[b"processor"] == b"_Minimal"


def test_write_parquet_filename_overrides_the_processor_name(tmp_path):
    import pyarrow.parquet as pq

    proc = _Minimal.__new__(_Minimal)
    proc._dataset = MagicMock()
    proc._dataset.version = "0.6.0"

    proc.write_parquet(tmp_path, filename="custom.parquet")

    assert pq.read_table(tmp_path / "custom.parquet").to_pandas()["x"].tolist() == [1, 2, 3]


def test_write_parquet_override_replaces_the_default(tmp_path):
    """SessionMetadataProcessor is the real ``write_parquet`` override in the
    codebase: it computes a genuine SessionMetadata and tags its ``Json[...]``
    fields with Parquet's native JSON logical type instead of an opaque string."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from aind_behavior_vr_foraging_packaging.processing import SessionMetadataProcessor

    ds = MagicMock()
    stream = ds.at.return_value.at.return_value.at.return_value
    stream.reader_params.path = "/data/behavior_815103_2025-11-05_22-52-21/behavior/Logs/session_input.json"
    stream.load.return_value.data = {"subject": "815103", "date": "2025-11-05T22:52:21Z"}
    ds.version = "1.0.0"

    proc = SessionMetadataProcessor(ds)
    proc.write_parquet(tmp_path)

    assert pq.read_table(tmp_path / "session.parquet").schema.field("session").type == pa.json_(pa.utf8())


# ---------------------------------------------------------------------------
# session_root — shared by SessionMetadataProcessor and process_session
# ---------------------------------------------------------------------------


def _dataset_with_stream_path(path):
    ds = MagicMock()
    ds.at.return_value.at.return_value.at.return_value.reader_params.path = path
    return ds


# Paths are written with forward slashes: pathlib reads those as separators on every
# platform, while a `C:\...` literal is one opaque component off Windows.


def test_session_root_from_standard_layout():
    from aind_behavior_vr_foraging_packaging._base import session_root

    ds = _dataset_with_stream_path("/data/behavior_815103_2025-11-05_22-52-21/behavior/Logs/session_input.json")
    assert session_root(ds) == Path("/data/behavior_815103_2025-11-05_22-52-21")


def test_session_root_anchors_on_behavior_not_depth():
    """Found via the `behavior/` component, so nesting the log deeper still resolves
    to the session root rather than to an inner directory."""
    from aind_behavior_vr_foraging_packaging._base import session_root

    ds = _dataset_with_stream_path("/data/my_session/behavior/Logs/nested/session_input.json")
    assert session_root(ds).name == "my_session"


def test_session_root_tolerates_a_root_named_like_the_anchor():
    """`behavior_754559_...` is not `behavior`; the exact match must not eat it."""
    from aind_behavior_vr_foraging_packaging._base import session_root

    ds = _dataset_with_stream_path("/d/behavior_754559_2024-08-26_09-24-17/behavior/Logs/session_input.json")
    assert session_root(ds).name == "behavior_754559_2024-08-26_09-24-17"


def test_session_root_without_anchor_raises():
    import pytest

    from aind_behavior_vr_foraging_packaging._base import DatasetProcessorError, session_root

    ds = _dataset_with_stream_path("/somewhere/else/session_input.json")
    with pytest.raises(DatasetProcessorError, match="session root"):
        session_root(ds)


def test_session_root_without_a_path_raises():
    import pytest

    from aind_behavior_vr_foraging_packaging._base import DatasetProcessorError, session_root

    with pytest.raises(DatasetProcessorError, match="source path"):
        session_root(_dataset_with_stream_path(None))
