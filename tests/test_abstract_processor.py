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
# session_root — shared by SessionMetadataProcessor and process_session
# ---------------------------------------------------------------------------


def _dataset_with_stream_path(path):
    ds = MagicMock()
    ds.at.return_value.at.return_value.at.return_value.reader_params.path = path
    return ds


def test_session_root_from_standard_layout():
    from pathlib import Path

    from aind_behavior_vr_foraging_packaging._base import session_root

    ds = _dataset_with_stream_path(r"C:\data\behavior_815103_2025-11-05_22-52-21\behavior\Logs\session_input.json")
    assert session_root(ds) == Path(r"C:\data\behavior_815103_2025-11-05_22-52-21")


def test_session_root_anchors_on_behavior_not_depth():
    """Found via the `behavior/` component, so nesting the log deeper still resolves
    to the session root rather than to an inner directory."""
    from aind_behavior_vr_foraging_packaging._base import session_root

    ds = _dataset_with_stream_path(r"C:\data\my_session\behavior\Logs\nested\session_input.json")
    assert session_root(ds).name == "my_session"


def test_session_root_tolerates_a_root_named_like_the_anchor():
    """`behavior_754559_...` is not `behavior`; the exact match must not eat it."""
    from aind_behavior_vr_foraging_packaging._base import session_root

    ds = _dataset_with_stream_path(r"C:\d\behavior_754559_2024-08-26_09-24-17\behavior\Logs\session_input.json")
    assert session_root(ds).name == "behavior_754559_2024-08-26_09-24-17"


def test_session_root_without_anchor_raises():
    import pytest

    from aind_behavior_vr_foraging_packaging._base import DatasetProcessorError, session_root

    ds = _dataset_with_stream_path(r"C:\somewhere\else\session_input.json")
    with pytest.raises(DatasetProcessorError, match="session root"):
        session_root(ds)


def test_session_root_without_a_path_raises():
    import pytest

    from aind_behavior_vr_foraging_packaging._base import DatasetProcessorError, session_root

    with pytest.raises(DatasetProcessorError, match="source path"):
        session_root(_dataset_with_stream_path(None))
