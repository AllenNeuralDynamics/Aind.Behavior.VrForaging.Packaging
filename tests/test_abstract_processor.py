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
