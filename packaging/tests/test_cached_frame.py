"""Unit tests for the opt-in ``cached_frame`` decorator (``_base.py``)."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from aind_behavior_vr_foraging_packaging._base import AbstractProcessor, cached_frame


class _Cached(AbstractProcessor):
    """Decorated: ``_compute`` runs at most once per instance."""

    def __init__(self) -> None:
        ds = MagicMock()
        ds.version = "0.6.0"
        super().__init__(ds)
        self.calls = 0

    @cached_frame
    def _compute(self) -> pd.DataFrame:
        self.calls += 1
        return pd.DataFrame({"x": [1, 2, 3]})


class _Uncached(AbstractProcessor):
    """Undecorated: the default, recomputes every call."""

    def __init__(self) -> None:
        ds = MagicMock()
        ds.version = "0.6.0"
        super().__init__(ds)
        self.calls = 0

    def _compute(self) -> pd.DataFrame:
        self.calls += 1
        return pd.DataFrame({"x": [1, 2, 3]})


class _Failing(AbstractProcessor):
    def __init__(self) -> None:
        ds = MagicMock()
        ds.version = "0.6.0"
        super().__init__(ds)
        self.calls = 0

    @cached_frame
    def _compute(self) -> pd.DataFrame:
        self.calls += 1
        raise ValueError("boom")


class TestCaching:
    def test_compute_runs_once_across_repeated_calls(self):
        """The point of the decorator: --write-nwb re-enters compute() via
        nwbize(), and that must not re-parse the underlying streams."""
        proc = _Cached()
        proc.compute()
        proc.compute()
        proc.compute()
        assert proc.calls == 1

    def test_returned_frames_are_equal_in_content(self):
        proc = _Cached()
        pd.testing.assert_frame_equal(proc.compute(), proc.compute())

    def test_provenance_still_stamped_on_every_call(self):
        proc = _Cached()
        for _ in range(2):
            df = proc.compute()
            assert df.attrs["dataset_version"] == "0.6.0"
            assert df.attrs["processor"] == "_Cached"


class TestNoSharedState:
    """``nwbize``'s documented no-shared-state guarantee must survive caching."""

    def test_each_call_returns_a_distinct_object(self):
        proc = _Cached()
        assert proc.compute() is not proc.compute()

    def test_mutating_a_returned_frame_does_not_leak_into_the_cache(self):
        proc = _Cached()
        first = proc.compute()
        first["injected"] = 99
        assert "injected" not in proc.compute().columns

    def test_mutating_a_returned_frame_does_not_leak_into_another_caller(self):
        proc = _Cached()
        a, b = proc.compute(), proc.compute()
        a.loc[0, "x"] = 999
        assert b.loc[0, "x"] == 1


class TestOptInAndScope:
    def test_undecorated_processor_still_recomputes(self):
        """Caching is opt-in — AbstractProcessor must not impose it."""
        proc = _Uncached()
        proc.compute()
        proc.compute()
        assert proc.calls == 2

    def test_cache_is_per_instance_not_per_class(self):
        """Processors are rebuilt per session by create_processors(), so the
        cache dies with the session and cannot go stale across datasets."""
        a, b = _Cached(), _Cached()
        a.compute()
        b.compute()
        assert a.calls == 1
        assert b.calls == 1
        assert a.__dict__["_frame_cache"] is not b.__dict__["_frame_cache"]

    def test_failure_is_not_cached_and_retries(self):
        proc = _Failing()
        for _ in range(2):
            with pytest.raises(ValueError, match="boom"):
                proc.compute()
        assert proc.calls == 2
