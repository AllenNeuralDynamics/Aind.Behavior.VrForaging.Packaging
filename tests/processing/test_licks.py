"""Tests for ``LicksProcessor._lick_onsets_from_state``.

The lick computation is exercised directly on an in-memory boolean lick-state
series (a ``channel0`` column of ``True``/``False`` values), with no dataset
involved. The two behaviors that are easy to get wrong are covered:

* repeating/duplicate raw values being collapsed to distinct transitions, and
* refractory-period filtering of spurious double-detections.

Each fixture is a small ``(time, channel0)`` table so the expected result can
be read off by hand.
"""

import pandas as pd

from aind_behavior_vr_foraging_packaging.processing._licks import LicksProcessor


def _state(samples: list[tuple[float, bool]]) -> pd.Series:
    """Build a boolean ``channel0`` lick-state series from ``(time, bool)`` samples."""
    times = [t for t, _ in samples]
    values = [v for _, v in samples]
    return pd.DataFrame({"channel0": values}, index=times)["channel0"]


def _compute(samples: list[tuple[float, bool]], *, refractory_period_s: float | None) -> pd.Series:
    return LicksProcessor._lick_onsets_from_state(_state(samples), refractory_period_s)


def _as_pairs(series: pd.Series) -> list[tuple[float, bool]]:
    """Render a result as ``[(time, bool), ...]`` for by-hand comparison."""
    return list(zip([round(float(t), 6) for t in series.index], [bool(v) for v in series.values]))


class TestRepeatingValues:
    """Only genuine state changes survive; consecutive duplicates collapse."""

    def test_consecutive_duplicates_collapsed(self) -> None:
        # state holds True,True (corrupted onset->onset), then False,False, then True,False.
        # only the transitions at 0.0, 0.2, 0.4, 0.5 should remain.
        samples = [(0.0, True), (0.1, True), (0.2, False), (0.3, False), (0.4, True), (0.5, False)]
        result = _compute(samples, refractory_period_s=None)
        assert _as_pairs(result) == [(0.0, True), (0.2, False), (0.4, True), (0.5, False)]
        assert result.name == "IsLickOnset"
        assert result.dtype == bool

    def test_all_false_has_no_onsets(self) -> None:
        # never any lick -> empty series (no onset to anchor on).
        result = _compute([(0.0, False), (0.1, False), (0.2, False)], refractory_period_s=None)
        assert len(result) == 0
        assert result.name == "IsLickOnset"

    def test_leading_offset_trimmed_to_first_onset(self) -> None:
        # series opens with an offset (False); result must start on the first onset.
        samples = [(0.0, False), (0.10, True), (0.15, False), (0.30, True)]
        result = _compute(samples, refractory_period_s=None)
        assert _as_pairs(result) == [(0.10, True), (0.15, False), (0.30, True)]


class TestRefractoryPeriod:
    """Onsets too close to the previous onset are dropped with their offset."""

    def test_spurious_double_detection_removed(self) -> None:
        # onsets at 0.00, 0.03 (only 0.03 s after -> spurious), 0.20.
        # the 0.03 onset AND its 0.04 offset are removed.
        samples = [(0.00, True), (0.02, False), (0.03, True), (0.04, False), (0.20, True), (0.25, False)]
        result = _compute(samples, refractory_period_s=0.05)
        assert _as_pairs(result) == [(0.00, True), (0.02, False), (0.20, True), (0.25, False)]

    def test_refractory_disabled_keeps_all(self) -> None:
        # same data, but refractory off -> nothing is removed.
        samples = [(0.00, True), (0.02, False), (0.03, True), (0.04, False), (0.20, True), (0.25, False)]
        result = _compute(samples, refractory_period_s=None)
        assert _as_pairs(result) == [
            (0.00, True),
            (0.02, False),
            (0.03, True),
            (0.04, False),
            (0.20, True),
            (0.25, False),
        ]

    def test_gap_equal_to_refractory_is_kept(self) -> None:
        # gap of exactly the refractory period is NOT a violation (criterion is strict <).
        samples = [(0.00, True), (0.005, False), (0.05, True), (0.06, False)]
        result = _compute(samples, refractory_period_s=0.05)
        assert _as_pairs(result) == [(0.00, True), (0.005, False), (0.05, True), (0.06, False)]

    def test_two_independent_violations_removed_in_one_pass(self) -> None:
        # two separate spurious pairs, each removed; the legitimate licks survive.
        samples = [
            (0.00, True),
            (0.01, False),  # lick A (kept)
            (0.02, True),
            (0.03, False),  # spurious 0.02 s after A -> removed
            (0.50, True),
            (0.51, False),  # lick B (kept)
            (0.52, True),
            (0.53, False),  # spurious 0.02 s after B -> removed
        ]
        result = _compute(samples, refractory_period_s=0.05)
        assert _as_pairs(result) == [(0.00, True), (0.01, False), (0.50, True), (0.51, False)]
