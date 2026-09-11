"""Round-trip test: ``trials`` parquet from pandas vs. from the NWB trials table.

The trial table is written two ways:

1. ``TrialTableProcessor.compute()`` → parquet (the pipeline's ``trials.parquet``).
2. ``TrialTableProcessor.nwbize()`` → NWB ``trials`` table → ``to_dataframe()`` → parquet.

Both describe the same sites, so the two parquet files must be identical *down to
their SHA-256*. Anything that silently drifts between the tabular and the NWB
representation — a dropped column, a reordered column, a widened dtype, a
``None`` that stops meaning "missing", a list column that degrades to a string —
changes the bytes and fails this test.

Only ``process_to_sites`` is stubbed (it is what needs a full raw session on
disk; see ``tests/integration/`` for that). ``_compute``, ``compute``,
``nwbize`` and ``pipeline._write_parquet`` all run for real.

Two normalizations are applied to the NWB-derived frame before writing, both
because NWB genuinely does not carry the information rather than because the
data differs:

* ``to_dataframe()`` returns the TimeIntervals row ids as the index; the
  tabular output uses a plain ``RangeIndex``.
* ``df.attrs`` provenance (package/dataset versions) is stamped by
  ``AbstractProcessor.compute``; the NWB trials table has no equivalent slot.
"""

import hashlib
import typing as t
from pathlib import Path

import aind_behavior_vr_foraging
import numpy as np
import pandas as pd
import pytest
from contraqctor.contract import Dataset, DataStream, DataStreamCollection

from aind_behavior_vr_foraging_packaging.models import Site
from aind_behavior_vr_foraging_packaging.pipeline import _write_parquet
from aind_behavior_vr_foraging_packaging.processing._trial_table import TrialTableProcessor

#: Pin the synthetic dataset to the installed data-contract version so the
#: processor takes the current (non-legacy) path without logging a mismatch.
DATASET_VERSION = aind_behavior_vr_foraging.__semver__


class _InMemoryStream(DataStream):
    """Serves its ``reader_params`` verbatim, so no session directory is needed."""

    def _reader(self, params: t.Any) -> t.Any:
        return params


def _rig_only_dataset() -> Dataset:
    """Minimal dataset exposing just ``Behavior::InputSchemas::Rig``.

    That single stream is all ``TrialTableProcessor.__init__`` touches, so the
    real constructor runs against it.
    """
    rig = _InMemoryStream("Rig", reader_params={})
    input_schemas = DataStreamCollection("InputSchemas", [rig])
    behavior = DataStreamCollection("Behavior", [input_schemas])
    return Dataset("SyntheticTrialDataset", version=DATASET_VERSION, data_streams=[behavior])


def _site(
    index: int,
    *,
    site_label: str,
    patch_label: str,
    odor_concentration: list[float],
    rewarded: bool,
    choice: bool,
    waited: bool | None,
) -> Site:
    """One site, with every optional field either populated or left missing."""
    start = float(index)
    return Site(
        start_time=start,
        stop_time=start + 0.9,
        start_position=30.0 * index,
        length=30.0,
        site_label=site_label,
        friction=0.25 if index % 3 == 0 else 0.0,
        patch_label=patch_label,
        odor_concentration=odor_concentration,
        # np.nan and None both mean "missing" here; using both exercises the
        # nwbize() None → np.nan coercion as well as the plain-NaN path.
        odor_onset_time=start + 0.05 if site_label == "OdorSite" else np.nan,
        reward_onset_time=start + 0.30 if rewarded else np.nan,
        reward_amount=5.0 if rewarded else None,
        reward_probability=0.9 if rewarded else None,
        reward_available=2.5 - 0.1 * index if rewarded else None,
        has_reward=rewarded,
        has_forced_rewards=index == 4,
        choice_cue_time=start + 0.20 if choice else np.nan,
        has_choice=choice,
        reward_delay_duration=0.10 if (rewarded and choice) else np.nan,
        has_waited_reward_delay=waited,
        last_stop_time=start + 0.18 if choice else None,
        last_stop_duration=0.02 if choice else None,
        velocity_at_last_stop=1.5 if choice else None,
        site_index=index,
        patch_index=index // 3,
        block_index=index // 6,
        site_index_in_patch=index % 3,
        site_index_in_block=index % 6,
        site_index_by_type=index // 2,
        site_index_in_patch_by_type=(index % 3) // 2,
        site_index_in_block_by_type=(index % 6) // 2,
        patch_index_by_type=index // 3,
        patch_index_in_block=(index // 3) % 2,
        patch_index_in_block_by_type=(index // 3) % 2,
    )


def _synthetic_sites() -> list[Site]:
    """Six sites spanning rewarded/unrewarded, choice/no-choice and missing bools."""
    specs = [
        # (site_label, patch_label, odor, rewarded, choice, waited)
        ("OdorSite", "A", [0.0, 1.0, 0.0], True, True, True),
        ("InterSite", "A", [0.0, 0.0, 0.0], False, False, None),
        ("OdorSite", "A", [0.0, 0.5, 0.25], False, True, False),
        ("OdorSite", "B", [1.0, 0.0, 0.0], True, True, True),
        ("InterSite", "B", [0.0, 0.0, 0.0], False, False, None),
        ("OdorSite", "B", [0.0, 0.0, 0.75], True, False, None),
    ]
    return [
        _site(
            i,
            site_label=site_label,
            patch_label=patch_label,
            odor_concentration=odor,
            rewarded=rewarded,
            choice=choice,
            waited=waited,
        )
        for i, (site_label, patch_label, odor, rewarded, choice, waited) in enumerate(specs)
    ]


class _StubbedTrialTableProcessor(TrialTableProcessor):
    """Real processor, with only the raw-session parsing replaced by fixed sites."""

    def __init__(self, sites: list[Site]) -> None:
        super().__init__(_rig_only_dataset())
        self._sites = sites

    def process_to_sites(self) -> list[Site]:
        return self._sites


def _nwb_trials_as_dataframe(nwb_file: t.Any, attrs: dict) -> pd.DataFrame:
    """Read the NWB ``trials`` table back into the tabular output's shape.

    Undoes only what NWB cannot represent: the ``id`` index and ``df.attrs``
    provenance. Column order, dtypes and values are left exactly as NWB
    returns them, so any drift there still breaks the checksum.
    """
    df = nwb_file.trials.to_dataframe().reset_index(drop=True)
    df.attrs.update(attrs)
    return df


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Map ``None`` to ``np.nan`` in object columns for value-level comparison.

    Parquet encodes both as a null, so they are indistinguishable on disk — but
    ``assert_frame_equal`` treats them as different. Applied to both frames so
    the comparison uses the same notion of "missing" the checksum does.
    """
    df = df.copy()
    for column in df.columns:
        if pd.api.types.is_object_dtype(df[column]):
            df[column] = df[column].map(lambda v: np.nan if v is None else v)
    return df


def _differing_columns(left: pd.DataFrame, right: pd.DataFrame) -> list[str]:
    """Column names whose values differ, for a readable checksum-failure message."""
    if list(left.columns) != list(right.columns):
        return sorted(set(left.columns) ^ set(right.columns)) or ["<column order>"]
    left, right = _normalize_missing(left), _normalize_missing(right)
    return [c for c in left.columns if not left[c].equals(right[c])]


@pytest.fixture
def roundtrip(tmp_path: Path, nwb_file: t.Any) -> dict:
    """Write both parquet files and return their paths, frames and digests."""
    processor = _StubbedTrialTableProcessor(_synthetic_sites())

    tabular = processor.compute()
    tabular_path = tmp_path / "trials_tabular.parquet"
    _write_parquet(tabular, tabular_path)

    processor.nwbize(nwb_file)
    from_nwb = _nwb_trials_as_dataframe(nwb_file, tabular.attrs)
    nwb_path = tmp_path / "trials_from_nwb.parquet"
    _write_parquet(from_nwb, nwb_path)

    return {
        "tabular": tabular,
        "from_nwb": from_nwb,
        "tabular_path": tabular_path,
        "nwb_path": nwb_path,
    }


class TestTrialsParquetRoundTrip:
    def test_checksums_match(self, roundtrip: dict) -> None:
        """The two parquet files are byte-identical."""
        tabular_digest = _sha256(roundtrip["tabular_path"])
        nwb_digest = _sha256(roundtrip["nwb_path"])
        assert tabular_digest == nwb_digest, (
            f"trials parquet differs between the pandas and NWB paths:\n"
            f"  tabular:  {tabular_digest}\n"
            f"  from NWB: {nwb_digest}\n"
            f"  differing columns: {_differing_columns(roundtrip['tabular'], roundtrip['from_nwb']) or '<none — metadata differs>'}"
        )

    def test_values_match(self, roundtrip: dict) -> None:
        """Every cell survives the NWB round trip.

        ``None`` and ``np.nan`` are normalized to one another first: ``nwbize``
        writes ``None`` as ``np.nan``, and parquet stores both as a null, so the
        distinction is not one the on-disk table can carry.
        """
        pd.testing.assert_frame_equal(
            _normalize_missing(roundtrip["tabular"]),
            _normalize_missing(roundtrip["from_nwb"]),
        )

    def test_columns_and_order_preserved(self, roundtrip: dict) -> None:
        """NWB keeps every trial column, in the order the tabular output uses."""
        assert list(roundtrip["from_nwb"].columns) == list(roundtrip["tabular"].columns)
        assert list(roundtrip["tabular"].columns) == list(Site.model_fields)

    def test_dtypes_preserved(self, roundtrip: dict) -> None:
        """Round-tripping through NWB must not widen or object-ify any column."""
        assert roundtrip["from_nwb"].dtypes.to_dict() == roundtrip["tabular"].dtypes.to_dict()

    def test_list_column_survives(self, roundtrip: dict) -> None:
        """``odor_concentration`` stays a per-row list of floats, not a string or scalar."""
        for source in ("tabular", "from_nwb"):
            values = roundtrip[source]["odor_concentration"]
            assert all(isinstance(v, list) for v in values), source
        assert [list(v) for v in roundtrip["from_nwb"]["odor_concentration"]] == [
            list(v) for v in roundtrip["tabular"]["odor_concentration"]
        ]

    def test_checksum_is_sensitive_to_data(self, tmp_path: Path, roundtrip: dict) -> None:
        """Guard against a vacuous pass: perturbing one value must change the digest.

        Without this, a bug that emptied both frames would still "match".
        """
        perturbed = roundtrip["tabular"].copy()
        perturbed.attrs.update(roundtrip["tabular"].attrs)
        perturbed.loc[0, "start_position"] += 1e-6
        perturbed_path = tmp_path / "trials_perturbed.parquet"
        _write_parquet(perturbed, perturbed_path)

        assert _sha256(perturbed_path) != _sha256(roundtrip["tabular_path"])
        assert not roundtrip["tabular"].empty
