import logging
import typing as t

import contraqctor.contract
import numpy as np
import pandas as pd
import semver
from pydantic import BaseModel

from .._base import AbstractProcessor
from ._trial_table import DatasetProcessorError, TrialTableProcessor

logger = logging.getLogger(__name__)

_LEGACY_OLFACTOMETER_CHANNEL_COUNT = 3


class LegacyTrialTableProcessor(TrialTableProcessor):
    """TrialTableProcessor for VR foraging datasets with schema version < 0.6.0.

    Key differences from TrialTableProcessor:
    - Block stream is optional; falls back to ActivePatch stream when absent.
    - Olfactometer always exposes 3 odor channels (channel 3 is the carrier).
    - OdorSpecification uses legacy format: {"index": int, "concentration": float}.
    - Choice cue is read from HarpBehavior.PwmStart with dynamic port detection
      (PwmDO1 in v0.3, PwmDO2 in v0.4+; port is inferred from which channel is active).
    - PatchStateAtReward is reconstructed from split PatchReward*.json files when absent.
    - HarpTreadmill is optional; friction defaults to 0 when absent.
    - IsStopped and velocity streams are absent; last_stop_* fields are always None.

    Note on NWB table naming: re-ingested legacy datasets use the current
    full-path naming convention for DynamicTables (e.g., "Behavior.HarpBehavior.PwmStart"),
    not the legacy stripped names (e.g., "HarpBehavior.PwmStart").
    """

    def __init__(self, dataset: contraqctor.contract.Dataset, *, raise_on_error: bool = False) -> None:
        # Bypass TrialTableProcessor.__init__ — InputSchemas/Rig is not present in legacy datasets.
        AbstractProcessor.__init__(self, dataset, raise_on_error=raise_on_error)
        if self.dataset_version >= semver.Version(major=0, minor=6, patch=0):
            raise DatasetProcessorError(
                f"LegacyTrialTableProcessor only supports datasets < 0.6.0, got {self.dataset_version}. "
                "Use TrialTableProcessor for current datasets."
            )
        if self.dataset_version != self.parser_version:
            logger.warning(
                "Dataset version %s does not match parser version %s",
                self.dataset_version,
                self.parser_version,
            )

    @staticmethod
    def _load_blocks(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:  # type: ignore[override]
        try:
            blocks = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("Block").load().data)
            blocks["block_count"] = range(len(blocks))
        except KeyError:
            # No Block stream: treat the whole session as a single block (block 0).
            # Using range(n_patches) would create spurious blocks — if there is no
            # block information, the safest assumption is one block.
            logger.info("No Block stream found; treating entire session as block 0.")
            blocks = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("ActivePatch").load().data)
            blocks["block_count"] = 0
        return blocks

    @staticmethod
    def _parse_speaker_choice_feedback(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:  # type: ignore[override]
        # The choice-cue speaker channel shifted between rig generations:
        # v0.3 used PwmDO1, v0.4+ used PwmDO2. Detect the active port dynamically
        # rather than hardcoding, so we handle both without a version branch.
        pwm = dataset.at("Behavior").at("HarpBehavior").load().at("PwmStart").load().data
        writes = pwm[pwm["MessageType"] == "WRITE"]
        do_cols = [c for c in writes.columns if c.startswith("PwmDO")]
        active_cols = [c for c in do_cols if writes[c].any()]
        if not active_cols:
            logger.warning("No active PwmDO channel found in PwmStart; choice cue times will be NaN.")
            return writes.iloc[:0]  # empty DataFrame with same columns
        if len(active_cols) > 1:
            logger.warning("Multiple active PwmDO channels found (%s); using %s.", active_cols, active_cols[0])
        col = active_cols[0]
        logger.debug("Using %s as choice-cue channel.", col)
        return writes[writes[col]]

    def _get_olfactometer_channel_count(self, dataset: contraqctor.contract.Dataset) -> int:
        return _LEGACY_OLFACTOMETER_CHANNEL_COUNT

    def _process_odor_concentration(self, odor_specification: BaseModel | dict | None, n_channels: int) -> list[float]:
        concentration = [0.0] * n_channels
        if odor_specification is None:
            return concentration
        if isinstance(odor_specification, BaseModel):
            odor_specification = odor_specification.model_dump()
        index = odor_specification.get("index")
        if not isinstance(index, int):
            raise TypeError(f"Legacy odor_specification.index must be an int, got {type(index).__name__}")
        concentration[index] = float(odor_specification.get("concentration", 0.0))
        return concentration

    def _parse_patch_state_at_reward(self, dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        # Try the unified stream first (introduced in 0.6.0).
        try:
            patches_state_at_reward = dataset.at("Behavior").at("SoftwareEvents").at("PatchStateAtReward").load().data
            expanded = pd.json_normalize(patches_state_at_reward["data"])
            expanded.index = patches_state_at_reward.index
            return patches_state_at_reward.join(expanded)
        except (KeyError, FileNotFoundError):
            pass

        # Fall back to separate streams present in 0.3.x – 0.5.x.
        try:
            amount_df = t.cast(
                pd.DataFrame,
                dataset.at("Behavior").at("SoftwareEvents").at("PatchRewardAmount").load().data,
            )
            available_df = t.cast(
                pd.DataFrame,
                dataset.at("Behavior").at("SoftwareEvents").at("PatchRewardAvailable").load().data,
            )
            prob_df = t.cast(
                pd.DataFrame,
                dataset.at("Behavior").at("SoftwareEvents").at("PatchRewardProbability").load().data,
            )
            active_patch_df = t.cast(
                pd.DataFrame,
                dataset.at("Behavior").at("SoftwareEvents").at("ActivePatch").load().data,
            )
        except (KeyError, FileNotFoundError) as exc:
            logger.warning("Could not load split reward streams (%s); reward metadata will be NaN.", exc)
            return pd.DataFrame(columns=["PatchId", "Amount", "Probability", "Available"])

        # The three split streams fire at the same harp frame — align on Amount's index.
        result = pd.DataFrame(
            {
                "Amount": amount_df["data"].values,
                "Available": available_df["data"].values,
                "Probability": prob_df["data"].values,
            },
            index=amount_df.index,
        )

        # Assign PatchId from the most recent ActivePatch event before each reward.
        patch_state_index = active_patch_df["data"].apply(
            lambda d: d.get("state_index", np.nan) if isinstance(d, dict) else np.nan
        )
        patch_lookup = patch_state_index.rename_axis("patch_time").reset_index(name="state_index")
        reward_times = result.rename_axis("reward_time").reset_index()[["reward_time"]]
        merged = pd.merge_asof(
            reward_times, patch_lookup, left_on="reward_time", right_on="patch_time", direction="backward"
        )
        result["PatchId"] = merged["state_index"].values

        return result

    @staticmethod
    def _parse_friction(dataset: contraqctor.contract.Dataset) -> pd.Series:  # type: ignore[override]
        try:
            d = dataset.at("Behavior").at("HarpTreadmill").at("BrakeCurrentSetPoint").load().data
            return d.loc[d["MessageType"] == "WRITE", "BrakeCurrentSetPoint"]
        except (KeyError, FileNotFoundError):
            logger.info("HarpTreadmill not found; friction will default to 0 for all sites.")
            return pd.Series(dtype=float)

    @staticmethod
    def _parse_is_stopped(dataset: contraqctor.contract.Dataset) -> None:  # type: ignore[override]
        return None

    def _parse_velocity(self, dataset: contraqctor.contract.Dataset) -> None:  # type: ignore[override]
        return None
