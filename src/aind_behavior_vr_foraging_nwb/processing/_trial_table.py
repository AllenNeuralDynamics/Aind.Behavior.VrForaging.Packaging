import logging
import typing as t

import contraqctor
import numpy as np
import pandas as pd
import semver
from aind_behavior_vr_foraging.task_logic import OdorMixture
from contraqctor.contract.json import PydanticModel
from pydantic import BaseModel, TypeAdapter

from .._base import AbstractProcessor
from ..models import Site
from .helper import slice_by_index

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from aind_behavior_vr_foraging_nwb.nwb_file import NdxEventsNWBFile
else:
    NdxEventsNWBFile = t.Any


class DatasetProcessorError(Exception):
    pass


class TrialTableProcessor(AbstractProcessor):
    def __init__(self, dataset: contraqctor.contract.Dataset, *, raise_on_error: bool = False) -> None:
        super().__init__(dataset, raise_on_error=raise_on_error)

        if self.dataset_version != self.parser_version:
            logger.warning(
                "Dataset version %s does not match parser version %s", self.dataset_version, self.parser_version
            )
        self.rig_configuration = self._ensure_json_not_pydantic(self.dataset["Behavior"]["InputSchemas"]["Rig"].load())

    @staticmethod
    def _ensure_json_not_pydantic(d: t.Any) -> dict:
        if isinstance(d, BaseModel):
            return d.model_dump()
        return d

    @staticmethod
    def _parse_speaker_choice_feedback(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        speaker_choice = dataset.at("Behavior").at("HarpBehavior").load().at("PwmStart").load().data
        speaker_choice = speaker_choice[(speaker_choice["MessageType"] == "WRITE") & (speaker_choice["PwmDO2"])]
        return speaker_choice

    @staticmethod
    def _parse_water_delivery(dataset: contraqctor.contract.Dataset) -> pd.Series:
        water_delivery = dataset.at("Behavior").at("HarpBehavior").load().at("OutputSet").load().data
        water_delivery = water_delivery[(water_delivery["MessageType"] == "WRITE") & (water_delivery["SupplyPort0"])][
            "SupplyPort0"
        ]
        return water_delivery

    @staticmethod
    def _parse_odor_onset(dataset: contraqctor.contract.Dataset) -> pd.Series:
        odor_onset = dataset.at("Behavior").at("HarpOlfactometer").load().at("EndValveState").load().data
        odor_onset = odor_onset[odor_onset["MessageType"] == "WRITE"]["EndValve0"]
        odor_onset = odor_onset[(odor_onset) & (~odor_onset.shift(1, fill_value=False))]
        return odor_onset

    @staticmethod
    def _parse_continuous_patch_state(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        patches_state = dataset.at("Behavior").at("SoftwareEvents").at("PatchState").load().data
        expanded = pd.json_normalize(patches_state["data"])
        expanded.index = patches_state.index
        patches_state = patches_state.join(expanded)
        return patches_state

    def _parse_patch_state_at_reward(self, dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        if self.dataset_version < semver.Version(major=0, minor=6, patch=0):
            raise DatasetProcessorError("PatchStateAtReward is only available in dataset version 0.6.0 and above")
        # TODO this is likely something we want to overload for 0.5.x to work.
        patches_state_at_reward = dataset.at("Behavior").at("SoftwareEvents").at("PatchStateAtReward").load().data
        expanded = pd.json_normalize(patches_state_at_reward["data"])
        expanded.index = patches_state_at_reward.index
        patches_state_at_reward = patches_state_at_reward.join(expanded)
        return patches_state_at_reward

    @staticmethod
    def _parse_wait_reward_outcome(dataset: contraqctor.contract.Dataset) -> pd.Series:
        try:
            return dataset.at("Behavior").at("SoftwareEvents").at("WaitRewardOutcome").load().data
        except FileNotFoundError:
            return pd.Series(dtype=bool)

    @staticmethod
    def _parse_reward_metadata(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        reward_metadata = dataset.at("Behavior").at("SoftwareEvents").at("GiveReward").load().data
        return reward_metadata

    @staticmethod
    def _as_dict(d: contraqctor.contract.DataStream | PydanticModel | BaseModel | dict) -> dict:
        if isinstance(d, (PydanticModel, contraqctor.contract.DataStream)):
            d = t.cast(BaseModel | dict, d.data)
        if isinstance(d, dict):
            return d
        if isinstance(d, BaseModel):
            return d.model_dump()
        else:
            raise TypeError(f"Cannot convert type {type(d)} to dict")

    @staticmethod
    def _parse_friction(dataset: contraqctor.contract.Dataset) -> pd.DataFrame:
        d = dataset.at("Behavior").at("HarpTreadmill").at("BrakeCurrentSetPoint").load().data
        return d.loc[d["MessageType"] == "WRITE", "BrakeCurrentSetPoint"]

    def _get_olfactometer_channel_count(self, dataset: contraqctor.contract.Dataset) -> int:
        extra_olfs = getattr(self.rig_configuration, "harp_olfactometer_extension", None)
        n_extra_channels = 4 * len(extra_olfs) if extra_olfs is not None else 0
        return (
            3 + n_extra_channels
        )  # The channel 3 is always used as carrier, therefore only 3 odor channels are available.

    def _process_odor_concentration(self, odor_specification: BaseModel | dict | None, n_channels: int) -> list[float]:

        concentration = [0.0] * n_channels
        if odor_specification is None:
            return concentration
        odor_specification = self._ensure_json_not_pydantic(odor_specification)
        return TypeAdapter(OdorMixture).validate_python(odor_specification)

    def process(self, nwb_file: NdxEventsNWBFile) -> NdxEventsNWBFile:
        sites = self.process_to_sites()
        for field_name, field in Site.model_fields.items():
            if field_name in ["start_time", "stop_time"]:
                continue
            nwb_file.add_trial_column(name=field_name, description=field.description)

        for site in sites:
            trial_data = site.model_dump()
            # Replace None with np.nan
            trial_data = {k: (np.nan if v is None else v) for k, v in trial_data.items()}
            nwb_file.add_trial(**trial_data)
        return nwb_file

    def process_to_sites(self) -> list[Site]:
        """
        Processes sites, patches, and blocks from the dataset and merges them.
        Returns a DataFrame with merged information.
        """
        dataset = self.dataset
        odor_sites = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("ActiveSite").load().data)
        patches = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("ActivePatch").load().data)
        patches["patch_count"] = range(len(patches))
        blocks = t.cast(pd.DataFrame, dataset.at("Behavior").at("SoftwareEvents").at("Block").load().data)
        blocks["block_count"] = range(len(blocks))

        # Merge nearest patch (backward in time)
        merged = pd.merge_asof(
            odor_sites.sort_index(),
            patches[["data", "patch_count"]].rename(columns={"data": "patch_data"}).sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
            suffixes=("", "_patch"),
        )
        merged["patch_index"] = merged["patch_data"].apply(lambda d: d["state_index"])

        # Merge nearest block (backward in time)
        merged = pd.merge_asof(
            merged.sort_index(),
            blocks[["block_count"]].sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
        )

        choice_feedback = self._parse_speaker_choice_feedback(dataset)
        water_delivery = self._parse_water_delivery(dataset)
        reward_metadata = self._parse_reward_metadata(dataset)
        odor_onset = self._parse_odor_onset(dataset)
        patch_state_at_reward = self._parse_patch_state_at_reward(dataset)
        friction = self._parse_friction(dataset)
        olfactometer_channel_count = self._get_olfactometer_channel_count(dataset)
        wait_reward_outcome = self._parse_wait_reward_outcome(dataset)

        # Precompute all trial indices
        merged["site_label"] = merged["data"].apply(lambda d: d["label"])
        merged["patch_label"] = merged["patch_data"].apply(lambda d: d["label"])

        # Site-level indices
        merged["_site_index_in_patch"] = merged.groupby("patch_count").cumcount()
        merged["_site_index_in_block"] = merged.groupby("block_count").cumcount()
        merged["_site_index_by_type"] = merged.groupby("site_label").cumcount()
        merged["_site_index_in_patch_by_type"] = merged.groupby(["patch_count", "site_label"]).cumcount()
        merged["_site_index_in_block_by_type"] = merged.groupby(["block_count", "site_label"]).cumcount()

        # Patch-level indices (computed on patches, then mapped back to sites via patch_count)
        patches_with_blocks = pd.merge_asof(
            patches.sort_index(),
            blocks[["block_count"]].sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
        )
        patches_with_blocks["patch_label"] = patches_with_blocks["data"].apply(lambda d: d["label"])
        patches_with_blocks["_patch_index_in_block"] = patches_with_blocks.groupby("block_count").cumcount()
        patches_with_blocks["_patch_index_by_type"] = patches_with_blocks.groupby("patch_label").cumcount()
        patches_with_blocks["_patch_index_in_block_by_type"] = patches_with_blocks.groupby(
            ["block_count", "patch_label"]
        ).cumcount()
        merged = merged.join(
            patches_with_blocks.set_index("patch_count")[
                ["_patch_index_in_block", "_patch_index_by_type", "_patch_index_in_block_by_type"]
            ],
            on="patch_count",
        )

        # Only mutable states that requires trial-based
        current_friction = 0  # Keeps track of the last known friction. Sites with null friction will not update this.

        sites: list[Site] = []
        # We reject the last site because it may not have completed and would require custom logic to handle
        for i in range(len(merged) - 1):
            # We generally assume that all relevant events happen within the software-event derived timestamp intervals
            # Note this may not always be true depending on system jitter, but it is generally a safe assumption.
            # If you find edge cases where this is not true, submit an issue so we can investigate and improve the parser.

            this_timestamp = t.cast(float, merged.index[i])
            next_timestamp = t.cast(float, merged.index[i + 1])

            this_site = merged.iloc[i]["data"]
            this_patch = merged.iloc[i]["patch_data"]

            site_choice_feedback = slice_by_index(choice_feedback, this_timestamp, next_timestamp)
            assert len(site_choice_feedback) <= 1, "Multiple speaker choices in site interval"

            site_odor_onset = slice_by_index(odor_onset, this_timestamp, next_timestamp)

            this_friction = slice_by_index(friction, this_timestamp, next_timestamp)
            if not this_friction.empty:
                current_friction = this_friction.values[-1]

            site_patch_state_at_reward = slice_by_index(patch_state_at_reward, this_timestamp, next_timestamp)
            site_patch_state_at_reward = site_patch_state_at_reward[
                site_patch_state_at_reward["PatchId"] == merged.iloc[i]["patch_index"]
            ]
            assert len(site_patch_state_at_reward) <= 1, "Multiple patch states at reward in site interval"

            ##
            row = merged.iloc[i]

            choice_time: float = (
                t.cast(float, site_choice_feedback.index[0]) if not site_choice_feedback.empty else np.nan
            )

            if site_odor_onset.empty and this_site["odor_specification"] is not None:
                # Sometimes the timestamp for the odor onset arrives slightly before the site. We should investigate
                # but for now we just log a warning and use the site onset instead after checking if this is the issue
                odor_onset_before_site = odor_onset[
                    (odor_onset.index < this_timestamp) & (odor_onset.index >= this_timestamp - 0.002)
                ]  # we use a 2ms conservative window
                if odor_onset_before_site.empty:
                    if self.raise_on_error:
                        raise DatasetProcessorError("No odor onset found in site interval")
                    else:
                        logger.warning("No odor onset found in site interval")
                        odor_onset_time = np.nan
                else:
                    logger.warning("Odor onset found slightly (<2ms) before site interval, using site onset instead")
                    odor_onset_time = this_timestamp
            else:
                # we always take the first odor onset in case animal goes in and out
                odor_onset_time = t.cast(float, site_odor_onset.index[0]) if not site_odor_onset.empty else np.nan

            site_water_delivery = slice_by_index(water_delivery, this_timestamp, next_timestamp)
            reward_metadata_sliced = slice_by_index(reward_metadata, this_timestamp, next_timestamp)
            if reward_metadata_sliced.empty or bool(reward_metadata_sliced["data"].fillna(0).eq(0).all()):
                # Note: for None or 0 reward metadata there won't be a hardware water delivery event
                # However, if the experimenter manually triggered a reward around this time, we should not count that
                # as a reward for this site either, so we make an explicit decision to set reward_onset_time to nan
                reward_onset_time = np.nan
            else:
                if len(site_water_delivery) == 0:
                    if self.raise_on_error:
                        raise DatasetProcessorError(
                            "Valid reward metadata found but no water delivery in site interval"
                        )
                    else:
                        logger.error("Valid reward metadata found but no water delivery in site interval")
                        reward_onset_time = np.nan
                elif len(reward_metadata_sliced) > 1:
                    closest_index = site_water_delivery.index.get_indexer([this_timestamp], method="nearest")[0]
                    reward_onset_time = t.cast(float, site_water_delivery.index[closest_index])
                else:
                    reward_onset_time = (
                        t.cast(float, site_water_delivery.index[0]) if not site_water_delivery.empty else np.nan
                    )

            wait_reward_outcome_sliced = slice_by_index(wait_reward_outcome, this_timestamp, next_timestamp)
            has_waited_reward_delay = (
                wait_reward_outcome_sliced.iloc[0]["data"]["IsSuccessfulWait"]
                if not wait_reward_outcome_sliced.empty
                else None
            )

            site = Site(
                start_time=this_timestamp,
                stop_time=next_timestamp,
                start_position=this_site["start_position"],
                length=this_site["length"],
                site_label=str(this_site["label"]),
                friction=current_friction,
                patch_label=str(this_patch["label"]),
                odor_concentration=self._process_odor_concentration(
                    this_patch["odor_specification"], olfactometer_channel_count
                ),
                patch_index=row["patch_count"],
                patch_index_in_block=row["_patch_index_in_block"],
                patch_index_by_type=row["_patch_index_by_type"],
                patch_index_in_block_by_type=row["_patch_index_in_block_by_type"],
                site_index=i,
                site_index_in_patch=row["_site_index_in_patch"],
                site_index_in_block=row["_site_index_in_block"],
                site_index_by_type=row["_site_index_by_type"],
                site_index_in_patch_by_type=row["_site_index_in_patch_by_type"],
                site_index_in_block_by_type=row["_site_index_in_block_by_type"],
                odor_onset_time=odor_onset_time,
                reward_onset_time=reward_onset_time,
                reward_amount=np.nan
                if site_patch_state_at_reward.empty
                else site_patch_state_at_reward.iloc[0]["Amount"],
                reward_probability=np.nan
                if site_patch_state_at_reward.empty
                else site_patch_state_at_reward.iloc[0]["Probability"],
                reward_available=np.nan
                if site_patch_state_at_reward.empty
                else site_patch_state_at_reward.iloc[0]["Available"],
                has_reward=np.isnan(reward_onset_time) == False,  # noqa: E712
                choice_cue_time=choice_time,
                has_choice=not site_choice_feedback.empty,
                reward_delay_duration=reward_onset_time - choice_time
                if reward_onset_time is not np.nan and choice_time is not None
                else np.nan,
                has_waited_reward_delay=has_waited_reward_delay,
                block_index=row["block_count"],
            )
            sites.append(site)
        return sites
