from typing import List, Optional

from pydantic import BaseModel, Field


class Site(BaseModel):
    """A model representing a virtual site in the VR foraging task."""

    start_time: float = Field(description="Start time, in software, for this site. (unit: second)")
    stop_time: float = Field(description="Stop time, in software, for this site. (unit: second)")
    start_position: float = Field(
        description="Start coordinate for this site in the VR environment. (unit: centimeter)"
    )
    length: float = Field(description="The length of the site. (unit: centimeter)")
    site_label: str = Field(description="Label of the site")
    friction: float = Field(description="Assigned friction for the site. (unit: percentage)")
    patch_label: str = Field(description="Patch type name")
    odor_concentration: List[float] = Field(
        description="An array representing the concentration levels of each odor channels. (unit: percentage)"
    )
    odor_onset_time: Optional[float] = Field(
        default=None, description="Time of odor onset. Will be null if no odor was delivered. (unit: second)"
    )
    reward_onset_time: Optional[float] = Field(
        default=None, description="Time when reward was delivered. (unit: second)"
    )
    reward_amount: Optional[float] = Field(default=None, description="Amount of reward delivered. (unit: milliliter)")
    reward_probability: Optional[float] = Field(
        default=None,
        description="Reward probability at the time of the reward delivery. Will be null if the reward is not sampled (e.g. has_choice is False). (unit: percentage)",
    )
    reward_available: Optional[float] = Field(
        default=None,
        description="Reward left at the time of reward delivery. Will be null if the reward is not sampled (e.g. has_choice is False). (unit: milliliter)",
    )
    has_reward: Optional[bool] = Field(default=None, description="Boolean whether reward was delivered, bool.")
    has_forced_rewards: bool = Field(
        default=False,
        description="Whether a forced/manual reward was delivered in this site interval. See events table (event_name='ManualWaterDelivery') for exact times.",
    )
    choice_cue_time: Optional[float] = Field(
        default=None,
        description="Time when choice cue was delivered. Also can be considered the stop cue. The choice tone is delivered when a stop is successful. (unit: second)",
    )
    has_choice: Optional[bool] = Field(default=None, description="Defines whether a choice occurred in the site.")
    reward_delay_duration: Optional[float] = Field(
        default=None, description="reward_onset_time - choice_cue_time. (unit: second)"
    )
    has_waited_reward_delay: Optional[bool] = Field(
        default=None,
        description="Boolean whether the mouse successfully waited through the reward delay to get the reward. Will be null if has_choice is false.",
    )
    # While this variable should ideally be called "stop_time", NWB reserves the names "start_time" and "stop_time".
    last_stop_time: Optional[float] = Field(
        default=None,
        description="Timestamp of the last stop (IsStopped transition to True) before the choice cue in this trial. Will be null if no choice occurred. (unit: second)",
    )
    last_stop_duration: Optional[float] = Field(
        default=None,
        description="Duration from last_stop_time to choice_cue_time. Will be null if no choice occurred. (unit: second)",
    )
    velocity_at_last_stop: Optional[float] = Field(
        default=None,
        description="Animal velocity at the timestamp closest to last_stop_time. Will be null if last_stop_time is null. (unit: cm/s)",
    )
    site_index: int = Field(description="Site number within the session")
    patch_index: int = Field(description="Patch number within the session")
    block_index: int = Field(description="Block number within the session")

    site_index_in_patch: int = Field(description="Site number within the patch")
    site_index_in_block: int = Field(description="Site number within the block")
    site_index_by_type: int = Field(description="Site number only counting sites of the same type (e.g. RewardSite)")
    site_index_in_patch_by_type: int = Field(
        description="Same as site_in_patch_index but only counting sites of the same type (e.g. RewardSite)"
    )
    site_index_in_block_by_type: int = Field(
        description="Same as site_in_block_index but only counting sites of the same type (e.g. RewardSite)"
    )

    patch_index_by_type: int = Field(description="Patch number only counting patches of the same label")
    patch_index_in_block: int = Field(description="Patch number within the block")
    patch_index_in_block_by_type: int = Field(
        description="Same as patch_in_block_index but only counting patches of the same label"
    )
