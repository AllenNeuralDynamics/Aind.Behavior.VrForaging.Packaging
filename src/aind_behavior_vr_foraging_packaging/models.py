import datetime
from typing import Any

from pydantic import BaseModel, Field, Json


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
    odor_concentration: list[float] = Field(
        description="An array representing the concentration levels of each odor channels. (unit: percentage)"
    )
    odor_onset_time: float | None = Field(
        default=None, description="Time of odor onset. Will be null if no odor was delivered. (unit: second)"
    )
    reward_onset_time: float | None = Field(default=None, description="Time when reward was delivered. (unit: second)")
    reward_amount: float | None = Field(default=None, description="Amount of reward delivered. (unit: milliliter)")
    reward_probability: float | None = Field(
        default=None,
        description="Reward probability at the time of the reward delivery. Will be null if the reward is not sampled (e.g. has_choice is False). (unit: percentage)",
    )
    reward_available: float | None = Field(
        default=None,
        description="Reward left at the time of reward delivery. Will be null if the reward is not sampled (e.g. has_choice is False). (unit: milliliter)",
    )
    has_reward: bool | None = Field(default=None, description="Boolean whether reward was delivered, bool.")
    has_forced_rewards: bool = Field(
        default=False,
        description="Whether a forced/manual reward was delivered in this site interval. See events table (event_name='ManualWaterDelivery') for exact times.",
    )
    choice_cue_time: float | None = Field(
        default=None,
        description="Time when choice cue was delivered. Also can be considered the stop cue. The choice tone is delivered when a stop is successful. (unit: second)",
    )
    has_choice: bool | None = Field(default=None, description="Defines whether a choice occurred in the site.")
    reward_delay_duration: float | None = Field(
        default=None, description="reward_onset_time - choice_cue_time. (unit: second)"
    )
    has_waited_reward_delay: bool | None = Field(
        default=None,
        description="Boolean whether the mouse successfully waited through the reward delay to get the reward. Will be null if has_choice is false.",
    )
    # While this variable should ideally be called "stop_time", NWB reserves the names "start_time" and "stop_time".
    last_stop_time: float | None = Field(
        default=None,
        description="Timestamp of the last stop (IsStopped transition to True) before the choice cue in this trial. Will be null if no choice occurred. (unit: second)",
    )
    last_stop_duration: float | None = Field(
        default=None,
        description="Duration from last_stop_time to choice_cue_time. Will be null if no choice occurred. (unit: second)",
    )
    velocity_at_last_stop: float | None = Field(
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


class SessionMetadata(BaseModel):
    """One-row model capturing session-level identity metadata.

    Used by :class:`~aind_behavior_vr_foraging_packaging.processing.SessionMetadataProcessor`
    to type-check and document the ``session.parquet`` output.
    """

    session_id: str = Field(
        description="Session directory name (AIND naming convention); the join key for every other table.",
    )
    subject_id: str = Field(
        description="Subject identifier, from the contraqctor Behavior/InputSchemas/Session stream.",
    )
    date: datetime.datetime = Field(
        description=(
            "Session start, as recorded. Timezone-aware for current sessions; legacy sessions "
            "carry no offset and stay naive."
        ),
    )
    dataset_version: str = Field(
        description="Dataset schema version recorded in the session (from tasklogic_input.json).",
    )
    data_contract_version: str = Field(
        description="Version of the aind-behavior-vr-foraging data-contract library used to parse this session.",
    )
    packaging_version: str = Field(
        description="Version of the aind-behavior-vr-foraging-packaging library that produced this output.",
    )
    session: Json[Any] = Field(
        description="The Session model instance used for the session.",
    )
    rig: Json[Any] = Field(
        description="The Rig model instance used for the session.",
    )
    task_logic: Json[Any] = Field(
        description="The Task Logic model instance used for the session.",
    )
    curriculum_enabled: bool | None = Field(
        default=None,
        description=(
            "Whether the session's TrainerState reports the subject as being on-curriculum. "
            "Null when the session has no trainer_state.json (curriculum tracking is optional)."
        ),
    )
    curriculum_name: str | None = Field(
        default=None,
        description="Name of the curriculum recorded in the session's TrainerState, if any.",
    )
    curriculum_stage_name: str | None = Field(
        default=None,
        description="Name of the curriculum stage recorded in the session's TrainerState, if any.",
    )
    trainer_state: Json[Any] | None = Field(
        default=None,
        description=(
            "Raw TrainerState payload from the session's behavior/trainer_state.json, verbatim "
            "(includes active_policies), for discoverability without a second pass over the "
            "dataset. Null when the session has no trainer_state.json."
        ),
    )
