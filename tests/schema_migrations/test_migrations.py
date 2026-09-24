"""Tests for explicit historical schema migrations."""

import copy
import math

from aind_behavior_vr_foraging_packaging.schema_migrations import migrate_documents


def _migrate(*, task_logic, rig=None, session=None, version="0.5.2"):
    return migrate_documents(
        source_version=version,
        session=session or {"root_path": "C:\\Data"},
        rig=rig or {},
        task_logic=task_logic,
    )


def test_migration_does_not_mutate_inputs_and_is_idempotent():
    session = {"root_path": "C:\\Data"}
    rig = {
        "screen": {"device_type": "Screen", "calibration": None},
        "calibration": {
            "water_valve": {
                "input": {"measurements": [1]},
                "output": {"slope": 2},
                "description": "water",
            }
        },
    }
    task = {"task_parameters": {"operation_control": {"audio_control": {"frequency": 10_000}}}}
    original = copy.deepcopy((session, rig, task))

    migrated = _migrate(session=session, rig=rig, task_logic=task)
    migrated_twice = migrate_documents(
        source_version="0.5.2",
        session=migrated[0],
        rig=migrated[1],
        task_logic=migrated[2],
    )

    assert (session, rig, task) == original
    assert migrated_twice == migrated
    assert migrated[1]["data_directory"] == "C:\\Data"
    assert migrated[1]["screen"]["device_type"] == "ScreenAssembly"
    assert "calibration" not in migrated[1]["screen"]
    assert migrated[1]["calibration"]["water_valve"]["measurements"] == [1]
    assert migrated[2]["task_parameters"]["operation_control"]["audio_control"]["frequency"] == 9_999


def test_flat_task_logic_is_wrapped_and_transition_matrix_is_unboxed():
    _, _, task = _migrate(
        version="0.3.0",
        task_logic={
            "schema_version": "0.3.0",
            "updaters": {},
            "operation_control": {},
            "task_mode_settings": {"task_mode": "FORAGING"},
            "environment_statistics": {
                "patches": [],
                "transition_matrix": {"data": [[1.0]]},
                "first_state": 0,
            },
        },
    )

    assert task["name"] == "AindVrForaging"
    assert "task_mode_settings" not in task
    block = task["task_parameters"]["environment"]["blocks"][0]
    assert block["environment_statistics"]["transition_matrix"] == [[1.0]]


def test_offset_percentage_is_converted_to_equivalent_gain():
    _, _, task = _migrate(
        task_logic={
            "task_parameters": {
                "updaters": {
                    "StopVelocityThreshold": {
                        "operation": "OffsetPercentage",
                        "increment": {"update": 0.0},
                        "decrement": {"update": -0.04},
                    }
                }
            }
        }
    )

    updater = task["task_parameters"]["updaters"]["StopVelocityThreshold"]
    assert updater["operation"] == "Gain"
    assert updater["increment"]["update"] == 1.0
    assert updater["decrement"]["update"] == 0.96


def test_legacy_power_reward_preserves_initial_value_and_decay_rate():
    _, _, task = _migrate(
        task_logic={
            "task_parameters": {
                "reward_specification": {
                    "reward_function": {
                        "amount": {"function_type": "ConstantFunction", "value": 5.0},
                        "probability": {
                            "function_type": "PowerFunction",
                            "minimum": 0.0,
                            "maximum": 0.9,
                            "a": 0.9,
                            "b": math.e,
                            "c": -0.1284,
                            "d": 0.0,
                        },
                        "available": {
                            "function_type": "LinearFunction",
                            "minimum": 0.0,
                            "maximum": 9999.0,
                            "a": -5.0,
                            "b": 2500.0,
                        },
                        "depletion_rule": "OnReward",
                    }
                }
            }
        }
    )

    functions = task["task_parameters"]["reward_specification"]["reward_function"]
    initial, update = functions
    assert initial["function_type"] == "OnThisPatchEntryRewardFunction"
    assert initial["probability"]["value"]["distribution_parameters"]["value"] == 0.9
    assert initial["available"]["value"]["distribution_parameters"]["value"] == 2500.0
    assert update["rule"] == "OnReward"
    assert update["probability"]["function_type"] == "ClampedMultiplicativeRateFunction"
    assert update["probability"]["rate"]["distribution_parameters"]["value"] == math.exp(-0.1284)
    assert update["available"]["function_type"] == "ClampedRateFunction"
    assert update["available"]["rate"]["distribution_parameters"]["value"] == -5.0


def test_pre_controller_cameras_and_treadmill_are_grouped():
    _, rig, _ = _migrate(
        version="0.3.0",
        task_logic={},
        rig={
            "face_camera": {"device_type": "SpinnakerCamera", "frame_rate": 120},
            "side_body_camera": None,
            "top_body_camera": None,
            "auxiliary_camera0": {"device_type": "WebCamera", "index": 0},
            "auxiliary_camera1": None,
            "treadmill": {
                "harp_board": {"port_name": "COM3", "serial_number": None},
                "settings": {"wheel_diameter": 15, "pulses_per_revolution": 8192},
            },
        },
    )

    assert list(rig["triggered_camera_controller"]["cameras"]) == ["face_camera"]
    assert rig["triggered_camera_controller"]["frame_rate"] == 120
    assert list(rig["monitoring_camera_controller"]["cameras"]) == ["auxiliary_camera0"]
    assert rig["harp_treadmill"]["device_type"] == "Treadmill"
    assert rig["harp_treadmill"]["port_name"] == ""
    assert rig["harp_treadmill"]["calibration"]["pulses_per_revolution"] == 8192
    assert rig["manipulator"]["device_type"] == "StepperDriver"
    assert rig["manipulator"]["port_name"] == ""


def test_missing_triggered_camera_rate_uses_zero_sentinel():
    _, rig, _ = _migrate(
        version="0.3.0",
        task_logic={},
        rig={
            "face_camera": {"device_type": "SpinnakerCamera"},
            "side_body_camera": None,
            "top_body_camera": None,
        },
    )

    assert rig["triggered_camera_controller"]["frame_rate"] == 0
