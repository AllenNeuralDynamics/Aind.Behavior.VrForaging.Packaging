"""Pure JSON migrations for historical VR Foraging schemas."""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping
from typing import Any

JsonObject = dict[str, Any]

_DEVICE_TYPES = {
    "behavior": "Behavior",
    "clockgenerator": "WhiteRabbit",
    "lickometer": "LicketySplit",
    "olfactometer": "Olfactometer",
    "screen": "ScreenAssembly",
    "Screen": "ScreenAssembly",
    "sniffdetector": "SniffDetector",
    "stepperdriver": "StepperDriver",
    "treadmill": "Treadmill",
}


def migrate_documents(
    *,
    source_version: str,
    session: Mapping[str, Any],
    rig: Mapping[str, Any],
    task_logic: Mapping[str, Any],
) -> tuple[JsonObject, JsonObject, JsonObject]:
    """Return migrated deep copies of a session, rig, and task-logic triplet."""
    session_out = copy.deepcopy(dict(session))
    rig_out = copy.deepcopy(dict(rig))
    task_out = copy.deepcopy(dict(task_logic))

    _migrate_task_logic(task_out)
    _migrate_rig(rig_out, session=session_out, task_logic=task_out, source_version=source_version)
    return session_out, rig_out, task_out


def _migrate_task_logic(task_logic: JsonObject) -> None:
    task_logic.pop("describedBy", None)

    if "task_parameters" not in task_logic and "environment_statistics" in task_logic:
        task_logic = _wrap_flat_task_logic(task_logic)

    _walk_task_value(task_logic)


def _wrap_flat_task_logic(task_logic: JsonObject) -> JsonObject:
    """Move the pre-0.4 flat task document into the current task envelope."""
    flat = dict(task_logic)
    flat.pop("schema_version", None)
    flat.pop("task_mode_settings", None)
    environment = flat.pop("environment_statistics")
    parameters = {
        "updaters": flat.pop("updaters", {}),
        "operation_control": flat.pop("operation_control", {}),
        "environment": {"blocks": [{"environment_statistics": environment}]},
    }
    return_value = {
        "name": "AindVrForaging",
        "task_parameters": parameters,
    }
    task_logic.clear()
    task_logic.update(return_value)
    return task_logic


def _walk_task_value(value: Any) -> None:
    if isinstance(value, list):
        for item in value:
            _walk_task_value(item)
        return
    if not isinstance(value, dict):
        return

    if "mininum" in value and "minimum" not in value:  # codespell:ignore mininum
        value["minimum"] = value.pop("mininum")  # codespell:ignore mininum
    transition_matrix = value.get("transition_matrix")
    if isinstance(transition_matrix, dict) and isinstance(transition_matrix.get("data"), list):
        value["transition_matrix"] = transition_matrix["data"]

    if value.get("operation") == "OffsetPercentage":
        value["operation"] = "Gain"
        for outcome in ("increment", "decrement"):
            update = value.get(outcome)
            if isinstance(update, dict) and isinstance(update.get("update"), (int, float)):
                update["update"] = 1 + update["update"]

    if value.get("function_type") == "OnThisPatchEntryFunction":
        value["function_type"] = "OnThisPatchEntryRewardFunction"

    if value.get("frequency") == 10_000:
        value["frequency"] = 9_999

    reward_function = value.get("reward_function")
    if isinstance(reward_function, dict) and "depletion_rule" in reward_function:
        value["reward_function"] = _migrate_legacy_reward_function(reward_function)

    for child in list(value.values()):
        _walk_task_value(child)


def _migrate_legacy_reward_function(legacy: JsonObject) -> list[JsonObject]:
    rule = legacy.get("depletion_rule", "OnReward")
    initial: JsonObject = {
        "function_type": "OnThisPatchEntryRewardFunction",
        "rule": "OnThisPatchEntry",
    }
    update: JsonObject = {"function_type": "PatchRewardFunction", "rule": rule}

    for field in ("amount", "probability", "available"):
        function = legacy.get(field)
        if not isinstance(function, dict):
            continue
        initial[field] = _set_value_function(_legacy_function_initial_value(function))
        update[field] = _legacy_update_function(function)

    return [initial, update]


def _legacy_function_initial_value(function: JsonObject) -> float:
    function_type = function.get("function_type")
    if function_type == "ConstantFunction":
        return float(function["value"])
    if function_type == "LinearFunction":
        return _clamp(float(function.get("b", 0)), function)
    if function_type == "PowerFunction":
        return _clamp(float(function.get("a", 1)) + float(function.get("d", 0)), function)
    if function_type == "LookupTableFunction":
        keys = function.get("lut_keys", [])
        values = function.get("lut_values", [])
        if not keys or len(keys) != len(values):
            raise ValueError("A legacy LookupTableFunction must have equally sized, non-empty keys and values")
        return float(values[0])
    raise ValueError(f"Unsupported legacy reward function: {function_type!r}")


def _legacy_update_function(function: JsonObject) -> JsonObject:
    function_type = function.get("function_type")
    if function_type == "ConstantFunction":
        return _set_value_function(float(function["value"]))
    if function_type == "LinearFunction":
        return {
            "function_type": "ClampedRateFunction",
            "minimum": function.get("minimum", 0),
            "maximum": function.get("maximum"),
            "rate": _scalar_distribution(float(function.get("a", 1))),
        }
    if function_type == "PowerFunction":
        offset = float(function.get("d", 0))
        if offset != 0:
            raise ValueError("PowerFunction with a non-zero additive offset cannot be migrated losslessly")
        return {
            "function_type": "ClampedMultiplicativeRateFunction",
            "minimum": function.get("minimum", 0),
            "maximum": function.get("maximum"),
            "rate": _scalar_distribution(float(function.get("b", math.e)) ** float(function.get("c", -1))),
        }
    if function_type == "LookupTableFunction":
        return copy.deepcopy(function)
    raise ValueError(f"Unsupported legacy reward function: {function_type!r}")


def _set_value_function(value: float) -> JsonObject:
    return {"function_type": "SetValueFunction", "value": _scalar_distribution(value)}


def _scalar_distribution(value: float) -> JsonObject:
    return {
        "family": "Scalar",
        "distribution_parameters": {"family": "Scalar", "value": value},
        "truncation_parameters": None,
        "scaling_parameters": None,
    }


def _clamp(value: float, function: JsonObject) -> float:
    minimum = float(function.get("minimum", value))
    maximum = float(function.get("maximum", value))
    return min(maximum, max(minimum, value))


def _migrate_rig(
    rig: JsonObject,
    *,
    session: JsonObject,
    task_logic: JsonObject,
    source_version: str,
) -> None:
    if _version_at_least(source_version, (1, 0)):
        return

    rig.setdefault("data_directory", session.get("root_path"))
    rig.pop("schema_version", None)
    rig.pop("harp_clock_repeaters", None)

    if "triggered_camera_controller" not in rig:
        _group_legacy_cameras(rig)
    if "harp_treadmill" not in rig and isinstance(rig.get("treadmill"), dict):
        _migrate_legacy_treadmill(rig)
    if "manipulator" not in rig:
        rig["manipulator"] = {
            "device_type": "StepperDriver",
            "who_am_i": 1130,
            "serial_number": None,
            "port_name": "",
        }

    calibration = rig.get("calibration")
    if isinstance(calibration, dict):
        calibration.pop("olfactometer", None)
        if isinstance(calibration.get("water_valve"), dict):
            calibration["water_valve"] = _flatten_io_block(calibration["water_valve"])

    _normalize_rig_devices(rig)

    olfactometer = rig.get("harp_olfactometer")
    if isinstance(olfactometer, dict) and not olfactometer.get("calibration"):
        olfactometer["calibration"] = _derive_olfactometer_calibration(task_logic)


def _normalize_rig_devices(rig: JsonObject) -> None:
    for device in rig.values():
        if not isinstance(device, dict) or not ("device_type" in device or "device_name" in device):
            continue
        device_type = device.get("device_type")
        if device_type in _DEVICE_TYPES:
            device["device_type"] = _DEVICE_TYPES[device_type]
        if device.get("device_type") == "WhiteRabbit":
            device["who_am_i"] = 1404
        if isinstance(device.get("calibration"), dict):
            device["calibration"] = _flatten_io_block(device["calibration"])
        elif device.get("calibration") is None and device.get("device_type") == "ScreenAssembly":
            device.pop("calibration", None)
        if device.get("device_type") == "StepperDriver" and isinstance(device.get("calibration"), dict):
            device["calibration"].pop("description", None)


def _flatten_io_block(block: JsonObject) -> JsonObject:
    flattened = dict(block.get("input", {})) | dict(block.get("output", {})) | dict(block)
    flattened.pop("input", None)
    flattened.pop("output", None)
    return flattened


def _group_legacy_cameras(rig: JsonObject) -> None:
    triggered: dict[str, JsonObject] = {}
    frame_rates: list[int] = []
    for key in ("face_camera", "top_body_camera", "side_body_camera"):
        camera = rig.pop(key, None)
        if not isinstance(camera, dict):
            continue
        camera = dict(camera)
        frame_rate = camera.pop("frame_rate", None)
        if isinstance(frame_rate, int):
            frame_rates.append(frame_rate)
        triggered[key] = camera
    rig["triggered_camera_controller"] = {
        "device_type": "CameraController",
        "cameras": triggered,
        "frame_rate": max(frame_rates, default=0),
    }

    monitoring: dict[str, JsonObject] = {}
    for key in ("auxiliary_camera0", "auxiliary_camera1"):
        camera = rig.pop(key, None)
        if isinstance(camera, dict):
            monitoring[key] = camera
    rig["monitoring_camera_controller"] = (
        {"device_type": "CameraController", "cameras": monitoring, "frame_rate": 30} if monitoring else None
    )


def _migrate_legacy_treadmill(rig: JsonObject) -> None:
    treadmill = rig.pop("treadmill")
    board = treadmill.get("harp_board", {})
    rig["harp_treadmill"] = {
        "device_type": "Treadmill",
        "who_am_i": 1402,
        "serial_number": board.get("serial_number"),
        "port_name": "",
        "calibration": treadmill.get("settings", {}),
    }


def _derive_olfactometer_calibration(task_logic: JsonObject) -> JsonObject:
    odors: dict[int, str] = {}

    def visit(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
            return
        if not isinstance(value, dict):
            return
        odor = value.get("odor_specification")
        if isinstance(odor, dict) and isinstance(odor.get("index"), int):
            odors.setdefault(odor["index"], str(value.get("label", f"Odor channel {odor['index']}")))
        for child in value.values():
            visit(child)

    visit(task_logic)
    channels: dict[str, JsonObject] = {}
    for index in range(3):
        channels[str(index)] = {
            "channel_index": index,
            "channel_type": "Odor",
            "flow_rate_capacity": 100,
            "flow_rate": 100,
            "odorant": odors.get(index, f"Unknown odor channel {index}"),
            "odorant_dilution": None,
        }
    channels["3"] = {
        "channel_index": 3,
        "channel_type": "Carrier",
        "flow_rate_capacity": 1000,
        "flow_rate": 100,
        "odorant": None,
        "odorant_dilution": None,
    }
    return {"channel_config": channels}


def _version_at_least(version: str, threshold: tuple[int, int]) -> bool:
    core = version.removeprefix("v").split("-", maxsplit=1)[0]
    try:
        parts = core.split(".")
        major, minor = int(parts[0]), int(parts[1])
    except (IndexError, TypeError, ValueError):
        return False
    return (major, minor) >= threshold
