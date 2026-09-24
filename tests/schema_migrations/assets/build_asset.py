"""Build the compact schema-migration transition corpus from a full export.

This is a maintainer utility, not part of the test suite. It intentionally
selects examples by document shape so that each explicit migration branch is
represented without checking a large external corpus into the repository.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

JsonObject = dict[str, Any]
Predicate = Callable[[str, JsonObject, JsonObject, JsonObject], bool]

SOURCE_COLUMNS = ("dataset_version", "session", "rig", "task_logic")
DEFAULT_OUTPUT = Path(__file__).with_name("session_schema_transitions.parquet")


def _contains(value: Any, predicate: Callable[[JsonObject], bool]) -> bool:
    if isinstance(value, dict):
        return predicate(value) or any(_contains(child, predicate) for child in value.values())
    if isinstance(value, list):
        return any(_contains(child, predicate) for child in value)
    return False


def _has_function(task: JsonObject, function_type: str) -> bool:
    return _contains(task, lambda value: value.get("function_type") == function_type)


def _has_legacy_reward_function(task: JsonObject, function_type: str) -> bool:
    return _contains(
        task,
        lambda value: (
            isinstance(value.get("reward_function"), dict)
            and "depletion_rule" in value["reward_function"]
            and _has_function(value["reward_function"], function_type)
        ),
    )


def _legacy_olfactometer_has_calibration(rig: JsonObject) -> bool:
    olfactometer = rig.get("harp_olfactometer")
    return isinstance(olfactometer, dict) and bool(olfactometer.get("calibration"))


RULES: dict[str, Predicate] = {
    "v0_3_flat_task_legacy_hardware": lambda version, _session, rig, task: (
        version == "0.3.0"
        and "task_parameters" not in task
        and "environment_statistics" in task
        and "triggered_camera_controller" not in rig
        and "treadmill" in rig
    ),
    "offset_percentage_to_gain": lambda _version, _session, _rig, task: _contains(
        task, lambda value: value.get("operation") == "OffsetPercentage"
    ),
    "derive_missing_olfactometer_calibration": lambda version, _session, rig, _task: (
        version.startswith("0.4") and not _legacy_olfactometer_has_calibration(rig)
    ),
    "flatten_device_olfactometer_calibration": lambda version, _session, rig, _task: (
        version.startswith("0.4") and _legacy_olfactometer_has_calibration(rig)
    ),
    "legacy_constant_reward": lambda _version, _session, _rig, task: _has_legacy_reward_function(
        task, "ConstantFunction"
    ),
    "legacy_linear_reward": lambda _version, _session, _rig, task: _has_legacy_reward_function(task, "LinearFunction"),
    "legacy_power_reward": lambda _version, _session, _rig, task: _has_legacy_reward_function(task, "PowerFunction"),
    "legacy_lookup_reward": lambda _version, _session, _rig, task: _has_legacy_reward_function(
        task, "LookupTableFunction"
    ),
    "wrapped_transition_matrix": lambda _version, _session, _rig, task: _contains(
        task,
        lambda value: (
            isinstance(value.get("transition_matrix"), dict)
            and isinstance(value["transition_matrix"].get("data"), list)
        ),
    ),
    "legacy_mininum_spelling": lambda _version, _session, _rig, task: _contains(
        task,
        lambda value: "mininum" in value,  # codespell:ignore mininum
    ),
    "audio_frequency_upper_bound": lambda _version, _session, _rig, task: _contains(
        task, lambda value: value.get("frequency") == 10_000
    ),
    "reward_discriminator_rename": lambda _version, _session, _rig, task: _has_function(
        task, "OnThisPatchEntryFunction"
    ),
    "nested_device_calibration": lambda version, _session, rig, _task: (
        version == "0.6.7"
        and _contains(
            rig,
            lambda value: (
                isinstance(value.get("calibration"), dict)
                and ("input" in value["calibration"] or "output" in value["calibration"])
            ),
        )
    ),
    "release_candidate": lambda version, _session, _rig, _task: version == "0.6.4-rc1",
    "current_v1_boundary": lambda version, _session, _rig, _task: version == "1.0.0",
    "current_latest": lambda version, _session, _rig, _task: version == "1.2.6",
}


def build_asset(source: Path, output: Path) -> tuple[int, dict[str, int]]:
    """Select one deterministic example per rule and write a compact parquet."""
    table = pq.read_table(source, columns=list(SOURCE_COLUMNS))
    columns = {name: table[name].to_pylist() for name in SOURCE_COLUMNS}
    matches: dict[str, int] = {}

    for row_index, values in enumerate(zip(*(columns[name] for name in SOURCE_COLUMNS), strict=True)):
        version, session_json, rig_json, task_json = values
        session = json.loads(session_json)
        rig = json.loads(rig_json)
        task = json.loads(task_json)
        for name, predicate in RULES.items():
            if name not in matches and predicate(str(version), session, rig, task):
                matches[name] = row_index
        if len(matches) == len(RULES):
            break

    missing = sorted(set(RULES) - set(matches))
    if missing:
        raise RuntimeError(f"No source row matched selection rule(s): {', '.join(missing)}")

    labels_by_row: dict[int, list[str]] = {}
    for label, row_index in matches.items():
        labels_by_row.setdefault(row_index, []).append(label)

    rows = []
    for row_index, labels in sorted(labels_by_row.items()):
        rows.append(
            {
                "case_id": "+".join(sorted(labels)),
                **{name: columns[name][row_index] for name in SOURCE_COLUMNS},
            }
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), output, compression="zstd")
    return len(rows), matches


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Full session.parquet export")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    row_count, matches = build_asset(args.source, args.output)
    print(f"Wrote {row_count} rows covering {len(matches)} rules to {args.output}")
    for name, row_index in matches.items():
        print(f"  {name}: source row {row_index}")


if __name__ == "__main__":
    main()
