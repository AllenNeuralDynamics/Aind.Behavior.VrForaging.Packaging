# Schema migration caveats and review notes

This file is a decision ledger for transformations that are lossy, inferred,
or otherwise deserve human review. Formal deserialization only proves that the
result satisfies the current models; it does not prove that every synthesized
field is historically exact.

The caller remains responsible for retaining the raw JSON. The harmonizer
returns only the three current model instances and does not currently return a
per-field audit trail.

## Known lossy transformations

| Area | Current behavior | What is lost or changed |
| --- | --- | --- |
| Audio frequency | Converts the legacy value `10000` Hz to `9999` Hz. | The value changes by 1 Hz because the current model has a hard upper bound of `9999`. |
| Flat task metadata | Removes the old `schema_version` and `task_mode_settings` fields while constructing the current task envelope. | The task mode and old task-schema marker are not represented in the current task model. The harness records the supplied source version separately. |
| `describedBy` | Removes the obsolete task-level `describedBy` key. | The old schema URL is not represented in the output model. |
| Rig version metadata | Removes the old rig `schema_version`. | The resulting model reports the current model version; the old version exists only in the raw input and harness source version. |
| Clock repeaters | Removes `harp_clock_repeaters`. | The current rig has no equivalent field. Any historical repeater topology is absent from the output. |
| Clock generator identity | Maps legacy `clockgenerator` to `WhiteRabbit` and changes `who_am_i` from `1158` to the current required `1404`. | This satisfies the current hardware type but does not mean the historical rig physically used a White Rabbit board. |

## Best-effort hardware reconstruction

Any device created because the current schema requires a device that did not
exist in the source schema receives `port_name=""`. The empty port is an
explicit sentinel that the synthesized device was not known to be connected.

### Pre-controller cameras

Individual `face_camera`, `top_body_camera`, and `side_body_camera` values are
grouped into a triggered controller. Auxiliary web cameras are grouped into a
monitoring controller.

- Camera dictionaries and their old field names are retained as controller
  entries.
- The triggered rate is the maximum camera rate found in the old document.
- If no triggered camera rate exists, `0` Hz is used as an explicit sentinel
  for an unknown or invalid acquisition rate.
- The monitoring controller rate is synthesized as `30` Hz.
- Current camera defaults added during deserialization, such as pixel format,
  ADC depth, region of interest, or gamma, were not necessarily explicit in the
  historical acquisition configuration.

### Legacy treadmill

The old treadmill stored a Behavior board plus wheel settings. The current VR
Foraging rig requires a Harp `Treadmill` device.

- Serial number, wheel diameter, pulses per revolution, and direction are
  retained when present.
- The device is assigned current `device_type="Treadmill"` and
  `who_am_i=1402`, even though the historical document may identify a Behavior
  board instead.
- The legacy board port is not assigned to the synthesized Harp treadmill;
  `port_name=""` marks it as not known to be connected.
- Newer calibration fields, including the brake lookup, come from current model
  defaults when absent.

This is structurally valid but should not be interpreted as an exact statement
of historical hardware identity.

### Missing manipulator

Pre-0.4 rigs do not contain a manipulator document, but the current model
requires one. The harmonizer creates a StepperDriver shell with:

- `who_am_i=1130`;
- no serial number;
- an empty port name;
- current default calibration and axis configuration.

This is invented compatibility metadata. It does not establish that a
manipulator was installed, connected, or configured that way.

### Missing olfactometer calibration

When a legacy rig has no device-level olfactometer calibration, the harmonizer
derives a minimally valid channel map from task patch labels:

- task odor indices `0` through `2` become odor channels;
- channel `3` is synthesized as the required carrier;
- flow capacities and flow rates use current conventional values;
- dilution is `null` because task mixture concentration is not equivalent to
  physical odorant dilution;
- a missing label becomes `Unknown odor channel N`.

The task tells us which odor was requested, but it cannot prove the physical
vial contents, dilution, or flow calibration. This reconstruction is therefore
best effort.

The old top-level `calibration.olfactometer` field is removed. A non-null value
would currently be discarded and requires a future reconciliation rule.

## Reward-function assumptions

The old task model expressed a single function of a depletion counter. The
current model separates initialization from rule-driven state updates. The
harmonizer creates an `OnThisPatchEntryRewardFunction` followed by a
`PatchRewardFunction`.

Mappings are:

| Legacy function | Current representation |
| --- | --- |
| `ConstantFunction(value)` | Scalar `SetValueFunction(value)` |
| `LinearFunction(a, b)` | Initial value `clamp(b)`, followed by `ClampedRateFunction(rate=a)` |
| `PowerFunction(a, b, c, d=0)` | Initial value `clamp(a)`, followed by `ClampedMultiplicativeRateFunction(rate=b**c)` |
| `LookupTableFunction` | Current lookup table, initialized from its first value |

These mappings match the mathematical forms, but they assume the current rule
tick and initialization semantics match the historical runtime.

Additional limitations:

- A power function with non-zero additive offset `d` cannot be represented by
  the current multiplicative updater. The harmonizer raises instead of silently
  approximating it.
- A lookup table is initialized from its first value. If its first key is not
  zero, that value may not match historical interpolation at zero.
- The historical spelling `mini` + `num` is treated as `minimum`.

`OffsetPercentage` to `Gain` is not considered lossy: its update is converted
to `1 + old_update`, matching the historical implementation.

## Calibration flattening

Legacy calibrations often contain `input` and `output` wrappers. They are
flattened using this precedence:

1. `input` fields;
2. `output` fields overriding equal input keys;
3. calibration-root fields overriding both.

If a document has different values at multiple levels, the current rule chooses
the highest-precedence value without emitting a warning.

Empty screen calibration is removed so the current screen model can apply its
default. Water-valve regression outputs are retained rather than recomputed.

## Cross-document and version assumptions

- `rig.data_directory` is copied from `session.root_path`. The path remains
  machine-specific historical metadata.
- Migrations are primarily selected by document shape. The supplied source
  version prevents legacy rig migrations from being applied to 1.x data. An
  unparsable version is conservatively treated as legacy.
- Rig and task internal versions may disagree. They are not forced to match;
  each document's shape and the current validators determine compatibility.
- Current Pydantic compatibility validators still perform their own coercions
  and inject current defaults after the explicit migrations.

## Intentionally preserved historical values

Historical FFmpeg arguments are not replaced with current operational
defaults. Issue 499 changed them for rigs that would be used for future
acquisition; changing them in historical metadata would incorrectly describe
how completed sessions were recorded.

Likewise, camera identity, exposure, gain, calibration measurements,
water-valve fit values, patch ordering, transition probabilities, subject, and
session timestamps are retained wherever the current schema can express them.
