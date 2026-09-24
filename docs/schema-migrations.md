# VR Foraging schema migrations

The schema migration layer converts historical session, rig, and task-logic JSON
documents into the three model classes installed with this package. It is
intended for reading historical acquisition metadata, not rewriting operational
rig configurations for future experiments.

See [Caveats and review notes](schema-migration-notes.md) for transformations
that are lossy, inferred, or require human interpretation.

## Update process

```mermaid
flowchart LR
    A[Session, rig, and task JSON] --> B[Decode and deep-copy]
    B --> C[Shape-driven task migrations]
    C --> D[Cross-document rig migrations]
    D --> E[Serialize migrated dictionaries]
    E --> F[Current model_validate_json]
    F --> G[Session, Rig, and TaskLogic models]
    F --> H[Structured per-document failures]
```

`schema_harmonizer` is the public triplet operation. The Parquet harness calls
it by default for every row and keeps processing after failures. Inputs are
deep-copied before migration and are never modified.

For large validation runs, pass `retain_rows=False`. The harness then retains
failure details and the success count without holding thousands of complete
Pydantic model triplets in memory. The default remains `True` when callers need
the successfully harmonized models.

The process is deliberately shape-driven. The dataset release selects broad
families, but each document is migrated from the fields it actually contains.
This matters for release candidates and for the observed row whose rig and task
carry different internal release versions.

## Migration families

| Source shape | Transformation |
| --- | --- |
| Current 1.x | No JSON migration; validate directly. |
| 0.5-0.6 rigs | Apply the upgrade pattern from VR Foraging issue 499: flatten calibration `input`/`output`, normalize device discriminators, and add `data_directory` from `session.root_path`. |
| Pre-0.5 rigs | Additionally group individual cameras into controllers, convert the legacy treadmill shape, remove the clock-repeater field, and construct required modern device shells. |
| Flat 0.3 task logic | Move task fields under `task_parameters`, create a one-block environment, and unwrap matrix `{data: ...}` containers. |
| Legacy reward functions | Convert the old single patch function into entry initialization plus rule-driven updates. Constant, linear, power, and lookup-table functions preserve their mathematical behavior. |
| Legacy updaters | Convert `OffsetPercentage` to the equivalent multiplicative `Gain`, using `1 + update`. |
| Legacy discriminators | Normalize screen, Harp device, and reward-function type names. |

The final step always serializes the migrated dictionaries and invokes:

```python
Session.model_validate_json(...)
AindVrForagingRig.model_validate_json(...)
AindVrForagingTaskLogic.model_validate_json(...)
```

A successful result therefore means all three outputs satisfy the current
formal schemas; it does not merely mean that migration code ran without an
exception.

## Pipeline integration

Session processing and aggregation can append migrated schemas without
replacing the source metadata:

```python
from aind_behavior_vr_foraging_packaging.schema_migrations import SchemaMigrationMode

process_session(path, output, schema_migration_mode=SchemaMigrationMode.FILL_MISSING)
process_sessions(paths, output, schema_migration_mode=SchemaMigrationMode.FILL_MISSING)
aggregate(sessions_dir, output, schema_migration_mode=SchemaMigrationMode.FILL_MISSING)
```

Arrow-based aggregators can apply the same operation directly to an existing
session table:

```python
from aind_behavior_vr_foraging_packaging.schema_migrations import (
    SchemaMigrationMode,
    append_migrated_schema_columns,
)
import pyarrow.parquet as pq

session_table = pq.read_table("session.parquet")
session_table = append_migrated_schema_columns(session_table)
```

The `session`, `batch`, and `aggregate` CLI commands expose the same enum as
`--schema-migration-mode`. Migrated output adds `session_migrated`,
`rig_migrated`, and `task_logic_migrated` to the session table. The original
`session`, `rig`, and `task_logic` columns remain unchanged, and
`dataset_version` continues to identify the source dataset release. In
`fill-missing` mode, existing non-null migrated cells are preserved and only
missing cells are generated. The resulting columns use Arrow's JSON logical
type so tables from different packaging runs can be concatenated.

To deliberately replace migrated values after the migration rules change, use
forced mode:

```python
session_table = append_migrated_schema_columns(
    session_table,
    mode=SchemaMigrationMode.FORCE,
)
aggregate(sessions_dir, output, schema_migration_mode=SchemaMigrationMode.FORCE)
```

Forced mode recomputes all three migrated documents from each row's raw
columns. It still leaves the raw columns unchanged. The `batch` and `aggregate`
CLI commands expose the three choices through `--schema-migration-mode`:
`DISABLED`, `FILL_MISSING`, and `FORCE`.

## Semantic compatibility decisions

Most migrations are lossless moves or discriminator renames. These cases need
special attention during review:

- Historical rigs before camera controllers are grouped without changing
  camera identity or acquisition settings.
- The legacy treadmill used a Behavior board. The current schema only accepts a
  Harp Treadmill, so the migration preserves its port, serial number, and wheel
  calibration while assigning the current treadmill device identity.
- Pre-0.4 rigs have no manipulator document. A required current manipulator
  shell is created with model defaults and an empty port.
- Missing old olfactometer calibration is reconstructed from task patch labels.
  Odor names and channel indices are retained; unknown dilution is represented
  as `null`, and channel 3 is the required carrier.
- Sixteen observed 0.5.1 task documents specify a 10,000 Hz cue, while the
  current model has a hard maximum of 9,999 Hz. The migration uses 9,999 Hz.
- A legacy power reward function with a non-zero additive offset cannot be
  represented by the current multiplicative updater. Such an unseen document
  raises an error rather than silently changing its behavior.

Historical FFmpeg arguments are retained. Issue 499 replaced them to prepare
rigs for future acquisition, but changing them here would misrepresent how an
already completed session was recorded.
