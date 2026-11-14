"""Generate the reference pages under ``docs/reference/``.

* ``processed-tables.md``: columns, types and descriptions of the tables this package writes.
  ``session`` and ``sites`` are read from their pydantic models; the stream tables render the
  docstring of the processor that writes them.
* ``raw-data-contract.md``: the raw data streams of the acquisition, read from the
  ``aind-behavior-vr-foraging`` data contract (via contraqctor).

Run from the repository root: ``uv run python scripts/generate_reference_docs.py``
"""

import inspect
import re
import typing as t
from pathlib import Path

from aind_behavior_vr_foraging import __semver__
from aind_behavior_vr_foraging.data_contract import dataset as contract_dataset
from pydantic import BaseModel
from pydantic.fields import FieldInfo

from aind_behavior_vr_foraging_packaging.models import SessionMetadata, Site
from aind_behavior_vr_foraging_packaging.processing import (
    EventsProcessor,
    LicksProcessor,
    PositionAndVelocityProcessor,
    SniffingProcessor,
    SoftwareEventsProcessor,
)
from aind_behavior_vr_foraging_packaging.processing._session_metadata import _is_json_marked

OUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "reference"
UNIT_RE = re.compile(r"\s*\(unit: ([^)]+)\)\.?\s*$")


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _type_name(field: FieldInfo) -> tuple[str, bool]:
    """Human-readable type and whether the column can be null."""
    if _is_json_marked(field):
        return "JSON string", t.get_origin(field.annotation) is t.Union or type(None) in t.get_args(field.annotation)
    annotation = field.annotation
    args = [a for a in t.get_args(annotation) if a is not type(None)]
    nullable = len(args) != len(t.get_args(annotation))
    base = args[0] if nullable and len(args) == 1 else annotation
    name = str(base).replace("typing.", "").replace("datetime.", "")
    name = getattr(base, "__name__", name) if not t.get_args(base) else name
    return name, nullable


def model_table(model: type[BaseModel]) -> str:
    rows = ["| Column | Type | Unit | Nullable | Description |", "| --- | --- | --- | --- | --- |"]
    for name, field in model.model_fields.items():
        type_name, nullable = _type_name(field)
        description = field.description or ""
        unit = ""
        if match := UNIT_RE.search(description):
            unit, description = match.group(1), description[: match.start()]
        rows.append(
            f"| `{name}` | `{type_name}` | {_cell(unit)} | {'yes' if nullable else ''} | {_cell(description)} |"
        )
    return "\n".join(rows)


# Tables written per session. Their columns are documented in each processor's class
# docstring, which is rendered here.
STREAM_PROCESSORS = [
    PositionAndVelocityProcessor,
    LicksProcessor,
    SniffingProcessor,
    SoftwareEventsProcessor,
    EventsProcessor,
]


def _summary(processor: type) -> str:
    return (inspect.getdoc(processor) or "").splitlines()[0].rstrip(".")


def processed_tables_page() -> str:
    parts = [
        "# Processed tables",
        "",
        (
            "These tables are written by this package from the raw acquisition data "
            "(see [Raw data contract](raw-data-contract.md) for the inputs). "
            "They are what you read when you follow the [data access patterns](../data/load-tables.md)."
        ),
        "",
        "| Table | Description | Where |",
        "| --- | --- | --- |",
        "| [`session`](#session) | One row per session | `<root>/session.parquet` |",
        "| [`sites`](#sites) | One row per site | `<root>/sites.parquet` |",
        *(
            f"| [`{p.__output_name__}`](#{p.__output_name__}) | {_summary(p)} "
            f"| `<source_s3_location>/{p.__output_name__}.parquet` |"
            for p in STREAM_PROCESSORS
        ),
        "",
        "`session_id` is the join key across all of them.",
        (
            "Every parquet file also carries packaging provenance in its schema metadata "
            "(`packaging_version`, `data_contract_version`, `dataset_version`, `processor`)."
        ),
        "",
        "## session",
        "",
        "One row per session, in the aggregated `session.parquet` catalogue.",
        "",
        model_table(SessionMetadata),
        "",
        '!!! tip "Skip the config columns"',
        (
            "    `session`, `rig`, `task_logic` and `trainer_state` hold the full serialized configuration "
            "and make up nearly all of the file. Drop them when you do not need them."
        ),
        "",
        "## sites",
        "",
        (
            "One row per site (the unit the animal visits in the corridor), in the aggregated `sites.parquet`, "
            "with a `session_id` column added when sessions are aggregated. "
            "Times are in seconds on the same clock as the stream tables below."
        ),
        "",
        model_table(Site),
        "",
        "## Stream tables",
        "",
        (
            "Large tables stored once per session, in the session's own processed asset "
            "(`source_s3_location` in `session.parquet`). They are not aggregated. "
            "Each description below is the docstring of the processor that writes the table."
        ),
    ]
    for p in STREAM_PROCESSORS:
        parts += [
            "",
            f"### {p.__output_name__}",
            "",
            f"::: {p.__module__.rsplit('.', 1)[0]}.{p.__name__}",
            "    options:",
            "      show_root_heading: false",
            "      show_root_toc_entry: false",
            "      show_source: false",
            "      members: false",
        ]
    return "\n".join(parts) + "\n"


def _walk(node, path: tuple[str, ...] = ()):
    """Yield ``(path, node)`` for every stream under *node* without loading any data."""
    yield path, node
    if node.is_collection and node.has_data:
        for child in node.data:
            yield from _walk(child, (*path, child.name))


def _file_of(node) -> str:
    params = node.reader_params
    path = getattr(params, "path", None)
    if path is None:
        return ""
    text = Path(str(path)).as_posix()
    return text.replace("<RootPath>/", "")


def contract_page() -> str:
    root = contract_dataset(Path("<RootPath>"), version=__semver__)
    parts = [
        "# Raw data contract",
        "",
        (
            "The raw acquisition data of a session, as described by the "
            f"[`aind-behavior-vr-foraging`](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging) data contract "
            f"(version `{__semver__}`), which [contraqctor](https://github.com/AllenNeuralDynamics/contraqctor) uses to load it."
        ),
        "",
        (
            "These are the *inputs* to the pipeline. For the tables it produces, see [Processed tables](processed-tables.md). "
            "To read these streams directly, see [Raw streams with contraqctor](../processing/raw-streams.md)."
        ),
        "",
        "Older sessions are loaded with the contract matching their dataset version, so some streams may be absent or differ.",
        "Paths are relative to the session root. A stream without a description is documented upstream.",
    ]
    for child in root.data:
        parts += [
            "",
            f"## {child.name}",
            "",
            child.description or "",
            "",
            "| Stream | Type | File | Description |",
            "| --- | --- | --- | --- |",
        ]
        for path, node in _walk(child, (child.name,)):
            if len(path) == 1:
                continue
            indent = "&nbsp;&nbsp;&nbsp;&nbsp;" * (len(path) - 2)
            label = f"{indent}`{path[-1]}`"
            file = f"`{_file_of(node)}`" if _file_of(node) else ""
            parts.append(f"| {label} | {type(node).__name__} | {file} | {_cell(node.description or '')} |")
    return "\n".join(parts) + "\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "processed-tables.md").write_text(processed_tables_page(), encoding="utf-8")
    (OUT_DIR / "raw-data-contract.md").write_text(contract_page(), encoding="utf-8")
    print(f"Wrote reference pages to {OUT_DIR}")


if __name__ == "__main__":
    main()
