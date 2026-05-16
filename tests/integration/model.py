from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ExpectedInvariants(BaseModel):
    """Lightweight scalar invariants asserted against the parsed dataset.

    All fields are optional; only the present ones are checked. Unknown
    fields are rejected at validation time so typos in the YAML manifest
    surface immediately rather than silently being skipped.
    """

    model_config = ConfigDict(extra="forbid")

    n_sites: Optional[int] = Field(
        default=None,
        description="Expected total number of sites in the parsed session. Asserted against the trial table row count.",
    )
    n_choices: Optional[int] = Field(
        default=None,
        description="Expected number of sites where has_choice is true.",
    )
    n_rewards: Optional[int] = Field(
        default=None,
        description="Expected number of sites where has_reward is true.",
    )
    n_blocks: Optional[int] = Field(
        default=None,
        description="Expected number of blocks in the session.",
    )
    n_patches: Optional[int] = Field(
        default=None,
        description="Expected number of patches in the session.",
    )
    nwb_validates: Optional[bool] = Field(
        default=None,
        description="If true, the generated NWB file must pass pynwb validation. If false or omitted, validation is skipped.",
    )


class DatasetEntry(BaseModel):
    """A single integration-test dataset entry in the manifest."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(
        description="Stable short handle used as the pytest test ID. Must be unique within the manifest. Avoid spaces; kebab-case recommended.",
    )
    uri: str = Field(
        description="Full s3:// URI of the dataset prefix (a 'folder' in S3). Must end with a trailing slash. Listed and downloaded recursively, subject to `exclude`.",
    )
    rationale: str = Field(
        description="Free-form note explaining why this dataset is in the suite — what it tests, what bug it caught, what edge case it represents. Printed alongside any failure to make triage fast.",
    )
    exclude: List[str] = Field(
        default_factory=list,
        description="Glob patterns (pathlib.PurePosixPath.match semantics, case-insensitive) matched against each S3 key relative to the dataset prefix. Any match excludes the object from download. Examples: 'videos/**', '**/*.avi', 'raw/calibration_*.bin'.",
    )
    expected: Optional[ExpectedInvariants] = Field(
        default=None,
        description="Optional scalar invariants to assert after parsing. If omitted, the dataset only gets the smoke test (parser must not crash).",
    )
    xfail: bool = Field(
        default=False,
        description="If true, the test is marked pytest.xfail(strict=True) — failure is expected, unexpected pass becomes a hard failure forcing removal of the marker. Use to keep known-broken datasets in the suite without blocking CI.",
    )
    xfail_reason: str = Field(
        default="",
        description="Human-readable explanation displayed alongside the xfail marker. Only meaningful when `xfail` is true.",
    )


class DatasetManifest(BaseModel):
    """Top-level manifest describing every integration-test dataset."""

    model_config = ConfigDict(extra="forbid")

    datasets: List[DatasetEntry] = Field(
        description="Ordered list of dataset entries. Order has no semantic meaning beyond display.",
    )


def load_manifest(path: Path) -> DatasetManifest:
    """Parse and validate the YAML manifest at `path`. Raises pydantic.ValidationError on schema problems."""
    return DatasetManifest.model_validate(yaml.safe_load(path.read_text()))
