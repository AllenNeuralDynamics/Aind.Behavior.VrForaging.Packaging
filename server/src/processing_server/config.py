"""Pipeline configuration — one YAML file, validated, ``extra="forbid"`` so a typo
fails loudly rather than being silently ignored. ``docker/compose.yaml`` mounts a
worked example.

Every field carries ``Field(description=...)`` rather than an attribute docstring:
a description is part of the model, so it reaches ``model_json_schema()`` and any
CLI generated from these models, where a docstring reaches nothing. Where the *why*
runs longer than a help string should, it stays as a comment above the field.

Env-var overrides use ``pydantic-settings`` with prefix ``VRF__`` and ``__`` as the
nested delimiter (e.g. ``VRF__WORKER__MAX_CONCURRENT_JOBS=5``), and take priority
over the YAML file — see :meth:`PipelineConfig.settings_customise_sources`.
"""

from datetime import datetime
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

_DEFAULT_SESSION_RE = r"^(behavior_)?\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"


class LegacyFallbackConfig(BaseModel):
    """Pass B — bounded legacy fallback for sessions predating typed acquisition metadata."""

    model_config = ConfigDict(extra="forbid")

    project_name: str = Field(
        default="Cognitive flexibility in patch foraging",
        description="DocDB data_description.project_name to match in the legacy pass.",
    )
    session_before: str = Field(
        default="2026-01-01",
        description="Only sessions acquired before this date qualify for the fallback. "
        "Acquisition/session start, NOT DocDB `created`.",
    )


class IngestionConfig(BaseModel):
    """Discovery. ``type: local`` is for testing and offline debugging; ``type: manifest``
    processes exactly the sessions named in a file and nothing else."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["docdb", "local", "manifest"] = Field(
        default="docdb",
        description="Discovery backend: `docdb` queries the metadata index, `local` scans "
        "`root` for session directories, `manifest` processes exactly the sessions in "
        "`manifest_file`.",
    )
    acquisition_types: list[str] = Field(
        default=["AindVrForaging"],
        description="`docdb` only — acquisition/session types treated as VR foraging.",
    )
    name_pattern: str = Field(
        default=_DEFAULT_SESSION_RE,
        description="Regex a session name must match to be ingested, for every source.",
    )
    legacy_fallback: LegacyFallbackConfig | None = Field(
        default=None,
        description="`docdb` only — enable the bounded project-name fallback for sessions "
        "with no typed acquisition metadata. Omit to disable it.",
    )
    buckets: list[str] = Field(
        default=[],
        description="Restrict discovery to assets in these buckets. Empty means no restriction.",
    )
    deny_list_file: Path | None = Field(
        default=None,
        description="File of session names to never ingest, one per line.",
    )
    interval_s: int = Field(
        default=300,
        description="Seconds between discovery sweeps. Rounded up to the next "
        "`worker.poll_interval_s` tick, and applied per worker process.",
    )
    subject_ids: list[str] = Field(
        default=[],
        description="Restrict discovery to these subjects. Empty means all.",
    )
    session_after: str | None = Field(
        default=None,
        description="Ignore sessions acquired before this date (`YYYY-MM-DD`).",
    )
    root: Path | None = Field(
        default=None,
        description="`type: local` only — directory scanned for session subdirectories.",
    )
    # The locations come from the file and may span several buckets; `input.store: s3`
    # takes the bucket from each `input_uri`, so nothing bucket-shaped is configured here.
    manifest_file: Path | None = Field(
        default=None,
        description="`type: manifest` only — JSON file of `{session_name, location}` entries. "
        "A finite set, so this pairs with `worker.exit_when_drained`.",
    )

    @model_validator(mode="after")
    def _type_has_its_input(self) -> "IngestionConfig":
        """A missing path is a config error, and it has to surface at startup: the
        alternative is a container that ingests nothing, drains immediately and reports
        a successful run over zero sessions."""
        if self.type == "manifest" and self.manifest_file is None:
            raise ValueError("ingestion.type is 'manifest' but ingestion.manifest_file is unset")
        if self.type == "local" and self.root is None:
            raise ValueError("ingestion.type is 'local' but ingestion.root is unset")
        return self


class InputConfig(BaseModel):
    """Input store. ``mount`` is the default: no copy, the read set decides."""

    model_config = ConfigDict(extra="forbid")

    store: Literal["s3", "mount", "local"] = Field(
        default="mount",
        description="How a session's bytes reach the processor: `mount` bind-mounts them, "
        "`s3` downloads the staged subset, `local` reads a directory already on this host.",
    )
    root: Path | None = Field(
        default=None,
        description="`mount`/`local` — identity-mapped host path holding the sessions.",
    )
    copy_files: bool = Field(
        default=False,
        description="`local` only — `false` bind-mounts the source directly, `true` copies it into the work volume.",
    )
    record_read_set: bool = Field(
        default=True,
        description="Record which files the processor actually opened, via `sys.addaudithook`, "
        "into `sidecar.staged.read_files`.",
    )


class StagingRule(BaseModel):
    """One include/exclude rule, evaluated under ``path``."""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(description="Session-relative directory the rule applies under. `''` is the session root.")
    include: list[str] = Field(default=["**/*"], description="Glob patterns to stage.")
    exclude: list[str] = Field(default=[], description="Glob patterns to drop from what `include` matched.")
    recursive: bool = Field(default=True, description="Whether `path` is descended into.")


_DEFAULT_STAGING_RULES = [
    StagingRule(path="", include=["*.json"], recursive=False),
    StagingRule(path="behavior", include=["**/*"], exclude=["**/*.mp4", "**/*.avi", "**/*.mkv"]),
    # No `exclude` here: `include` is already the complete specification (only
    # these two extensions), so an `exclude: ["**/*"]` would blanket-exclude
    # everything the include list had just matched — self-defeating under a
    # plain include-then-exclude engine.
    StagingRule(path="behavior-videos", include=["**/*.csv", "**/*.json"]),
    # Pre-correction originals — never read by any processor, cheap and
    # useful for provenance. Not read by any current processor; a real hit was
    # never observed, but they cost ~135 KB and answer "what did this field say
    # before it was corrected" if that is ever asked.
    StagingRule(path="original_metadata", include=["*.json"]),
]


class StagingConfig(BaseModel):
    """Applies to ``store: s3``/``local``; advisory (sidecar-only) under ``mount``."""

    model_config = ConfigDict(extra="forbid")

    rules: list[StagingRule] = Field(
        default_factory=lambda: list(_DEFAULT_STAGING_RULES),
        description="Ordered include/exclude rules deciding which of a session's files are staged.",
    )
    verify_present: list[str] = Field(
        default=["data_description.json"],
        description="Files that must be among the staged set, or the job fails as `data` "
        "before a container is launched.",
    )
    max_session_bytes: int = Field(
        default=2_000_000_000,
        description="Refuse to stage a session whose selected files exceed this.",
    )


class OutputConfig(BaseModel):
    """Output store. Data first, ``output.metadata.json`` last (commit marker)."""

    model_config = ConfigDict(extra="forbid")

    store: Literal["s3", "local"] = Field(default="s3", description="Where published output goes.")
    uri: str = Field(description="Root of the output store, e.g. `s3://bucket/prefix` or `/out`.")
    overwrite: bool = Field(
        default=False,
        description="Re-publish a session whose output already exists. `false` marks such jobs `skipped`.",
    )
    max_aggregate_bytes: int = Field(default=5_000_000_000, description="Size ceiling for one aggregate table.")
    # One prefix for every outcome, so `jobs.log_uri` means the same kind of thing in
    # every row rather than depending on how the job ended.
    log_prefix: str = Field(
        default="logs/",
        description="Per-job log prefix, under `{uri}/{release}/`.",
    )


class ProcessorConfig(BaseModel):
    """The processor image the worker launches per job."""

    model_config = ConfigDict(extra="forbid")

    image: str = Field(
        default="ghcr.io/allenneuraldynamics/aind-behavior-vr-foraging-packaging",
        description="Processor image, without a tag or digest.",
    )
    digest: str | None = Field(
        default=None,
        description="`sha256:…` image digest, required unless `allow_unpinned`. Pin by digest, never by tag.",
    )
    allow_unpinned: bool = Field(
        default=False,
        description="Run without a digest and without a pinned worker image. Local development only — "
        "nothing will identify the code that produced a campaign's output.",
    )
    write_nwb: bool = Field(default=True, description="Write NWB alongside parquet. Much slower.")
    exclude_processors: list[str] = Field(default=[], description="Processor names to skip in every session.")
    job_timeout_s: int = Field(default=3600, description="Kill one session's container after this long.")
    cpus: float = Field(default=2, description="CPU limit passed to the processor container.")
    memory: str = Field(default="8g", description="Memory limit passed to the processor container.")
    reprocess_on_digest_change: Literal["none", "filtered", "all"] = Field(
        default="none",
        description="What a new image digest reprocesses: `none`, `all`, or `filtered` "
        "(only sessions carrying the `reprocess` tag).",
    )


class WorkerConfig(BaseModel):
    """Claim loop, lease reaping, disk gate."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default="worker-1", description="Identifies this worker in leases and heartbeats.")
    ledger: Path = Field(default=Path("/var/lib/vrf/jobs.sqlite"), description="SQLite job ledger.")
    work_volume: str = Field(
        default="vrf_work",
        description="Named Docker volume shared with the processor containers, mounted at `/work`.",
    )
    max_concurrent_jobs: int = Field(default=3, description="Processor containers in flight at once.")
    # Checked before claiming rather than during: a job that dies on ENOSPC burns one of
    # `max_attempts` for a reason that has nothing to do with the session.
    min_free_disk_bytes: int = Field(
        default=20_000_000_000,
        description="Refuse to claim work below this much free space on the work volume.",
    )
    keep_work_dir: bool = Field(
        default=False,
        description="Debugging: keep job directories instead of reclaiming them. Disables the "
        "exit-side cleanup and the sweep, but not the entry-side cleanup. Unbounded growth — "
        "never leave on for a campaign.",
    )
    lease_seconds: int = Field(
        default=5400,
        description="How long a claim is held before another worker may reap it as expired.",
    )
    max_attempts: int = Field(
        default=3,
        description="Attempts before a transiently-failing job becomes `dead`. `data`/`code` "
        "failures are terminal on the first attempt.",
    )
    poll_interval_s: int = Field(default=30, description="Seconds to sleep when there is nothing to claim.")
    # `retrying` does not trigger it: a transient failure with attempts left is not a
    # failure yet, and stopping on one would fire on a blip in S3 rather than on bad data.
    # Nothing is aggregated on this path either — an aggregate over a partial set would be
    # published as though the run had finished.
    fail_fast: bool = Field(
        default=False,
        description="Exit at the first session that fails for good, instead of working through "
        "the rest. Independent of `exit_when_drained`; useful when a run is a canary for a "
        "code change rather than a campaign.",
    )
    # Deliberately not inferred from `ingestion.type` — "does this process exit?" is the
    # single most consequential thing about how a container behaves, and it should be
    # readable in one field rather than derived from another.
    exit_when_drained: bool = Field(
        default=False,
        description="Exit once every session job for this release is terminal, instead of polling "
        "forever. With `ingestion.type: manifest` this turns the worker into a batch run: process "
        "the list, aggregate, exit with a status.",
    )


class AggregationConfig(BaseModel):
    """Once-a-day re-aggregation, inside the worker. Runs for as long as the worker lives.

    A wall-clock schedule rather than an interval: aggregation reads every published
    session, so it belongs at a quiet hour, not on a poll loop.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Whether the worker aggregates at all.")
    at: str = Field(
        default="03:00",
        description="Local clock time to aggregate, `HH:MM` in 24-hour time, once per day in `timezone`.",
    )
    timezone: str = Field(
        default="America/Los_Angeles",
        description="IANA zone name for `at`. Validated at startup, so a typo fails then rather than at 3am.",
    )
    job_timeout_s: int = Field(default=3600, description="Lease length for one aggregate job.")

    @field_validator("at")
    @classmethod
    def _valid_at(cls, value: str) -> str:
        hour, _, minute = value.partition(":")
        if not (hour.isdigit() and minute.isdigit() and 0 <= int(hour) <= 23 and 0 <= int(minute) <= 59):
            raise ValueError(f"aggregation.at must be HH:MM in 24-hour time, got {value!r}")
        return value

    @field_validator("timezone")
    @classmethod
    def _valid_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except Exception as exc:
            raise ValueError(f"aggregation.timezone {value!r} is not a known IANA zone: {exc}") from exc
        return value

    def scheduled_time(self, now_local: datetime) -> datetime:
        """Today's scheduled moment, in *now_local*'s zone."""
        hour, minute = (int(part) for part in self.at.split(":"))
        return now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)


class DashboardConfig(BaseModel):
    """One sortable sessions table with a whitelisted set of queue actions."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Serve the dashboard.")
    # Inside a container behind a published port this has to be 0.0.0.0, or the port
    # forwarder cannot reach into the container's netns; restrict the host side instead.
    bind: str = Field(
        default="127.0.0.1",
        description="Address to listen on. There is no authentication, so do not expose this — "
        "reach it over an SSH tunnel.",
    )
    port: int = Field(default=8080, description="Port to listen on.")
    refresh_s: int = Field(default=30, description="Seconds between browser auto-refreshes.")
    allow_actions: bool = Field(
        default=True,
        description="Permit queue actions. `false` serves a read-only view (`mode=ro`).",
    )
    confirm_threshold: int = Field(
        default=25,
        description="Bulk actions affecting more rows than this require a confirmation step.",
    )


class LoggingConfig(BaseModel):
    """One log per job attempt, published to the output store then removed locally."""

    model_config = ConfigDict(extra="forbid")

    level: str = Field(default="INFO", description="Root log level.")
    dir: Path = Field(default=Path("/var/lib/vrf/logs"), description="Where per-job logs are written locally.")
    upload: bool = Field(
        default=True,
        description="Publish each log under `output.log_prefix`, then delete the local copy. "
        "`false` keeps them local forever, and nothing prunes them.",
    )
    max_capture_bytes: int = Field(default=50_000_000, description="Cap on one job's captured log.")
    suppress: list[str] = Field(
        default=[],
        description="Logger-name prefixes to drop entirely, e.g. pynwb's DynamicTable boilerplate.",
    )


class PipelineConfig(BaseSettings):
    """Root pipeline configuration, loaded from YAML via :meth:`from_yaml`.

    ``extra="forbid"`` at every level: a typo'd key fails validation rather than
    being silently ignored. Env-var overrides (``VRF__SECTION__FIELD``) take
    priority over the YAML file — see :meth:`settings_customise_sources`.
    """

    model_config = SettingsConfigDict(extra="forbid", env_prefix="VRF__", env_nested_delimiter="__")

    release: str = Field(
        description="Campaign name (e.g. `manuscript-2026-08-13`), used as the output prefix. One release, one digest.",
    )
    ingestion: IngestionConfig = Field(default=IngestionConfig(), description="Which sessions to process.")
    input: InputConfig = Field(default=InputConfig(), description="How their bytes reach the processor.")
    staging: StagingConfig = Field(default=StagingConfig(), description="Which of a session's files are staged.")
    output: OutputConfig = Field(description="Where results are published.")
    processor: ProcessorConfig = Field(default=ProcessorConfig(), description="The image launched per session.")
    worker: WorkerConfig = Field(default=WorkerConfig(), description="Claim loop, leases, disk gate, exit policy.")
    aggregation: AggregationConfig = Field(default=AggregationConfig(), description="The daily aggregate.")
    dashboard: DashboardConfig = Field(default=DashboardConfig(), description="The read-mostly web view.")
    logging: LoggingConfig = Field(default=LoggingConfig(), description="Log capture and publishing.")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Env vars override the YAML file (loaded via init kwargs in :meth:`from_yaml`)."""
        return (env_settings, init_settings)

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        """Load and validate config from a YAML file at *path*."""
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a YAML mapping at the top level, got {type(data).__name__}")
        return cls(**data)
