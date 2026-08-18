"""Pipeline configuration — one YAML file, validated, ``extra="forbid"`` so a typo
fails loudly rather than being silently ignored. ``docker/compose.yaml`` mounts a
worked example; docstrings here are deliberately short.

Env-var overrides use ``pydantic-settings`` with prefix ``VRF__`` and ``__`` as the
nested delimiter (e.g. ``VRF__WORKER__MAX_CONCURRENT_JOBS=5``), and take priority
over the YAML file — see :meth:`PipelineConfig.settings_customise_sources`.
"""

from datetime import datetime
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

_DEFAULT_SESSION_RE = r"^(behavior_)?\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"


class LegacyFallbackConfig(BaseModel):
    """Pass B — bounded legacy fallback for sessions predating typed acquisition metadata."""

    model_config = ConfigDict(extra="forbid")

    project_name: str = "Cognitive flexibility in patch foraging"
    session_before: str = "2026-01-01"
    """Acquisition/session start cutoff, NOT DocDB ``created``."""


class IngestionConfig(BaseModel):
    """Discovery. ``type: local`` is for testing and offline debugging."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["docdb", "local"] = "docdb"
    acquisition_types: list[str] = ["AindVrForaging"]
    name_pattern: str = _DEFAULT_SESSION_RE
    legacy_fallback: LegacyFallbackConfig | None = None
    buckets: list[str] = []
    deny_list_file: Path | None = None
    interval_s: int = 300
    """Ingest-timer poll interval, inside the worker. Rounded up to the next
    ``worker.poll_interval_s`` tick, and per worker process."""
    subject_ids: list[str] = []
    session_after: str | None = None
    root: Path | None = None
    """``type: local`` only — directory scanned for session subdirectories."""


class InputConfig(BaseModel):
    """Input store. ``mount`` is the default: no copy, the read set decides."""

    model_config = ConfigDict(extra="forbid")

    store: Literal["s3", "mount", "local"] = "mount"
    root: Path | None = None
    """``mount``/``local`` — identity-mapped host path holding the sessions."""
    copy_files: bool = False
    """``local`` only — ``False`` bind-mounts the source directly, ``True`` copies it."""
    record_read_set: bool = True
    """``sys.addaudithook`` read-set recording → ``sidecar.staged.read_files``."""


class StagingRule(BaseModel):
    """One include/exclude rule, evaluated under ``path``."""

    model_config = ConfigDict(extra="forbid")

    path: str
    include: list[str] = ["**/*"]
    exclude: list[str] = []
    recursive: bool = True


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

    rules: list[StagingRule] = Field(default_factory=lambda: list(_DEFAULT_STAGING_RULES))
    verify_present: list[str] = ["data_description.json"]
    max_session_bytes: int = 2_000_000_000


class OutputConfig(BaseModel):
    """Output store. Data first, ``output.metadata.json`` last (commit marker)."""

    model_config = ConfigDict(extra="forbid")

    store: Literal["s3", "local"] = "s3"
    uri: str
    overwrite: bool = False
    max_aggregate_bytes: int = 5_000_000_000
    log_prefix: str = "logs/"
    """Per-job logs, under ``{uri}/{release}/``. One prefix for every outcome, so
    ``jobs.log_uri`` means the same kind of thing in every row."""


class ProcessorConfig(BaseModel):
    """The processor image the worker launches per job."""

    model_config = ConfigDict(extra="forbid")

    image: str = "ghcr.io/allenneuraldynamics/aind-behavior-vr-foraging-packaging"
    digest: str | None = None
    """``sha256:…`` — required unless ``allow_unpinned``. Pin by digest, never by tag."""
    allow_unpinned: bool = False
    write_nwb: bool = True
    exclude_processors: list[str] = []
    job_timeout_s: int = 3600
    cpus: float = 2
    memory: str = "8g"
    reprocess_on_digest_change: Literal["none", "filtered", "all"] = "none"
    """``filtered`` reprocesses sessions carrying the ``reprocess`` tag."""


class WorkerConfig(BaseModel):
    """Claim loop, lease reaping, disk gate."""

    model_config = ConfigDict(extra="forbid")

    id: str = "worker-1"
    ledger: Path = Path("/var/lib/vrf/jobs.sqlite")
    work_volume: str = "vrf_work"
    max_concurrent_jobs: int = 3
    min_free_disk_bytes: int = 20_000_000_000
    """Refuse to claim below this much free space. Checked before claiming: a job
    that dies on ENOSPC burns one of ``max_attempts``."""
    keep_work_dir: bool = False
    """Debugging: keep job directories. Disables the exit-side cleanup and the
    sweep, but not the entry-side cleanup. Never leave on for a campaign — unbounded."""
    lease_seconds: int = 5400
    max_attempts: int = 3
    poll_interval_s: int = 30


class AggregationConfig(BaseModel):
    """Once-a-day re-aggregation, inside the worker. Runs for as long as the worker lives.

    A wall-clock schedule rather than an interval: aggregation reads every published
    session, so it belongs at a quiet hour, not on a poll loop.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    at: str = "03:00"
    """Local clock time to run, ``HH:MM`` (24-hour), once per day in :attr:`timezone`."""
    timezone: str = "America/Los_Angeles"
    """IANA name. Validated here so a typo fails at startup rather than at 3am."""
    job_timeout_s: int = 3600

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

    enabled: bool = True
    bind: str = "127.0.0.1"
    """NOT ``0.0.0.0`` — there is no auth; reach it over an SSH tunnel."""
    port: int = 8080
    refresh_s: int = 30
    allow_actions: bool = True
    """``False`` reverts to a read-only view (``mode=ro``)."""
    confirm_threshold: int = 25
    """Bulk actions over this many rows require a confirmation step."""


class LoggingConfig(BaseModel):
    """One log per job attempt, published to the output store then removed locally."""

    model_config = ConfigDict(extra="forbid")

    level: str = "INFO"
    dir: Path = Path("/var/lib/vrf/logs")
    upload: bool = True
    """Publish each log under ``output.log_prefix``, then delete the local copy.
    ``False`` keeps them local forever, and nothing prunes them."""
    max_capture_bytes: int = 50_000_000
    suppress: list[str] = []
    """Logger-name prefixes to drop entirely, e.g. pynwb's DynamicTable boilerplate."""


class PipelineConfig(BaseSettings):
    """Root pipeline configuration, loaded from YAML via :meth:`from_yaml`.

    ``extra="forbid"`` at every level: a typo'd key fails validation rather than
    being silently ignored. Env-var overrides (``VRF__SECTION__FIELD``) take
    priority over the YAML file — see :meth:`settings_customise_sources`.
    """

    model_config = SettingsConfigDict(extra="forbid", env_prefix="VRF__", env_nested_delimiter="__")

    release: str
    """Campaign name (e.g. ``manuscript-2026-08-13``) → output prefix. One release, one digest."""
    ingestion: IngestionConfig = IngestionConfig()
    input: InputConfig = InputConfig()
    staging: StagingConfig = StagingConfig()
    output: OutputConfig
    processor: ProcessorConfig = ProcessorConfig()
    worker: WorkerConfig = WorkerConfig()
    aggregation: AggregationConfig = AggregationConfig()
    dashboard: DashboardConfig = DashboardConfig()
    logging: LoggingConfig = LoggingConfig()

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
