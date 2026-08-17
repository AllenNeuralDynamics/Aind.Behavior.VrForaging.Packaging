"""Pipeline configuration — one YAML file, validated, ``extra="forbid"`` so a typo
fails loudly rather than being silently ignored. ``docker/compose.yaml`` mounts a
worked example; docstrings here are deliberately short.

Env-var overrides use ``pydantic-settings`` with prefix ``VRF__`` and ``__`` as the
nested delimiter (e.g. ``VRF__WORKER__MAX_CONCURRENT_JOBS=5``), and take priority
over the YAML file — see :meth:`PipelineConfig.settings_customise_sources`.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

_DEFAULT_SESSION_RE = r"^(behavior_)?\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"


class LegacyFallbackConfig(BaseModel):
    """§3 Pass B — bounded legacy fallback for sessions predating typed acquisition metadata."""

    model_config = ConfigDict(extra="forbid")

    project_name: str = "Cognitive flexibility in patch foraging"
    session_before: str = "2026-01-01"
    """Acquisition/session start cutoff, NOT DocDB ``created`` — see §3."""


class IngestionConfig(BaseModel):
    """§3 — discovery. ``type: local`` is for testing and offline debugging."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["docdb", "local"] = "docdb"
    acquisition_types: list[str] = ["AindVrForaging"]
    name_pattern: str = _DEFAULT_SESSION_RE
    legacy_fallback: LegacyFallbackConfig | None = None
    buckets: list[str] = []
    deny_list_file: Path | None = None
    interval_s: int = 3600
    """Ingest-timer poll interval, inside the worker."""
    subject_ids: list[str] = []
    session_after: str | None = None
    root: Path | None = None
    """``type: local`` only — directory scanned for session subdirectories."""


class InputConfig(BaseModel):
    """§10 — input store. ``mount`` is the default: no copy, the read set decides."""

    model_config = ConfigDict(extra="forbid")

    store: Literal["s3", "mount", "local"] = "mount"
    root: Path | None = None
    """``mount``/``local`` — identity-mapped host path (§4a) holding the sessions."""
    copy_files: bool = False
    """``local`` only — ``False`` bind-mounts the source directly, ``True`` copies it."""
    record_read_set: bool = True
    """``sys.addaudithook`` read-set recording → ``sidecar.staged.read_files`` (§10)."""


class StagingRule(BaseModel):
    """One include/exclude rule, evaluated under ``path`` (§10)."""

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
    # plain include-then-exclude engine (§10).
    StagingRule(path="behavior-videos", include=["**/*.csv", "**/*.json"]),
    # Pre-correction originals — never read by any processor (§10), cheap and
    # useful for provenance. Not read by any current processor; a real hit was
    # never observed, but they cost ~135 KB and answer "what did this field say
    # before it was corrected" if that is ever asked.
    StagingRule(path="original_metadata", include=["*.json"]),
]


class StagingConfig(BaseModel):
    """§10 — applies to ``store: s3``/``local``; advisory (sidecar-only) under ``mount``."""

    model_config = ConfigDict(extra="forbid")

    rules: list[StagingRule] = Field(default_factory=lambda: list(_DEFAULT_STAGING_RULES))
    verify_present: list[str] = ["data_description.json"]
    max_session_bytes: int = 2_000_000_000


class OutputConfig(BaseModel):
    """§10b — output store. Data first, ``output.metadata.json`` last (commit marker)."""

    model_config = ConfigDict(extra="forbid")

    store: Literal["s3", "local"] = "s3"
    uri: str
    overwrite: bool = False
    max_aggregate_bytes: int = 5_000_000_000
    failed_log_prefix: str = "failed/"


class ProcessorConfig(BaseModel):
    """§12 — the processor image the worker launches per job."""

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
    """``filtered`` reprocesses sessions carrying the ``reprocess`` tag (§6.3, §16)."""


class WorkerConfig(BaseModel):
    """§4/§7 — claim loop, lease reaping, disk gate."""

    model_config = ConfigDict(extra="forbid")

    id: str = "worker-1"
    ledger: Path = Path("/var/lib/vrf/jobs.sqlite")
    work_volume: str = "vrf_work"
    max_concurrent_jobs: int = 3
    max_disk_bytes: int = 200_000_000_000
    lease_seconds: int = 5400
    max_attempts: int = 3
    poll_interval_s: int = 30


class DashboardConfig(BaseModel):
    """§16 — one sortable sessions table with a whitelisted set of queue actions."""

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
    """§16 — per-job logs, never rotated (measured: ~78 MB for a 4700-session campaign)."""

    model_config = ConfigDict(extra="forbid")

    level: str = "INFO"
    dir: Path = Path("/var/lib/vrf/logs")
    upload: bool = True
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
    """Campaign name (e.g. ``manuscript-2026-08-13``) → output prefix. One release, one digest (§6)."""
    ingestion: IngestionConfig = IngestionConfig()
    input: InputConfig = InputConfig()
    staging: StagingConfig = StagingConfig()
    output: OutputConfig
    processor: ProcessorConfig = ProcessorConfig()
    worker: WorkerConfig = WorkerConfig()
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
