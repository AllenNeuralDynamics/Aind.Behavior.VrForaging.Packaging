"""CLI entry point for the experiment export pipeline.

Provides the ``vr-foraging-export`` command, which runs two independent phases:

1. **process_sessions** — run all processors on every raw session directory.
2. **aggregate** — concatenate per-session parquets into subject/dataset outputs.

Usage examples::

    # Full run
    aind-vr-export --input-dir /data/raw --output-dir /data/export

    # Skip sniffing, write a log file
    aind-vr-export --input-dir /data/raw --output-dir /data/export \\
        --exclude-processors sniffing software_events \\
        --log-file /data/export/run.log

    # Re-aggregate only (sessions/ already written by a previous run)
    aind-vr-export --input-dir /data/raw --output-dir /data/export \\
        --skip-processing
"""

import logging
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, CliApp, SettingsConfigDict

from .export_pipeline import (
    AggregationRule,
    Aggregator,
    aggregate,
    process_sessions,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------


def _setup_logging(log_file: Path | None = None, level: int = logging.INFO) -> None:
    """Configure the root logger.

    A ``StreamHandler`` is added only when no handlers exist yet (avoids
    duplicating pytest's own capture handler). A ``FileHandler`` is *always*
    added when *log_file* is provided.
    """
    root = logging.getLogger()
    root.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    if not root.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        root.addHandler(ch)

    if log_file is not None:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)


# ---------------------------------------------------------------------------
# Settings / CLI
# ---------------------------------------------------------------------------


class ExportSettings(BaseSettings):
    """Pydantic-settings model whose fields map 1-to-1 to CLI flags.

    ``CliApp.run(ExportSettings)`` parses ``sys.argv``, validates all fields,
    and calls :meth:`cli_cmd`.
    """

    model_config = SettingsConfigDict(cli_parse_args=True, cli_kebab_case=True)

    # ---- Required ----
    input_dir: Path
    """Root folder whose immediate subdirectories are raw session directories."""
    output_dir: Path
    """Destination root for the experiment export."""

    # ---- Logging ----
    log_file: Path | None = None
    """Path to a log file. Created if absent; appended to if it already exists."""

    # ---- Processor filter (by output_name) ----
    include_processors: list[str] = []
    """Processor output names to run (empty = all). E.g. ``sites licks``."""
    exclude_processors: list[str] = []
    """Processor output names to skip. E.g. ``sniffing software_events``."""

    # ---- Aggregation ----
    dataset_tables: list[str] = ["sites"]
    """Table names to write as flat aggregate parquet files (one file per table, all sessions)."""

    # ---- Phase control ----
    skip_processing: bool = False
    """Skip Phase 1; useful to re-aggregate after sessions/ is already written."""
    skip_aggregation: bool = False
    """Skip Phase 2; write only per-session parquets."""

    raise_on_error: bool = False
    """Raise on the first processor failure (default: log and continue)."""

    workers: int = 1
    """Number of parallel threads for Phase 1. 1 = sequential (default)."""

    # ------------------------------------------------------------------ #

    def _build_aggregator(self) -> Aggregator:
        return Aggregator(rules=[AggregationRule(t) for t in self.dataset_tables])

    def cli_cmd(self) -> None:
        _setup_logging(self.log_file)
        logger.info("=== aind-vr-export started ===")
        logger.info("  input_dir  : %s", self.input_dir)
        logger.info("  output_dir : %s", self.output_dir)

        dataset_paths = sorted(p for p in self.input_dir.iterdir() if p.is_dir())
        if not dataset_paths:
            logger.warning("No subdirectories found under %s — nothing to do.", self.input_dir)
            return

        logger.info("  sessions found: %d", len(dataset_paths))
        sessions_dir = self.output_dir / "sessions"

        if not self.skip_processing:
            process_sessions(
                dataset_paths,
                self.output_dir,
                include_processors=self.include_processors,
                exclude_processors=self.exclude_processors,
                raise_on_error=self.raise_on_error,
                max_workers=self.workers,
            )

        if not self.skip_aggregation:
            aggregator = self._build_aggregator()
            aggregate(sessions_dir, self.output_dir, aggregator)

        logger.info("=== aind-vr-export complete ===")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    CliApp.run(ExportSettings)
