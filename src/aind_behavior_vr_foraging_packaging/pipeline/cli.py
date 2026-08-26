"""The ``vr-foraging-packaging`` command: one subcommand per pipeline function.

    session   --input-dir <one raw session>    --output-dir <dest>
    batch     --input-dir <folder of sessions> --output-dir <dest>
    aggregate --input-dir <a sessions/ tree>   --output-dir <dest>

Every flag maps onto a parameter of the function the subcommand names; the only
logic here is rejecting combinations that cannot work.
"""

import logging
import sys
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, CliApp, CliSubCommand, SettingsConfigDict

from .batch import aggregate, process_sessions
from .session import process_session

logger = logging.getLogger(__name__)


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
# Shared option groups
# ---------------------------------------------------------------------------


class _Command(BaseModel):
    """Base for every subcommand: shared I/O flags and logging setup.

    Subclasses implement :meth:`run`; ``cli_cmd`` is the hook pydantic-settings
    dispatches to, and exists only so logging is configured exactly once, in one
    place, for all three.
    """

    input_dir: Path
    """What this subcommand reads. See each subcommand for what it must contain."""
    output_dir: Path = Path(".")
    """Where results are written. Defaults to the current directory."""

    log_file: Path | None = None
    """Path to a log file. Created if absent; appended to if it already exists."""

    def cli_cmd(self) -> None:
        _setup_logging(self.log_file)
        logger.info("=== vr-foraging-packaging %s ===", type(self).__name__)
        logger.info("  input_dir  : %s", self.input_dir)
        logger.info("  output_dir : %s", self.output_dir)
        self.run()
        logger.info("=== complete ===")

    def run(self) -> None:  # pragma: no cover - overridden by every subclass
        raise NotImplementedError


class _ProcessingCommand(_Command):
    """Adds the options that select processors and output formats.

    Shared by ``session`` and ``batch`` because both ultimately call
    :func:`~.session.process_session`, which is what these map onto.
    """

    include_processors: list[str] = []
    """Processor output names to run (empty = all). E.g. ``sites licks``."""
    exclude_processors: list[str] = []
    """Processor output names to skip. E.g. ``sniffing software_events``."""

    strict_parsing: bool = False
    """Treat a known, anticipated data anomaly as fatal instead of logging it and
    falling back to a degraded-but-meaningful output. Does not gate general
    exceptions — an unexpected failure always propagates either way."""

    write_parquet: bool = True
    """Write per-session parquet tables."""
    write_nwb: bool = False
    """Write one NWB-Zarr store per session, named ``behavior.nwb.zarr``."""


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


class SessionCommand(_ProcessingCommand):
    """Export ONE raw session directory.

    ``--input-dir`` is the session root itself, not a folder containing sessions.
    Outputs land directly in ``--output-dir`` (no ``sessions/`` level), because
    there is only one session to keep apart.
    """

    def run(self) -> None:
        process_session(
            self.input_dir,
            self.output_dir,
            include=self.include_processors,
            exclude=self.exclude_processors,
            strict_parsing=self.strict_parsing,
            write_parquet=self.write_parquet,
            write_nwb=self.write_nwb,
        )


class BatchCommand(_ProcessingCommand):
    """Export a FOLDER of raw session directories, then aggregate.

    ``--input-dir`` is scanned one level deep: every immediate subdirectory is
    taken to be one raw session. Per-session outputs go to
    ``--output-dir/sessions/{session_id}/``, experiment-level files to
    ``--output-dir`` itself.
    """

    workers: int = 1
    """Number of parallel threads for the per-session phase. 1 = sequential."""
    clean: bool = True
    """Delete --output-dir before writing, so a re-run never mixes two invocations."""
    skip_aggregation: bool = False
    """Write only per-session outputs. Aggregate later with the `aggregate` subcommand."""

    def run(self) -> None:
        if not self.write_parquet and not self.skip_aggregation:
            raise ValueError(
                "--no-write-parquet leaves aggregation nothing to read. "
                "Pass --skip-aggregation as well, or keep parquet output on."
            )

        session_paths = sorted(p for p in self.input_dir.iterdir() if p.is_dir())
        if not session_paths:
            logger.warning("No subdirectories found under %s — nothing to do.", self.input_dir)
            return
        logger.info("  sessions found: %d", len(session_paths))

        process_sessions(
            session_paths,
            self.output_dir,
            include_processors=self.include_processors,
            exclude_processors=self.exclude_processors,
            strict_parsing=self.strict_parsing,
            max_workers=self.workers,
            clean=self.clean,
            write_parquet=self.write_parquet,
            write_nwb=self.write_nwb,
        )

        if not self.skip_aggregation:
            aggregate(self.output_dir / "sessions", self.output_dir)


class AggregateCommand(_Command):
    """Concatenate already-exported sessions into experiment-level tables.

    ``--input-dir`` is a ``sessions/`` tree — one subdirectory per session, each
    holding the parquets a previous ``session`` or ``batch`` run wrote. Nothing
    is re-processed and nothing is deleted, so this is safe to re-run.
    """

    def run(self) -> None:
        aggregate(self.input_dir, self.output_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


class Cli(BaseSettings):
    """Root parser: dispatches to whichever subcommand was named."""

    model_config = SettingsConfigDict(
        cli_parse_args=True,
        cli_kebab_case=True,
        cli_implicit_flags=True,
        cli_prog_name="vr-foraging-packaging",
    )

    session: CliSubCommand[SessionCommand] = Field(description="Export one raw session directory.")
    batch: CliSubCommand[BatchCommand] = Field(
        description="Export a folder of raw session directories, then aggregate."
    )
    aggregate: CliSubCommand[AggregateCommand] = Field(
        description="Aggregate an already-exported sessions/ tree; re-processes nothing."
    )

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)


def main() -> None:
    CliApp.run(Cli)
