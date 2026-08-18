"""Pipelines: one session, many sessions, and the CLI over both.

Three layers, each a thin wrapper over the one below:

* :mod:`.session` — everything about a single session: version dispatch,
  processor construction and filtering, and writing parquet/NWB.
* :mod:`.batch` — many sessions (Phase 1) plus experiment-level aggregation
  (Phase 2). Adds only what is genuinely multi-session.
* :mod:`.cli` — the ``vr-foraging-packaging`` command, one subcommand per
  public function above.
"""

from .batch import AGGREGATED_TABLES, SESSION_TABLE, aggregate, aggregate_tables, process_sessions
from .session import (
    create_processors,
    filter_processors,
    process_session,
    resolve_position_velocity_processor,
    resolve_site_table_processor,
)

__all__ = [
    "AGGREGATED_TABLES",
    "SESSION_TABLE",
    "aggregate",
    "aggregate_tables",
    "create_processors",
    "filter_processors",
    "process_session",
    "process_sessions",
    "resolve_position_velocity_processor",
    "resolve_site_table_processor",
]
