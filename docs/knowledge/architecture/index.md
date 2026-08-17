# Architecture

How the code is structured. The design is deliberately simple: a single
abstract base class defines the processor contract, concrete processors each
own one output, and a thin pipeline layer dispatches on dataset version and
fans out. That layer is the `pipeline/` package, in three tiers — one session
([session.md](session.md)), many
([batch.md](batch.md)), and the command line
([cli.md](cli.md)) — each built entirely on the one before it.

Read in this order:

- [processor-abstraction.md](processor-abstraction.md) — `AbstractProcessor`: the contract every processor implements (`_compute`/`compute`, `nwbize`, `output_name`, provenance stamping).
- [session.md](session.md) — `create_processors`, `process_session`, and the `resolve_*` per-processor getters; version dispatch and parquet writing for **one** session.
- [site-table.md](site-table.md) — `SiteTableProcessor` and the `Site` model — the most complex processor and the core scientific output.
- [continuous-and-event-streams.md](continuous-and-event-streams.md) — Position/velocity, licks, sniffing, and software events processors.
- [nwb-packaging.md](nwb-packaging.md) — `NwbSession`: building the base `NWBFile`, stamping provenance, and driving `nwbize()`.
- [batch.md](batch.md) — `process_sessions` / `aggregate`: **many** sessions into one queryable parquet export.
- [cli.md](cli.md) — the `vr-foraging-packaging` command: one subcommand per pipeline function.
- [data-contract-and-versioning.md](data-contract-and-versioning.md) — The `contraqctor` dataset, Harp streams, AIND metadata, and the three versions the code tracks.
- [orchestration.md](orchestration.md) — the **second distribution**: how one session becomes one container, the `output.metadata.json` sidecar, what the work volume holds and for how long, and how to run the whole pipeline on a laptop.

## Repo layout

Two distributions, one repo (a `uv` workspace). The dependency runs one way —
orchestration → packaging — and `tests/test_package_boundary.py` enforces it.

```
src/aind_behavior_vr_foraging_packaging/     # PUBLISHED to PyPI
├── __init__.py           # __version__, __semver__ (pep440_to_semver)
├── _base.py              # AbstractProcessor, DatasetProcessorError, cached_frame
├── _provenance.py        # PackagingProvenance — the only definition of the version keys
├── models.py             # Site (pydantic) — site table row schema
├── pipeline/
│   ├── session.py        # create_processors, process_session, resolve_* getters (one session)
│   ├── batch.py          # process_sessions, aggregate, AGGREGATED_TABLES (many sessions)
│   └── cli.py            # `vr-foraging-packaging` — one subcommand per function above
├── acquisition/
│   └── helper.py         # DataFrame → NWB-safe coercions
├── nwb_file/
│   └── __init__.py       # NwbSession
└── processing/
    ├── _site_table.py                  # SiteTableProcessor
    ├── _legacy_site_table.py           # LegacySiteTableProcessor (schema < 0.6.0)
    ├── _position_and_velocity.py        # PositionAndVelocityProcessor
    ├── _legacy_position_and_velocity.py # LegacyPositionAndVelocityProcessor
    ├── _licks.py                        # LicksProcessor
    ├── _sniffing.py                     # SniffingProcessor
    ├── _software_events.py              # SoftwareEventsProcessor
    ├── _events.py                       # EventsProcessor (derived events → EventsTable)
    ├── _session_metadata.py             # SessionMetadataProcessor (one row per session)
    └── _helper.py                       # slice_by_index, get_closest_from_timestamp

orchestration/src/aind_behavior_vr_foraging_orchestration/   # NEVER published
├── sidecar.py            # output.metadata.json + SidecarRecorder
├── process.py            # what runs inside a container: one session + its sidecar
├── ledger.py  models.py  # SQLite job queue
├── sources/  stores/     # which sessions exist / how bytes move
├── staging.py  runner.py  worker.py  dashboard.py
└── cli.py                # `vr-foraging-orchestrator`
```

Processor modules are private (`_`-prefixed); the public surface is
re-exported from `processing/__init__.py`.
