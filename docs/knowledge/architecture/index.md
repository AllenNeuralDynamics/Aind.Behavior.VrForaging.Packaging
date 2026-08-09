# Architecture

How the code is structured. The design is deliberately simple: a single
abstract base class defines the processor contract, concrete processors each
own one output, and a thin pipeline layer dispatches on dataset version and
fans out. That layer comes in two tiers — one session
([session-pipeline.md](session-pipeline.md)) and many
([export-pipeline.md](export-pipeline.md)) — with the second built entirely on
the first.

Read in this order:

- [processor-abstraction.md](processor-abstraction.md) — `AbstractProcessor`: the contract every processor implements (`_compute`/`compute`, `nwbize`, `output_name`, provenance stamping).
- [session-pipeline.md](session-pipeline.md) — `create_processors`, `run_session`, and the per-processor getters; version dispatch and parquet writing for **one** session.
- [site-table.md](site-table.md) — `SiteTableProcessor` and the `Site` model — the most complex processor and the core scientific output.
- [continuous-and-event-streams.md](continuous-and-event-streams.md) — Position/velocity, licks, sniffing, and software events processors.
- [nwb-packaging.md](nwb-packaging.md) — `NwbSession`: building the base `NWBFile`, stamping provenance, and driving `nwbize()`.
- [export-pipeline.md](export-pipeline.md) — `process_sessions` / `aggregate` and the `aind-vr-export` CLI: **many** sessions into one queryable parquet export.
- [data-contract-and-versioning.md](data-contract-and-versioning.md) — The `contraqctor` dataset, Harp streams, AIND metadata, and the three versions the code tracks.

## Package layout

```
src/aind_behavior_vr_foraging_packaging/
├── __init__.py           # __version__, __semver__ (pep440_to_semver)
├── _base.py              # AbstractProcessor
├── _provenance.py        # PackagingProvenance — the only definition of the version keys
├── models.py             # Site (pydantic) — site table row schema
├── session_pipeline.py   # create_processors, run_session, getters, parquet writer (one session)
├── export_pipeline.py    # process_sessions, aggregate, Aggregator (many sessions)
├── cli.py                # `aind-vr-export` entry point (pydantic-settings CliApp)
├── acquisition/
│   └── helper.py         # DataFrame → NWB-safe coercions
├── nwb_file/
│   └── __init__.py       # NwbSession
└── processing/
    ├── _site_table.py                  # SiteTableProcessor + DatasetProcessorError
    ├── _legacy_site_table.py           # LegacySiteTableProcessor (schema < 0.6.0)
    ├── _position_and_velocity.py        # PositionAndVelocityProcessor
    ├── _legacy_position_and_velocity.py # LegacyPositionAndVelocityProcessor
    ├── _licks.py                        # LicksProcessor
    ├── _sniffing.py                     # SniffingProcessor
    ├── _software_events.py              # SoftwareEventsProcessor
    ├── _events.py                       # EventsProcessor (derived events → EventsTable)
    ├── _session_metadata.py             # SessionMetadataProcessor (one row per session)
    └── _helper.py                       # slice_by_index, get_closest_from_timestamp
```

Processor modules are private (`_`-prefixed); the public surface is
re-exported from `processing/__init__.py`.
