# Architecture

How the code is structured. The design is deliberately simple: a single
abstract base class defines the processor contract, concrete processors each
own one output, and a thin pipeline layer dispatches on dataset version and
fans out.

Read in this order:

- [processor-abstraction.md](processor-abstraction.md) — `AbstractProcessor`: the contract every processor implements (`_compute`/`compute`, `nwbize`, `output_name`, provenance stamping).
- [pipeline.md](pipeline.md) — `create_processors`, `run_session`, and the per-processor getters; version dispatch and parquet writing.
- [trial-table.md](trial-table.md) — `TrialTableProcessor` and the `Site` model — the most complex processor and the core scientific output.
- [continuous-and-event-streams.md](continuous-and-event-streams.md) — Position/velocity, licks, sniffing, and software events processors.
- [nwb-packaging.md](nwb-packaging.md) — `NwbSession`: building an `NdxEventsNWBFile` and driving `nwbize()`.
- [data-contract-and-versioning.md](data-contract-and-versioning.md) — The `contraqctor` dataset, Harp streams, AIND metadata, and the three versions the code tracks.

## Package layout

```
src/aind_behavior_vr_foraging_packaging/
├── __init__.py          # __version__, __semver__ (pep440_to_semver)
├── _base.py             # AbstractProcessor
├── models.py            # Site (pydantic) — trial table row schema
├── pipeline.py          # create_processors, run_session, getters, parquet writer
├── cli.py               # `curriculum` entry point (currently a stub)
├── acquisition/
│   └── helper.py        # DataFrame → NWB-safe coercions
├── nwb_file/
│   └── __init__.py      # NwbSession, _AindDataSchemaJson
└── processing/
    ├── _trial_table.py                  # TrialTableProcessor + DatasetProcessorError
    ├── _legacy_trial_table.py           # LegacyTrialTableProcessor (schema < 0.6.0)
    ├── _position_and_velocity.py        # PositionAndVelocityProcessor
    ├── _legacy_position_and_velocity.py # LegacyPositionAndVelocityProcessor
    ├── _licks.py                        # LicksProcessor
    ├── _sniffing.py                     # SniffingProcessor
    ├── _software_events.py              # SoftwareEventsProcessor
    └── _helper.py                       # slice_by_index, get_closest_from_timestamp
```

Processor modules are private (`_`-prefixed); the public surface is
re-exported from `processing/__init__.py`.
