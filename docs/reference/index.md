# Reference

Look things up here: every column of every table, every raw stream, and the Python API.

## Processed tables vs raw streams

There are two kinds of data, documented on separate pages. Make sure you are looking at the right one.

| | [Processed tables](processed-tables.md) | [Raw data contract](raw-data-contract.md) |
| --- | --- | --- |
| **What** | Analysis-ready parquet tables (and NWB) | The files the rig wrote to disk |
| **Made by** | This package | The task and rig software |
| **Format** | Parquet, one column per variable | Harp binaries, JSON, CSV, video, ... |
| **Where** | The [shared dataset](../data/index.md), or your own [export](../processing/many-sessions.md) | Each session's raw data asset |
| **Read with** | Polars or pandas | [contraqctor](../processing/raw-streams.md) |

For analysis, start from the processed tables.
Both pages are generated from the code, so they always match the installed versions.

## Python API

For running this package yourself ([Process raw sessions](../processing/index.md)):

| Page | Covers |
| --- | --- |
| [Session pipeline](../api/session.md) | `process_session`, `create_processors` |
| [Export pipeline](../api/batch.md) | `process_sessions`, `aggregate` |
| [CLI](../api/cli.md) | The `vr-foraging-packaging` command |
| [Processors](../api/processors.md) | One class per table, and how to write your own |
| [NWB export](../api/nwb.md) | `NwbSession` |

## For maintainers

[Schema migrations](../schema-migrations.md) explains how older sessions are brought up to the current schema, and [its review notes](../schema-migration-notes.md) record the decisions behind it.
