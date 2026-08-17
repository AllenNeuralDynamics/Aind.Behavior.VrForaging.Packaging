---
type: Component
title: Containerized pipeline — how the orchestration layer runs the processor
description: How one session becomes one container, what the sidecar is for, where the trust boundaries are, and how to run the whole thing on a laptop without S3, DocDB or a registry.
resource: orchestration/src/aind_behavior_vr_foraging_orchestration/
tags: [architecture, orchestration, docker, ledger, sidecar, testing, workspace]
timestamp: 2026-08-17T00:00:00Z
---

Two distributions live in this repo, and the split is load-bearing:

| | `aind-behavior-vr-foraging-packaging` | `aind-behavior-vr-foraging-orchestration` |
|---|---|---|
| Where | `src/` | `orchestration/src/` |
| Published | yes, to PyPI | **never** (`Private :: Do Not Upload`) |
| Answers | *turn this session into tables* | *run 4700 of those, and remember what happened* |
| Knows about | nothing below it | the packaging package |

The dependency runs **one way**. `pipeline/session.py` reports per-processor
outcomes through a generic `on_output` / `on_error` callback pair; it has never
heard of `output.metadata.json`, containers, or the ledger. That is enforced by
[`tests/test_package_boundary.py`](../../../tests/test_package_boundary.py), which
greps the AST of every published module — including function bodies, where a lazy
import would otherwise hide.

# How a session becomes a container

```
                       ┌─────────────────────────── HOST ────────────────────────────┐
                       │                                                             │
  DocDB ──discover──▶  │  ingest ──▶ ┌─────────┐                                     │
  (or a local dir)     │             │ ledger  │  SQLite. One row per (session,       │
                       │             │ .sqlite │  release, code fingerprint).         │
                       │             └────┬────┘                                     │
                       │                  │ claim (atomic, leased)                   │
                       │                  ▼                                          │
                       │            ┌───────────┐                                    │
                       │            │  worker   │  vr-foraging-orchestrator work      │
                       │            └─────┬─────┘                                    │
                       │                  │                                          │
                       │   1. stage       │   mount (default) or download to         │
                       │      input ──────┤   /work/{job}/in/{session_name}          │
                       │                  │                                          │
                       │   2. docker run ─┼──────────────┐                            │
                       │                  │              │                            │
                       │   5. classify ◀──┤              │   ┌── CONTAINER ────────┐  │
                       │      exit code   │              └──▶│ --network=none      │  │
                       │      × sidecar   │                  │                     │  │
                       │                  │                  │ vr-foraging-        │  │
                       │   6. publish ────┼──▶ S3 or local    │ orchestrator        │  │
                       │      output      │                  │   process           │  │
                       │                  │                  │                     │  │
                       │   7. record ─────┘                  │  ├ load dataset     │  │
                       │      in ledger                      │  ├ process_session  │  │
                       │                                     │  │   on_output ─┐   │  │
                       │  serve ──▶ dashboard (one table)     │  │   on_error ──┤   │  │
                       │                                     │  └ sidecar   ◀──┘   │  │
                       └─────────────────────────────────────┴─────────────────────┴──┘
                                                                       │
                                    /work/{job}/out/  ◀────────────────┘
                                      session.parquet, sites.parquet, …
                                      output.metadata.json
```

Three things in that picture are easy to get wrong, so they are worth naming.

**The container is offline.** `--network=none`. Everything it reads is already on
a mount or staged to the work volume, and everything it writes goes to
`/work/{job}/out` for the *worker* to publish. A processor cannot reach S3 even by
accident, which is also why credentials are a worker-only concern.

**The session's identity is its input directory's name.** `session_id` in every
table comes from `--input-dir`'s basename, unconditionally. So the worker mounts
each session *at* its true name — `/mnt/{session_name}`, or
`in/{session_name}` when staged. Get this wrong and nothing errors: every table is
simply stamped with the wrong session. It is why there is no `--session-name`
flag, and why `Worker._resolve_mount` is tested as an invariant rather than by its
paths.

**`-v` is interpreted by the daemon, not by the caller.** A containerized worker
launching sibling containers through the mounted docker socket must pass *host*
paths. The named work volume (`-v vrf_work:/work`) sidesteps this — the daemon
resolves it by name — and a pass-through mount gets one extra identity-mapped
bind mount, which `doctor` verifies before a campaign starts.

# The sidecar, and why it exists

`output.metadata.json`, one per session, written by the container. It is the only
channel for per-processor detail across the container boundary: once the process
exits, all the worker has is an exit code and a directory.

Crucially it does **not** change error behaviour. A failing processor still
propagates, so the container still exits nonzero; the recorder just notes the
failure on the way past. Recording is not tolerance —
`SidecarRecorder.on_error` re-raises after appending.

That gives `classify` two independent signals, and it needs both:

| exit | sidecar | verdict | why |
|---|---|---|---|
| timeout | — | `failed` / `timeout` | may not have written one |
| 137 | — | `failed` / `infra` | OOM-killed |
| any | unparsable | `failed` / `code` | our own writer is broken |
| 0 | missing | `failed` / `code` | died before writing; exit 0 is a lie |
| ≠0 | missing | `failed` / `code` | died before or outside the processor loop |
| any | `status=error` | `failed` / **`data`** | a named processor failed — *this session* is bad |
| ≠0 | `status=ok` | `failed` / `code` | contradiction; believe the exit code |
| 0 | `status=ok` | `completed` | |

`data` and `code` are both terminal (no retry — three identical stack traces help
nobody), so the distinction is for triage: it is the column you sort by to tell
"400 sessions our parser can't handle" from "the last release is broken".

# Running it locally

No S3, no DocDB, no registry, no ledger server. Everything below works on a
laptop against the integration cache.

## 1. One session, no container at all

The fastest loop. `process` is just a Python entry point:

```bash
uv sync                            # both workspace members, editable
SESSION=$(ls -d tests/integration/.cache/*/*/ | head -1)

uv run vr-foraging-orchestrator process \
    --input-dir "$SESSION" --output-dir /tmp/out

cat /tmp/out/output.metadata.json
```

Use this to debug a processor. The sidecar tells you which one failed and how many
warnings it logged; the traceback is on stdout.

## 2. One session, in a container

```bash
docker build -f docker/Dockerfile -t vrf:dev .
./docker/smoke-test.sh vrf:dev "$SESSION"
```

[`docker/smoke-test.sh`](../../../docker/smoke-test.sh) is the same script CI runs
after a release build — it mounts the session at its real name, runs `process`,
and asserts on the sidecar (status, session name, every processor `ok`, and, when
given a digest, that the image reported its own identity honestly). It prints the
output directory so a failure can be picked apart.

Omit the digest argument for a local build: provenance is then honestly recorded
as `unpinned`, and the digest assertions are skipped rather than faked.

## 3. The whole loop — worker, ledger, dashboard

Point the config at local directories and let the worker launch real containers:

```yaml
# /tmp/vrf/config.yaml
release: local-test
ingestion:
  type: local                      # a directory scan instead of DocDB
  root: /abs/path/to/tests/integration/.cache
input:
  store: local
  copy_files: false                # pass through; no staging copy
output:
  store: local
  uri: /tmp/vrf/out
worker:
  ledger: /tmp/vrf/jobs.sqlite
processor:
  image: vrf                       # your local build
  allow_unpinned: true             # local images have no digest
logging:
  dir: /tmp/vrf/logs
```

```bash
uv run vr-foraging-orchestrator doctor   --config /tmp/vrf/config.yaml
uv run vr-foraging-orchestrator ingest   --config /tmp/vrf/config.yaml --dry-run
uv run vr-foraging-orchestrator ingest   --config /tmp/vrf/config.yaml
uv run vr-foraging-orchestrator work     --config /tmp/vrf/config.yaml --once
uv run vr-foraging-orchestrator status   --config /tmp/vrf/config.yaml
uv run vr-foraging-orchestrator serve    --config /tmp/vrf/config.yaml   # :8080
```

Run `doctor` first, always. It checks the work volume is writable, the Docker
daemon is reachable, and an image is actually pinned or explicitly unpinned —
the three things that are cheap to verify and expensive to discover mid-campaign.

`--dry-run` on `ingest` before the real thing, and `work --once` before
`work` — the loop otherwise runs forever by design.

When a single job misbehaves, run exactly it in the foreground:

```bash
uv run vr-foraging-orchestrator work --config … --job-id <id>
uv run vr-foraging-orchestrator show --config … --job-id <id>   # row + event history
cat /tmp/vrf/logs/<id>.log                                      # the container's own output
```

## What the tests cover without Docker

| Suite | Covers |
|-------|--------|
| `orchestration/tests/test_process.py` | the container's command: sidecar on success, on a processor failure, and when the dataset never opens |
| `orchestration/tests/test_classify.py` | the truth table above, plus the argv fed to the **real** parser |
| `orchestration/tests/test_worker.py` | claim → run → publish with a fake runner; the session-name invariant |
| `orchestration/tests/test_ledger.py` | claim races, lease reaping, rerun semantics, migrations |
| `tests/test_package_boundary.py` | the one-way dependency |

The argv test is the one to keep. Every other assertion about a command line can
only prove a string is present in a list — never that the CLI *accepts* it. A
rejected flag exits 2 having processed nothing, with the reason visible only
inside the container log. It caught exactly that: `--exclude-processors a b` does
not parse, because pydantic-settings gives a list field an `append` action, so the
flag must be repeated per value.

# See also

- [session.md](session.md) — `process_session`, and the `on_output`/`on_error` hooks
- [batch.md](batch.md) — `aggregate`, and its `include` predicate
- [error-policy.md](../conventions/error-policy.md) — why any processor failure fails the whole session
- [`orchestration/README.md`](../../../orchestration/README.md) — module-by-module map
