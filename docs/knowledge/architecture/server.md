---
type: Component
title: Containerized pipeline — how the server layer runs the processor
description: How one session becomes one container, what the sidecar is for, how the daily aggregate is published, where the trust boundaries are, and how to run the whole thing on a laptop without S3, DocDB or a registry.
resource: server/src/processing_server/
tags: [architecture, server, docker, ledger, sidecar, aggregation, ingestion, testing, workspace]
timestamp: 2026-08-19T12:00:00Z
---

> **Editable diagram:** [`docs/diagrams/server.drawio`](../../diagrams/server.drawio)
> — three pages (runtime, packages, provenance). Opens in
> [app.diagrams.net](https://app.diagrams.net) or the VS Code *Draw.io Integration*
> extension. The ASCII sketch below is the same runtime picture, kept inline so
> this file is useful in a terminal.

Two distributions live in this repo, and the split is load-bearing:

| | `aind-behavior-vr-foraging-packaging` | `processing-server` |
|---|---|---|
| Where | `src/` | `server/src/` |
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
                       │            │  worker   │  vr-foraging-server work            │
                       │            └─────┬─────┘                                    │
                       │                  │                                          │
                       │   1. stage       │   mount (default) or download to         │
                       │      input ──────┤   /work/{job}/in/{session_name}          │
                       │                  │                                          │
                       │   2. docker run ─┼──────────────┐                            │
                       │                  │              │                            │
                       │   5. classify ◀──┤              │   ┌── CONTAINER ────────┐  │
                       │      exit code   │              └──▶│ no AWS credentials  │  │
                       │      × sidecar   │                  │                     │  │
                       │                  │                  │ vr-foraging-server  │  │
                       │   6. publish ────┼──▶ S3 or local    │                     │  │
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

**The container has a network, but no credentials.** It reads its session from a
mount or the work volume and writes to `/work/{job}/out` for the *worker* to
publish, and it cannot upload anywhere: nothing passes credentials into it,
`docker run` does not inherit the worker's environment, and no `~/.aws` is
mounted. S3 stays a worker-only concern.

It is not `--network=none`, and that must not come back. `contraqctor` resolves
Harp device registers at load time by fetching `harp-tech/whoami` and a
per-device `device.yml` over HTTPS, and no session carries a local copy. Offline,
every Harp device group resolves to zero streams and raises `Data must be a list
of DataStreams` — which `classify` then charges to the *data*, because that is
exactly what a genuine parse failure looks like. Measured: same session, same
image, exit 0 with a network and exit 1 without. Vendoring those schemas into the
image and feeding them to `contraqctor` via `DeviceYmlByFile` would make offline
operation possible again.

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

# What lives on the work volume, and for how long

Nothing, in the steady state. The volume's high-water mark is *concurrent jobs ×
one session*, not the campaign: `Worker.process_job` owns `/work/{job_id}` for
exactly one attempt and removes it in a `finally`, and staged input goes even
earlier — `input_store.release` runs as soon as the container exits, before
classify and publish. Under `copy_files: false` (a pass-through mount) the input
side is zero and only `out/` is ever written there.

The load-bearing half is the cleanup on **entry**, not the one on exit:

```python
job_dir = self.work_dir / job.job_id
shutil.rmtree(job_dir, ignore_errors=True)   # a previous attempt may have died mid-write
job_dir.mkdir(parents=True, exist_ok=True)
try:
    self._run_job(job, job_dir)
finally:
    shutil.rmtree(job_dir, ignore_errors=True)
```

`job_id` is stable across attempts — reaping an expired lease returns the *same*
row to `pending` — so an attempt killed mid-write left its wreckage at exactly
the path the retry is about to use, and `mkdir(exist_ok=True)` adopts it. That is
not untidiness: `publish` ships `out/` wholesale and the sidecar is rewritten
every time, so a stale parquet from attempt N reaches the output store inside a
session the ledger records as a clean success. Cleaning on entry is also the only
ordering that survives a kill at all — nothing can `finally` its way out of
SIGKILL, whereas whatever killed attempt N cannot stop attempt N+1 from starting
clean. `tests/test_worker.py::TestWorkDirLifecycle` is where that is pinned down.

**Three owners, one per job state.** A directory left behind by a crash is
reclaimed by whichever of these owns the state its job is in:

| state | reclaimed by | why not the others |
|---|---|---|
| `running` | nobody | another worker's live job — `claim` sets `running` before any `mkdir`, which is what makes the check race-free without mtime guesswork |
| `pending`, `retrying` | the next attempt's entry-side cleanup | it must clear the directory anyway; leaving it to the retry also closes the read-then-delete window a sweeper would open against a job being claimed right now |
| `completed`, `failed`, `dead`, `skipped` | `Worker.sweep_work_dir` | terminal — nothing is ever coming back for it |

`sweep_work_dir` runs each tick of the claim loop, beside
`reap_expired_leases`, and is the filesystem half of the same phase: reaping is
what moves a crashed job into a state the sweep may reclaim. The ledger decides,
never the filesystem — several workers share the volume, and deleting a live
job's directory is worse than the leak it fixes. Anything whose name is not a job
id the ledger knows is **reported and left alone**.

One residual, accepted rather than papered over: a `pending` job whose attempt
crashed and which is never retried (the campaign ends, or it sits behind higher
priority forever) keeps its directory. Bounded by the crash count, and reclaimed
if it is ever attempted again.

**`min_free_disk_bytes` is checked before claiming, not during.** A job claimed
onto a full volume dies on ENOSPC and burns one of `max_attempts`, so an
unguarded worker chews through real sessions three attempts at a time instead of
waiting for space. Refusing to claim leaves the queue as it was. `free_disk_bytes`
walks up to the nearest existing ancestor: measuring a `work_dir` that does not
exist yet returns "unknown", and unknown does not block — which made the guard a
silent no-op on exactly the first tick.

`worker.keep_work_dir` (or `work --keep-work`) suppresses both the exit-side
cleanup and the sweep, for reading a `code`-error job's directory before it
disappears. It does *not* suppress the entry-side cleanup — preserving a
directory must never leak it into the next attempt's output. Never leave it on
for a campaign.

**Logs are not on the work volume.** One `{job_id}.log` per attempt in
`logging.dir`, published to `{output.uri}/{release}/logs/{job_id}/_log.txt` and
then deleted locally. Uniformity is the point rather than the ~78 MB a
4700-session campaign saves: `log_uri` used to hold a worker-local path on
success and an output-store URI on failure, so nothing could follow the column
without first guessing which it had. The local copy survives a failed publish,
and `log_uri` then records the local path — a reachable log in the wrong place
beats a URI pointing at nothing.

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

# Two shapes: a server, and a campaign

The same worker runs both, and one config field decides which.

**A server** (`ingestion.type: docdb`) polls for new sessions every
`ingestion.interval_s`, processes them as they appear, re-aggregates daily, and runs
until it is stopped. There is no end state.

**A campaign** (`ingestion.type: manifest` + `worker.exit_when_drained: true`) processes
exactly the sessions named in a file, aggregates once, and exits with a status. The
manifest is a JSON file of `{session_name, location}` entries:

```json
{"sessions": [{"session_name": "707349_2024-04-17_10-34-09",
               "location": "s3://aind-open-data/707349_2024-04-17_10-34-09"}]}
```

A bare top-level list works too. Sibling keys some dedup step may leave beside
`sessions` — `ambiguous`, `unmatched` — are **reported and not processed**, since an
entry that landed in one of those has no location to read.

Why a manifest rather than a narrower DocDB query: a manuscript's session list is
decided, reviewed and then has to *stay* decided. A query is evaluated at run time and
may answer differently next month; a file in version control cannot.

## No source filters on the shape of a name

There is no `name_pattern` anywhere. Every source ingests everything it finds: every
subdirectory under `ingestion.root`, every entry in a manifest that has a location, and
every DocDB record matching the *semantic* filters (`data_level: raw`, acquisition type,
project name) with no `{"name": {"$regex": …}}` clause.

A name-shaped filter cannot distinguish "not a session" from "a session named
unexpectedly", and it discards both without a word. What replaces it is failing loudly:
a directory that is not a session is rejected in staging by `staging.verify_present`,
as a `data` failure naming the file that was missing. That is a row in the ledger someone
can act on, rather than an absence nobody can see.

The cost is real and worth stating: the local integration cache contains two empty
`…;C` download artifacts beside its seven sessions, so a local ingest now queues nine
jobs and two of them fail in staging. Previously they were skipped in silence. Under
`worker.fail_fast` those two would end the run — which is the correct signal that the
ingest root has something in it that should not be there.

## What the manifest source does not do

- **No watermark.** `discover` ignores `since` and yields the whole list every sweep.
  A finite static set has nothing to be incremental about, and a cursor would only
  create a way for a run to skip part of the list it was handed. `job_key` uniqueness
  absorbs the re-delivery, so adding lines to the file works without resetting anything.
- **Nothing is derived from a session's name.** Not its shape, not the subject, not the
  date. `subject_id` and `session_start` both live in the session's own metadata, which
  only the processor opens, and both are filled in from its sidecar on completion. The
  ledger backfills with `COALESCE`, so a value present at discovery is never corrected
  later — which is exactly why a name-derived guess must not be written there: it would
  *win* over the record. A manifest carries identity, and nothing else. The folder name carries a
  timestamp and treating it as acquisition metadata is a guess. The real value is inside
  the session, in the Session stream the processor parses, and the sidecar already
  computes it — so `_finish_success` writes it back on completion. The ledger uses
  `COALESCE(session_start, ?)`, so this is a *backfill*: a source that knew the value at
  discovery (DocDB reads a metadata index) keeps its own, and `local` gets the same
  correction a manifest does.
- **No per-bucket configuration.** Locations come from the file and may span buckets;
  `S3InputStore` takes the bucket from each `input_uri`.

Everything is validated **when the source is constructed**, which `doctor` does as a
pre-flight: a missing mount, unparsable JSON, a missing `sessions` key, an entry with no
location, a misnamed session, a duplicate. A 1700-session campaign must not discover on
session 1699 that the file had a typo.

## How a drained run decides it is finished

`worker.exit_when_drained` is read from the config rather than inferred from the source
type. Whether a container exits is the most consequential thing about how it behaves, so
it should be legible in one field instead of derived from another.

The exit condition is deliberately *not* "the claim loop found nothing to do":

- **Every session job for the release must be terminal**, read via
  `ledger.count_active`. A job in `retrying` with a future `next_eligible_at` is
  unclaimable right now and still outstanding — those two states look identical from the
  claim loop and are opposite in meaning.
- **At least one ingest sweep must have succeeded.** A source that raises every time
  otherwise looks exactly like a source with nothing left to give.
- **Zero jobs is a failure, not a clean drain.** It is the shape of every
  misconfiguration that matters — an unmounted manifest, a mistyped release — and all of
  them would otherwise exit 0 having processed nothing.

Then it aggregates, **bypassing the `aggregation.at` wall clock**: a batch run that
finishes at 14:00 must not exit without the table it was run to produce. The verdict comes
from the *published* manifest's watermark, not from the aggregate job this step queued —
an unchanged watermark makes `enqueue_aggregate` decline identically whether an earlier
scheduled aggregation succeeded or failed, so trusting our own job row would let a failed
aggregation exit 0.

Exit code: **0 only if every session completed (or was skipped) and the published
aggregate covers this run's watermark**; 1 if any session is `failed`/`dead`. `skipped` is
not a failure — it means the output was already there and `output.overwrite` is false.
One bad session therefore fails a 1700-session run; which ones failed is in the ledger
(`status`, `show`), and `rerun` retries them.

`worker.fail_fast` stops at the first session that fails *for good*, instead of working
through the rest — the useful mode when a run is a canary for a code change rather than a
campaign, since the answer arrives after one session instead of 1700. It is independent of
`exit_when_drained`, because it makes even a long-running worker exit. Two deliberate
details: `retrying` does not trigger it, so a blip in S3 does not end the run; and nothing
is aggregated on this path, because an aggregate over a partial set would be published as
though the campaign had finished.

# The aggregate output

A worker keeps an aggregate current for as long as it is alive: once a day at
`aggregation.at` in `aggregation.timezone`, and on demand via `vrf-server
aggregate`. The layout under the release prefix:

```
{release}/sessions/{session}/session.parquet      per-session, written by the container
{release}/sessions/{session}/output.metadata.json
{release}/aggregate/2026-08-18/session.parquet    one prefix per day, immutable once written
{release}/aggregate/2026-08-18/sites.parquet
{release}/aggregate/2026-08-18/output.metadata.json
{release}/aggregate/latest/                       a full copy of the newest dated prefix
```

**Dated prefixes are written first and then never touched.** A run cannot damage
an earlier aggregate, because the only prefix it writes into is its own day's —
and a same-day rerun replaces that day rather than accumulating copies of it.
Storage is the cheap side of the trade: one small aggregate a day is not a growth
problem worth managing, and "the aggregate as of some date" is the question people
actually ask.

**`latest` is a full copy, not a pointer.** A reader wanting current data should
not have to fetch a pointer, parse it and follow it; on `s3` the copy is
server-side, so promoting costs no upload. It also means `latest` is readable by
anything that can read a parquet path, with no convention to learn.

**The marker goes last, in both prefixes.** These are individual object writes
rather than a `publish()`, so nothing else imposes an order — and without the
marker last there is no way to tell a finished aggregate from one caught with a
single table uploaded. `latest` is only replaced after its source day is complete,
so a run that dies partway leaves every past day intact and, at worst, `latest`
briefly absent — which reads as "rebuilding", not as a torn aggregate.

`latest` sorts **above** every date, because digits precede letters: `max()` over
the children of `aggregate/` returns the mirror, not the newest real aggregate.
Anything scanning that prefix filters to date-shaped names first (`Worker.aggregate_days`).

## What decides whether a run happens

The **watermark**: a digest over the sorted `(session_name, job_id)` pairs of every
completed session. `job_id` rather than a count, because a recompute inserts a new
job row for the same session — a count would miss it and leave the aggregate
permanently stale for exactly the case that motivated re-aggregating.

The dedupe is then the ledger's own `job_key` uniqueness, with the watermark
standing in for the processor fingerprint: an unchanged set produces the same key
and the insert is a no-op. No separate watermark storage, and the same mechanism
that makes routine ingestion idempotent.

"Already ran today" is read from the ledger, not from process state, so a restart
cannot re-trigger a run. The schedule catches up rather than skipping: a worker
that was down at 03:00 aggregates at its next tick.

## Why there is no local staging

Aggregation reads published parquet straight out of the output store —
`read_object` in, `write_object` out — and never touches the work volume. Object
storage has no batch read, so a session's tables are two ordinary `GetObject`
calls either way; landing them on disk first would add a copy and buy nothing.
Parquet has to seek to its footer, so it cannot be consumed as a forward-only
stream regardless, and range requests only pay off for reading *part* of a file,
whereas aggregation reads every row of both tables.

The reads are latency-bound rather than bandwidth-bound, so they run on a thread
pool — and results are reassembled in **sorted session order, never completion
order**, or an unchanged input set would produce different bytes and no digest
over the output would mean anything.

This is also why there is no derived cache to invalidate on the worker side: the
concatenation is rebuilt per run from the store and thrown away, so a recomputed
session cannot leave stale rows behind.

One corrupt per-session parquet is logged and left out rather than raising.
Aggregation runs on a schedule over a growing set, so failing the whole run would
mean no aggregate at all for anyone until someone deletes the bad file. The
shortfall is visible in the manifest's row counts.

# Running it locally

No S3, no DocDB, no registry, no database service. Everything below works on a
laptop against the integration cache.

## 1. One session, no container at all

The fastest loop. `process` is just a Python entry point:

```bash
uv sync                            # both workspace members, editable
SESSION=$(ls -d packaging/tests/integration/.cache/*/*/ | head -1)

uv run vr-foraging-server process \
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
  root: /abs/path/to/packaging/tests/integration/.cache
input:
  store: local
  copy_files: false                # pass through; no staging copy
output:
  store: local
  uri: /tmp/vrf/out
worker:
  ledger: /tmp/vrf/jobs.sqlite
  min_free_disk_bytes: 1000000000  # 1 GB; the 20 GB default is a campaign figure
processor:
  image: vrf                       # your local build
  allow_unpinned: true             # local images have no digest
logging:
  dir: /tmp/vrf/logs
```

`allow_unpinned: true` relaxes *both* halves of the provenance chain — the
processor's digest and the worker's own `VRF_WORKER_IMAGE_URI` — because a run
allowed to be unreproducible is allowed to be unreproducible on both sides. A
laptop could not satisfy either.

```bash
uv run vr-foraging-server doctor   --config /tmp/vrf/config.yaml
uv run vr-foraging-server ingest   --config /tmp/vrf/config.yaml --dry-run
uv run vr-foraging-server ingest   --config /tmp/vrf/config.yaml
uv run vr-foraging-server work     --config /tmp/vrf/config.yaml --once
uv run vr-foraging-server status   --config /tmp/vrf/config.yaml
uv run vr-foraging-server serve    --config /tmp/vrf/config.yaml   # :8080
```

Run `doctor` first, always. It checks the work volume is writable, the Docker
daemon is reachable, there is room to claim, and that both the processor *and the
worker itself* are pinned or explicitly unpinned — the things that are cheap to
verify and expensive to discover mid-campaign. An unpinned worker is otherwise
only visible in hindsight, once the ledger cannot say what published 4700
sessions.

It also *reports* stranded work directories without touching them: reclaiming
them belongs to the claim loop, and `doctor` stays read-only.

`--dry-run` on `ingest` before the real thing, and `work --once` before
`work` — the loop otherwise runs forever by design.

When a single job misbehaves, run exactly it in the foreground and keep the
evidence:

```bash
uv run vr-foraging-server work --config … --job-id <id> --keep-work
uv run vr-foraging-server show --config … --job-id <id>   # row + event history
ls /work/<id>/out                                               # only with --keep-work
```

`--keep-work` exists because the work directory is normally gone before you can
read it. The container's own log is published rather than left local — `show`'s
`log_uri` is where it went, which for a local output store is a path you can
`cat` directly.

## What the tests cover without Docker

| Suite | Covers |
|-------|--------|
| `server/tests/test_process.py` | the container's command: sidecar on success, on a processor failure, and when the dataset never opens |
| `server/tests/test_classify.py` | the truth table above, plus the argv fed to the **real** parser |
| `server/tests/test_worker.py` | claim → run → publish with a fake runner; the session-name invariant; work-directory lifetime, the sweeper's state ownership, the disk guard, log publishing, worker provenance |
| `server/tests/test_ledger.py` | claim races, lease reaping, rerun semantics, the additive column migration |
| `tests/test_package_boundary.py` | the one-way dependency |

Only `runner.run`/`runner.classify` are faked — the ledger, both local stores,
staging and publishing all run for real, so a test that publishes is really
writing files. What that cannot reach is the containerized worker itself: a
host-run worker sees `work_dir` on the host filesystem while the container it
launches sees the named volume, and those are different places. That mismatch is
the §4(a) hazard, so the volume-visibility assertion belongs to `doctor` rather
than to the test suite.

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
- [`server/README.md`](../../../server/README.md) — module-by-module map
