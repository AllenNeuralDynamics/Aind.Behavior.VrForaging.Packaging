# Run a batch campaign

Process a fixed list of sessions end-to-end: stage from S3, run the processor in a container per session, write parquet and NWB output locally, and aggregate. The run exits when every session is terminal.

This guide uses five public-bucket sessions as a smoke test — no AWS credentials required. The [scaling up](#scaling-up-to-the-full-run) section at the end covers what changes for a production run over private data.

---

## Prerequisites

| Requirement | Check | Notes |
|-------------|-------|-------|
| Docker Desktop | `docker version` | Must be running |
| Repo cloned | — | All paths are repo-relative |

---

## 1. Build the image

One image serves all three roles: worker, processor container, and dashboard.

```bash
docker build -f docker/Dockerfile -t vrf:latest .
```

Rebuild whenever the working tree changes — a stale image predates your edits.

---

## 2. Create the campaign files

Create `scratch/campaign/` and add three files.

### `manifest.json`

A fixed list of sessions to process. These five are from the public `aind-open-data` bucket — no credentials needed.

```json title="scratch/campaign/manifest.json"
{"sessions": [
  {"session_name": "707349_2024-04-17_10-34-09", "location": "s3://aind-open-data/707349_2024-04-17_10-34-09"},
  {"session_name": "707349_2024-04-18_10-35-08", "location": "s3://aind-open-data/707349_2024-04-18_10-35-08"},
  {"session_name": "707349_2024-04-19_10-43-00", "location": "s3://aind-open-data/707349_2024-04-19_10-43-00"},
  {"session_name": "707349_2024-04-22_10-58-20", "location": "s3://aind-open-data/707349_2024-04-22_10-58-20"},
  {"session_name": "707349_2024-04-23_10-46-13", "location": "s3://aind-open-data/707349_2024-04-23_10-46-13"}
]}
```

### `config.yaml`

All paths are container-side paths (mounted in the compose file below).

```yaml title="scratch/campaign/config.yaml"
release: manuscript-smoke          # becomes the output prefix

ingestion:
  type: manifest
  manifest_file: /etc/vrf/manifest.json

input:
  store: s3
  anonymous: true                  # unsigned requests — works with aind-open-data

output:
  store: local
  uri: /out                        # mounted to scratch/campaign-out/ below

worker:
  exit_when_drained: true          # process the list and exit

processor:
  image: vrf                       # local build tag; default is the ghcr.io production image
  allow_unpinned: true             # local build — no registry digest
  write_nwb: true
```

!!! note "Key settings"
    `exit_when_drained: true` is what makes this a batch job — without it the worker polls indefinitely. `aggregation.enabled: true` builds the cross-session tables on the way out, bypassing the scheduled `at` time.

### `compose.yaml`

```yaml title="scratch/campaign/compose.yaml"
services:
  worker:
    image: vrf:latest
    restart: "no"                  # must be "no" — not unless-stopped
    group_add: ["0"]
    environment:
      - VRF_WORKER_IMAGE_URI=vrf:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - vrf_work:/work
      - vrf_ledger:/var/lib/vrf
      - ./config.yaml:/etc/vrf/config.yaml:ro
      - ./manifest.json:/etc/vrf/manifest.json:ro
      - ../campaign-out:/out

  dashboard:                       # optional — see Dashboard section below
    image: vrf:latest
    command: ["serve", "--config", "/etc/vrf/config.yaml"]
    volumes:
      - vrf_ledger:/var/lib/vrf
      - ./config.yaml:/etc/vrf/config.yaml:ro
      - ../campaign-out:/out:ro
    ports: ["127.0.0.1:8080:8080"]
    restart: "no"

volumes:
  vrf_work:
    name: vrf_work
  vrf_ledger:
    name: vrf_ledger
```

!!! warning "`restart: \"no\"` is required"
    `unless-stopped` (Docker's server default) would restart the worker every time it drains and exits, looping forever. Use `"no"` so the exit code reaches you.

Create the output directory, then move into the campaign folder for all subsequent commands:

```bash
mkdir -p scratch/campaign-out
cd scratch/campaign
```

---

## 3. Pre-flight

```bash
docker compose run --rm worker doctor --config /etc/vrf/config.yaml
```

Expected output:

```
Manifest manifest.json: 5 session(s) to process
Manifest manifest.json holds 5 session(s)
OK — no problems found.
```

`doctor` checks: Docker socket reachable, work volume writable, image pin consistent, disk above the floor, and every manifest entry usable. It does **not** open a bucket — that is exercised by the run itself.

---

## 4. Run

```bash
docker compose run --rm worker work --config /etc/vrf/config.yaml
echo "exit=$?"
```

Watch for, in order:

```
Manifest manifest.json: 5 session(s) to process
Ingest sweep (manifest): 5 new job(s)
[707349_2024-04-17_10-34-09] container exited 0
…
Aggregated 5 session(s) into /out/manuscript-smoke/aggregate/2026-08-20/ …: session=5, sites=5982
Drained release 'manuscript-smoke': completed=5
exit=0
```

`"Running vrf unpinned"` and `"Connection pool is full"` warnings are expected and harmless for a local build with concurrent S3 list calls.

Exit codes: `0` if every session completed or was deliberately skipped. `1` if any session failed, if the aggregate failed, or if no jobs existed (e.g. the manifest is not mounted).

---

## 5. Verify output

```
scratch/campaign-out/manuscript-smoke/
  sessions/
    707349_2024-04-17_10-34-09/
      events.parquet
      licks.parquet
      position_velocity.parquet
      session.parquet
      sites.parquet
      sniffing.parquet
      software_events.parquet
      707349_2024-04-17_10-34-09.nwb.zarr/   ← write_nwb: true
      output.metadata.json
    … (4 more sessions, same shape)
  aggregate/
    2026-08-20/
      session.parquet              ← 5 rows
      sites.parquet                ← 5 982 rows
      output.metadata.json
    latest/                        ← copy of the most recent day
  logs/
    {job_id}/_log.txt              ← one per session attempt
```

Quick sanity check from the repo root:

```bash
python -c "
import pandas as pd
df = pd.read_parquet('scratch/campaign-out/manuscript-smoke/aggregate/latest/session.parquet')
print(df[['session_id', 'subject_id', 'date']].to_string(index=False))
"
```

---

## Dashboard (optional)

Start the dashboard alongside or after the worker to watch progress and trigger reruns:

```bash
docker compose up dashboard
```

Open [http://localhost:8080](http://localhost:8080) in a browser. Log links work because `../campaign-out:/out:ro` is mounted into the service. Actions (requeue, skip, tag) work because the ledger is mounted read-write.

---

## Inspecting and re-running

```bash
# Job counts by status
docker compose run --rm worker status --config /etc/vrf/config.yaml

# Which sessions failed and why
docker compose run --rm -T worker export --config /etc/vrf/config.yaml | python -c "
import csv, sys
for r in csv.DictReader(sys.stdin):
    if r['status'] in ('failed', 'dead'):
        print(r['session_name'], r['error_kind'], (r['error'] or '')[:90])
"

# Requeue failed sessions
docker compose run --rm worker rerun --config /etc/vrf/config.yaml --failed --dry-run
docker compose run --rm worker rerun --config /etc/vrf/config.yaml --failed --confirm --reason "reason here"
```

Use `--dead` instead of `--failed` to catch sessions that exhausted all retries on a transient error (e.g. expired credentials).

---

## Re-aggregate

Rebuild the flat tables against already-published output — without re-running the processor. Use this after a partial run, after requeuing failed sessions, or whenever you want the aggregate to reflect the current state of `sessions/`.

```bash
# Preview: show session count, watermark, and whether a rebuild is needed
docker compose run --rm worker aggregate --config /etc/vrf/config.yaml --dry-run

# Rebuild (skips automatically if the watermark is unchanged)
docker compose run --rm worker aggregate --config /etc/vrf/config.yaml

# Force a rebuild even when the watermark matches
docker compose run --rm worker aggregate --config /etc/vrf/config.yaml --force
```

The command is watermark-gated: it hashes the set of completed sessions and skips if nothing has changed since the last aggregate. Exit code `0` if the aggregate completed or was already current; `1` if it failed.

Works against any output store — point `config.yaml` at `output.uri: s3://…` and add credentials to the worker service, same as the [full run](#scaling-up-to-the-full-run).

---

## Scaling up to the full run

Three things change for a production run over private data:

**1. Use the full manifest.**

```bash
cp /path/to/session_manifest.json scratch/campaign/manifest.json
```

**2. Turn off anonymous access and add credentials.**

In `config.yaml`, remove (or set `false`) `input.anonymous`. Then add the AWS credential mount to the worker in `compose.yaml` — see the [AWS S3 guide](aws-s3.md) for the SSO setup:

```yaml
environment:
  - VRF_WORKER_IMAGE_URI=vrf:latest
  - AWS_PROFILE=aind-scientist
volumes:
  # … existing volumes, plus:
  - C:/Users/<you>/.aws:/home/runner/.aws   # rw — botocore refreshes the token
```

**3. Rename the release** so the output prefix is distinct from the smoke test run:

```yaml
release: manuscript-2026-08-20
```

Re-running after an interruption is safe — `job_key` uniqueness makes an unchanged manifest a no-op, so the worker picks up where it stopped.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Every session retries then ends `dead` | Expired credentials — `StoreTransientError` is retryable | Refresh SSO on the host; `rerun --dead` |
| `ProfileNotFound` or empty `.aws` | `${HOME}` unset in this shell, or wrong profile name | Use an absolute path for the `.aws` mount |
| `Missing required files … data_description.json` | Wrong S3 prefix in the manifest | Terminal failure — correct the location |
| exits 1, "no session jobs exist" | Manifest not mounted, or release name mismatch | Check the volume mount and `release:` field |
| Worker restarts after draining | `restart: unless-stopped` | Change to `restart: "no"` |
| `staged size exceeds max_session_bytes` | Session larger than the 2 GB default | Raise `staging.max_session_bytes` |
| Container exits 137 | OOM | Raise `processor.memory` |
| Output directory empty | `/out` mounted `:ro`, or wrong `output.uri` | Remove `:ro`; verify the container-side path |
