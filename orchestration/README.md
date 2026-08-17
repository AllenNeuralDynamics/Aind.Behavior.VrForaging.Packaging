# aind-behavior-vr-foraging-orchestration

Runs [`aind-behavior-vr-foraging-packaging`](../README.md) at scale: discover
sessions, queue them in an inspectable ledger, process each one in an ephemeral
container, publish the outputs, and answer *what is pending / running / failed /
done, and which code produced a given output*.

**Never published to PyPI.** It is deployment machinery for our own hosts — it
pins a container digest, and its config models describe our S3 layout and our
DocDB instance. The `Private :: Do Not Upload` classifier makes an accidental
publish fail rather than succeed.

Install it from the repo root, which is a `uv` workspace:

```bash
uv sync                 # both packages, editable
```

## Layout

| Module | Responsibility |
|--------|----------------|
| `sidecar.py` | `output.metadata.json` — the per-session reproducibility record, and the `SidecarRecorder` that fills one in |
| `ledger.py` | SQLite: schema, migrations, atomic claim/lease, transitions, rerun, tags |
| `models.py` | `Job`, `JobStatus`, `ErrorKind` |
| `sources/` | Discovery — *which* sessions exist (DocDB, or a local directory scan) |
| `stores/` | Transfer — *how* bytes arrive and leave (mount / S3 / local) |
| `staging.py` | Include-exclude rules, manifests, disk budget |
| `runner.py` | `docker run` argv, log capture, and the exit-code × sidecar verdict |
| `worker.py` | Claim → stage → run → classify → publish → record |
| `dashboard.py` | One sortable table of sessions, with each run's log linked from its row |
| `cli.py` | `vr-foraging-orchestrator` |

The dependency runs one way — orchestration → packaging — which is why the
sidecar lives here. `pipeline/session.py` reports per-processor outcomes through
a generic `on_output`/`on_error` callback pair and knows nothing about this
package or its file format.

## See also

- [`docker/`](../docker/) — the image, the compose sketch, and `smoke-test.sh`
- [`docs/knowledge/architecture/orchestration.md`](../docs/knowledge/architecture/orchestration.md) —
  how the containerized pipeline fits together, and how to run it locally
