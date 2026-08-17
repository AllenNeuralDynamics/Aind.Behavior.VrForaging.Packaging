#!/usr/bin/env bash
# Run one real session through a processor image and check what came out.
#
# The same script CI runs after building an image, kept standalone so you can
# point it at any image — a local `docker build`, a released digest, someone
# else's fork — without a GitHub Actions runner.
#
#   ./docker/smoke-test.sh <image-ref> <session-dir> [expected-digest]
#
#   image-ref        anything `docker run` accepts. Prefer a digest
#                    (repo@sha256:…) over a tag; a tag moves.
#   session-dir      one RAW session directory (the one containing `behavior/`).
#   expected-digest  optional `sha256:…`. When given, the sidecar's recorded
#                    digest must match it and provenance must be `pinned-digest`
#                    — i.e. the image told the truth about which image it is.
#                    Omit for a local unpinned build.
#
# Examples:
#
#   # A local build against a cached integration session
#   docker build -f docker/Dockerfile -t vrf:dev .
#   ./docker/smoke-test.sh vrf:dev "$(find tests/integration/.cache -mindepth 2 -maxdepth 2 -type d | head -1)"
#
#   # A released image, checking it reports its own digest correctly
#   ./docker/smoke-test.sh \
#     ghcr.io/allenneuraldynamics/aind-behavior-vr-foraging-packaging@sha256:abc… \
#     /data/raw/behavior_808728_2025-01-01_10-00-00 \
#     sha256:abc…
#
# Exits nonzero on the first problem. Leaves its output directory behind and
# prints the path, so a failure can be inspected.

set -euo pipefail

IMAGE="${1:?usage: smoke-test.sh <image-ref> <session-dir> [expected-digest]}"
SESSION_DIR="${2:?usage: smoke-test.sh <image-ref> <session-dir> [expected-digest]}"
EXPECTED_DIGEST="${3:-}"

if [ ! -d "$SESSION_DIR" ]; then
  echo "smoke-test: not a directory: $SESSION_DIR" >&2
  exit 2
fi
if [ ! -d "$SESSION_DIR/behavior" ]; then
  echo "smoke-test: $SESSION_DIR has no behavior/ — is it a raw session root?" >&2
  exit 2
fi

SESSION_NAME=$(basename "$SESSION_DIR")
OUT_DIR=$(mktemp -d)
echo "smoke-test: image   $IMAGE"
echo "smoke-test: session $SESSION_NAME"
echo "smoke-test: output  $OUT_DIR"

# Mounted at /in/$SESSION_NAME, not /in: the processor takes a session's identity
# from its input directory's own name, so a generic mount point would stamp every
# table with session_id="in" and raise nothing. Same invariant the worker upholds
# (Worker._resolve_mount).
docker run --rm \
  -v "$(realpath "$SESSION_DIR")":"/in/$SESSION_NAME":ro \
  -v "$OUT_DIR":/out \
  ${EXPECTED_DIGEST:+-e "PROCESSOR_IMAGE_URI=$IMAGE"} \
  "$IMAGE" \
  process --input-dir "/in/$SESSION_NAME" --output-dir /out

SIDECAR="$OUT_DIR/output.metadata.json"
if [ ! -f "$SIDECAR" ]; then
  echo "smoke-test: no output.metadata.json in $OUT_DIR — the process died before writing one" >&2
  exit 1
fi

python3 - "$SIDECAR" "$SESSION_NAME" "$EXPECTED_DIGEST" <<'PYEOF'
import json
import sys

sidecar_path, session_name, expected_digest = sys.argv[1], sys.argv[2], sys.argv[3]
data = json.loads(open(sidecar_path, encoding="utf-8").read())

failures = []


def check(ok: bool, message: str) -> None:
    if not ok:
        failures.append(message)


check(data["status"] == "ok", f"session status is {data['status']!r}, not 'ok'")
check(
    data["session_name"] == session_name,
    f"sidecar session_name {data['session_name']!r} != mounted {session_name!r} "
    "— every table is stamped with the wrong session",
)
check(bool(data["processors"]), "no processors ran at all")
for p in data["processors"]:
    check(p["status"] == "ok", f"processor {p['name']}: {p.get('error')}")

code = data["code"]
if expected_digest:
    container = code.get("container")
    check(container is not None, "PROCESSOR_IMAGE_URI was set but no container recorded")
    if container is not None:
        check(
            container["digest"] == expected_digest,
            f"sidecar digest {container['digest']!r} != launched {expected_digest!r}",
        )
    check(code["provenance"] == "pinned-digest", f"provenance is {code['provenance']!r}")
    check("dev" not in code["version"], f"non-release version string: {code['version']!r}")

if failures:
    print("smoke-test FAILED:", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)
    sys.exit(1)

rows = {p["name"]: p.get("rows") for p in data["processors"]}
# warn_count is a derived property on the model, not a serialized field — sum it.
warnings = sum(p.get("warn_count", 0) for p in data["processors"])
print(f"smoke-test OK: {data['session_name']} v{code['version']} ({code['provenance']})")
print(f"               {len(data['processors'])} processors, rows={rows}")
print(f"               {warnings} warning(s) — see the container log")
PYEOF
