"""Output parquet files for all configured legacy sessions (v0.3 – v0.5).

Usage:
    uv run scripts/test_legacy_session.py
    uv run scripts/test_legacy_session.py --id 716458_2024-05-13_09-03-55
    uv run scripts/test_legacy_session.py --local-path /path/to/session
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

CACHE_ROOT = Path(__file__).parent.parent / "tests" / "integration" / ".cache"
OUTPUT_DIR = Path(__file__).parent.parent / "results"
DEFAULT_EXCLUDES = ("**/*.mp4", "**/*.avi", "**/*.mkv")


@dataclass
class SessionConfig:
    session_id: str
    bucket: str


SESSIONS: list[SessionConfig] = [
    SessionConfig(session_id="716458_2024-05-13_09-03-55", bucket="aind-open-data"),
    SessionConfig(session_id="behavior_754580_2024-12-18_09-28-16", bucket="aind-private-data-prod-o5171v"),
    SessionConfig(session_id="behavior_789919_2025-05-06_20-12-53", bucket="aind-private-data-prod-o5171v"),
]


# ---------------------------------------------------------------------------
# S3 download (public buckets only)
# ---------------------------------------------------------------------------


def _is_excluded(rel_key: str, patterns: tuple[str, ...]) -> bool:
    return any(PurePosixPath(rel_key.lower()).match(p.lower()) for p in patterns)


def download_session(bucket: str, session_id: str, cache_root: Path) -> Path:
    """Download all non-excluded objects for *session_id* from a public S3 bucket."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    prefix = f"{session_id}/"
    local_root = cache_root / bucket / session_id

    if local_root.exists():
        log.info("Using cached session at %s", local_root)
        return local_root

    log.info("Downloading s3://%s/%s ...", bucket, prefix)
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED, connect_timeout=10, read_timeout=60))

    paginator = s3.get_paginator("list_objects_v2")
    objects: list[tuple[str, int]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            rel_key = key[len(prefix) :]
            if rel_key and not _is_excluded(rel_key, DEFAULT_EXCLUDES):
                objects.append((key, obj.get("Size", 0)))

    log.info("  %d objects (%.1f MB)", len(objects), sum(s for _, s in objects) / 1024 / 1024)
    for i, (key, _) in enumerate(objects, 1):
        dest = cache_root / bucket / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(dest))
        if i % 20 == 0 or i == len(objects):
            print(f"  {i}/{len(objects)} files downloaded", end="\r", flush=True)
    print(flush=True)
    return local_root


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------


def _read_contract_version(session_path: Path) -> str:
    tasklogic_path = session_path / "behavior" / "Logs" / "tasklogic_input.json"
    if not tasklogic_path.exists():
        raise FileNotFoundError(f"tasklogic_input.json not found at {tasklogic_path}")
    data = json.loads(tasklogic_path.read_text())
    return data.get("schema_version") or data["version"]


def _resolve_loader_version(version: str) -> str:
    import semver as _semver

    v = _semver.Version.parse(version)
    if v < _semver.Version.parse("0.4.0"):
        return "0.4.0"
    return version


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def process(session_path: Path) -> None:
    from aind_behavior_vr_foraging.data_contract import dataset

    from aind_behavior_vr_foraging_packaging.pipeline import run_session

    version = _read_contract_version(session_path)
    loader_version = _resolve_loader_version(version)
    log.info("Contract version: %s (loader: %s)", version, loader_version)

    ds = dataset(session_path, version=loader_version)
    run_session(ds, OUTPUT_DIR / session_path.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", default=None, help="Run only the session with this ID.")
    parser.add_argument("--local-path", type=Path, default=None, help="Run a single local path.")
    args = parser.parse_args()

    if args.local_path:
        process(args.local_path)
        return

    targets = [s for s in SESSIONS if args.id is None or s.session_id == args.id]
    if not targets:
        log.error("No session with id=%s found in SESSIONS list.", args.id)
        sys.exit(1)

    failed: list[str] = []
    for cfg in targets:
        local_root = CACHE_ROOT / cfg.bucket / cfg.session_id
        if not local_root.exists():
            if cfg.bucket == "aind-open-data":
                local_root = download_session(cfg.bucket, cfg.session_id, CACHE_ROOT)
            else:
                log.error("Session %s not in cache. Private bucket — download manually.", cfg.session_id)
                failed.append(cfg.session_id)
                continue
        try:
            process(local_root)
            log.info("✓ %s", cfg.session_id)
        except Exception as exc:
            log.error("✗ %s FAILED: %s", cfg.session_id, exc)
            failed.append(cfg.session_id)

    if failed:
        log.error("FAILED: %s", ", ".join(failed))
        sys.exit(1)
    log.info("Done. %d session(s) processed.", len(targets))


if __name__ == "__main__":
    main()
