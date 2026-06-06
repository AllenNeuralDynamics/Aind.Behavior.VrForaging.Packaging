"""Integration test fixtures and helpers.

pytest_configure patches NwbSession to use local JSON files instead of the
AIND metadata API, so integration tests work without network access to DocDB.
"""

import json
import logging
import os
import time
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

import boto3
import pytest
from botocore import UNSIGNED
from botocore.client import BaseClient
from botocore.config import Config

from .model import DatasetEntry, DatasetManifest, load_manifest

_log = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "datasets.yml"
CACHE_ROOT = Path(__file__).parent / ".cache"

# Flat JSON dict keyed by "<bucket>/<key>" → ETag, plus per-dataset sentinels.
# Lives at cache root so it cannot be mistaken for dataset data.
ETAG_INDEX_PATH = CACHE_ROOT / "_etags.json"
_DATASET_COMPLETE_KEY_PREFIX = "_dataset_complete:"

# Used for warm-cache validation: 1 HEAD call instead of a full prefix listing.
# Value stored in the sentinel: {"etag": "<etag>", "total_bytes": <int>}
_SENTINEL_FILE = "data_description.json"

DEFAULT_EXCLUDES: tuple[str, ...] = ("**/*.mp4", "**/*.avi", "**/*.mkv")

_manifest: DatasetManifest = load_manifest(MANIFEST_PATH)


def pytest_configure(config: pytest.Config) -> None:
    """Swap NwbSession._get_aind_data_schema_json to read local JSON files."""
    from aind_behavior_vr_foraging_packaging.nwb_file import NwbSession, _AindDataSchemaJson

    def _from_root(self: NwbSession) -> _AindDataSchemaJson:
        return _AindDataSchemaJson.from_root_path(self.root_path)

    setattr(NwbSession, "_get_aind_data_schema_json", _from_root)


@pytest.fixture(scope="session")
def s3_client() -> BaseClient:
    """Anonymous S3 client for accessing public buckets."""
    return boto3.client(
        "s3",
        config=Config(
            signature_version=UNSIGNED,
            connect_timeout=10,
            read_timeout=60,
            retries={"max_attempts": 2},
        ),
    )


@pytest.fixture(scope="session", autouse=True)
def ensure_datasets_cached(s3_client: BaseClient) -> None:
    """Download all manifest datasets to the local cache before any test runs."""
    entries = _manifest.datasets
    total = len(entries)
    for i, entry in enumerate(entries, start=1):
        print(f"\n[{i}/{total}] Preparing dataset: {entry.id} ...", flush=True)
        try:
            download_dataset(s3_client, entry, CACHE_ROOT)
            print(f"[{i}/{total}] {entry.id}: ready.", flush=True)
        except Exception as exc:
            print(f"[{i}/{total}] {entry.id}: SKIPPED ({exc})", flush=True)
            _log.warning("[%d/%d] Could not prepare dataset %s: %s", i, total, entry.id, exc)


def _is_excluded(rel_key: str, patterns: list[str]) -> bool:
    lowered = rel_key.lower()
    return any(PurePosixPath(lowered).match(p.lower()) for p in patterns)


def _format_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _load_etag_index(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _save_etag_index(path: Path, index: dict[str, str]) -> None:
    """Atomically write the ETag index."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(index, indent=2, sort_keys=True))
    os.replace(tmp, path)


def _validate_sentinel(s3: BaseClient, bucket: str, prefix: str, local_root: Path, stored: str) -> bool:
    """Return True if the local cache is still valid.

    Checks local directory size (no S3) and the ETag of ``_SENTINEL_FILE`` (1 HEAD call).
    Returns False on any mismatch or unrecognised sentinel format, triggering a re-download.
    """
    try:
        data = json.loads(stored)
    except (json.JSONDecodeError, TypeError):
        return False

    local_size = sum(f.stat().st_size for f in local_root.rglob("*") if f.is_file())
    if local_size != data.get("total_bytes", -1):
        _log.info("Local size %s != expected — cache stale.", _format_bytes(local_size))
        return False

    key = f"{prefix}{_SENTINEL_FILE}"
    try:
        remote_etag = s3.head_object(Bucket=bucket, Key=key)["ETag"].strip('"')
    except Exception:
        _log.warning("Could not HEAD %s — treating cache as stale.", key)
        return False

    if remote_etag != data.get("etag"):
        _log.info("ETag mismatch for %s — cache stale.", _SENTINEL_FILE)
        return False

    return True


def _fetch(
    s3: BaseClient, bucket: str, key: str, remote_etag: str, cache_root: Path, etag_index: dict[str, str]
) -> bool:
    """Download *key* if missing or ETag changed. Returns True if a download occurred."""
    cache_key = f"{bucket}/{key}"
    cached_path = cache_root / bucket / key
    cached_path.parent.mkdir(parents=True, exist_ok=True)

    if cached_path.exists() and etag_index.get(cache_key) == remote_etag:
        return False

    s3.download_file(bucket, key, str(cached_path))
    etag_index[cache_key] = remote_etag
    return True


def download_dataset(s3: BaseClient, entry: DatasetEntry, cache_root: Path) -> Path:
    """Download all non-excluded objects for *entry* and return the local root path.

    On a warm cache, validates via ``_validate_sentinel`` (local size + 1 HEAD call)
    and returns immediately if valid. On a cold cache or stale sentinel, lists the S3
    prefix, downloads any missing/changed files, then writes a new sentinel.
    """
    parsed = urlparse(entry.uri)
    bucket: str = parsed.netloc
    prefix: str = parsed.path.lstrip("/")
    local_root = cache_root / bucket / prefix.rstrip("/")

    etag_index = _load_etag_index(ETAG_INDEX_PATH)
    complete_key = f"{_DATASET_COMPLETE_KEY_PREFIX}{entry.id}"

    if complete_key in etag_index:
        if _validate_sentinel(s3, bucket, prefix, local_root, etag_index[complete_key]):
            _log.info("Dataset %s: cache valid.", entry.id)
            return local_root
        _log.info("Dataset %s: cache stale, re-downloading.", entry.id)

    effective_excludes = list(entry.exclude) + list(DEFAULT_EXCLUDES)
    paginator = s3.get_paginator("list_objects_v2")
    objects: list[tuple[str, str]] = []
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            rel_key = key[len(prefix) :]
            if not rel_key or _is_excluded(rel_key, effective_excludes):
                continue
            objects.append((key, obj["ETag"].strip('"')))
            total_bytes += int(obj.get("Size", 0))

    _log.info("Downloading dataset %s — %d objects, %s", entry.id, len(objects), _format_bytes(total_bytes))
    print(f"  Downloading {len(objects)} objects ({_format_bytes(total_bytes)}) ...", flush=True)
    start = time.monotonic()

    fetched = 0
    for j, (key, etag) in enumerate(objects, start=1):
        if _fetch(s3, bucket, key, etag, cache_root, etag_index):
            fetched += 1
        if j % 10 == 0 or j == len(objects):
            print(f"  {j}/{len(objects)} files checked ({fetched} downloaded)", end="\r", flush=True)
    print(flush=True)  # newline after \r progress

    _log.info(
        "Dataset %s ready in %.1fs (%d fetched, %d cache hits)",
        entry.id,
        time.monotonic() - start,
        fetched,
        len(objects) - fetched,
    )

    sentinel_etag = etag_index.get(f"{bucket}/{prefix}{_SENTINEL_FILE}", "")
    etag_index[complete_key] = json.dumps({"etag": sentinel_etag, "total_bytes": total_bytes})
    _save_etag_index(ETAG_INDEX_PATH, etag_index)

    return local_root
