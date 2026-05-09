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

# Single per-cache JSON index of S3 ETags, keyed by "<bucket>/<key>". Lives at
# the cache root rather than next to each cached file so that nothing inside
# any dataset's local prefix can be mistaken for data by glob-based parsers.
ETAG_INDEX_PATH = CACHE_ROOT / "_etags.json"

# Video files are large, slow to download, and never used by the parser —
# opt back in only if a future test needs them.
DEFAULT_EXCLUDES: tuple[str, ...] = ("**/*.mp4", "**/*.avi", "**/*.mkv")

_manifest: DatasetManifest = load_manifest(MANIFEST_PATH)


def pytest_configure(config: pytest.Config) -> None:
    """Swap NwbSession._get_aind_data_schema_json to read local JSON files.

    Scoped to the integration/ directory; does not affect the unit-test suite.
    """
    from aind_behavior_vr_foraging_nwb.nwb_file import NwbSession, _AindDataSchemaJson

    def _from_root(self: NwbSession) -> _AindDataSchemaJson:
        return _AindDataSchemaJson.from_root_path(self.root_path)

    setattr(NwbSession, "_get_aind_data_schema_json", _from_root)


@pytest.fixture(scope="session")
def s3_client() -> BaseClient:
    """Anonymous S3 client for accessing public buckets."""
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def _is_excluded(rel_key: str, patterns: list[str]) -> bool:
    """Return True if *rel_key* matches any exclude pattern (case-insensitive)."""
    lowered = rel_key.lower()
    return any(PurePosixPath(lowered).match(p.lower()) for p in patterns)


def _format_bytes(n: int) -> str:
    """Human-readable byte count for log lines."""
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _load_etag_index(path: Path) -> dict[str, str]:
    """Read the cache-wide ETag index, or return {} if absent."""
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _save_etag_index(path: Path, index: dict[str, str]) -> None:
    """Atomically persist the ETag index via temp-file + os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(index, indent=2, sort_keys=True))
    os.replace(tmp, path)


def _fetch(s3: BaseClient, bucket: str, key: str, cache_root: Path, etag_index: dict[str, str]) -> bool:
    """Download *key* from *bucket* to the local cache, skipping if ETag matches.

    *etag_index* is mutated in place when a download occurs and persisted
    after each successful fetch so a partial run leaves a recoverable state.

    Returns True when a network download was performed, False on a cache hit.
    """
    head = s3.head_object(Bucket=bucket, Key=key)
    remote_etag: str = head["ETag"].strip('"')
    cache_key = f"{bucket}/{key}"

    cached_path = cache_root / bucket / key
    cached_path.parent.mkdir(parents=True, exist_ok=True)

    if cached_path.exists() and etag_index.get(cache_key) == remote_etag:
        return False

    s3.download_file(bucket, key, str(cached_path))
    etag_index[cache_key] = remote_etag
    _save_etag_index(ETAG_INDEX_PATH, etag_index)
    return True


def download_dataset(s3: BaseClient, entry: DatasetEntry, cache_root: Path) -> Path:
    """Download all non-excluded objects for *entry* and return the local root path.

    Parameters
    ----------
    s3:
        An authenticated (or anonymous) boto3 S3 client.
    entry:
        Manifest entry describing the dataset.
    cache_root:
        Parent directory for the local cache tree.

    Returns
    -------
    Path
        Local directory equivalent to the S3 prefix — suitable for passing
        directly to ``NwbSession(...)``.
    """
    parsed = urlparse(entry.uri)
    bucket: str = parsed.netloc
    prefix: str = parsed.path.lstrip("/")

    effective_excludes = list(entry.exclude) + list(DEFAULT_EXCLUDES)
    etag_index = _load_etag_index(ETAG_INDEX_PATH)

    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            rel_key = key[len(prefix) :]
            if not rel_key:
                continue
            if _is_excluded(rel_key, effective_excludes):
                continue
            keys.append(key)
            total_bytes += int(obj.get("Size", 0))

    _log.info(
        "Downloading dataset %s — %d objects, %s",
        entry.id,
        len(keys),
        _format_bytes(total_bytes),
    )
    start = time.monotonic()

    fetched = 0
    for key in keys:
        if _fetch(s3, bucket, key, cache_root, etag_index):
            fetched += 1

    elapsed = time.monotonic() - start
    _log.info(
        "Dataset %s ready in %.1fs (%d fetched, %d cache hits)",
        entry.id,
        elapsed,
        fetched,
        len(keys) - fetched,
    )

    return cache_root / bucket / prefix.rstrip("/")
