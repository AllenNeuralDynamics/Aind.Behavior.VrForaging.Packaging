"""Integration test fixtures and helpers.

pytest_configure patches NwbSession to use local JSON files instead of the
AIND metadata API, so integration tests work without network access to DocDB.
"""

from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

import boto3
import pytest
from botocore import UNSIGNED
from botocore.client import BaseClient
from botocore.config import Config

from .model import DatasetEntry, DatasetManifest, load_manifest

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

MANIFEST_PATH = Path(__file__).parent / "datasets.yml"
CACHE_ROOT = Path(__file__).parent / ".cache"

# Glob patterns excluded from every download regardless of the per-entry
# `exclude` list. Video files are large, slow to download, and never used
# by the parser — opt back in only if a future test needs them.
DEFAULT_EXCLUDES: tuple[str, ...] = ("**/*.mp4", "**/*.avi", "**/*.mkv")

# Load manifest at collection time so schema errors surface immediately.
_manifest: DatasetManifest = load_manifest(MANIFEST_PATH)


# ---------------------------------------------------------------------------
# Patch NwbSession to skip DocDB and use local JSON files instead
# ---------------------------------------------------------------------------


def pytest_configure(config: pytest.Config) -> None:
    """Swap NwbSession._get_aind_data_schema_json to the local-path variant.

    This runs when conftest.py is loaded (i.e. only for the integration/
    directory), so it does not affect the unit-test suite.
    """
    from aind_behavior_vr_foraging_nwb.nwb_file import NwbSession, _AindDataSchemaJson

    def _from_root(self: NwbSession) -> _AindDataSchemaJson:
        return _AindDataSchemaJson.from_root_path(self.root_path)

    setattr(NwbSession, "_get_aind_data_schema_json", _from_root)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def s3_client() -> BaseClient:
    """Anonymous S3 client for accessing public buckets."""
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------


def _is_excluded(rel_key: str, patterns: list[str]) -> bool:
    """Return True if *rel_key* matches any exclude pattern (case-insensitive)."""
    lowered = rel_key.lower()
    return any(PurePosixPath(lowered).match(p.lower()) for p in patterns)


def _needs_download(cached_path: Path, remote_etag: str) -> bool:
    """Return True when the cached file is absent or stale."""
    etag_path = cached_path.with_suffix(cached_path.suffix + ".etag")
    if not cached_path.exists() or not etag_path.exists():
        return True
    return etag_path.read_text().strip() != remote_etag.strip('"')


def _fetch(s3: BaseClient, bucket: str, key: str, cache_root: Path) -> Path:
    """Download *key* from *bucket* to the local cache, skipping if ETag matches."""
    head = s3.head_object(Bucket=bucket, Key=key)
    remote_etag: str = head["ETag"]

    cached_path = cache_root / bucket / key
    cached_path.parent.mkdir(parents=True, exist_ok=True)

    if not _needs_download(cached_path, remote_etag):
        return cached_path  # cache hit

    s3.download_file(bucket, key, str(cached_path))
    etag_path = cached_path.with_suffix(cached_path.suffix + ".etag")
    etag_path.write_text(remote_etag)
    return cached_path


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

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            rel_key = key[len(prefix) :]
            if not rel_key:
                # Skip the prefix "directory" object itself if it exists
                continue
            if _is_excluded(rel_key, effective_excludes):
                continue
            _fetch(s3, bucket, key, cache_root)

    return cache_root / bucket / prefix.rstrip("/")
