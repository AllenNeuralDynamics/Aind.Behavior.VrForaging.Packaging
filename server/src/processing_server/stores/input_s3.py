"""``s3`` input store — the download fallback: Docker Desktop/Windows hosts
where FUSE is unavailable, a hard offline-reproducibility guarantee, or the
integration-test cache. Lists once, filters on metadata, downloads only what
the staging rules selected — never a full 16 GB session.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

import boto3
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from ..config import StagingConfig
from ..staging import ObjectRef, build_manifest, missing_required, select, within_budget
from . import PreparedInput, StoreDataError, StoreTransientError, parse_s3_uri

logger = logging.getLogger(__name__)

_DOWNLOAD_WORKERS = 8


class S3InputStore:
    """Lists a session prefix, applies staging rules, downloads the selected keys."""

    name: Literal["s3", "mount", "local"] = "s3"

    def __init__(self, staging: StagingConfig | None = None, *, client: BaseClient | None = None) -> None:
        self._staging = staging or StagingConfig()
        # Ambient credentials (instance role / profile / env) — NOT anonymous:
        # ~77% of raw sessions are in a private bucket.
        self._client = client or boto3.client("s3")

    def list_objects(self, session_uri: str) -> list[ObjectRef]:
        bucket, prefix = parse_s3_uri(session_uri)
        refs: list[ObjectRef] = []
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    rel = obj["Key"][len(prefix) :]
                    if not rel:
                        continue
                    refs.append(ObjectRef(key=rel, size=int(obj.get("Size", 0)), etag=obj.get("ETag", "").strip('"')))
        except (BotoCoreError, ClientError) as exc:
            raise StoreTransientError(f"Listing {session_uri} failed: {exc}") from exc
        return refs

    def prepare(self, session_uri: str, refs: list[ObjectRef], dest_dir: Path) -> PreparedInput:
        bucket, prefix = parse_s3_uri(session_uri)
        selected = select(refs, self._staging.rules)
        missing = missing_required(selected, self._staging.verify_present)
        if missing:
            raise StoreDataError(f"Missing required files in {session_uri}: {missing}")
        if not within_budget(selected, self._staging.max_session_bytes):
            raise StoreDataError(
                f"{session_uri}: staged size exceeds max_session_bytes ({self._staging.max_session_bytes})"
            )

        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)

        def _fetch(ref: ObjectRef) -> None:
            target = dest / ref.key
            target.parent.mkdir(parents=True, exist_ok=True)
            self._client.download_file(bucket, prefix + ref.key, str(target))

        try:
            with ThreadPoolExecutor(max_workers=_DOWNLOAD_WORKERS) as executor:
                list(executor.map(_fetch, selected))
        except (BotoCoreError, ClientError) as exc:
            raise StoreTransientError(f"Downloading {session_uri} failed: {exc}") from exc

        manifest = build_manifest("s3", selected, self._staging)
        return PreparedInput(host_path=dest, read_only=True, manifest=manifest)

    def release(self, prepared: PreparedInput) -> None:
        import shutil

        shutil.rmtree(prepared.host_path, ignore_errors=True)
