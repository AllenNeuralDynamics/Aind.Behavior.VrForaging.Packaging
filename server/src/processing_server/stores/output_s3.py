"""``s3`` output store — threaded upload; ``output.metadata.json`` written
last, so its presence is a commit marker that ``reconcile`` and ``iter_completed``
can trust.
"""

import json
import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

import boto3
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from . import PublishManifest, StoreTransientError, parse_s3_uri

logger = logging.getLogger(__name__)

_SIDECAR_NAME = "output.metadata.json"
_UPLOAD_WORKERS = 8


def _is_not_found(exc: ClientError) -> bool:
    code = exc.response.get("Error", {}).get("Code", "")
    return code in ("404", "NoSuchKey", "NotFound")


class S3OutputStore:
    name: Literal["s3", "local"] = "s3"

    def __init__(self, *, client: BaseClient | None = None) -> None:
        self._client = client or boto3.client("s3")

    def exists(self, dest_uri: str) -> bool:
        bucket, prefix = parse_s3_uri(dest_uri)
        try:
            self._client.head_object(Bucket=bucket, Key=prefix + _SIDECAR_NAME)
            return True
        except ClientError as exc:
            if _is_not_found(exc):
                return False
            raise StoreTransientError(f"HEAD {dest_uri}{_SIDECAR_NAME} failed: {exc}") from exc

    def publish(self, src: Path, dest_uri: str) -> PublishManifest:
        bucket, prefix = parse_s3_uri(dest_uri)
        src = Path(src)
        files = [p for p in src.rglob("*") if p.is_file()]
        sidecar = src / _SIDECAR_NAME
        others = [p for p in files if p != sidecar]
        total = sum(p.stat().st_size for p in files)

        def _upload(p: Path) -> None:
            key = prefix + p.relative_to(src).as_posix()
            self._client.upload_file(str(p), bucket, key)

        try:
            with ThreadPoolExecutor(max_workers=_UPLOAD_WORKERS) as executor:
                list(executor.map(_upload, others))
            if sidecar.exists():  # written LAST — the commit marker
                _upload(sidecar)
        except (BotoCoreError, ClientError) as exc:
            raise StoreTransientError(f"Publishing to {dest_uri} failed: {exc}") from exc

        return PublishManifest(
            files=len(files), bytes=total, dest_uri=dest_uri, sidecar_uri=f"s3://{bucket}/{prefix}{_SIDECAR_NAME}"
        )

    def iter_completed(self, prefix: str) -> Iterator[tuple[str, dict]]:
        bucket, key_prefix = parse_s3_uri(prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                session_prefix = cp["Prefix"]
                session_name = session_prefix.rstrip("/").rsplit("/", 1)[-1]
                try:
                    obj = self._client.get_object(Bucket=bucket, Key=session_prefix + _SIDECAR_NAME)
                    data = json.loads(obj["Body"].read())
                except ClientError as exc:
                    if _is_not_found(exc):
                        continue  # interrupted publish, not a completed session
                    raise StoreTransientError(f"Reading sidecar under {session_prefix} failed: {exc}") from exc
                yield session_name, data
