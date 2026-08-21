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

from . import SIDECAR_NAME, PublishManifest, StoreTransientError, parse_s3_object_uri, parse_s3_uri

logger = logging.getLogger(__name__)

_SIDECAR_NAME = SIDECAR_NAME
_UPLOAD_WORKERS = 8
#: `delete_objects` takes at most 1000 keys per call.
_DELETE_BATCH = 1000


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

    def _keys_under(self, bucket: str, prefix: str) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def _delete_keys(self, bucket: str, keys: list[str]) -> int:
        for i in range(0, len(keys), _DELETE_BATCH):
            batch = keys[i : i + _DELETE_BATCH]
            self._client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
        return len(keys)

    def publish(self, src: Path, dest_uri: str) -> PublishManifest:
        bucket, prefix = parse_s3_uri(dest_uri)
        src = Path(src)
        files = [p for p in src.rglob("*") if p.is_file()]
        sidecar = src / _SIDECAR_NAME
        others = [p for p in files if p != sidecar]
        total = sum(p.stat().st_size for p in files)
        written = {prefix + p.relative_to(src).as_posix() for p in files}

        def _upload(p: Path) -> None:
            key = prefix + p.relative_to(src).as_posix()
            self._client.upload_file(str(p), bucket, key)

        try:
            existing = self._keys_under(bucket, prefix)
            # Uncommit before touching data: the old marker must not vouch for a
            # half-replaced prefix.
            if prefix + _SIDECAR_NAME in existing:
                self._delete_keys(bucket, [prefix + _SIDECAR_NAME])
            with ThreadPoolExecutor(max_workers=_UPLOAD_WORKERS) as executor:
                list(executor.map(_upload, others))
            stale = [k for k in existing if k not in written and k != prefix + _SIDECAR_NAME]
            if stale:
                logger.info("Removing %d stale object(s) under %s", len(stale), dest_uri)
                self._delete_keys(bucket, stale)
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

    def read_object(self, uri: str) -> bytes | None:
        bucket, key = parse_s3_object_uri(uri)
        try:
            return bytes(self._client.get_object(Bucket=bucket, Key=key)["Body"].read())
        except ClientError as exc:
            if _is_not_found(exc):
                return None
            raise StoreTransientError(f"Reading {uri} failed: {exc}") from exc

    def write_object(self, uri: str, payload: bytes) -> int:
        bucket, key = parse_s3_object_uri(uri)
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=payload)
        except (BotoCoreError, ClientError) as exc:
            raise StoreTransientError(f"Writing {uri} failed: {exc}") from exc
        return len(payload)

    def copy_prefix(self, src_uri: str, dest_uri: str) -> int:
        src_bucket, src_prefix = parse_s3_uri(src_uri)
        dest_bucket, dest_prefix = parse_s3_uri(dest_uri)

        def _copy(key: str) -> None:
            rel = key[len(src_prefix) :]
            self._client.copy_object(
                Bucket=dest_bucket, Key=dest_prefix + rel, CopySource={"Bucket": src_bucket, "Key": key}
            )

        try:
            keys = [k for k in self._keys_under(src_bucket, src_prefix) if not k.endswith("/")]
            marker = src_prefix + _SIDECAR_NAME
            with ThreadPoolExecutor(max_workers=_UPLOAD_WORKERS) as executor:
                list(executor.map(_copy, [k for k in keys if k != marker]))
            if marker in keys:  # marker last, same as publish
                _copy(marker)
        except (BotoCoreError, ClientError) as exc:
            raise StoreTransientError(f"Copying {src_uri} to {dest_uri} failed: {exc}") from exc
        return len(keys)

    def delete_prefix(self, uri: str) -> int:
        bucket, prefix = parse_s3_uri(uri)
        try:
            keys = self._keys_under(bucket, prefix)
            if not keys:
                return 0
            return self._delete_keys(bucket, keys)
        except (BotoCoreError, ClientError) as exc:
            raise StoreTransientError(f"Deleting {uri} failed: {exc}") from exc

    def list_children(self, uri: str) -> list[str]:
        bucket, prefix = parse_s3_uri(uri)
        paginator = self._client.get_paginator("list_objects_v2")
        names: list[str] = []
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    names.append(cp["Prefix"].rstrip("/").rsplit("/", 1)[-1])
        except (BotoCoreError, ClientError) as exc:
            raise StoreTransientError(f"Listing {uri} failed: {exc}") from exc
        return sorted(names)
