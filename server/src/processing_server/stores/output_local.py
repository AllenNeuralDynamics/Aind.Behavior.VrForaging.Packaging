"""``local`` output store — a local or mounted destination.
Same commit-marker discipline as :mod:`output_s3`: ``output.metadata.json``
written last via temp-name + ``os.replace``, so a concurrent reader never
observes a half-written file.
"""

import json
import logging
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

from . import SIDECAR_NAME, PublishManifest, _uri_to_path

logger = logging.getLogger(__name__)

_SIDECAR_NAME = SIDECAR_NAME


def _atomic_copy(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dest)


def _prune(dest: Path, keep: set[str]) -> None:
    """Delete files under *dest* that this publish did not write."""
    for p in sorted((q for q in dest.rglob("*") if q.is_file()), reverse=True):
        if p.relative_to(dest).as_posix() not in keep:
            p.unlink(missing_ok=True)
    for d in sorted((q for q in dest.rglob("*") if q.is_dir()), reverse=True):
        if not any(d.iterdir()):
            d.rmdir()


class LocalOutputStore:
    name: Literal["s3", "local"] = "local"

    def exists(self, dest_uri: str) -> bool:
        return (_uri_to_path(dest_uri) / _SIDECAR_NAME).exists()

    def publish(self, src: Path, dest_uri: str) -> PublishManifest:
        src = Path(src)
        dest = _uri_to_path(dest_uri)
        dest.mkdir(parents=True, exist_ok=True)
        files = [p for p in src.rglob("*") if p.is_file()]
        sidecar = src / _SIDECAR_NAME
        keep = {p.relative_to(src).as_posix() for p in files}
        # Uncommit before touching data: the old marker must not vouch for a
        # half-replaced directory.
        (dest / _SIDECAR_NAME).unlink(missing_ok=True)
        total = 0
        for p in files:
            if p == sidecar:
                continue
            _atomic_copy(p, dest / p.relative_to(src))
            total += p.stat().st_size
        _prune(dest, keep)
        if sidecar.exists():  # written LAST — the commit marker
            _atomic_copy(sidecar, dest / _SIDECAR_NAME)
            total += sidecar.stat().st_size
        return PublishManifest(files=len(files), bytes=total, dest_uri=str(dest), sidecar_uri=str(dest / _SIDECAR_NAME))

    def iter_completed(self, prefix: str) -> Iterator[tuple[str, dict]]:
        root = _uri_to_path(prefix)
        if not root.is_dir():
            return
        for d in sorted(p for p in root.iterdir() if p.is_dir()):
            sidecar = d / _SIDECAR_NAME
            if sidecar.exists():
                yield d.name, json.loads(sidecar.read_text(encoding="utf-8"))

    def read_object(self, uri: str) -> bytes | None:
        path = _uri_to_path(uri)
        return path.read_bytes() if path.is_file() else None

    def write_object(self, uri: str, payload: bytes) -> int:
        path = _uri_to_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return len(payload)

    def copy_prefix(self, src_uri: str, dest_uri: str) -> int:
        src = _uri_to_path(src_uri)
        dest = _uri_to_path(dest_uri)
        if not src.is_dir():
            return 0
        files = [p for p in src.rglob("*") if p.is_file()]
        sidecar = src / _SIDECAR_NAME
        dest.mkdir(parents=True, exist_ok=True)
        for p in files:
            if p == sidecar:
                continue
            _atomic_copy(p, dest / p.relative_to(src))
        if sidecar.exists():  # marker last, same as publish
            _atomic_copy(sidecar, dest / _SIDECAR_NAME)
        return len(files)

    def delete_prefix(self, uri: str) -> int:
        path = _uri_to_path(uri)
        if not path.exists():
            return 0
        n = sum(1 for p in path.rglob("*") if p.is_file())
        shutil.rmtree(path, ignore_errors=True)
        return n

    def list_children(self, uri: str) -> list[str]:
        root = _uri_to_path(uri)
        if not root.is_dir():
            return []
        return sorted(p.name for p in root.iterdir() if p.is_dir())
