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

from . import PublishManifest, _uri_to_path

logger = logging.getLogger(__name__)

_SIDECAR_NAME = "output.metadata.json"


def _atomic_copy(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dest)


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
        total = 0
        for p in files:
            if p == sidecar:
                continue
            _atomic_copy(p, dest / p.relative_to(src))
            total += p.stat().st_size
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
