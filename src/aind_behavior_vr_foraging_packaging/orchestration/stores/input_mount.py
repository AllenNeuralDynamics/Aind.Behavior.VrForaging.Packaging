"""``mount`` input store — the default (§10). Almost no code: a pass-through that
reports a path and copies nothing. What makes the path a directory (a genuinely
local/mounted filesystem, or host-level Mountpoint-S3) is outside this process.
"""

import logging
from pathlib import Path
from typing import Literal

from ..config import StagingConfig
from ..staging import ObjectRef, build_manifest, missing_required, select
from . import PreparedInput, StoreDataError, _uri_to_path

logger = logging.getLogger(__name__)


class MountInputStore:
    """Reports the existing session path unchanged; ``prepare`` copies nothing."""

    name: Literal["s3", "mount", "local"] = "mount"

    def __init__(self, staging: StagingConfig | None = None) -> None:
        self._staging = staging or StagingConfig()

    def list_objects(self, session_uri: str) -> list[ObjectRef]:
        root = _uri_to_path(session_uri)
        if not root.is_dir():
            raise StoreDataError(f"Session path does not exist or is not mounted: {root}")
        return [
            ObjectRef(key=p.relative_to(root).as_posix(), size=p.stat().st_size) for p in root.rglob("*") if p.is_file()
        ]

    def prepare(self, session_uri: str, refs: list[ObjectRef], dest_dir: Path) -> PreparedInput:
        root = _uri_to_path(session_uri)
        if not root.is_dir():
            raise StoreDataError(f"Session path does not exist or is not mounted: {root}")
        # Rules are advisory here — they describe what *would* be read (for the
        # sidecar's `available_*` figures), not what is actually made accessible;
        # the whole mounted tree is already visible to the processor (§10).
        selected = select(refs, self._staging.rules)
        missing = missing_required(selected, self._staging.verify_present)
        if missing:
            raise StoreDataError(f"Missing required files under {root}: {missing}")
        manifest = build_manifest("mount", selected, self._staging)
        return PreparedInput(host_path=root, read_only=True, manifest=manifest)

    def release(self, prepared: PreparedInput) -> None:
        """No-op: a mount store never owns the path it returns."""
