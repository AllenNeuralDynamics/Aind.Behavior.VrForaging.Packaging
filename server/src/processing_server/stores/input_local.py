"""``local`` input store — really just ``mount`` without the network.

Points at a directory; either bind-mounts it directly (``copy_files: false``,
the default — used by the integration-test cache) or copies the staged subset
into the job's work directory (``copy_files: true``).
"""

import logging
import shutil
from pathlib import Path
from typing import Literal

from ..config import StagingConfig
from ..staging import ObjectRef, build_manifest, missing_required, select, within_budget
from . import PreparedInput, StoreDataError, _uri_to_path

logger = logging.getLogger(__name__)


class LocalInputStore:
    name: Literal["s3", "mount", "local"] = "local"

    def __init__(self, staging: StagingConfig | None = None, *, copy_files: bool = False) -> None:
        self._staging = staging or StagingConfig()
        self._copy_files = copy_files

    def list_objects(self, session_uri: str) -> list[ObjectRef]:
        root = _uri_to_path(session_uri)
        if not root.is_dir():
            raise StoreDataError(f"Session path does not exist: {root}")
        return [
            ObjectRef(key=p.relative_to(root).as_posix(), size=p.stat().st_size) for p in root.rglob("*") if p.is_file()
        ]

    def prepare(self, session_uri: str, refs: list[ObjectRef], dest_dir: Path) -> PreparedInput:
        root = _uri_to_path(session_uri)
        if not root.is_dir():
            raise StoreDataError(f"Session path does not exist: {root}")
        selected = select(refs, self._staging.rules)
        missing = missing_required(selected, self._staging.verify_present)
        if missing:
            raise StoreDataError(f"Missing required files under {root}: {missing}")

        if not self._copy_files:
            manifest = build_manifest("local", selected, self._staging)
            return PreparedInput(host_path=root, read_only=True, manifest=manifest)

        if not within_budget(selected, self._staging.max_session_bytes):
            raise StoreDataError(f"{root}: staged size exceeds max_session_bytes ({self._staging.max_session_bytes})")

        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        for ref in selected:
            target = dest / ref.key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / ref.key, target)
        manifest = build_manifest("local", selected, self._staging)
        return PreparedInput(host_path=dest, read_only=True, manifest=manifest)

    def release(self, prepared: PreparedInput) -> None:
        """A no-op for a pass-through mount; removes the copy when ``copy_files: true``."""
        if self._copy_files:
            shutil.rmtree(prepared.host_path, ignore_errors=True)
