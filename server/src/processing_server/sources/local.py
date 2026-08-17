"""``LocalSource`` — directory scan. What makes the whole system testable and
debuggable without network access. No optional dependency required.
"""

import re
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from ..models import SessionRef

_DEFAULT_SESSION_RE = r"^(behavior_)?\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"


class LocalSource:
    """Discover sessions as immediate subdirectories of *root*.

    ``cursor`` is the directory's mtime (ISO 8601), used the same way DocDB's
    ``created`` is: a watermark for "what's new since I last looked", not a
    stand-in for acquisition time.
    """

    name: Literal["docdb", "local"] = "local"

    def __init__(self, root: Path | str, *, name_pattern: str = _DEFAULT_SESSION_RE) -> None:
        self.root = Path(root)
        self._pattern = re.compile(name_pattern)

    def discover(self, since: str | None) -> Iterator[SessionRef]:
        if not self.root.is_dir():
            return
        since_dt = datetime.fromisoformat(since) if since else None
        entries = sorted((p for p in self.root.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime)
        for p in entries:
            if not self._pattern.match(p.name):
                continue
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            if since_dt is not None and mtime < since_dt:
                continue
            yield SessionRef(
                session_name=p.name,
                input_uri=p.resolve().as_uri(),
                cursor=mtime.isoformat(),
                discovered_by="local",
            )
