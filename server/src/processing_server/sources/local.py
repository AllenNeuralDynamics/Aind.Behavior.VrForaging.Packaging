"""``LocalSource`` — directory scan. What makes the whole system testable and
debuggable without network access. No optional dependency required.
"""

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from ..models import SessionRef


class LocalSource:
    """Discover sessions as immediate subdirectories of *root*.

    **Every** subdirectory, with no name filtering. A directory under the ingest root
    is there to be processed; one that turns out not to be a session fails in staging
    with a named reason (``verify_present``), which is more useful than being silently
    passed over — a name-shaped filter cannot tell "not a session" from "a session
    named unexpectedly", and quietly discards both.

    ``cursor`` is the directory's mtime (ISO 8601), used the same way DocDB's
    ``created`` is: a watermark for "what's new since I last looked", not a
    stand-in for acquisition time.
    """

    name: Literal["docdb", "local", "manifest"] = "local"

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def discover(self, since: str | None) -> Iterator[SessionRef]:
        if not self.root.is_dir():
            return
        since_dt = datetime.fromisoformat(since) if since else None
        entries = sorted((p for p in self.root.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime)
        for p in entries:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            if since_dt is not None and mtime < since_dt:
                continue
            yield SessionRef(
                session_name=p.name,
                input_uri=p.resolve().as_uri(),
                cursor=mtime.isoformat(),
                discovered_by="local",
            )
