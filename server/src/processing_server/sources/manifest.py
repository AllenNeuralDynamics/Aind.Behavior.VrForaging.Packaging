"""``ManifestSource`` — process exactly the sessions named in a file, then stop.

The campaign case: a manuscript's session list has already been decided, reviewed and
committed to a file, and what the run has to do is process *that set* — not whatever
DocDB reports today. Reproducibility comes from the file rather than from a query that
may return something different next month.

Finite by construction, so it pairs with ``worker.exit_when_drained``: the container
processes the list, aggregates, and exits.
"""

import json
import logging
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from ..models import SessionRef

logger = logging.getLogger(__name__)

_DEFAULT_SESSION_RE = r"^(behavior_)?\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"

#: The subject id is the leading numeric field of the session name, by the naming
#: convention ``_DEFAULT_SESSION_RE`` already encodes.
_SUBJECT_RE = re.compile(r"^(?:behavior_)?(\d+)_")

#: Sibling keys a dedup/matching step may leave beside ``sessions``. Reported, never
#: processed — an entry that reached one of these has no location to read.
_REJECT_KEYS = ("ambiguous", "unmatched")


class ManifestError(Exception):
    """The manifest is unusable. Raised at construction so a container fails at startup
    rather than after reporting a successful run over zero sessions."""


class ManifestSource:
    """Read ``{"sessions": [{"session_name": ..., "location": ...}, ...]}``.

    A bare top-level list of the same objects is also accepted, since that is what a
    one-off script tends to produce.

    ``discover`` **ignores** *since*. A manifest is a finite, static set, so a watermark
    would only create a way for the run to skip part of the list it was given; every
    sweep re-yields everything and ``job_key`` uniqueness absorbs the duplicates. Adding
    lines to the file therefore works without resetting anything.
    """

    name: Literal["docdb", "local", "manifest"] = "manifest"

    def __init__(self, path: Path | str, *, name_pattern: str = _DEFAULT_SESSION_RE) -> None:
        self.path = Path(path)
        self._pattern = re.compile(name_pattern)
        self._entries = self._load()

    def __len__(self) -> int:
        return len(self._entries)

    def _load(self) -> list[tuple[str, str]]:
        """Parse and validate the whole file up front; return ``[(session_name, uri)]``.

        Everything that can be wrong with a manifest is wrong *before* any session is
        processed, so it is all reported in one pass at startup: a 1700-session campaign
        should not discover on session 1699 that the file had a typo.
        """
        entries, skipped, considered = self._collect(self._items())
        for note in skipped:
            logger.warning("Manifest %s: %s", self.path.name, note)
        if not entries:
            raise ManifestError(
                f"Manifest {self.path} yielded no usable sessions out of {considered} entr(ies) — "
                "every one was missing a location, misnamed or a duplicate"
            )
        logger.info(
            "Manifest %s: %d session(s) to process%s",
            self.path.name,
            len(entries),
            f", {len(skipped)} entr(ies) skipped" if skipped else "",
        )
        return entries

    def _items(self) -> list[Any]:
        """The raw session entries, whichever accepted top-level shape the file uses."""
        if not self.path.is_file():
            raise ManifestError(f"Manifest {self.path} does not exist (is it mounted into the container?)")
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ManifestError(f"Manifest {self.path} is not readable JSON: {exc}") from exc

        if isinstance(raw, list):
            return raw
        if not isinstance(raw, dict):
            raise ManifestError(f"Manifest {self.path} must hold an object or a list, not {type(raw).__name__}")
        if "sessions" not in raw:
            raise ManifestError(f"Manifest {self.path} is an object without a 'sessions' key; got {sorted(raw)}")
        for key in _REJECT_KEYS:
            rejected = raw.get(key) or []
            if rejected:
                logger.warning(
                    "Manifest %s lists %d %s entr(ies) — these have no location and are NOT processed: %s%s",
                    self.path.name,
                    len(rejected),
                    key,
                    ", ".join(str(_name_of(r)) for r in rejected[:5]),
                    ", …" if len(rejected) > 5 else "",
                )
        items = raw["sessions"]
        if not isinstance(items, list):
            raise ManifestError(f"Manifest {self.path}: 'sessions' must be a list, not {type(items).__name__}")
        return items

    def _collect(self, items: list[Any]) -> tuple[list[tuple[str, str]], list[str], int]:
        """Validate each entry. Returns ``(usable, complaints, considered)``."""
        entries: list[tuple[str, str]] = []
        seen: dict[str, str] = {}
        skipped: list[str] = []
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                skipped.append(f"[{index}] is a {type(item).__name__}, not an object")
                continue
            name, location = str(item.get("session_name") or ""), str(item.get("location") or "")
            if not name or not location:
                skipped.append(f"[{index}] {name or '<unnamed>'}: missing session_name or location")
            elif not self._pattern.match(name):
                skipped.append(f"[{index}] {name}: does not match the expected session-name pattern")
            elif name in seen:
                # Deduped here rather than left to `job_key`, so the count this source
                # reports is the number of sessions that will actually be processed.
                if seen[name] != location:
                    skipped.append(f"[{index}] {name}: listed twice with different locations, keeping the first")
            else:
                seen[name] = location
                entries.append((name, location))
        return entries, skipped, len(items)

    def discover(self, since: str | None) -> Iterator[SessionRef]:
        for name, location in self._entries:
            subject = _SUBJECT_RE.match(name)
            yield SessionRef(
                session_name=name,
                input_uri=location,
                subject_id=subject.group(1) if subject else None,
                # No cursor: see the class docstring. And no `session_start` — the folder
                # name carries a timestamp, but inferring acquisition metadata from a
                # directory name is a guess, and a wrong one is worse than a blank.
                cursor=None,
                discovered_by="manifest",
            )


def _name_of(entry: Any) -> Any:
    """A rejected entry may be a bare name or an object; report either."""
    return entry.get("session_name", entry) if isinstance(entry, dict) else entry
