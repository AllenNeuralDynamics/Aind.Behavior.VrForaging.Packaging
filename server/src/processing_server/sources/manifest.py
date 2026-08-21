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
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from ..models import SessionRef

logger = logging.getLogger(__name__)

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

    Nothing is inferred from a session's name — not its shape, not the subject, not the
    date. A name is an identifier to be carried, and the metadata it superficially
    resembles is read from the session itself.
    """

    name: Literal["docdb", "local", "manifest"] = "manifest"

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
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
                "every one was missing a session_name or a location, or was a duplicate"
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
                # The only rejection left. A name this source cannot use is one it cannot
                # locate — nothing about the *shape* of a name disqualifies it, because
                # the file is a decision someone already made.
                skipped.append(f"[{index}] {name or '<unnamed>'}: missing session_name or location")
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
            # Identity only, and nothing derived from it. `subject_id` and `session_start`
            # both live in the session's own metadata, which only the processor opens, and
            # both are filled in from its sidecar on completion. Reading them out of the
            # name instead would be a guess that *wins*: the ledger backfills with
            # `COALESCE`, so a value present at discovery is never corrected later.
            yield SessionRef(
                session_name=name,
                input_uri=location,
                cursor=None,  # a finite static set has no watermark; see the class docstring
                discovered_by="manifest",
            )


def _name_of(entry: Any) -> Any:
    """A rejected entry may be a bare name or an object; report either."""
    return entry.get("session_name", entry) if isinstance(entry, dict) else entry
