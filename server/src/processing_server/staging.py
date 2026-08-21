"""Staging rule engine — a pure, store-independent decision layer.

Given the *listing* of a session's objects (never their bytes — that is a
store's job), decides which ones to fetch. All decisions happen on
metadata, which is why listing is cheap and downloading is not: nothing here
transfers a single byte.

Applies to ``store: s3``/``local``. Under ``store: mount`` these rules are
advisory only — used for the sidecar's ``available_*`` figures — since nothing
is copied; the processor's own reads decide what actually moves.
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal

from .config import StagingConfig, StagingRule


@dataclass(frozen=True)
class ObjectRef:
    """One input object, identified relative to the session root."""

    key: str
    """POSIX-relative, e.g. ``"behavior/SoftwareEvents/Block.json"``."""
    size: int
    etag: str | None = None


@dataclass(frozen=True)
class InputManifest:
    """What the staging rules selected — recorded in the ledger and the sidecar."""

    store: Literal["s3", "mount", "local"]
    available_files: int
    available_bytes: int
    include: list[str]
    exclude: list[str]
    truncated: bool = False
    """``True`` if ``max_session_bytes`` stopped the listing short."""


def _segments(key: str) -> list[str]:
    return [s for s in key.split("/") if s]


def _path_matches(key: str, rule_path: str) -> bool:
    """Rules match case-insensitively on the leading path segment (``Behavior``
    vs ``behavior`` capitalization varies across legacy sessions)."""
    if rule_path == "":
        return True
    segs = _segments(key)
    return bool(segs) and segs[0].lower() == rule_path.lower()


def _relative_to_rule(key: str, rule_path: str) -> str:
    if rule_path == "":
        return key
    parts = key.split("/", 1)
    return parts[1] if len(parts) > 1 else ""


def _rule_covers(key: str, rule: StagingRule) -> bool:
    if not _path_matches(key, rule.path):
        return False
    if not rule.recursive and "/" in _relative_to_rule(key, rule.path):
        return False
    return True


def _glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate ``*``/``**``/``?`` to a regex matching the whole string.

    Deliberately not ``pathlib.PurePath.match()``: that method requires ``**``
    to consume at least one full path segment, so ``"metadata.csv".match("**/*.csv")``
    is ``False`` — a single-level file never matches a "match at any depth"
    pattern. Here ``**/`` matches zero or more segments, so depth 0 counts too.
    """
    i, n = 0, len(pattern)
    out: list[str] = []
    while i < n:
        if pattern[i : i + 3] == "**/":
            out.append(r"(?:.*/)?")
            i += 3
        elif pattern[i : i + 2] == "**":
            out.append(r".*")
            i += 2
        elif pattern[i] == "*":
            out.append(r"[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append(r"[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("(?s:" + "".join(out) + r")\Z")


def _glob_match(rel_key: str, patterns: list[str]) -> bool:
    lowered = rel_key.lower()
    return any(_glob_to_regex(p.lower()).match(lowered) for p in patterns)


def select(objects: Iterable[ObjectRef], rules: list[StagingRule]) -> list[ObjectRef]:
    """Apply include-then-exclude rules, evaluated per rule under its ``path``.

    A key belongs to the *first* rule whose ``path`` covers it (in config
    order); a key covered by no rule is not staged — this is a narrow allow-list,
    not a deny-list, so a new file type appearing anywhere is skipped by
    default rather than swept in because nobody excluded it.
    """
    selected: list[ObjectRef] = []
    for obj in objects:
        rule = next((r for r in rules if _rule_covers(obj.key, r)), None)
        if rule is None:
            continue
        rel = _relative_to_rule(obj.key, rule.path)
        if not _glob_match(rel, rule.include):
            continue
        if rule.exclude and _glob_match(rel, rule.exclude):
            continue
        selected.append(obj)
    return selected


def missing_required(selected: list[ObjectRef], required: list[str]) -> list[str]:
    """Return the entries of *required* (matched by filename) absent from *selected*.

    Backs ``verify_present``: after staging, required files (e.g.
    ``data_description.json``) must exist and be non-empty, or the job fails
    as ``transient`` before a container starts — the cheap defence against the
    "empty input directory → exit 0" trap.
    """
    names = {PurePosixPath(o.key).name for o in selected if o.size > 0}
    return [r for r in required if r not in names]


def total_bytes(objects: Iterable[ObjectRef]) -> int:
    return sum(o.size for o in objects)


def within_budget(objects: Iterable[ObjectRef], max_session_bytes: int) -> bool:
    """Computed from the listing, before any transfer — a pathological session
    is refused rather than half-fetched."""
    return total_bytes(objects) <= max_session_bytes


def build_manifest(
    store: Literal["s3", "mount", "local"],
    selected: list[ObjectRef],
    config: StagingConfig,
    *,
    truncated: bool = False,
) -> InputManifest:
    return InputManifest(
        store=store,
        available_files=len(selected),
        available_bytes=total_bytes(selected),
        include=[p for r in config.rules for p in r.include],
        exclude=[p for r in config.rules for p in r.exclude],
        truncated=truncated,
    )
