"""Stores — *how* bytes arrive and leave. Independent of *which*
sessions exist (that is ``pipeline.sources``'s job).

``mount`` and ``s3`` read the same bytes; the difference is when they move and
who decides which ones. ``mount`` is the default: the processor's own
reads decide, which is both smaller (33 MB vs 262 MB, measured) and correct by
construction, since the read set is register-level and not knowable in advance.
"""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import unquote, urlparse

from ..staging import InputManifest, ObjectRef

#: A bare Windows drive path (``C:\...`` / ``C:/...``) is otherwise misparsed by
#: `urlparse` as scheme ``"c"`` — a single-letter "scheme" indistinguishable
#: from a drive letter. Checked before `urlparse` runs at all (this project
#: supports Windows).
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")


class StoreError(Exception):
    """Base of the store error hierarchy. Failures classify on the concrete subclass,
    not on parsing an error string — the layer that knows what went wrong decides."""


class StoreTransientError(StoreError):
    """Timeout, throttling, 5xx, connection reset → retry (``error_kind='transient'``)."""


class StoreDataError(StoreError):
    """Missing session, ``verify_present`` failed, bad manifest → ``error_kind='data'``."""


class StoreConfigError(StoreError):
    """Bad credentials, bucket not found, mount absent → ``error_kind='infra'`` (fails
    identically for every job, so it is an environment problem, not 4700 data problems)."""


@dataclass(frozen=True)
class PreparedInput:
    """What a store's :meth:`InputStore.prepare` hands back to the worker."""

    host_path: Path
    """What the worker passes to ``docker run -v`` (identity-mapped). Its
    basename is the session name either way — see ``Worker._resolve_mount``."""
    read_only: bool
    """``True`` for ``mount``, and ``True`` for staged copies too, by policy —
    the processor never has a reason to write to its input."""
    manifest: InputManifest


class InputStore(Protocol):
    name: Literal["s3", "mount", "local"]

    def list_objects(self, session_uri: str) -> list[ObjectRef]:
        """Metadata only — never transfers bytes."""
        ...

    def prepare(self, session_uri: str, refs: list[ObjectRef], dest_dir: Path) -> PreparedInput:
        """Make the session readable at a host path.

        ``s3`` downloads *refs* into *dest_dir*. ``local`` copies into
        *dest_dir*, or returns the source path unchanged when configured with
        ``copy_files: false``. ``mount`` returns the existing path unchanged,
        copying nothing, and ignores *dest_dir* entirely.

        *dest_dir* is chosen by the worker rather than derived here, because the
        directory's *name* is load-bearing: the processor reads the session's
        identity from it.
        """
        ...

    def release(self, prepared: PreparedInput) -> None:
        """Undo ``prepare``. Always called, including on failure — a job that
        dies must not leak disk. A no-op for ``mount`` and for a pass-through
        ``local`` store: a store that returns a path it does not own must be
        able to say "do not delete this"."""
        ...


@dataclass(frozen=True)
class PublishManifest:
    files: int
    bytes: int
    dest_uri: str
    sidecar_uri: str
    """Written last; its presence is the commit marker."""


class OutputStore(Protocol):
    name: Literal["s3", "local"]

    def exists(self, dest_uri: str) -> bool:
        """``True`` when a *complete* output is already present — i.e. the sidecar
        exists. Backs ``output.overwrite: false``, which marks such jobs ``skipped``."""
        ...

    def publish(self, src: Path, dest_uri: str) -> PublishManifest:
        """Upload ``src/**`` to *dest_uri*. Uploads ``output.metadata.json`` last."""
        ...

    def iter_completed(self, prefix: str) -> Iterator[tuple[str, dict]]:
        """Yield ``(session_name, parsed_sidecar)`` for every completed session under
        *prefix* — skips prefixes whose sidecar is missing (interrupted publishes,
        not completed sessions). This is what ``reconcile`` rebuilds the ledger from."""
        ...


def _uri_to_path(uri: str) -> Path:
    """Accept a ``file://`` URI, a plain POSIX path, or a plain Windows path.

    A bare ``Path(parsed.path)`` on ``file:///C:/Users/...`` would keep the
    leading slash before the drive letter, producing the invalid
    ``\\C:\\Users\\...`` rather than ``C:\\Users\\...`` — so that one leading
    slash is stripped whenever what follows it is a drive letter.
    """
    if _WINDOWS_DRIVE_RE.match(uri):
        return Path(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        path = unquote(parsed.path)
        stripped = path.lstrip("/")
        if _WINDOWS_DRIVE_RE.match(stripped):
            path = stripped
        return Path(path)
    if parsed.scheme == "":
        return Path(uri)
    raise StoreConfigError(f"Not a local path or file:// URI: {uri!r}")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)``, prefix always ending in ``/``."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise StoreConfigError(f"Not an s3:// URI: {uri!r}")
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return parsed.netloc, prefix


def get_input_store(name: str, **kwargs: Any) -> InputStore:
    """Construct the named :class:`InputStore`. Imports are lazy: ``mount``/``local``
    (stdlib only) work without the ``pipeline`` extra's ``boto3`` dependency."""
    if name == "s3":
        from .input_s3 import S3InputStore

        return S3InputStore(**kwargs)
    if name == "mount":
        from .input_mount import MountInputStore

        return MountInputStore(**kwargs)
    if name == "local":
        from .input_local import LocalInputStore

        return LocalInputStore(**kwargs)
    raise ValueError(f"Unknown input store {name!r}; available: 's3', 'mount', 'local'")


def get_output_store(name: str, **kwargs: Any) -> OutputStore:
    if name == "s3":
        from .output_s3 import S3OutputStore

        return S3OutputStore(**kwargs)
    if name == "local":
        from .output_local import LocalOutputStore

        return LocalOutputStore(**kwargs)
    raise ValueError(f"Unknown output store {name!r}; available: 's3', 'local'")
