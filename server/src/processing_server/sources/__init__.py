"""Discovery — answers *which* sessions exist and where their bytes live.

Fetching the bytes is a store's job (``pipeline.stores``), not a source's;
the split matters because DocDB indexes assets in buckets the pipeline may read
by an entirely different route.
"""

from collections.abc import Iterator
from typing import Any, Literal, Protocol

from ..models import SessionRef


class Source(Protocol):
    """One discovery backend. An S3 prefix-listing source was considered and dropped,
    since DocDB already indexes everything we care about — but a *manifest* is not a
    discovery shortcut, it is a different question: "process exactly this set", asked
    when the set has already been decided and has to stay decided."""

    name: Literal["docdb", "local", "manifest"]

    def discover(self, since: str | None) -> Iterator[SessionRef]:
        """Yield sessions whose cursor is >= *since*, oldest first.

        *since* is this source's persisted watermark (``ledger.get_watermark``),
        or ``None`` for a first full sweep. Comparison is inclusive (``$gte``) so
        a boundary timestamp is re-delivered rather than skipped — the ``job_key``
        uniqueness constraint absorbs the resulting duplicate.

        A source over a finite, static set may ignore *since* entirely and yield its
        whole set every sweep; see :class:`~.manifest.ManifestSource`.
        """
        ...


def get_source(name: str, **kwargs: Any) -> Source:
    """Construct the named :class:`Source` implementation.

    Imports are lazy so that, e.g., ``LocalSource`` (pure stdlib) works without
    the ``pipeline`` extra's ``aind-data-access-api`` dependency installed.
    """
    if name == "docdb":
        from .docdb import DocDbSource

        return DocDbSource(**kwargs)
    if name == "local":
        from .local import LocalSource

        return LocalSource(**kwargs)
    if name == "manifest":
        from .manifest import ManifestSource

        return ManifestSource(**kwargs)
    raise ValueError(f"Unknown source {name!r}; available: 'docdb', 'local', 'manifest'")
