"""Discovery — answers *which* sessions exist and where their bytes live (§3).

Fetching the bytes is a store's job (§10, ``pipeline.stores``), not a source's;
the split matters because DocDB indexes assets in buckets the pipeline may read
by an entirely different route.
"""

from collections.abc import Iterator
from typing import Any, Literal, Protocol

from ..models import SessionRef


class Source(Protocol):
    """One discovery backend. Two is enough (§3) — an S3 prefix-listing source was
    considered and dropped, since DocDB already indexes everything we care about."""

    name: Literal["docdb", "local"]

    def discover(self, since: str | None) -> Iterator[SessionRef]:
        """Yield sessions whose cursor is >= *since*, oldest first.

        *since* is this source's persisted watermark (``ledger.get_watermark``),
        or ``None`` for a first full sweep. Comparison is inclusive (``$gte``) so
        a boundary timestamp is re-delivered rather than skipped — the ``job_key``
        uniqueness constraint (§6) absorbs the resulting duplicate.
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
    raise ValueError(f"Unknown source {name!r}; available: 'docdb', 'local'")
