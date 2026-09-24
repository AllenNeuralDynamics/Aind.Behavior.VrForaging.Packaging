"""Integration tests for full historical schema corpora declared in YAML."""

from pathlib import Path

import pytest

from aind_behavior_vr_foraging_packaging.schema_migrations import run_parquet_harness

from .model import SchemaCorpusEntry, load_manifest

_MANIFEST = load_manifest(Path(__file__).with_name("datasets.yml"))


@pytest.mark.integration
@pytest.mark.parametrize("entry", _MANIFEST.schema_corpora, ids=lambda entry: entry.id)
def test_external_schema_corpus(entry: SchemaCorpusEntry, cached_schema_corpora: dict[str, Path]):
    corpus = cached_schema_corpora.get(entry.id)
    if corpus is None:
        pytest.skip(f"Schema corpus {entry.id!r} was not available from public S3")

    result = run_parquet_harness(corpus, retain_rows=False)

    result.raise_for_failures()
    assert result.passed
