"""Unit tests for DocDbSource against a mocked MetadataDbClient — no network."""

from unittest.mock import MagicMock

from processing_server.sources.docdb import DocDbSource


def _record(name, *, created, acquisition_start=None, session_start=None, project=None, subject=None, loc=None):
    doc = {
        "_id": f"id-{name}",
        "name": name,
        "created": created,
        "location": loc or f"s3://bucket/{name}",
    }
    if project is not None:
        doc["data_description"] = {"project_name": project}
    if acquisition_start is not None:
        doc["acquisition"] = {"acquisition_start_time": acquisition_start}
    if session_start is not None:
        doc["session"] = {"session_start_time": session_start}
    if subject is not None:
        doc["subject"] = {"subject_id": subject}
    return doc


def _make_client(pass_a_docs, pass_b_docs):
    """Mock returning pass_a_docs for the type-based query, pass_b_docs for the
    legacy-fallback query — distinguished by the presence of `project_name` in
    the filter (the two queries have structurally different shapes)."""
    client = MagicMock()

    def _retrieve(filter_query: dict | None = None, **kwargs):
        filter_query = filter_query or {}
        if "data_description.project_name" in filter_query:
            return pass_b_docs
        return pass_a_docs

    client.retrieve_docdb_records.side_effect = _retrieve
    return client


class TestPassA:
    def test_yields_typed_sessions(self):
        docs = [
            _record(
                "behavior_1_2025-01-01_00-00-00",
                created="2025-01-01T00:00:00Z",
                acquisition_start="2025-01-01T00:00:00Z",
                subject="1",
            )
        ]
        client = _make_client(docs, [])
        source = DocDbSource(client=client, legacy_project_name=None)
        refs = list(source.discover(None))
        assert len(refs) == 1
        assert refs[0].session_name == "behavior_1_2025-01-01_00-00-00"
        assert refs[0].discovered_by == "docdb:pass-a"
        assert refs[0].subject_id == "1"
        assert refs[0].input_uri == "s3://bucket/behavior_1_2025-01-01_00-00-00"

    def test_v1_session_start_time_used_when_no_acquisition(self):
        docs = [_record("s1", created="c1", session_start="2024-05-01T00:00:00Z")]
        client = _make_client(docs, [])
        source = DocDbSource(client=client, legacy_project_name=None)
        refs = list(source.discover(None))
        assert refs[0].session_start is not None
        assert refs[0].session_start.year == 2024

    def test_mixed_datetime_formats_both_parse(self):
        docs = [
            _record("s1", created="c1", acquisition_start="2024-08-26 16:24:17.031552+00:00"),
            _record("s2", created="c2", acquisition_start="2026-04-09T11:44:16Z"),
        ]
        client = _make_client(docs, [])
        source = DocDbSource(client=client, legacy_project_name=None)
        refs = list(source.discover(None))
        assert all(r.session_start is not None for r in refs)

    def test_missing_location_is_skipped_not_crashed(self):
        docs = [{"_id": "x", "name": "s1", "created": "c1"}]  # no `location`
        client = _make_client(docs, [])
        source = DocDbSource(client=client, legacy_project_name=None)
        assert list(source.discover(None)) == []

    def test_since_passed_through_as_gte_filter(self):
        client = _make_client([], [])
        source = DocDbSource(client=client, legacy_project_name=None)
        list(source.discover("2026-01-01T00:00:00Z"))
        call_kwargs = client.retrieve_docdb_records.call_args.kwargs
        assert call_kwargs["filter_query"]["created"] == {"$gte": "2026-01-01T00:00:00Z"}

    def test_query_filters_on_acquisition_type_or_session_type(self):
        client = _make_client([], [])
        source = DocDbSource(client=client, legacy_project_name=None, acquisition_types=["AindVrForaging"])
        list(source.discover(None))
        q = client.retrieve_docdb_records.call_args.kwargs["filter_query"]
        assert q["$or"] == [
            {"acquisition.acquisition_type": {"$in": ["AindVrForaging"]}},
            {"session.session_type": {"$in": ["AindVrForaging"]}},
        ]
        assert q["data_description.data_level"] == "raw"


class TestPassB:
    def test_legacy_session_before_cutoff_is_kept(self):
        docs_b = [
            _record(
                "legacy_1",
                created="c1",
                acquisition_start="2024-07-15T00:00:00Z",
                project="Cognitive flexibility in patch foraging",
            )
        ]
        client = _make_client([], docs_b)
        source = DocDbSource(
            client=client,
            legacy_project_name="Cognitive flexibility in patch foraging",
            legacy_session_before="2026-01-01",
        )
        refs = list(source.discover(None))
        assert len(refs) == 1
        assert refs[0].discovered_by == "docdb:pass-b"

    def test_session_on_or_after_cutoff_is_dropped(self):
        """A Pass-B match at/after the cutoff signals a metadata bug upstream,
        not a legacy session — it must not be silently absorbed."""
        docs_b = [
            _record(
                "recent",
                created="c1",
                acquisition_start="2026-03-01T00:00:00Z",
                project="Cognitive flexibility in patch foraging",
            )
        ]
        client = _make_client([], docs_b)
        source = DocDbSource(
            client=client,
            legacy_project_name="Cognitive flexibility in patch foraging",
            legacy_session_before="2026-01-01",
        )
        assert list(source.discover(None)) == []

    def test_disabled_when_legacy_project_name_is_none(self):
        docs_b = [_record("legacy_1", created="c1", acquisition_start="2024-07-15T00:00:00Z")]
        client = _make_client([], docs_b)
        source = DocDbSource(client=client, legacy_project_name=None)
        assert list(source.discover(None)) == []

    def test_pass_b_query_shape(self):
        client = _make_client([], [])
        source = DocDbSource(client=client, legacy_project_name="Cognitive flexibility in patch foraging")
        list(source.discover(None))
        # second call is Pass B (first call is Pass A)
        q = client.retrieve_docdb_records.call_args_list[1].kwargs["filter_query"]
        assert q["data_description.project_name"] == "Cognitive flexibility in patch foraging"
        assert q["acquisition.acquisition_type"] == {"$exists": False}
        assert q["session.session_type"] == {"$exists": False}


class TestBothPasses:
    def test_pass_a_and_pass_b_both_yielded(self):
        docs_a = [_record("typed_1", created="c1", acquisition_start="2025-01-01T00:00:00Z")]
        docs_b = [
            _record(
                "legacy_1",
                created="c2",
                acquisition_start="2024-07-15T00:00:00Z",
                project="Cognitive flexibility in patch foraging",
            )
        ]
        client = _make_client(docs_a, docs_b)
        source = DocDbSource(client=client, legacy_project_name="Cognitive flexibility in patch foraging")
        refs = list(source.discover(None))
        assert {r.session_name for r in refs} == {"typed_1", "legacy_1"}
        assert {r.discovered_by for r in refs} == {"docdb:pass-a", "docdb:pass-b"}
