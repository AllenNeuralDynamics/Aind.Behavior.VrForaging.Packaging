"""Unit tests for the mount/local input and output stores (§10, §10b) — no network."""

import json
from pathlib import Path

import pytest

from processing_server.stores import StoreDataError
from processing_server.stores.input_local import LocalInputStore
from processing_server.stores.input_mount import MountInputStore
from processing_server.stores.output_local import LocalOutputStore


def _make_session(root, name: str = "sess") -> "Path":
    d = root / name
    (d / "behavior").mkdir(parents=True)
    (d / "data_description.json").write_text("{}")
    (d / "behavior" / "Block.json").write_text("{}")
    return d


class TestMountInputStore:
    def test_list_objects_enumerates_files(self, tmp_path):
        session = _make_session(tmp_path)
        store = MountInputStore()
        refs = store.list_objects(session.as_uri())
        keys = {r.key for r in refs}
        assert "data_description.json" in keys
        assert "behavior/Block.json" in keys

    def test_prepare_returns_path_unchanged_read_only(self, tmp_path):
        session = _make_session(tmp_path)
        store = MountInputStore()
        refs = store.list_objects(session.as_uri())
        prepared = store.prepare(session.as_uri(), refs, tmp_path / "job1")
        assert prepared.host_path == session
        assert prepared.read_only is True
        assert prepared.manifest.store == "mount"

    def test_prepare_raises_on_missing_required_file(self, tmp_path):
        d = tmp_path / "sess_no_desc"
        d.mkdir()
        (d / "behavior").mkdir()
        store = MountInputStore()
        refs = store.list_objects(d.as_uri())
        with pytest.raises(StoreDataError):
            store.prepare(d.as_uri(), refs, tmp_path / "job1")

    def test_list_objects_raises_on_missing_directory(self, tmp_path):
        store = MountInputStore()
        with pytest.raises(StoreDataError):
            store.list_objects((tmp_path / "does_not_exist").as_uri())

    def test_release_is_a_no_op(self, tmp_path):
        session = _make_session(tmp_path)
        store = MountInputStore()
        refs = store.list_objects(session.as_uri())
        prepared = store.prepare(session.as_uri(), refs, tmp_path / "job1")
        store.release(prepared)
        assert session.exists()  # nothing was deleted


class TestLocalInputStore:
    def test_pass_through_by_default(self, tmp_path):
        session = _make_session(tmp_path)
        store = LocalInputStore()  # copy_files=False
        refs = store.list_objects(session.as_uri())
        prepared = store.prepare(session.as_uri(), refs, tmp_path / "job1" / "in" / "sess_A")
        assert prepared.host_path == session
        store.release(prepared)
        assert session.exists()  # pass-through — never deleted

    def test_copy_files_true_copies_and_release_cleans_up(self, tmp_path):
        """Stages into the destination the worker chose, verbatim. The store does
        not append an ``in/`` of its own: the directory's *name* carries the
        session's identity into the container, so only the worker gets to pick it."""
        session = _make_session(tmp_path)
        store = LocalInputStore(copy_files=True)
        refs = store.list_objects(session.as_uri())
        dest = tmp_path / "job1" / "in" / "sess_A"
        prepared = store.prepare(session.as_uri(), refs, dest)
        assert prepared.host_path == dest
        assert (prepared.host_path / "data_description.json").exists()
        assert session.exists()  # original untouched

        store.release(prepared)
        assert not prepared.host_path.exists()  # the copy is gone
        assert session.exists()  # the original is not


class TestLocalOutputStore:
    def test_exists_false_before_publish(self, tmp_path):
        store = LocalOutputStore()
        assert store.exists(str(tmp_path / "dest")) is False

    def test_publish_writes_sidecar_last_and_exists_becomes_true(self, tmp_path):
        src = tmp_path / "out"
        src.mkdir()
        (src / "sites.parquet").write_bytes(b"x")
        (src / "output.metadata.json").write_text(json.dumps({"status": "ok"}))

        dest = tmp_path / "published"
        store = LocalOutputStore()
        manifest = store.publish(src, str(dest))

        assert manifest.files == 2
        assert (dest / "sites.parquet").exists()
        assert (dest / "output.metadata.json").exists()
        assert store.exists(str(dest)) is True

    def test_publish_without_sidecar_leaves_exists_false(self, tmp_path):
        """No sidecar in src (e.g. it was never written) — publish still moves the
        other files, but `exists()` (the commit marker) stays False (§10b)."""
        src = tmp_path / "out"
        src.mkdir()
        (src / "sites.parquet").write_bytes(b"x")
        dest = tmp_path / "published"

        store = LocalOutputStore()
        store.publish(src, str(dest))
        assert (dest / "sites.parquet").exists()
        assert store.exists(str(dest)) is False

    def test_iter_completed_skips_sessions_without_sidecar(self, tmp_path):
        root = tmp_path / "sessions"
        (root / "done").mkdir(parents=True)
        (root / "done" / "output.metadata.json").write_text(json.dumps({"session_name": "done"}))
        (root / "interrupted").mkdir(parents=True)
        (root / "interrupted" / "sites.parquet").write_bytes(b"x")  # no sidecar

        store = LocalOutputStore()
        completed = dict(store.iter_completed(str(root)))
        assert set(completed) == {"done"}

    def test_iter_completed_empty_for_missing_root(self, tmp_path):
        store = LocalOutputStore()
        assert list(store.iter_completed(str(tmp_path / "nope"))) == []
