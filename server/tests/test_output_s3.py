"""Unit tests for the ``s3`` output store against an in-memory fake client.

The S3 path is the one that cannot be exercised by a local run, and its bugs live in
two places a local store never visits: key arithmetic (``key[len(prefix):]``) and
write ordering (the commit marker must land last, and be removed first). Both are
cheap to pin down with a fake and expensive to discover in a campaign.
"""

import io
import json
from pathlib import Path
from typing import cast

import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from processing_server.stores import SIDECAR_NAME
from processing_server.stores.output_s3 import S3OutputStore

BUCKET = "b"


class _FakeS3:
    """Just the surface the store calls, plus an event log to assert ordering."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.events: list[tuple[str, str]] = []

    # -- paginator -------------------------------------------------------
    def get_paginator(self, name: str) -> "_FakeS3":
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket: str, Prefix: str = "", Delimiter: str | None = None):
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
        if Delimiter is None:
            yield {"Contents": [{"Key": k} for k in keys]}
            return
        contents: list[dict] = []
        prefixes: set[str] = set()
        for k in keys:
            rest = k[len(Prefix) :]
            if Delimiter in rest:
                prefixes.add(Prefix + rest.split(Delimiter)[0] + Delimiter)
            else:
                contents.append({"Key": k})
        yield {"Contents": contents, "CommonPrefixes": [{"Prefix": p} for p in sorted(prefixes)]}

    # -- objects ---------------------------------------------------------
    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        self.objects[(Bucket, Key)] = Path(Filename).read_bytes()
        self.events.append(("put", Key))

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body
        self.events.append(("put", Key))

    def copy_object(self, Bucket: str, Key: str, CopySource: dict) -> None:
        self.objects[(Bucket, Key)] = self.objects[(CopySource["Bucket"], CopySource["Key"])]
        self.events.append(("copy", Key))

    def delete_objects(self, Bucket: str, Delete: dict) -> None:
        for obj in Delete["Objects"]:
            self.objects.pop((Bucket, obj["Key"]), None)
            self.events.append(("del", obj["Key"]))

    def head_object(self, Bucket: str, Key: str) -> dict:
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    def get_object(self, Bucket: str, Key: str) -> dict:
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


@pytest.fixture
def store_and_client():
    client = _FakeS3()
    # The fake is duck-typed to the handful of calls the store makes, not to BaseClient.
    return S3OutputStore(client=cast(BaseClient, client)), client


def _src(tmp_path: Path, name: str, files: dict[str, bytes], *, sidecar: dict | None = None) -> Path:
    d = tmp_path / name
    d.mkdir(parents=True, exist_ok=True)
    for rel, data in files.items():
        target = d / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    if sidecar is not None:
        (d / SIDECAR_NAME).write_text(json.dumps(sidecar))
    return d


class TestPublish:
    def test_marker_lands_last(self, tmp_path, store_and_client):
        store, client = store_and_client
        src = _src(tmp_path, "s", {"a.parquet": b"a", "n/b.parquet": b"b"}, sidecar={"status": "ok"})
        store.publish(src, f"s3://{BUCKET}/out/sess/")
        puts = [k for kind, k in client.events if kind == "put"]
        assert puts[-1].endswith(SIDECAR_NAME), "the commit marker must be the final write"
        assert len(puts) == 3

    def test_marker_is_removed_before_data_is_replaced(self, tmp_path, store_and_client):
        store, client = store_and_client
        dest = f"s3://{BUCKET}/out/sess/"
        store.publish(_src(tmp_path, "one", {"a.parquet": b"a"}, sidecar={"n": 1}), dest)

        client.events.clear()
        store.publish(_src(tmp_path, "two", {"a.parquet": b"a2"}, sidecar={"n": 2}), dest)
        kinds = [(kind, k) for kind, k in client.events if k.endswith(SIDECAR_NAME) or k.endswith("a.parquet")]
        assert kinds[0] == ("del", f"out/sess/{SIDECAR_NAME}"), "uncommit must precede the data write"
        assert kinds[-1] == ("put", f"out/sess/{SIDECAR_NAME}")

    def test_stale_keys_are_removed(self, tmp_path, store_and_client):
        """A rerun producing fewer files must not leave the old ones to be read forever."""
        store, client = store_and_client
        dest = f"s3://{BUCKET}/out/sess/"
        store.publish(_src(tmp_path, "one", {"a.parquet": b"a", "gone.parquet": b"g"}, sidecar={"n": 1}), dest)
        store.publish(_src(tmp_path, "two", {"a.parquet": b"a2"}, sidecar={"n": 2}), dest)
        assert (BUCKET, "out/sess/gone.parquet") not in client.objects
        assert client.objects[(BUCKET, "out/sess/a.parquet")] == b"a2"

    def test_untouched_neighbours_survive(self, tmp_path, store_and_client):
        """Pruning is scoped to the prefix being published, not the release."""
        store, client = store_and_client
        client.objects[(BUCKET, "out/other/keep.parquet")] = b"k"
        store.publish(_src(tmp_path, "one", {"a.parquet": b"a"}, sidecar={"n": 1}), f"s3://{BUCKET}/out/sess/")
        assert (BUCKET, "out/other/keep.parquet") in client.objects


class TestReadSide:
    def test_write_object_then_read_object_round_trips(self, store_and_client):
        """The pair aggregation runs on: parquet in and out of the store with nothing
        landing on local disk in between."""
        store, _ = store_and_client
        assert store.write_object(f"s3://{BUCKET}/agg/2026-08-18/sites.parquet", b"aaa") == 3
        assert store.read_object(f"s3://{BUCKET}/agg/2026-08-18/sites.parquet") == b"aaa"

    def test_write_object_uses_the_key_verbatim(self, store_and_client):
        """`parse_s3_object_uri`, not `parse_s3_uri`: the latter appends `/` to normalise
        a prefix, which turns a key into a directory that nothing can read back."""
        store, client = store_and_client
        store.write_object(f"s3://{BUCKET}/agg/latest/{SIDECAR_NAME}", b"{}")
        assert (BUCKET, f"agg/latest/{SIDECAR_NAME}") in client.objects

    def test_read_object_reads_one_key(self, store_and_client):
        """A key is not a prefix: `parse_s3_uri` would append `/` and match nothing,
        which is what silently broke reading the live aggregate's manifest."""
        store, client = store_and_client
        client.objects[(BUCKET, f"out/agg/{SIDECAR_NAME}")] = b'{"watermark": "w"}'
        raw = store.read_object(f"s3://{BUCKET}/out/agg/{SIDECAR_NAME}")
        assert raw is not None and json.loads(raw)["watermark"] == "w"

    def test_read_object_of_a_missing_key_is_none(self, store_and_client):
        store, _ = store_and_client
        assert store.read_object(f"s3://{BUCKET}/out/agg/{SIDECAR_NAME}") is None

    def test_delete_prefix_is_scoped_and_counted(self, tmp_path, store_and_client):
        store, client = store_and_client
        client.objects[(BUCKET, "out/sess/a")] = b"a"
        client.objects[(BUCKET, "out/sess/b")] = b"b"
        client.objects[(BUCKET, "out/keep/c")] = b"c"
        assert store.delete_prefix(f"s3://{BUCKET}/out/sess/") == 2
        assert list(client.objects) == [(BUCKET, "out/keep/c")]

    def test_delete_prefix_of_nothing_is_zero(self, store_and_client):
        store, _ = store_and_client
        assert store.delete_prefix(f"s3://{BUCKET}/nope/") == 0

    def test_list_children_returns_immediate_dirs_only(self, store_and_client):
        store, client = store_and_client
        client.objects[(BUCKET, "hist/2026-08-01/sites.parquet")] = b"x"
        client.objects[(BUCKET, "hist/2026-08-02/sites.parquet")] = b"x"
        client.objects[(BUCKET, "hist/2026-08-02/n/deep.parquet")] = b"x"
        client.objects[(BUCKET, "hist/loose.txt")] = b"x"
        assert store.list_children(f"s3://{BUCKET}/hist/") == ["2026-08-01", "2026-08-02"]


class TestCopyPrefix:
    def test_copies_every_key_with_marker_last(self, tmp_path, store_and_client):
        store, client = store_and_client
        store.publish(
            _src(tmp_path, "s", {"a.parquet": b"a", "n/b.parquet": b"b"}, sidecar={"n": 1}), f"s3://{BUCKET}/agg/"
        )
        client.events.clear()
        n = store.copy_prefix(f"s3://{BUCKET}/agg/", f"s3://{BUCKET}/hist/2026-08-17/")
        assert n == 3
        assert client.objects[(BUCKET, "hist/2026-08-17/n/b.parquet")] == b"b"
        copies = [k for kind, k in client.events if kind == "copy"]
        assert copies[-1].endswith(SIDECAR_NAME), "an archived aggregate must also commit last"

    def test_exists_follows_the_marker(self, tmp_path, store_and_client):
        store, client = store_and_client
        assert store.exists(f"s3://{BUCKET}/agg/") is False
        store.publish(_src(tmp_path, "s", {"a.parquet": b"a"}, sidecar={"n": 1}), f"s3://{BUCKET}/agg/")
        assert store.exists(f"s3://{BUCKET}/agg/") is True

    def test_iter_completed_skips_a_prefix_without_a_marker(self, tmp_path, store_and_client):
        store, client = store_and_client
        client.objects[(BUCKET, "out/sessions/a/sites.parquet")] = b"x"
        client.objects[(BUCKET, "out/sessions/a/" + SIDECAR_NAME)] = b'{"status": "ok"}'
        client.objects[(BUCKET, "out/sessions/b/sites.parquet")] = b"x"  # interrupted publish
        assert [name for name, _ in store.iter_completed(f"s3://{BUCKET}/out/sessions/")] == ["a"]


class TestAggregatingAgainstS3:
    """The worker's whole aggregation path over the ``s3`` store.

    Everything else here tests the store in isolation. This is the only place the key
    arithmetic, the write ordering and the worker's prefix construction are exercised
    *together* against S3 semantics — and a real bucket is not available to any test.
    """

    def _worker(self, tmp_path, store):
        from processing_server.config import PipelineConfig
        from processing_server.worker import Worker

        config = PipelineConfig(
            release="rel1",
            output={"store": "s3", "uri": f"s3://{BUCKET}/out"},
            input={"store": "local"},
            worker={"ledger": str(tmp_path / "jobs.sqlite")},
            processor={"allow_unpinned": True},
            logging={"dir": str(tmp_path / "logs")},
        )
        return Worker(config, worker_id="w1", work_dir=tmp_path / "work", output_store=store)

    def _publish_session(self, tmp_path, store, name: str, job_id: str) -> None:
        import pandas as pd

        d = tmp_path / "src" / name
        d.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"session_id": [name], "subject_id": ["s1"]}).to_parquet(d / "session.parquet")
        pd.DataFrame({"session_id": [name] * 3, "site_index": [0, 1, 2]}).to_parquet(d / "sites.parquet")
        (d / SIDECAR_NAME).write_text(json.dumps({"session_name": name, "status": "ok", "job_id": job_id}))
        store.publish(d, f"s3://{BUCKET}/out/rel1/sessions/{name}/")

    def test_a_full_run_writes_a_dated_prefix_and_a_latest_mirror(self, tmp_path, store_and_client):
        store, client = store_and_client
        worker = self._worker(tmp_path, store)
        try:
            self._publish_session(tmp_path, store, "sess_A", "j1")
            self._publish_session(tmp_path, store, "sess_B", "j2")

            job_id, watermark, n = worker.enqueue_aggregate()
            assert job_id is not None and n == 2
            claimed = worker.ledger.force_claim(job_id, "w1", 600)
            assert claimed is not None
            client.events.clear()
            worker.process_aggregate_job(claimed)
            done = worker.ledger.get_job(job_id)
            assert done is not None and done.status == "completed", done.error

            days = worker.aggregate_days()
            assert len(days) == 1, f"expected one dated prefix, got {days}"
            day = days[0]
            for prefix in (f"out/rel1/aggregate/{day}", "out/rel1/aggregate/latest"):
                for leaf in ("session.parquet", "sites.parquet", SIDECAR_NAME):
                    assert (BUCKET, f"{prefix}/{leaf}") in client.objects, f"missing {prefix}/{leaf}"

            manifest = json.loads(client.objects[(BUCKET, f"out/rel1/aggregate/latest/{SIDECAR_NAME}")])
            assert manifest["watermark"] == watermark
            assert sorted(manifest["sessions"]) == ["sess_A", "sess_B"]
            assert manifest["tables"] == {"session": 2, "sites": 6}
        finally:
            worker.close()

    def test_the_dated_marker_is_put_after_every_table(self, tmp_path, store_and_client):
        """`write_object` is a bare `put_object`; only the worker orders these."""
        store, client = store_and_client
        worker = self._worker(tmp_path, store)
        try:
            self._publish_session(tmp_path, store, "sess_A", "j1")
            job_id, _, _ = worker.enqueue_aggregate()
            assert job_id is not None
            claimed = worker.ledger.force_claim(job_id, "w1", 600)
            assert claimed is not None
            client.events.clear()
            worker.process_aggregate_job(claimed)

            puts = [k for kind, k in client.events if kind == "put"]
            assert puts, "nothing was put"
            assert puts[-1].endswith(SIDECAR_NAME), f"the marker was not put last: {puts}"
            copies = [k for kind, k in client.events if kind == "copy"]
            assert copies and copies[-1].endswith(SIDECAR_NAME), f"the mirror did not commit last: {copies}"
            assert all("latest" not in k for k in puts), (
                "`latest` should be a server-side copy of the dated prefix, not re-uploaded"
            )
        finally:
            worker.close()

    def test_the_tables_are_never_downloaded(self, tmp_path, store_and_client):
        """Streaming means `get_object`, not `download_file` onto the work volume."""
        store, client = store_and_client
        worker = self._worker(tmp_path, store)
        try:
            self._publish_session(tmp_path, store, "sess_A", "j1")
            job_id, _, _ = worker.enqueue_aggregate()
            assert job_id is not None
            claimed = worker.ledger.force_claim(job_id, "w1", 600)
            assert claimed is not None
            worker.process_aggregate_job(claimed)
            assert not (tmp_path / "work").exists(), "aggregation created a work dir"
        finally:
            worker.close()
