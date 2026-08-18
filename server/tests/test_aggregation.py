"""Unit tests for continuous aggregation — watermark, dated prefixes, the `latest` mirror.

No network and no real aggregation: `aggregate` itself belongs to the packaging
library and is tested there. What matters here is everything around it — which runs
happen at all, and what survives one that fails.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from processing_server.config import PipelineConfig
from processing_server.models import Job
from processing_server.sidecar import SIDECAR_NAME, aggregate_watermark, session_token
from processing_server.stores import StoreTransientError
from processing_server.stores.output_local import LocalOutputStore
from processing_server.worker import Worker


def _config(tmp_path, **overrides) -> PipelineConfig:
    data = {
        "release": "rel1",
        "output": {"store": "local", "uri": str(tmp_path / "out")},
        "input": {"store": "local"},
        "worker": {"ledger": str(tmp_path / "jobs.sqlite")},
        "processor": {"allow_unpinned": True},
        "logging": {"dir": str(tmp_path / "logs")},
    }
    data.update(overrides)
    return PipelineConfig(**data)


def _make_worker(tmp_path, config=None, output_store=None) -> Worker:
    config = config or _config(tmp_path)
    return Worker(
        config,
        worker_id="w1",
        work_dir=tmp_path / "work",
        output_store=output_store or LocalOutputStore(),
    )


def _publish_session(worker: Worker, name: str, *, job_id: str, status: str = "ok", real_parquet: bool = True) -> None:
    """Put a completed session on the output store, as the session path would.

    Real parquet by default: `aggregate_tables` returns nothing when it finds nothing
    joinable, so a placeholder byte string would exercise that path rather than the
    aggregation these tests are about.
    """
    root = Path(worker.config.output.uri) / worker.config.release / "sessions" / name
    root.mkdir(parents=True, exist_ok=True)
    if real_parquet:
        import pandas as pd

        pd.DataFrame({"session_id": [name], "subject_id": ["s1"]}).to_parquet(root / "session.parquet")
        pd.DataFrame({"session_id": [name] * 3, "site_index": [0, 1, 2]}).to_parquet(root / "sites.parquet")
    else:
        (root / "sites.parquet").write_bytes(b"not-parquet")
    (root / SIDECAR_NAME).write_text(
        json.dumps({"session_name": name, "status": status, "job_id": job_id, "finished_at": "2026-01-01T00:00:00Z"})
    )


def _aggregate_root(worker: Worker) -> Path:
    return Path(worker.config.output.uri) / worker.config.release / "aggregate"


def _latest_dir(worker: Worker) -> Path:
    return _aggregate_root(worker) / "latest"


def _aggregate_now(worker: Worker, *, force: bool = False) -> tuple["Job", str]:
    """Queue, claim and run one aggregate job; return its final row and the watermark."""
    job_id, watermark, _ = worker.enqueue_aggregate(force=force)
    assert job_id is not None, "expected a job to be queued"
    claimed = worker.ledger.force_claim(job_id, "w1", 600)
    assert claimed is not None
    worker.process_aggregate_job(claimed)
    done = worker.ledger.get_job(job_id)
    assert done is not None
    return done, watermark


class TestWatermark:
    """The digest has to notice a recompute, which is what a count cannot do."""

    def test_token_prefers_job_id(self):
        assert session_token({"job_id": "j1", "finished_at": "t"}) == "j1"

    def test_token_falls_back_to_finished_at(self):
        assert session_token({"finished_at": "2026-01-01T00:00:00Z"}) == "2026-01-01T00:00:00Z"

    def test_token_never_raises_on_a_sidecar_with_neither(self):
        assert session_token({}) == "unversioned"

    def test_order_does_not_matter(self):
        assert aggregate_watermark({"a": "1", "b": "2"}) == aggregate_watermark({"b": "2", "a": "1"})

    def test_adding_a_session_changes_it(self):
        assert aggregate_watermark({"a": "1"}) != aggregate_watermark({"a": "1", "b": "2"})

    def test_removing_a_session_changes_it(self):
        assert aggregate_watermark({"a": "1", "b": "2"}) != aggregate_watermark({"a": "1"})

    def test_recomputing_a_session_changes_it(self):
        """The whole reason this is a digest and not a count."""
        assert aggregate_watermark({"a": "job1"}) != aggregate_watermark({"a": "job2"})


class TestSchedule:
    """Once a day at a wall-clock time, with catch-up and no double-runs."""

    TZ = ZoneInfo("America/Los_Angeles")

    def _at(self, y, m, d, hh, mm=0):
        return datetime(y, m, d, hh, mm, tzinfo=self.TZ)

    def test_not_due_before_the_scheduled_hour(self, tmp_path):
        worker = _make_worker(tmp_path)
        assert worker.aggregation_due(self._at(2026, 8, 17, 2, 59)) is False
        worker.close()

    def test_due_once_the_hour_has_passed(self, tmp_path):
        worker = _make_worker(tmp_path)
        assert worker.aggregation_due(self._at(2026, 8, 17, 3, 1)) is True
        worker.close()

    def test_not_due_again_after_todays_run(self, tmp_path):
        """The guard is the ledger, so this holds across a worker restart too."""
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        worker.enqueue_aggregate()
        assert worker.aggregation_due(datetime.now(timezone.utc)) is False
        worker.close()

    def test_due_again_the_next_day(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        worker.enqueue_aggregate()
        tomorrow = datetime.now(self.TZ) + timedelta(days=1)
        assert worker.aggregation_due(tomorrow.replace(hour=4, minute=0)) is True
        worker.close()

    def test_a_worker_that_was_down_at_the_hour_catches_up(self, tmp_path):
        """Skipping the day would leave the aggregate stale until tomorrow."""
        worker = _make_worker(tmp_path)
        assert worker.aggregation_due(self._at(2026, 8, 17, 11, 30)) is True
        worker.close()

    def test_disabled_is_never_due(self, tmp_path):
        worker = _make_worker(tmp_path, _config(tmp_path, aggregation={"enabled": False}))
        assert worker.aggregation_due(self._at(2026, 8, 17, 3, 1)) is False
        worker.close()

    def test_a_utc_now_is_converted_into_the_configured_zone(self, tmp_path):
        """10:00 UTC is 03:00 in Los Angeles (PDT) — due there, not yet due in UTC."""
        worker = _make_worker(tmp_path, _config(tmp_path, aggregation={"timezone": "America/Los_Angeles"}))
        assert worker.aggregation_due(datetime(2026, 8, 17, 10, 5, tzinfo=timezone.utc)) is True
        utc_worker = _make_worker(tmp_path, _config(tmp_path, aggregation={"timezone": "UTC"}))
        assert utc_worker.aggregation_due(datetime(2026, 8, 17, 2, 5, tzinfo=timezone.utc)) is False
        worker.close()
        utc_worker.close()

    @pytest.mark.parametrize("bad", ["3am", "25:00", "03:60", "0300", ""])
    def test_a_malformed_time_fails_at_config_load(self, tmp_path, bad):
        with pytest.raises(ValidationError):
            _config(tmp_path, aggregation={"at": bad})

    def test_an_unknown_timezone_fails_at_config_load(self, tmp_path):
        """Better than discovering it at 3am in a container with no tz database."""
        with pytest.raises(ValidationError):
            _config(tmp_path, aggregation={"timezone": "Mars/Olympus_Mons"})


class TestEnqueue:
    def test_nothing_published_queues_nothing(self, tmp_path):
        worker = _make_worker(tmp_path)
        job_id, _, n = worker.enqueue_aggregate()
        assert (job_id, n) == (None, 0)
        worker.close()

    def test_first_run_queues(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        job_id, _, n = worker.enqueue_aggregate()
        assert job_id is not None and n == 1
        worker.close()

    def test_unchanged_set_is_a_noop(self, tmp_path):
        """Dedupe is the ledger's own job_key uniqueness, not a second mechanism."""
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        assert worker.enqueue_aggregate()[0] is not None
        assert worker.enqueue_aggregate()[0] is None
        worker.close()

    def test_a_recompute_queues_again(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        assert worker.enqueue_aggregate()[0] is not None
        _publish_session(worker, "sess_A", job_id="j2")  # same session, new run
        assert worker.enqueue_aggregate()[0] is not None
        worker.close()

    def test_force_queues_even_when_unchanged(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        worker.enqueue_aggregate()
        assert worker.enqueue_aggregate()[0] is None
        assert worker.enqueue_aggregate(force=True)[0] is not None
        worker.close()

    def test_failed_sessions_are_not_contributors(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1", status="error")
        assert worker.contributing_sessions() == {}
        worker.close()

    def test_queued_job_is_kind_aggregate(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        job_id, _, _ = worker.enqueue_aggregate()
        assert job_id is not None
        job = worker.ledger.get_job(job_id)
        assert job is not None and job.kind == "aggregate"
        worker.close()


class TestPublishIsCommittal:
    """`publish` replaces a destination rather than writing over it."""

    def test_stale_files_are_removed(self, tmp_path):
        store = LocalOutputStore()
        dest = tmp_path / "dest"
        first = tmp_path / "first"
        first.mkdir()
        (first / "a.parquet").write_bytes(b"a")
        (first / "gone.parquet").write_bytes(b"g")
        (first / SIDECAR_NAME).write_text("{}")
        store.publish(first, str(dest))

        second = tmp_path / "second"
        second.mkdir()
        (second / "a.parquet").write_bytes(b"a2")
        (second / SIDECAR_NAME).write_text("{}")
        store.publish(second, str(dest))

        assert not (dest / "gone.parquet").exists(), "a shrinking output left an orphan behind"
        assert (dest / "a.parquet").read_bytes() == b"a2"

    def test_marker_is_absent_while_data_is_being_replaced(self, tmp_path, monkeypatch):
        """A reader mid-republish must not find an old marker vouching for new data."""
        import processing_server.stores.output_local as mod

        seen: list[bool] = []
        dest = tmp_path / "dest"
        store = LocalOutputStore()
        first = tmp_path / "first"
        first.mkdir()
        (first / "a.parquet").write_bytes(b"a")
        (first / SIDECAR_NAME).write_text("{}")
        store.publish(first, str(dest))
        assert (dest / SIDECAR_NAME).exists()

        # Sample the destination marker on every data-file write of the *second*
        # publish, i.e. after the uncommit and before the new marker lands.
        real = mod._atomic_copy

        def spy(src, target):
            if target.name != SIDECAR_NAME:
                seen.append((dest / SIDECAR_NAME).exists())
            return real(src, target)

        monkeypatch.setattr(mod, "_atomic_copy", spy)
        second = tmp_path / "second"
        second.mkdir()
        (second / "a.parquet").write_bytes(b"a2")
        (second / SIDECAR_NAME).write_text("{}")
        store.publish(second, str(dest))
        assert seen, "the spy never fired — the test would pass vacuously"
        assert not any(seen), "the previous marker survived into the data-write window"


class TestLocalStoreReadSide:
    def test_write_object_round_trips(self, tmp_path):
        store = LocalOutputStore()
        uri = str(tmp_path / "nested" / "deeper" / "a.parquet")
        assert store.write_object(uri, b"aaa") == 3, "byte count is what the ledger records"
        assert store.read_object(uri) == b"aaa", "parents were not created"

    def test_write_object_replaces(self, tmp_path):
        store = LocalOutputStore()
        uri = str(tmp_path / "a.parquet")
        store.write_object(uri, b"first")
        store.write_object(uri, b"second")
        assert store.read_object(uri) == b"second"

    def test_delete_prefix_is_idempotent(self, tmp_path):
        store = LocalOutputStore()
        d = tmp_path / "d"
        d.mkdir()
        (d / "x").write_bytes(b"x")
        assert store.delete_prefix(str(d)) == 1
        assert store.delete_prefix(str(d)) == 0

    def test_list_children_only_lists_dirs(self, tmp_path):
        root = tmp_path / "root"
        (root / "2026-08-01").mkdir(parents=True)
        (root / "2026-08-02").mkdir()
        (root / "loose.txt").write_text("x")
        assert LocalOutputStore().list_children(str(root)) == ["2026-08-01", "2026-08-02"]


class _MarkerWriteFails(LocalOutputStore):
    def write_object(self, uri, payload):
        if uri.endswith(SIDECAR_NAME):
            raise StoreTransientError("boom")
        return super().write_object(uri, payload)


class TestFailureLeavesTheAggregateReadable:
    def test_a_failure_before_the_marker_spares_latest(self, tmp_path):
        """Why the dated prefix is written first and `latest` replaced only afterwards:
        a run that dies partway leaves `latest` exactly as it was."""
        worker = _make_worker(tmp_path, output_store=_MarkerWriteFails())
        latest = _latest_dir(worker)
        latest.mkdir(parents=True)
        (latest / "sites.parquet").write_bytes(b"good")
        (latest / SIDECAR_NAME).write_text(json.dumps({"watermark": "old", "created_at": "2026-08-01T00:00:00Z"}))

        _publish_session(worker, "sess_A", job_id="j1")
        job, _ = _aggregate_now(worker)

        assert job.status in {"pending", "retrying", "failed"}
        assert (latest / "sites.parquet").read_bytes() == b"good", "a failed run destroyed the readable aggregate"
        assert json.loads((latest / SIDECAR_NAME).read_text())["watermark"] == "old"
        worker.close()

    def test_the_incomplete_dated_prefix_has_no_marker(self, tmp_path):
        """And the half-written dated copy is not mistakable for a finished one."""
        worker = _make_worker(tmp_path, output_store=_MarkerWriteFails())
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)

        days = worker.aggregate_days()
        assert days, "the tables were never written — the test proves nothing"
        torn = _aggregate_root(worker) / days[0]
        assert (torn / "session.parquet").exists(), "expected the data-write window to be reached"
        assert not (torn / SIDECAR_NAME).exists(), "an incomplete aggregate carries a completion marker"
        worker.close()


class TestEmptyAggregateIsNotPublished:
    def test_nothing_joinable_fails_the_job_and_spares_the_readable_aggregate(self, tmp_path):
        """`aggregate_tables` returns nothing rather than raising. Publishing that would
        replace a good aggregate with an empty one and record the job as a success."""
        worker = _make_worker(tmp_path)
        latest = _latest_dir(worker)
        latest.mkdir(parents=True)
        (latest / "session.parquet").write_bytes(b"good")
        (latest / SIDECAR_NAME).write_text(json.dumps({"watermark": "old", "created_at": "2026-08-01T00:00:00Z"}))

        _publish_session(worker, "sess_A", job_id="j1", real_parquet=False)
        job, _ = _aggregate_now(worker)

        assert job.status != "completed"
        assert "no tables" in (job.error or "")
        assert (latest / "session.parquet").read_bytes() == b"good"
        assert worker.aggregate_days() == [], "an empty aggregate was written to a dated prefix"
        worker.close()


class TestSuccessfulRun:
    def test_writes_a_dated_aggregate_and_mirrors_it(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        _publish_session(worker, "sess_B", job_id="j2")
        job, watermark = _aggregate_now(worker)

        assert job.status == "completed"
        days = worker.aggregate_days()
        assert len(days) == 1
        dated = _aggregate_root(worker) / days[0]
        latest = _latest_dir(worker)

        for root in (dated, latest):
            assert (root / "session.parquet").exists() and (root / "sites.parquet").exists()
            manifest = json.loads((root / SIDECAR_NAME).read_text())
            assert manifest["watermark"] == watermark
            assert manifest["kind"] == "aggregate"
            assert sorted(manifest["sessions"]) == ["sess_A", "sess_B"]
            assert manifest["tables"]["sites"] == 6  # 3 rows x 2 sessions

        assert (latest / "session.parquet").read_bytes() == (dated / "session.parquet").read_bytes(), (
            "`latest` is meant to be a full copy of the dated aggregate, not a rebuild"
        )
        worker.close()

    def test_the_dated_prefix_is_named_for_the_manifests_own_day(self, tmp_path):
        """Not for `now`: a run that straddles midnight must write, mirror and log one
        day, not two."""
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)
        day = worker.aggregate_days()[0]
        assert json.loads((_latest_dir(worker) / SIDECAR_NAME).read_text())["created_at"][:10] == day
        worker.close()

    def test_nothing_is_staged_on_the_work_volume(self, tmp_path):
        """The point of streaming: no session is ever landed on disk."""
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)
        work = tmp_path / "work"
        assert not work.exists() or list(work.iterdir()) == [], f"aggregation left {work} populated"
        worker.close()


class TestDatedAggregatesAreImmutable:
    def test_a_new_day_leaves_the_previous_day_untouched(self, tmp_path, monkeypatch):
        import processing_server.worker as mod

        worker = _make_worker(tmp_path)
        monkeypatch.setattr(mod, "_manifest_day", lambda manifest: "2026-08-01")
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)

        monkeypatch.setattr(mod, "_manifest_day", lambda manifest: "2026-08-02")
        _publish_session(worker, "sess_B", job_id="j2")
        _aggregate_now(worker)

        root = _aggregate_root(worker)
        assert worker.aggregate_days() == ["2026-08-01", "2026-08-02"]
        first = json.loads((root / "2026-08-01" / SIDECAR_NAME).read_text())
        assert sorted(first["sessions"]) == ["sess_A"], "yesterday's aggregate was rewritten"
        assert sorted(json.loads((root / "2026-08-02" / SIDECAR_NAME).read_text())["sessions"]) == [
            "sess_A",
            "sess_B",
        ]
        assert sorted(json.loads((_latest_dir(worker) / SIDECAR_NAME).read_text())["sessions"]) == [
            "sess_A",
            "sess_B",
        ], "`latest` did not follow the newest dated aggregate"
        worker.close()

    def test_a_same_day_rerun_replaces_that_day(self, tmp_path, monkeypatch):
        """One prefix per day, so a manual rerun corrects the day rather than
        accumulating copies of it."""
        import processing_server.worker as mod

        worker = _make_worker(tmp_path)
        monkeypatch.setattr(mod, "_manifest_day", lambda manifest: "2026-08-01")
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)
        _publish_session(worker, "sess_B", job_id="j2")
        _aggregate_now(worker)

        assert worker.aggregate_days() == ["2026-08-01"]
        day = _aggregate_root(worker) / "2026-08-01"
        assert sorted(json.loads((day / SIDECAR_NAME).read_text())["sessions"]) == ["sess_A", "sess_B"]
        worker.close()

    def test_a_shrinking_rerun_leaves_no_orphan_table(self, tmp_path, monkeypatch):
        import processing_server.worker as mod

        worker = _make_worker(tmp_path)
        monkeypatch.setattr(mod, "_manifest_day", lambda manifest: "2026-08-01")
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)
        stale = _aggregate_root(worker) / "2026-08-01" / "gone.parquet"
        stale.write_bytes(b"stale")

        _publish_session(worker, "sess_B", job_id="j2")
        _aggregate_now(worker)
        assert not stale.exists(), "the dated prefix was written into rather than replaced"
        worker.close()


class TestLatestSortsAboveEveryDate:
    """`max()` over the children of `aggregate/` returns `latest`, because digits sort
    before letters. Anything scanning that prefix has to filter to dated names first or
    it will pick the mirror — or, worse, prune it."""

    def test_max_child_would_be_the_mirror(self):
        assert max(["2026-08-18", "2026-12-31", "latest"]) == "latest"

    def test_aggregate_days_excludes_the_mirror(self, tmp_path):
        worker = _make_worker(tmp_path)
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)
        children = LocalOutputStore().list_children(worker.aggregate_uri())
        assert "latest" in children, "the mirror was not written — the test proves nothing"
        assert worker.aggregate_days() == [c for c in children if c != "latest"]
        worker.close()

    def test_undated_children_are_ignored(self, tmp_path):
        worker = _make_worker(tmp_path)
        root = _aggregate_root(worker)
        for name in ("2026-08-01", "latest", "scratch", "2026-8-1"):
            (root / name).mkdir(parents=True)
        assert worker.aggregate_days() == ["2026-08-01"]
        worker.close()


class TestTheMarkerGoesLast:
    def test_no_table_is_written_after_the_marker(self, tmp_path):
        """`write_object` imposes no ordering of its own — unlike `publish`, which is why
        this has to be asserted at the worker rather than at the store."""

        class _Recording(LocalOutputStore):
            def __init__(self):
                super().__init__()
                self.writes: list[str] = []

            def write_object(self, uri, payload):
                self.writes.append(uri.rsplit("/", 1)[-1])
                return super().write_object(uri, payload)

        store = _Recording()
        worker = _make_worker(tmp_path, output_store=store)
        _publish_session(worker, "sess_A", job_id="j1")
        _aggregate_now(worker)

        assert store.writes, "nothing was written"
        assert store.writes[-1] == SIDECAR_NAME, f"marker was not written last: {store.writes}"
        assert store.writes.count(SIDECAR_NAME) == 1, "the dated marker was written more than once"
        worker.close()


class TestLocalReadObject:
    def test_reads_one_file(self, tmp_path):
        (tmp_path / "m.json").write_text('{"a": 1}')
        assert LocalOutputStore().read_object(str(tmp_path / "m.json")) == b'{"a": 1}'

    def test_missing_is_none(self, tmp_path):
        assert LocalOutputStore().read_object(str(tmp_path / "nope.json")) is None

    def test_a_directory_is_none_not_an_error(self, tmp_path):
        assert LocalOutputStore().read_object(str(tmp_path)) is None


class TestKindDispatch:
    def test_an_aggregate_job_does_not_go_down_the_session_path(self, tmp_path):
        """Without dispatch, `claim` hands an aggregate row to the session path, which
        tries to stage a session that does not exist."""

        class _Recording(Worker):
            calls: list[str] = []

            def process_job(self, job: Job) -> None:
                self.calls.append("session")

            def process_aggregate_job(self, job: Job) -> None:
                self.calls.append("aggregate")

        worker = _Recording(
            _config(tmp_path), worker_id="w1", work_dir=tmp_path / "work", output_store=LocalOutputStore()
        )
        _publish_session(worker, "sess_A", job_id="j1")
        worker.enqueue_aggregate()
        assert worker.claim_and_process_one() is True
        assert worker.calls == ["aggregate"]
        worker.close()


@pytest.mark.parametrize("store_name", ["local"])
def test_output_store_satisfies_the_read_side_protocol(store_name):
    """`read_object`/`write_object`/`copy_prefix`/`delete_prefix`/`list_children` are
    what make aggregation store-agnostic; a store missing one fails at runtime, deep
    inside a job."""
    from processing_server.stores import get_output_store

    store = get_output_store(store_name)
    for method in (
        "exists",
        "publish",
        "iter_completed",
        "read_object",
        "write_object",
        "copy_prefix",
        "delete_prefix",
        "list_children",
    ):
        assert callable(getattr(store, method)), f"{store_name} is missing {method}"
