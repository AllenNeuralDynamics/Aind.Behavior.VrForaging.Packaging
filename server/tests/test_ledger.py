"""Unit tests for the SQLite ledger (§5-§8, §16) — no network, no Docker."""

import threading

import pytest
from processing_server.ledger import Ledger, job_key
from processing_server.models import Job


def _make_ledger(tmp_path) -> Ledger:
    return Ledger(tmp_path / "jobs.sqlite")


def _job(ledger: Ledger, job_id: str) -> Job:
    """``ledger.get_job`` narrowed to non-``None`` for chained assertions below."""
    job = ledger.get_job(job_id)
    assert job is not None
    return job


def _upsert(ledger: Ledger, session_name: str, *, priority: int = 0, fingerprint: str = "fp1") -> str:
    jid = ledger.upsert_job(
        kind="session",
        release="rel",
        asset_id=None,
        processor_fingerprint=fingerprint,
        input_store="mount",
        input_uri=f"file:///{session_name}",
        output_uri=f"file:///out/{session_name}",
        session_name=session_name,
        priority=priority,
    )
    assert jid is not None
    return jid


class TestJobKey:
    def test_stable_for_same_inputs(self):
        assert job_key("session", "s1", None, "fp", 0) == job_key("session", "s1", None, "fp", 0)

    def test_differs_by_session_name(self):
        """Regression: asset_id alone collided across sessions with no asset_id (LocalSource)."""
        assert job_key("session", "s1", None, "fp", 0) != job_key("session", "s2", None, "fp", 0)

    def test_differs_by_run_count(self):
        assert job_key("session", "s1", None, "fp", 0) != job_key("session", "s1", None, "fp", 1)

    def test_differs_by_fingerprint(self):
        assert job_key("session", "s1", None, "fp1", 0) != job_key("session", "s1", None, "fp2", 0)


class TestUpsert:
    def test_idempotent_on_job_key(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_A")
            second = ledger.upsert_job(
                kind="session",
                release="rel",
                asset_id=None,
                processor_fingerprint="fp1",
                input_store="mount",
                input_uri="file:///sess_A",
                output_uri="file:///out/sess_A",
                session_name="sess_A",
            )
            assert second is None
            jobs = ledger.list_jobs()
            assert len(jobs) == 1

    def test_two_sessions_no_asset_id_both_insert(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_A")
            _upsert(ledger, "sess_B")
            assert len(ledger.list_jobs()) == 2


class TestClaim:
    def test_empty_queue_returns_none(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            assert ledger.claim("worker-1", 60) is None

    def test_claim_sets_running_and_worker_id(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_A")
            job = ledger.claim("worker-1", 60)
            assert job is not None
            assert job.status == "running"
            assert job.worker_id == "worker-1"
            assert job.attempts == 1

    def test_priority_order(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_low", priority=0)
            _upsert(ledger, "sess_high", priority=5)
            job = ledger.claim("worker-1", 60)
            assert job is not None
            assert job.session_name == "sess_high"

    def test_concurrent_claim_never_double_claims(self, tmp_path):
        """The single highest-value guard (§7): N threads, each its own connection,
        racing for one job — exactly one must win."""
        path = tmp_path / "jobs.sqlite"
        with Ledger(path) as setup:
            _upsert(setup, "sess_A")

        winners: list[str] = []
        lock = threading.Lock()

        def _try_claim(worker_id: str) -> None:
            with Ledger(path) as ledger:
                job = ledger.claim(worker_id, 60)
                if job is not None:
                    with lock:
                        winners.append(worker_id)

        threads = [threading.Thread(target=_try_claim, args=(f"w{i}",)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(winners) == 1

    def test_force_claim_specific_job(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_low", priority=0)
            target = _upsert(ledger, "sess_high", priority=5)
            job = ledger.force_claim(target, "worker-1", 60)
            assert job is not None
            assert job.job_id == target

    def test_force_claim_already_running_returns_none(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            assert ledger.force_claim(jid, "worker-2", 60) is None


class TestLeaseReap:
    def test_reaps_to_pending_when_attempts_remain(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", lease_seconds=-1)  # already expired
            n = ledger.reap_expired_leases()
            assert n == 1
            assert _job(ledger, jid).status == "pending"

    def test_reaps_to_dead_when_attempts_exhausted(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            for _ in range(3):
                job = ledger.claim("worker-1", lease_seconds=-1)
                assert job is not None
                ledger.reap_expired_leases()
            # After 3 attempts (== default max_attempts), the 4th expired lease is dead.
            job = _job(ledger, jid)
            assert job.attempts == 3
            assert job.status == "dead"


class TestOutcomes:
    def test_complete_job_ok(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            ledger.complete_job(jid, partial=False, exit_code=0)
            job = _job(ledger, jid)
            assert job.status == "completed"
            assert job.partial is False
            assert job.finished_at is not None

    def test_complete_job_partial(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            ledger.complete_job(jid, partial=True)
            assert _job(ledger, jid).partial is True

    @pytest.mark.parametrize("error_kind", ["transient", "infra", "timeout"])
    def test_fail_job_retryable_becomes_retrying(self, tmp_path, error_kind):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            status = ledger.fail_job(jid, error_kind=error_kind, error="boom")
            assert status == "retrying"
            job = _job(ledger, jid)
            assert job.status == "retrying"
            assert job.next_eligible_at is not None
            assert job.error_kind == error_kind

    @pytest.mark.parametrize("error_kind", ["data", "code"])
    def test_fail_job_terminal_kinds_never_retry(self, tmp_path, error_kind):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            status = ledger.fail_job(jid, error_kind=error_kind, error="boom")
            assert status == "failed"
            assert _job(ledger, jid).status == "failed"

    def test_fail_job_exhausted_retries_becomes_dead(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            for _ in range(3):
                ledger.claim("worker-1", 60)
                status = ledger.fail_job(jid, error_kind="transient", error="boom")
                if status != "dead":
                    # retrying jobs are not claimable until next_eligible_at; force it open again
                    ledger._conn.execute(
                        "UPDATE jobs SET status='pending', next_eligible_at=NULL WHERE job_id=?", (jid,)
                    )
            assert status == "dead"


class TestRerun:
    def test_keeps_old_row_and_creates_new_pending(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            ledger.fail_job(jid, error_kind="data", error="bad data")

            new_id = ledger.rerun(jid, reason="fixed upstream")
            old = _job(ledger, jid)
            new = _job(ledger, new_id)

            assert old.status == "failed"  # untouched
            assert new.status == "pending"
            assert new.run_count == old.run_count + 1
            assert new.rerun_of == jid
            assert new.job_key != old.job_key

    def test_rerun_of_missing_job_raises(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            with pytest.raises(KeyError):
                ledger.rerun("does-not-exist")


class TestPriority:
    def test_set_only_affects_pending(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)  # now running
            ledger.set_priority(jid, value=99)
            assert _job(ledger, jid).priority == 0  # unchanged — not pending

    def test_top_and_bottom_are_relative_to_the_whole_pending_set(self, tmp_path):
        """`bottom`'s MIN includes the target job's own current row — so a job
        already at the minimum just moves one further below itself, which still
        satisfies "below every other pending job" without needing a self-exclusion."""
        with _make_ledger(tmp_path) as ledger:
            a = _upsert(ledger, "sess_A", priority=0)
            b = _upsert(ledger, "sess_B", priority=5)
            c = _upsert(ledger, "sess_C", priority=2)

            ledger.priority_top(a)
            assert _job(ledger, a).priority == 6  # max(0, 5, 2) + 1

            ledger.priority_bottom(b)
            assert _job(ledger, b).priority == 1  # min(6, 5, 2) - 1

            others = {_job(ledger, c).priority, _job(ledger, a).priority}
            assert _job(ledger, b).priority < min(others)

    def test_bump(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            a = _upsert(ledger, "sess_A", priority=6)
            ledger.set_priority(a, bump=-2)
            assert _job(ledger, a).priority == 4


class TestSkip:
    def test_skip_pending(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.skip(jid)
            assert _job(ledger, jid).status == "skipped"

    def test_skip_running_no_op(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            ledger.skip(jid)  # guarded to pending only
            assert _job(ledger, jid).status == "running"

    def test_skip_running_explicit(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            ledger.skip_running(jid, "output already exists")
            assert _job(ledger, jid).status == "skipped"


class TestTags:
    def test_add_remove_and_list(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            ledger.add_tag("sess_A", "reprocess")
            ledger.add_tag("sess_A", "suspect", note="looks odd")
            assert set(ledger.tags_for("sess_A")) == {"reprocess", "suspect"}
            ledger.remove_tag("sess_A", "suspect")
            assert ledger.tags_for("sess_A") == ["reprocess"]

    def test_survives_rerun(self, tmp_path):
        """Tags key on session_name, not job_id — must survive supersession (§16)."""
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.add_tag("sess_A", "reprocess")
            ledger.claim("worker-1", 60)
            ledger.fail_job(jid, error_kind="data", error="bad")
            ledger.rerun(jid)
            assert "reprocess" in ledger.tags_for("sess_A")

    def test_sessions_with_tag(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            ledger.add_tag("sess_A", "reprocess")
            ledger.add_tag("sess_B", "reprocess")
            ledger.add_tag("sess_C", "suspect")
            assert set(ledger.sessions_with_tag("reprocess")) == {"sess_A", "sess_B"}


class TestListJobsAndFilters:
    def test_filter_by_status(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_A")
            _upsert(ledger, "sess_B")
            ledger.claim("worker-1", 60)  # claims sess_A or sess_B (highest priority tie -> oldest)
            pending = ledger.list_jobs(status="pending")
            running = ledger.list_jobs(status="running")
            assert len(pending) == 1
            assert len(running) == 1

    def test_filter_by_tag(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_A")
            _upsert(ledger, "sess_B")
            ledger.add_tag("sess_A", "reprocess")
            tagged = ledger.list_jobs(tag="reprocess")
            assert [j.session_name for j in tagged] == ["sess_A"]

    def test_filter_by_session_name_like(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "behavior_100_2025-01-01_00-00-00")
            _upsert(ledger, "behavior_200_2025-01-01_00-00-00")
            matches = ledger.list_jobs(session_name_like="_100_")
            assert len(matches) == 1

    def test_tags_column_joined(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_A")
            ledger.add_tag("sess_A", "reprocess")
            job = ledger.list_jobs()[0]
            assert job.tags == "reprocess"


class TestWatermark:
    def test_get_none_when_unset(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            assert ledger.get_watermark("docdb") is None

    def test_set_then_get(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            ledger.set_watermark("docdb", "2026-01-01T00:00:00Z")
            assert ledger.get_watermark("docdb") == "2026-01-01T00:00:00Z"


class TestEventsAndCountActive:
    def test_events_recorded_through_lifecycle(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            jid = _upsert(ledger, "sess_A")
            ledger.claim("worker-1", 60)
            ledger.complete_job(jid)
            events = ledger.list_events(jid)
            transitions = [(e["from_status"], e["to_status"]) for e in events]
            assert (None, "pending") in transitions
            assert ("pending", "running") in transitions
            assert ("running", "completed") in transitions

    def test_count_active(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            _upsert(ledger, "sess_A")
            _upsert(ledger, "sess_B")
            assert ledger.count_active("rel") == 2

            claimed = ledger.claim("worker-1", 60)
            assert claimed is not None
            assert ledger.count_active("rel") == 2  # running still counts as active

            ledger.complete_job(claimed.job_id)
            assert ledger.count_active("rel") == 1  # one terminal, one still pending


class TestJobStatuses:
    """The sweeper's read (§4a): status only, batched, silent about ids it has never
    seen — a directory named after nothing must not be mistaken for a finished job."""

    def test_maps_known_ids_and_omits_unknown(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            a = _upsert(ledger, "sess_A")
            b = _upsert(ledger, "sess_B")
            ledger.claim("worker-1", 60)
            statuses = ledger.job_statuses([a, b, "not-a-job-id"])
            assert set(statuses) == {a, b}
            assert sorted(statuses.values()) == ["pending", "running"]

    def test_empty_input_is_not_a_query(self, tmp_path):
        with _make_ledger(tmp_path) as ledger:
            assert ledger.job_statuses([]) == {}

    def test_chunks_past_sqlites_parameter_limit(self, tmp_path):
        """`SQLITE_MAX_VARIABLE_NUMBER` is 999 on older builds, and the number of
        stranded directories is not bounded by anything we control."""
        with _make_ledger(tmp_path) as ledger:
            ids = [_upsert(ledger, f"sess_{i:04d}") for i in range(1200)]
            statuses = ledger.job_statuses(ids)
            assert len(statuses) == 1200


class TestAdditiveMigration:
    def test_column_added_to_a_ledger_that_predates_it(self, tmp_path):
        """`CREATE TABLE IF NOT EXISTS` is a no-op against an existing table, so a new
        column would never reach a ledger someone already has — and the failure would
        surface as an OperationalError on the next heartbeat, mid-campaign."""
        path = tmp_path / "jobs.sqlite"
        with Ledger(path) as ledger:
            ledger._conn.execute("ALTER TABLE workers DROP COLUMN worker_image")
            cols = {r["name"] for r in ledger._conn.execute("PRAGMA table_info(workers)")}
            assert "worker_image" not in cols

        with Ledger(path) as ledger:  # reopening applies the migration
            cols = {r["name"] for r in ledger._conn.execute("PRAGMA table_info(workers)")}
            assert "worker_image" in cols
            ledger.heartbeat("w1", running_jobs=0, worker_image="ghcr.io/x@sha256:abc")
            row = ledger.get_worker("w1")
            assert row is not None and row["worker_image"] == "ghcr.io/x@sha256:abc"
