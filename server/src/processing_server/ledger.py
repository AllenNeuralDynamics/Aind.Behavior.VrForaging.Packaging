"""SQLite ledger — the authoritative record of job/session state.

One file on the ledger volume. Each caller (worker, CLI invocation, dashboard
request) opens its own :class:`Ledger` (its own ``sqlite3.Connection``) against
that file — do not share one instance across threads. WAL mode lets the
worker's writes and the dashboard's queue actions coexist without contention:
both hold short transactions, and ``busy_timeout`` absorbs the rest.

Migrations are intentionally simple: every table is ``CREATE TABLE IF NOT
EXISTS``, plus :func:`_add_missing_columns` for columns added to a table that
already exists in someone's ledger. Additive only — a column drop, rename or
type change still needs a real migration path, which is future work.
"""

import hashlib
import logging
import sqlite3
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from .models import ErrorKind, Job, JobStatus

logger = logging.getLogger(__name__)

#: Retry policy. Everything else (``data``, ``code``) is terminal immediately.
RETRYABLE_KINDS = frozenset({"transient", "infra", "timeout"})

_SCHEMA_VERSION = 1

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS jobs (
    job_id            TEXT PRIMARY KEY,
    job_key           TEXT NOT NULL UNIQUE,
    kind              TEXT NOT NULL,
    release           TEXT NOT NULL,

    session_name      TEXT,
    subject_id        TEXT,
    session_start     TEXT,
    asset_id          TEXT,
    input_store       TEXT,
    input_uri         TEXT NOT NULL,
    output_uri        TEXT NOT NULL,

    status            TEXT NOT NULL,
    partial           INTEGER NOT NULL DEFAULT 0,
    attempts          INTEGER NOT NULL DEFAULT 0,
    max_attempts      INTEGER NOT NULL DEFAULT 3,
    run_count         INTEGER NOT NULL DEFAULT 0,
    rerun_of          TEXT,
    priority          INTEGER NOT NULL DEFAULT 0,
    next_eligible_at  TEXT,
    worker_id         TEXT,
    lease_expires_at  TEXT,

    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT,
    duration_s        REAL,
    t_stage_s         REAL,
    t_run_s           REAL,
    t_publish_s       REAL,

    exit_code         INTEGER,
    error_kind        TEXT,
    error             TEXT,
    sidecar           TEXT,
    log_uri           TEXT,
    staged_bytes      INTEGER,
    read_files        INTEGER,
    read_bytes        INTEGER,
    output_bytes      INTEGER,
    warn_count        INTEGER NOT NULL DEFAULT 0,
    failed_processors TEXT,

    image_ref         TEXT,
    image_digest      TEXT,
    git_commit        TEXT,
    packaging_version TEXT,
    data_contract_version TEXT,
    dataset_version   TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON jobs (status, priority DESC, next_eligible_at);
CREATE INDEX IF NOT EXISTS idx_jobs_session   ON jobs (session_name);
CREATE INDEX IF NOT EXISTS idx_jobs_lease     ON jobs (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_jobs_gate      ON jobs (release, kind, status);

CREATE TABLE IF NOT EXISTS job_events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    attempt     INTEGER NOT NULL,
    at          TEXT NOT NULL,
    from_status TEXT,
    to_status   TEXT NOT NULL,
    worker_id   TEXT,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_job ON job_events (job_id, attempt);

CREATE TABLE IF NOT EXISTS ingest_watermarks (
    source_name TEXT PRIMARY KEY,
    cursor      TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- `worker_image`: the worker's own image; the `jobs` columns are the processor's.
-- Comments stay OUTSIDE the column list — SQLite re-parses this after DROP COLUMN, and
-- a trailing `--` among the columns fails with "incomplete input".
CREATE TABLE IF NOT EXISTS workers (
    worker_id       TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    heartbeat_at    TEXT NOT NULL,
    running_jobs    INTEGER NOT NULL DEFAULT 0,
    disk_free_bytes INTEGER,
    worker_image    TEXT
);

CREATE TABLE IF NOT EXISTS session_tags (
    session_name TEXT NOT NULL,
    tag          TEXT NOT NULL,
    added_at     TEXT NOT NULL,
    added_by     TEXT,
    note         TEXT,
    PRIMARY KEY (session_name, tag)
);
CREATE INDEX IF NOT EXISTS idx_tags_tag ON session_tags (tag);

CREATE TABLE IF NOT EXISTS schema_meta (version INTEGER NOT NULL);
"""


def job_key(
    kind: str, session_name: str | None, asset_id: str | None, processor_fingerprint: str, run_count: int
) -> str:
    """Content-addressed job identity. Routine ingestion upserts on this key.

    ``session_name`` is included alongside ``asset_id``: DocDB assets always have
    a unique ``asset_id``, but ``LocalSource`` (testing/offline debugging)
    does not set one, and ``asset_id`` alone would then collide across every
    session in one release.
    """
    raw = f"{kind}|{session_name}|{asset_id}|{processor_fingerprint}|{run_count}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


#: Columns added after a table shipped. ``CREATE TABLE IF NOT EXISTS`` is a no-op on an
#: existing table, so a new column would never reach a ledger someone already has.
_ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (("workers", "worker_image", "TEXT"),)


def _add_missing_columns(conn: sqlite3.Connection) -> None:
    """Apply :data:`_ADDED_COLUMNS` to an existing ledger. Idempotent, additive only."""
    for table, column, decl in _ADDED_COLUMNS:
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            logger.info("Ledger migration: adding %s.%s", table, column)
            # Interpolated because DDL takes no parameters; values are module constants.
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _row_to_job(row: sqlite3.Row, tags: str | None = None) -> Job:
    data: dict[str, Any] = dict(row)
    data["partial"] = bool(data["partial"])
    data["tags"] = tags
    return Job.model_validate(data)


class Ledger:
    """One connection to the ledger SQLite file. Not thread-safe — one per thread/process."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None (autocommit): transactions are managed explicitly with
        # BEGIN IMMEDIATE / COMMIT / ROLLBACK, per the CAS claim below.
        self._conn = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.executescript(_SCHEMA)
        _add_missing_columns(self._conn)
        row = self._conn.execute("SELECT version FROM schema_meta").fetchone()
        if row is None:
            self._conn.execute("INSERT INTO schema_meta (version) VALUES (?)", (_SCHEMA_VERSION,))

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def upsert_job(
        self,
        *,
        kind: Literal["session", "aggregate"],
        release: str,
        asset_id: str | None,
        processor_fingerprint: str,
        input_store: str | None,
        input_uri: str,
        output_uri: str,
        session_name: str | None = None,
        subject_id: str | None = None,
        session_start: str | None = None,
        run_count: int = 0,
        rerun_of: str | None = None,
        priority: int = 0,
    ) -> str | None:
        """Insert a new job row unless its ``job_key`` already exists.

        Returns the new ``job_id``, or ``None`` if a row with this ``job_key``
        already exists (the routine, at-least-once-ingestion no-op).
        """
        key = job_key(kind, session_name, asset_id, processor_fingerprint, run_count)
        jid = str(uuid.uuid4())
        now = _iso(_now())
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self._conn.execute(
                """INSERT INTO jobs (
                    job_id, job_key, kind, release, session_name, subject_id, session_start,
                    asset_id, input_store, input_uri, output_uri, status, run_count, rerun_of,
                    priority, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,?,?,?)
                ON CONFLICT(job_key) DO NOTHING""",
                (
                    jid,
                    key,
                    kind,
                    release,
                    session_name,
                    subject_id,
                    session_start,
                    asset_id,
                    input_store,
                    input_uri,
                    output_uri,
                    run_count,
                    rerun_of,
                    priority,
                    now,
                    now,
                ),
            )
            inserted = cur.rowcount == 1
            if inserted:
                self._record_event(jid, attempt=0, from_status=None, to_status="pending")
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return jid if inserted else None

    # ------------------------------------------------------------------
    # Claim / lease
    # ------------------------------------------------------------------

    def claim(self, worker_id: str, lease_seconds: int) -> Job | None:
        """Atomically claim the highest-priority eligible ``pending`` job.

        ``BEGIN IMMEDIATE`` takes the write lock before the read, and the
        ``WHERE status='pending'`` guard on the ``UPDATE`` is the compare-and-swap
        that makes concurrent claimants from separate connections mutually
        exclusive — this is the single highest-value guard in the ledger.
        """
        now = _now()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                """SELECT job_id FROM jobs
                    WHERE status = 'pending' AND (next_eligible_at IS NULL OR next_eligible_at <= ?)
                    ORDER BY priority DESC, created_at LIMIT 1""",
                (_iso(now),),
            ).fetchone()
            if row is None:
                self._conn.execute("COMMIT")
                return None
            jid = self._claim_locked(row["job_id"], worker_id, lease_seconds, now)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return self.get_job(jid) if jid else None

    def force_claim(self, job_id: str, worker_id: str, lease_seconds: int) -> Job | None:
        """Claim a specific job regardless of priority ordering — ``work --job-id``,
        the debug-one-session path. Same CAS guard as :meth:`claim`; returns
        ``None`` if the job is not currently ``pending``."""
        now = _now()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            jid = self._claim_locked(job_id, worker_id, lease_seconds, now)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return self.get_job(jid) if jid else None

    def _claim_locked(self, job_id: str, worker_id: str, lease_seconds: int, now: datetime) -> str | None:
        """Must be called with the write lock already held (inside ``BEGIN IMMEDIATE``).
        Commits and returns *job_id* on success; commits and returns ``None`` if the
        compare-and-swap lost the race (job no longer ``pending``)."""
        lease_expires_at = _iso(now + timedelta(seconds=lease_seconds))
        cur = self._conn.execute(
            """UPDATE jobs SET status='running', worker_id=?, attempts=attempts+1,
                               started_at=?, lease_expires_at=?, updated_at=?
                WHERE job_id=? AND status='pending'""",
            (worker_id, _iso(now), lease_expires_at, _iso(now), job_id),
        )
        if cur.rowcount == 0:
            self._conn.execute("COMMIT")
            return None
        self._record_event(job_id, attempt=None, from_status="pending", to_status="running", worker_id=worker_id)
        self._conn.execute("COMMIT")
        return job_id

    def renew_lease(self, job_id: str, lease_seconds: int) -> None:
        lease_expires_at = _iso(_now() + timedelta(seconds=lease_seconds))
        self._conn.execute(
            "UPDATE jobs SET lease_expires_at=?, updated_at=? WHERE job_id=? AND status='running'",
            (lease_expires_at, _iso(_now()), job_id),
        )

    def reap_expired_leases(self) -> int:
        """Move ``running`` jobs whose lease has expired back to ``pending`` (or ``dead``
        if retries are exhausted). Without this, a worker crash strands jobs in
        ``running`` forever. Returns the number of jobs reaped."""
        now = _iso(_now())
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self._conn.execute(
                "SELECT job_id, attempts, max_attempts FROM jobs WHERE status='running' AND lease_expires_at < ?",
                (now,),
            ).fetchall()
            for row in rows:
                jid = row["job_id"]
                if row["attempts"] >= row["max_attempts"]:
                    self._conn.execute("UPDATE jobs SET status='dead', updated_at=? WHERE job_id=?", (now, jid))
                    self._record_event(
                        jid, attempt=None, from_status="running", to_status="dead", detail="lease expired"
                    )
                else:
                    self._conn.execute(
                        "UPDATE jobs SET status='pending', worker_id=NULL, lease_expires_at=NULL, updated_at=? "
                        "WHERE job_id=?",
                        (now, jid),
                    )
                    self._record_event(
                        jid, attempt=None, from_status="running", to_status="pending", detail="lease expired"
                    )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return len(rows)

    # ------------------------------------------------------------------
    # Outcome recording
    # ------------------------------------------------------------------

    def complete_job(self, job_id: str, *, partial: bool = False, **fields: Any) -> None:
        """Record a successful (possibly partial) run. Publishing must have already
        happened — this only records the ledger-side outcome."""
        self._finish(job_id, status="completed", partial=partial, error_kind=None, error=None, **fields)

    def fail_job(self, job_id: str, *, error_kind: ErrorKind, error: str, **fields: Any) -> JobStatus:
        """Record a failed run and apply the retry policy.

        ``transient``/``infra``/``timeout`` retry with backoff until
        ``max_attempts`` is reached, then become ``dead``. ``data``/``code`` are
        terminal immediately as ``failed`` — retrying a deterministic parse
        failure three times only produces three identical stack traces.
        Returns the status actually recorded.
        """
        row = self._conn.execute("SELECT attempts, max_attempts FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(f"No such job: {job_id}")

        if error_kind not in RETRYABLE_KINDS:
            status: JobStatus = "failed"
        elif row["attempts"] >= row["max_attempts"]:
            status = "dead"
        else:
            status = "retrying"

        backoff_s = min(3600, 30 * (2 ** max(0, row["attempts"] - 1)))
        next_eligible_at = _iso(_now() + timedelta(seconds=backoff_s)) if status == "retrying" else None
        self._finish(
            job_id,
            status=status,
            partial=False,
            error_kind=error_kind,
            error=error,
            next_eligible_at=next_eligible_at,
            **fields,
        )
        if status == "retrying":
            # Retrying is not terminal — clear worker/lease and go back to pending-eligible-later.
            self._conn.execute(
                "UPDATE jobs SET status='retrying', worker_id=NULL, lease_expires_at=NULL WHERE job_id=?",
                (job_id,),
            )
        return status

    def _finish(
        self,
        job_id: str,
        *,
        status: JobStatus,
        partial: bool,
        error_kind: ErrorKind | None,
        error: str | None,
        next_eligible_at: str | None = None,
        exit_code: int | None = None,
        sidecar: str | None = None,
        log_uri: str | None = None,
        staged_bytes: int | None = None,
        read_files: int | None = None,
        read_bytes: int | None = None,
        output_bytes: int | None = None,
        warn_count: int = 0,
        failed_processors: str | None = None,
        t_stage_s: float | None = None,
        t_run_s: float | None = None,
        t_publish_s: float | None = None,
        image_ref: str | None = None,
        image_digest: str | None = None,
        git_commit: str | None = None,
        packaging_version: str | None = None,
        data_contract_version: str | None = None,
        dataset_version: str | None = None,
    ) -> None:
        now = _now()
        row = self._conn.execute("SELECT started_at, status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(f"No such job: {job_id}")
        duration_s = None
        if row["started_at"]:
            duration_s = (now - datetime.fromisoformat(row["started_at"])).total_seconds()

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """UPDATE jobs SET
                    status=?, partial=?, next_eligible_at=?, finished_at=?, updated_at=?, duration_s=?,
                    t_stage_s=COALESCE(?, t_stage_s), t_run_s=COALESCE(?, t_run_s), t_publish_s=COALESCE(?, t_publish_s),
                    exit_code=?, error_kind=?, error=?, sidecar=?, log_uri=?,
                    staged_bytes=?, read_files=?, read_bytes=?, output_bytes=?, warn_count=?, failed_processors=?,
                    image_ref=?, image_digest=?, git_commit=?, packaging_version=?, data_contract_version=?,
                    dataset_version=?
                WHERE job_id=?""",
                (
                    status,
                    int(partial),
                    next_eligible_at,
                    _iso(now),
                    _iso(now),
                    duration_s,
                    t_stage_s,
                    t_run_s,
                    t_publish_s,
                    exit_code,
                    error_kind,
                    error,
                    sidecar,
                    log_uri,
                    staged_bytes,
                    read_files,
                    read_bytes,
                    output_bytes,
                    warn_count,
                    failed_processors,
                    image_ref,
                    image_digest,
                    git_commit,
                    packaging_version,
                    data_contract_version,
                    dataset_version,
                    job_id,
                ),
            )
            self._record_event(job_id, attempt=None, from_status=row["status"], to_status=status, detail=error or "")
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # Re-run / re-process, priority & skip, tags
    # ------------------------------------------------------------------

    def rerun(self, job_id: str, *, reason: str | None = None, requested_by: str = "cli") -> str:
        """Re-queue the session behind *job_id* for another attempt.

        The previous row is kept — its outcome stays queryable — and a new row
        with an incremented ``run_count`` (hence a new ``job_key``) is inserted
        as ``pending``, linked back via ``rerun_of``.
        """
        old = self.get_job(job_id)
        if old is None:
            raise KeyError(f"No such job: {job_id}")
        new_id = str(uuid.uuid4())
        now = _iso(_now())
        fingerprint_source = old.image_digest or old.packaging_version or "unknown"
        new_key = job_key(old.kind, old.session_name, old.asset_id, fingerprint_source, old.run_count + 1)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """INSERT INTO jobs (
                    job_id, job_key, kind, release, session_name, subject_id, session_start,
                    asset_id, input_store, input_uri, output_uri, status, run_count, rerun_of,
                    priority, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,?,?,?)""",
                (
                    new_id,
                    new_key,
                    old.kind,
                    old.release,
                    old.session_name,
                    old.subject_id,
                    old.session_start,
                    old.asset_id,
                    old.input_store,
                    old.input_uri,
                    old.output_uri,
                    old.run_count + 1,
                    job_id,
                    old.priority,
                    now,
                    now,
                ),
            )
            self._record_event(new_id, attempt=0, from_status=None, to_status="pending", detail=reason or "")
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        logger.info(
            "Re-queued %s (session=%s) as %s, requested by %s: %s",
            job_id,
            old.session_name,
            new_id,
            requested_by,
            reason,
        )
        return new_id

    def set_priority(self, job_id: str, *, value: int | None = None, bump: int | None = None) -> None:
        """Set or bump a **pending** job's priority. No-op on a non-pending job."""
        if value is not None:
            self._conn.execute(
                "UPDATE jobs SET priority=?, updated_at=? WHERE job_id=? AND status='pending'",
                (value, _iso(_now()), job_id),
            )
        elif bump is not None:
            self._conn.execute(
                "UPDATE jobs SET priority=priority+?, updated_at=? WHERE job_id=? AND status='pending'",
                (bump, _iso(_now()), job_id),
            )

    def priority_top(self, job_id: str) -> None:
        row = self._conn.execute("SELECT MAX(priority) AS p FROM jobs WHERE status='pending'").fetchone()
        top = (row["p"] or 0) + 1
        self.set_priority(job_id, value=top)

    def priority_bottom(self, job_id: str) -> None:
        row = self._conn.execute("SELECT MIN(priority) AS p FROM jobs WHERE status='pending'").fetchone()
        bottom = (row["p"] or 0) - 1
        self.set_priority(job_id, value=bottom)

    def skip(self, job_id: str) -> None:
        """Remove a pending job from the queue — reversible via :meth:`rerun`."""
        self._conn.execute(
            "UPDATE jobs SET status='skipped', updated_at=? WHERE job_id=? AND status='pending'",
            (_iso(_now()), job_id),
        )

    def skip_running(self, job_id: str, reason: str) -> None:
        """Mark a claimed job ``skipped`` without running it — the ``output.overwrite:
        false`` short-circuit: the output already exists, so there is nothing to do."""
        self._finish(job_id, status="skipped", partial=False, error_kind=None, error=reason)

    def add_tag(self, session_name: str, tag: str, *, added_by: str = "cli", note: str | None = None) -> None:
        self._conn.execute(
            "INSERT INTO session_tags (session_name, tag, added_at, added_by, note) VALUES (?,?,?,?,?) "
            "ON CONFLICT(session_name, tag) DO UPDATE SET note=excluded.note",
            (session_name, tag, _iso(_now()), added_by, note),
        )

    def remove_tag(self, session_name: str, tag: str) -> None:
        self._conn.execute("DELETE FROM session_tags WHERE session_name=? AND tag=?", (session_name, tag))

    def tags_for(self, session_name: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT tag FROM session_tags WHERE session_name=? ORDER BY tag", (session_name,)
        ).fetchall()
        return [r["tag"] for r in rows]

    def sessions_with_tag(self, tag: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT session_name FROM session_tags WHERE tag=? ORDER BY session_name", (tag,)
        ).fetchall()
        return [r["session_name"] for r in rows]

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def heartbeat(
        self,
        worker_id: str,
        *,
        running_jobs: int,
        disk_free_bytes: int | None = None,
        worker_image: str | None = None,
    ) -> None:
        now = _iso(_now())
        self._conn.execute(
            """INSERT INTO workers (worker_id, started_at, heartbeat_at, running_jobs, disk_free_bytes, worker_image)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    heartbeat_at=excluded.heartbeat_at,
                    running_jobs=excluded.running_jobs,
                    disk_free_bytes=excluded.disk_free_bytes,
                    worker_image=excluded.worker_image""",
            (worker_id, now, now, running_jobs, disk_free_bytes, worker_image),
        )

    def get_worker(self, worker_id: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM workers WHERE worker_id=?", (worker_id,)).fetchone()

    def list_workers(self) -> list[sqlite3.Row]:
        """Every worker that has ever heartbeated, most recently seen first —
        the dashboard/`status` header line."""
        return self._conn.execute("SELECT * FROM workers ORDER BY heartbeat_at DESC").fetchall()

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_job(self, job_id: str) -> Job | None:
        row = self._conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            return None
        tags = ",".join(self.tags_for(row["session_name"])) if row["session_name"] else None
        return _row_to_job(row, tags)

    def job_statuses(self, job_ids: Sequence[str]) -> dict[str, JobStatus]:
        """Map each of *job_ids* to its status, omitting ids this ledger never saw.

        For :meth:`Worker.sweep_work_dir`. Chunked: SQLite caps bound parameters per
        statement (999 on older builds) and the stranded-dir count is unbounded.
        """
        out: dict[str, JobStatus] = {}
        ids = list(job_ids)
        for start in range(0, len(ids), 500):
            chunk = ids[start : start + 500]
            # Only the placeholder count is interpolated; every job id is still bound.
            placeholders = ",".join("?" * len(chunk))
            rows = self._conn.execute(
                f"SELECT job_id, status FROM jobs WHERE job_id IN ({placeholders})",
                chunk,
            ).fetchall()
            for row in rows:
                out[row["job_id"]] = row["status"]
        return out

    def get_latest_job_for_session(self, session_name: str) -> Job | None:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE session_name=? ORDER BY run_count DESC LIMIT 1", (session_name,)
        ).fetchone()
        if row is None:
            return None
        return _row_to_job(row, ",".join(self.tags_for(session_name)))

    def list_jobs(
        self,
        *,
        status: str | None = None,
        release: str | None = None,
        tag: str | None = None,
        session_name_like: str | None = None,
        order_by: str = "priority DESC, created_at",
        limit: int | None = None,
    ) -> list[Job]:
        """The dashboard/`status`/`show` read path — every column is sortable."""
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("j.status = ?")
            params.append(status)
        if release:
            clauses.append("j.release = ?")
            params.append(release)
        if session_name_like:
            clauses.append("j.session_name LIKE ?")
            params.append(f"%{session_name_like}%")
        if tag:
            clauses.append("j.session_name IN (SELECT session_name FROM session_tags WHERE tag = ?)")
            params.append(tag)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_clause = f"LIMIT {int(limit)}" if limit else ""
        # order_by is developer-controlled (not user input) throughout this codebase — column
        # names come from a fixed allow-list in the CLI/dashboard layer, never from a raw request.
        sql = f"SELECT j.* FROM jobs j {where} ORDER BY {order_by} {limit_clause}"
        rows = self._conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            tags = ",".join(self.tags_for(row["session_name"])) if row["session_name"] else None
            results.append(_row_to_job(row, tags))
        return results

    def get_watermark(self, source_name: str) -> str | None:
        row = self._conn.execute("SELECT cursor FROM ingest_watermarks WHERE source_name=?", (source_name,)).fetchone()
        return row["cursor"] if row else None

    def set_watermark(self, source_name: str, cursor: str) -> None:
        now = _iso(_now())
        self._conn.execute(
            """INSERT INTO ingest_watermarks (source_name, cursor, updated_at) VALUES (?,?,?)
                ON CONFLICT(source_name) DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at""",
            (source_name, cursor, now),
        )

    def list_events(self, job_id: str) -> list[sqlite3.Row]:
        """Full transition history for one job — the ``show`` command."""
        return self._conn.execute("SELECT * FROM job_events WHERE job_id=? ORDER BY event_id", (job_id,)).fetchall()

    def latest_job_created_at(self, release: str, kind: str) -> str | None:
        """When a job of *kind* was last queued for *release*, or ``None``.

        The whole state a once-a-day schedule needs, and it lives in the ledger rather
        than in the worker — so a restart cannot re-trigger a run that already happened.
        Served by the ``(release, kind, status)`` index.
        """
        row = self._conn.execute(
            "SELECT MAX(created_at) AS at FROM jobs WHERE release=? AND kind=?", (release, kind)
        ).fetchone()
        return row["at"] if row and row["at"] else None

    def count_active(self, release: str, *, kind: str = "session") -> int:
        """Sessions still in flight (pending/running/retrying) for *release* — the
        aggregate gate: closed while this is nonzero, open once every session
        job is terminal (including ``failed``/``dead``/``skipped``)."""
        row = self._conn.execute(
            """SELECT COUNT(*) AS n FROM jobs
                WHERE release=? AND kind=? AND status IN ('pending','running','retrying')""",
            (release, kind),
        ).fetchone()
        return row["n"]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _record_event(
        self,
        job_id: str,
        *,
        attempt: int | None,
        from_status: str | None,
        to_status: str,
        worker_id: str | None = None,
        detail: str = "",
    ) -> None:
        if attempt is None:
            row = self._conn.execute("SELECT attempts FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            attempt = row["attempts"] if row else 0
        self._conn.execute(
            """INSERT INTO job_events (job_id, attempt, at, from_status, to_status, worker_id, detail)
                VALUES (?,?,?,?,?,?,?)""",
            (job_id, attempt, _iso(_now()), from_status, to_status, worker_id, detail),
        )
