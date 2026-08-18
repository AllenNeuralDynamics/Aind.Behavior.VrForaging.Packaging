"""``vr-foraging-server`` — the server CLI.

Two audiences in one command, split by whether ``--config`` is needed:

``process`` runs INSIDE a processor container — one session, one sidecar, no ledger
and no credentials. It is here rather than in ``vr-foraging-packaging`` because the
sidecar belongs to this package.

Everything else is host-side: discovery, the ledger, the worker that launches
those containers, and the dashboard over the result.

Plain ``argparse`` subparsers, unlike the packaging CLI's pydantic-settings
models — these subcommands' argument shapes are too heterogeneous to share one.
"""

import argparse
import csv
import logging
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import PipelineConfig
    from .ledger import Ledger
    from .models import Job
    from .worker import Worker

# Nothing host-side is imported at module scope: `process` is the only subcommand that
# runs in the container, and it needs no ledger, sources or stores. Host-side commands
# import what they need inside their own function.
logger = logging.getLogger(__name__)


def _setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
        root.addHandler(handler)


def _load_config(path: Path) -> "PipelineConfig":
    from .config import PipelineConfig

    return PipelineConfig.from_yaml(path)


def _ledger(path: Path | str) -> "Ledger":
    from .ledger import Ledger

    return Ledger(path)


def _worker(config: "PipelineConfig", *, worker_id: str = "cli") -> "Worker":
    from .worker import Worker

    return Worker(config, worker_id=worker_id)


# ---------------------------------------------------------------------------
# process — the container's own command
# ---------------------------------------------------------------------------


def cmd_process(args: argparse.Namespace) -> None:
    """Process one session. Failures propagate, so the container exits nonzero —
    and the sidecar naming the culprit is on disk regardless."""
    from .process import process_one_session

    metadata = process_one_session(
        args.input_dir,
        args.output_dir,
        strict_parsing=args.strict_parsing,
        include=args.include_processors,
        exclude=args.exclude_processors,
        write_parquet=args.write_parquet,
        write_nwb=args.write_nwb,
    )
    logger.info(
        "[%s] %s — %d processor(s), %d warning(s)",
        metadata.session_name,
        metadata.status,
        len(metadata.processors),
        metadata.warn_count,
    )


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


def cmd_ingest(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    worker = _worker(config)
    try:
        if args.dry_run:
            source = worker.source()
            since = worker.ledger.get_watermark(source.name)
            count = 0
            for ref in source.discover(since):
                print(f"{ref.discovered_by}\t{ref.session_name}\t{ref.input_uri}")
                count += 1
                if args.limit and count >= args.limit:
                    break
            print(f"--dry-run: {count} session(s) listed (limit={args.limit or 'none'}); nothing written.")
            return
        n = worker.ingest_once()
        print(f"Ingested {n} new job(s).")
    finally:
        worker.close()


# ---------------------------------------------------------------------------
# work
# ---------------------------------------------------------------------------


def cmd_work(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    if args.keep_work:
        # One-way, so `keep_work_dir: true` in YAML is not overridden by the flag's absence.
        config.worker.keep_work_dir = True
    worker = _worker(config, worker_id=config.worker.id)
    try:
        if args.job_id:
            job = worker.ledger.force_claim(args.job_id, worker.worker_id, config.worker.lease_seconds)
            if job is None:
                print(f"Job {args.job_id} is not pending (already claimed, or does not exist).")
                return
            # A --job-id run claims a real job and publishes real output, so it records
            # its provenance too. Only `run_forever` used to heartbeat.
            worker.heartbeat(running_jobs=1)
            worker.process_job(job)
            print(f"Processed {args.job_id} — see `vr-foraging-server show --job-id {args.job_id}`.")
            return
        worker.run_forever(once=args.once)
    finally:
        worker.close()


# ---------------------------------------------------------------------------
# status / show
# ---------------------------------------------------------------------------


def cmd_status(args: argparse.Namespace) -> None:
    with _ledger(_load_config(args.config).worker.ledger) as ledger:
        jobs = ledger.list_jobs(release=args.release, status=args.kind if args.kind != "session" else None)
        counts: dict[str, int] = {}
        for j in jobs:
            counts[j.status] = counts.get(j.status, 0) + 1
        print("Status counts:")
        for status, n in sorted(counts.items()):
            print(f"  {status:10s} {n}")
        print(f"  {'total':10s} {len(jobs)}")

        for w in ledger.list_workers():
            print(
                f"\nworker {w['worker_id']}: last heartbeat {w['heartbeat_at']}, "
                f"{w['running_jobs']} running, disk_free={w['disk_free_bytes']}\n"
                f"  image: {w['worker_image'] or '(unrecorded — see doctor)'}"
            )


def cmd_show(args: argparse.Namespace) -> None:
    with _ledger(_load_config(args.config).worker.ledger) as ledger:
        job = ledger.get_job(args.job_id) if args.job_id else ledger.get_latest_job_for_session(args.session)
        if job is None:
            print("No such job/session.")
            return
        print(job.model_dump_json(indent=2))
        print("\nEvents:")
        for ev in ledger.list_events(job.job_id):
            print(f"  {ev['at']}  {ev['from_status']} -> {ev['to_status']}  {ev['detail'] or ''}")


# ---------------------------------------------------------------------------
# rerun
# ---------------------------------------------------------------------------


def _select_jobs_for_rerun(ledger: "Ledger", args: argparse.Namespace) -> list["Job"]:
    if args.session:
        job = ledger.get_latest_job_for_session(args.session)
        return [job] if job else []
    if args.tag:
        sessions = ledger.sessions_with_tag(args.tag)
        jobs = [ledger.get_latest_job_for_session(s) for s in sessions]
        return [j for j in jobs if j is not None]
    if args.failed:
        return ledger.list_jobs(status="failed")
    if args.dead:
        return ledger.list_jobs(status="dead")
    if args.subject:
        return [j for j in ledger.list_jobs() if j.subject_id == args.subject]
    if args.all:
        return ledger.list_jobs()
    return []


def cmd_rerun(args: argparse.Namespace) -> None:
    if args.all and not args.confirm:
        print("Refusing `rerun --all` without `--confirm` — this is deliberate.")
        sys.exit(1)
    with _ledger(_load_config(args.config).worker.ledger) as ledger:
        targets = _select_jobs_for_rerun(ledger, args)
        if not targets:
            print("No matching jobs.")
            return
        if args.dry_run:
            for j in targets:
                print(f"would rerun: {j.session_name} (job_id={j.job_id}, status={j.status})")
            print(f"--dry-run: {len(targets)} job(s) would be re-queued; nothing written.")
            return
        for j in targets:
            new_id = ledger.rerun(j.job_id, reason=args.reason, requested_by="cli")
            print(f"{j.session_name}: {j.job_id} -> {new_id}")
        print(f"Re-queued {len(targets)} job(s).")


# ---------------------------------------------------------------------------
# tag / priority
# ---------------------------------------------------------------------------


def _resolve_sessions(ledger: "Ledger", args: argparse.Namespace) -> list[str]:
    if args.session:
        return [args.session]
    if args.tag:
        return ledger.sessions_with_tag(args.tag)
    if getattr(args, "failed", False):
        return [j.session_name for j in ledger.list_jobs(status="failed") if j.session_name]
    return []


def cmd_tag(args: argparse.Namespace) -> None:
    with _ledger(_load_config(args.config).worker.ledger) as ledger:
        sessions = _resolve_sessions(ledger, args)
        if not sessions:
            print("No matching sessions.")
            return
        if args.dry_run:
            for s in sessions:
                print(f"would tag {s}: +{args.add or ''} -{args.remove or ''}")
            return
        for s in sessions:
            if args.add:
                ledger.add_tag(s, args.add, added_by="cli", note=args.note)
            if args.remove:
                ledger.remove_tag(s, args.remove)
        print(f"Updated tags on {len(sessions)} session(s).")


def cmd_priority(args: argparse.Namespace) -> None:
    with _ledger(_load_config(args.config).worker.ledger) as ledger:
        sessions = _resolve_sessions(ledger, args)
        jobs = [ledger.get_latest_job_for_session(s) for s in sessions]
        jobs = [j for j in jobs if j is not None]
        if not jobs:
            print("No matching sessions.")
            return
        for j in jobs:
            if args.set is not None:
                ledger.set_priority(j.job_id, value=args.set)
            elif args.bump is not None:
                ledger.set_priority(j.job_id, bump=args.bump)
            elif args.top:
                ledger.priority_top(j.job_id)
            elif args.bottom:
                ledger.priority_bottom(j.job_id)
        print(f"Updated priority on {len(jobs)} session(s) (no-op on any that are not pending).")


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------


def cmd_aggregate(args: argparse.Namespace) -> None:
    """Aggregate now, for any output store.

    There is deliberately no in-flight gate. This runs against a server that ingests
    continuously, so "nothing is in flight" is a state that may never occur — the
    watermark, not the queue, decides whether there is work to do. Aggregating over
    whatever is complete right now is the point; the next run picks up the rest.
    """
    from .sidecar import aggregate_watermark

    config = _load_config(args.config)
    worker = _worker(config, worker_id=f"aggregate-{uuid.uuid4().hex[:8]}")
    try:
        contributions = worker.contributing_sessions()
        watermark = aggregate_watermark(contributions)
        current = worker.read_aggregate_manifest()
        print(f"{len(contributions)} completed session(s) under {worker.sessions_prefix()}")
        print(
            f"watermark {watermark}"
            + (f" (latest aggregate: {current.get('watermark')})" if current else " (no aggregate published yet)")
        )
        if args.dry_run:
            unchanged = bool(current and current.get("watermark") == watermark)
            days = worker.aggregate_days()
            print(f"{len(days)} dated aggregate(s) present" + (f", newest {days[-1]}" if days else ""))
            print("Up to date — would do nothing." if unchanged else "Would rebuild.")
            return

        job_id, _, n = worker.enqueue_aggregate(force=args.force)
        if job_id is None:
            print("Aggregate is already current (watermark unchanged). Use --force to rebuild anyway.")
            return
        job = worker.ledger.force_claim(job_id, worker.worker_id, config.aggregation.job_timeout_s)
        if job is None:
            print(f"Queued aggregate job {job_id}, but another worker claimed it first — it is running there.")
            return
        worker.process_aggregate_job(job)
        final = worker.ledger.get_job(job_id)
        status = final.status if final else "unknown"
        print(f"Aggregate job {job_id} finished: {status} ({n} session(s))")
        if status != "completed":
            raise SystemExit(1)
        # Which prefix it landed in is the one thing a human running this by hand cannot
        # work out for themselves — the day comes from the manifest, not from `today`.
        published = worker.read_aggregate_manifest()
        day = str(published.get("created_at", ""))[:10] if published else ""
        if day:
            print(f"  wrote    {worker.aggregate_day_uri(day)}")
        print(f"  mirrored {worker.aggregate_latest_uri()}")
    finally:
        worker.close()


# ---------------------------------------------------------------------------
# reap / doctor
# ---------------------------------------------------------------------------


def cmd_reap(args: argparse.Namespace) -> None:
    with _ledger(_load_config(args.config).worker.ledger) as ledger:
        n = ledger.reap_expired_leases()
        print(f"Reaped {n} expired lease(s).")


def cmd_doctor(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    worker = _worker(config, worker_id=f"doctor-{uuid.uuid4().hex[:8]}")
    try:
        problems = worker.doctor()
    finally:
        worker.close()
    if not problems:
        print("OK — no problems found.")
        return
    print(f"{len(problems)} problem(s) found:")
    for p in problems:
        print(f"  - {p}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------


def cmd_reconcile(args: argparse.Namespace) -> None:
    from .stores import get_output_store

    config = _load_config(args.config)
    output_store = get_output_store(config.output.store)
    release_prefix = f"{config.output.uri.rstrip('/')}/{config.release}/sessions/"

    adopted = 0
    with _ledger(config.worker.ledger) as ledger:
        for session_name, sidecar in output_store.iter_completed(release_prefix):
            existing = ledger.get_latest_job_for_session(session_name)
            if existing is not None:
                continue
            if args.dry_run:
                print(f"would adopt: {session_name}")
                adopted += 1
                continue
            job_id = ledger.upsert_job(
                kind="session",
                release=config.release,
                asset_id=None,
                processor_fingerprint=sidecar.get("code", {}).get("version", "unknown"),
                input_store=config.input.store,
                input_uri=sidecar.get("input_uri") or "",
                output_uri=f"{release_prefix}{session_name}/",
                session_name=session_name,
                subject_id=sidecar.get("subject_id"),
            )
            if job_id is not None:
                ledger.complete_job(job_id, partial=sidecar.get("status") == "partial")
                adopted += 1
    verb = "would adopt" if args.dry_run else "adopted"
    print(f"reconcile: {verb} {adopted} session(s) from {release_prefix}")


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


def cmd_export(args: argparse.Namespace) -> None:
    with _ledger(_load_config(args.config).worker.ledger) as ledger:
        jobs = ledger.list_jobs(release=args.release, limit=100_000)
    if not jobs:
        print("No jobs to export.")
        return
    fieldnames = list(Job.model_fields.keys())
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    for j in jobs:
        writer.writerow(j.model_dump())


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


def cmd_serve(args: argparse.Namespace) -> None:
    from . import dashboard

    config = _load_config(args.config)
    dashboard.serve(config.dashboard, str(config.worker.ledger))


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _add_config_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument("--config", type=Path, required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vr-foraging-server", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    # `process` takes no --config: it is what runs INSIDE a processor container,
    # where there is no ledger, no S3 and no daemon — only a mounted session and a
    # place to write. Everything else here is host-side and needs the config.
    p = sub.add_parser("process", help="Process ONE session and write its sidecar (runs in the container)")
    p.add_argument("--input-dir", type=Path, required=True, help="One raw session root; its name is the session id")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--strict-parsing", action="store_true")
    p.add_argument("--include-processors", action="append", default=[], metavar="NAME")
    p.add_argument("--exclude-processors", action="append", default=[], metavar="NAME")
    p.add_argument("--no-write-parquet", dest="write_parquet", action="store_false")
    p.add_argument("--write-nwb", action="store_true")
    p.set_defaults(func=cmd_process)

    p = sub.add_parser("ingest", help="Discover new sessions and enqueue jobs")
    _add_config_arg(p)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("work", help="Run the claim loop (or one job)")
    _add_config_arg(p)
    p.add_argument("--once", action="store_true", help="Process at most one job, then exit")
    p.add_argument("--job-id", default=None, help="Force-claim and run exactly this job")
    p.add_argument(
        "--keep-work",
        action="store_true",
        help="Debugging: leave each job's work directory on the volume instead of deleting it. "
        "Pair with --job-id to inspect one failure; never leave it on for a campaign.",
    )
    p.set_defaults(func=cmd_work)

    p = sub.add_parser("status", help="Ledger counts and worker heartbeat")
    _add_config_arg(p)
    p.add_argument("--kind", default="session")
    p.add_argument("--release", default=None)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("show", help="One job's row + full event history")
    _add_config_arg(p)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--job-id", default=None)
    g.add_argument("--session", default=None)
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("rerun", help="Re-queue sessions for another attempt")
    _add_config_arg(p)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--session", default=None)
    g.add_argument("--tag", default=None)
    g.add_argument("--failed", action="store_true")
    g.add_argument("--dead", action="store_true")
    g.add_argument("--subject", default=None)
    g.add_argument("--all", action="store_true")
    p.add_argument("--reason", default=None)
    p.add_argument("--confirm", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_rerun)

    p = sub.add_parser("tag", help="Attach/remove durable session labels")
    _add_config_arg(p)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--session", default=None)
    g.add_argument("--tag", default=None, help="Select sessions already carrying this tag")
    g.add_argument("--failed", action="store_true")
    p.add_argument("--add", default=None)
    p.add_argument("--remove", default=None)
    p.add_argument("--note", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_tag)

    p = sub.add_parser("priority", help="Adjust queue priority for pending jobs")
    _add_config_arg(p)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--session", default=None)
    g.add_argument("--tag", default=None)
    a = p.add_mutually_exclusive_group(required=True)
    a.add_argument("--set", type=int, default=None)
    a.add_argument("--bump", type=int, default=None)
    a.add_argument("--top", action="store_true")
    a.add_argument("--bottom", action="store_true")
    p.set_defaults(func=cmd_priority)

    p = sub.add_parser("aggregate", help="Rebuild the flat aggregate tables")
    _add_config_arg(p)
    p.add_argument("--force", action="store_true", help="Bypass the in-flight gate")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_aggregate)

    p = sub.add_parser("reap", help="Move expired-lease jobs back to pending/dead")
    _add_config_arg(p)
    p.set_defaults(func=cmd_reap)

    p = sub.add_parser("reconcile", help="Rebuild ledger rows from the output tree")
    _add_config_arg(p)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_reconcile)

    p = sub.add_parser("export", help="Ledger -> CSV on stdout")
    _add_config_arg(p)
    p.add_argument("--release", default=None)
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("serve", help="Run the dashboard")
    _add_config_arg(p)
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("doctor", help="Volume/Docker/credential preflight")
    _add_config_arg(p)
    p.set_defaults(func=cmd_doctor)

    return parser


def main(argv: list[str] | None = None) -> None:
    _setup_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
