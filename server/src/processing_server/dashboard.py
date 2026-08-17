"""Dashboard — one sortable table of sessions, per-job log links, and a
whitelisted set of queue actions. stdlib ``http.server`` only.

No authentication: bind to ``127.0.0.1`` (the default) and reach it over an
SSH tunnel. A CSRF token in the action form is the only defense needed at
that trust boundary — see ``dashboard.allow_actions`` to disable writes
entirely and fall back to a read-only view.
"""

import html
import logging
import secrets
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .config import DashboardConfig
from .ledger import Ledger
from .models import Job

logger = logging.getLogger(__name__)

#: Fixed allow-list — sorting is a query parameter, never raw user SQL.
_SORTABLE_COLUMNS = [
    "session_name",
    "status",
    "error",
    "error_kind",
    "failed_processors",
    "warn_count",
    "priority",
    "attempts",
    "run_count",
    "duration_s",
    "t_run_s",
    "read_bytes",
    "finished_at",
    "session_start",
    "subject_id",
    "created_at",
]

#: The whitelisted write surface — nothing here ever touches an outcome
#: field (exit_code, error, sidecar, image_digest, …); those are the worker's alone.
_ACTIONS = {"queue", "priority_top", "priority_bottom", "priority_bump", "priority_drop", "skip", "tag", "untag"}


def _esc(v: object) -> str:
    return "" if v is None else html.escape(str(v))


def _age(iso: str | None) -> str:
    if not iso:
        return "never"
    try:
        then = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    delta = datetime.now(timezone.utc) - then
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    return f"{s // 3600}h ago"


def _worker_header(ledger: Ledger) -> str:
    workers = ledger.list_workers()
    if not workers:
        return "<p><em>No worker has ever heartbeated.</em></p>"
    parts = []
    for w in workers:
        free = w["disk_free_bytes"]
        free_str = f"{free / 1e9:.0f} GB free" if free is not None else "disk unknown"
        # Digest only: the full ref is a ~120-char URI that would swamp a one-line
        # header, and the repository half is identical for every worker anyway.
        image = w["worker_image"]
        image_str = f"@{image.rsplit('@', 1)[1][:19]}…" if image and "@" in image else "image unrecorded"
        parts.append(
            f"worker {_esc(w['worker_id'])} · last seen {_age(w['heartbeat_at'])} · "
            f"{w['running_jobs']} running · {free_str} · {_esc(image_str)}"
        )
    return "<p>" + " &nbsp;|&nbsp; ".join(parts) + "</p>"


def _row_html(job: Job, csrf_token: str) -> str:
    log_link = f'<a href="/log/{job.job_id}">log</a>' if job.log_uri else ""
    return f"""<tr>
<td><input type="checkbox" name="job_id" value="{_esc(job.job_id)}" form="actions-form"></td>
<td><a href="/session/{_esc(job.session_name)}">{_esc(job.session_name)}</a></td>
<td>{_esc(job.status)}{" (partial)" if job.partial else ""}</td>
<td>{_esc(job.error_kind)}</td>
<td>{_esc(job.error)}</td>
<td>{_esc(job.failed_processors)}</td>
<td>{job.warn_count}</td>
<td>{job.priority}</td>
<td>{_esc(job.tags)}</td>
<td>{job.attempts}</td>
<td>{job.run_count}</td>
<td>{f"{job.duration_s:.1f}" if job.duration_s is not None else ""}</td>
<td>{_esc(job.finished_at)}</td>
<td>{log_link}</td>
</tr>"""


_STYLE = """
body { font-family: system-ui, sans-serif; margin: 1.5rem; }
table { border-collapse: collapse; width: 100%; font-size: 0.85rem; }
th, td { border: 1px solid #ccc; padding: 0.25rem 0.5rem; text-align: left; }
th a { color: inherit; text-decoration: none; }
th a:hover { text-decoration: underline; }
tr:nth-child(even) { background: #f7f7f7; }
form.filters, form.actions { margin-bottom: 1rem; }
input[type=text] { padding: 0.2rem; }
button { padding: 0.25rem 0.6rem; }
"""


def render_index(
    ledger: Ledger,
    *,
    sort: str,
    status: str | None,
    release: str | None,
    tag: str | None,
    q: str | None,
    csrf_token: str,
    allow_actions: bool,
) -> str:
    order_by = f"{sort} DESC" if sort in _SORTABLE_COLUMNS else "priority DESC, created_at"
    jobs = ledger.list_jobs(status=status, release=release, tag=tag, session_name_like=q, order_by=order_by, limit=2000)

    header_cells = [
        "",
        "session_name",
        "status",
        "error_kind",
        "error",
        "failed_processors",
        "warn_count",
        "priority",
        "tags",
        "attempts",
        "run_count",
        "duration_s",
        "finished_at",
        "log",
    ]
    th_cells = []
    for c in header_cells:
        if c in _SORTABLE_COLUMNS:
            th_cells.append(f'<th><a href="?sort={c}">{c}</a></th>')
        else:
            th_cells.append(f"<th>{c}</th>")

    rows = "\n".join(_row_html(j, csrf_token) for j in jobs)

    actions_html = ""
    if allow_actions:
        actions_html = f"""
<form class="actions" method="post" action="/action" id="actions-form">
  <input type="hidden" name="csrf_token" value="{csrf_token}">
  <button name="action" value="queue">Queue (rerun)</button>
  <button name="action" value="priority_top">Priority: top</button>
  <button name="action" value="priority_bottom">Priority: bottom</button>
  <button name="action" value="priority_bump">Priority: +1</button>
  <button name="action" value="priority_drop">Priority: -1</button>
  <button name="action" value="skip">Remove from queue</button>
  <input type="text" name="tag_name" placeholder="tag name" size="12">
  <button name="action" value="tag">Tag</button>
  <button name="action" value="untag">Untag</button>
  <input type="hidden" name="confirm" value="0">
</form>"""

    return f"""<!doctype html><html><head><title>vr-foraging-server</title><style>{_STYLE}</style></head>
<body>
<h1>Session queue</h1>
{_worker_header(ledger)}
<form class="filters" method="get">
  <input type="text" name="q" placeholder="session name contains…" value="{_esc(q or "")}">
  <input type="text" name="status" placeholder="status" value="{_esc(status or "")}">
  <input type="text" name="release" placeholder="release" value="{_esc(release or "")}">
  <input type="text" name="tag" placeholder="tag" value="{_esc(tag or "")}">
  <button type="submit">Filter</button>
</form>
{actions_html}
<table>
<thead><tr>{"".join(th_cells)}</tr></thead>
<tbody>
{rows}
</tbody>
</table>
<p>{len(jobs)} row(s) shown (limit 2000). Refreshes every {{refresh_s}}s.</p>
</body></html>"""


def render_confirm(action: str, job_ids: list[str], csrf_token: str) -> str:
    hidden = "\n".join(f'<input type="hidden" name="job_id" value="{_esc(j)}">' for j in job_ids)
    return f"""<!doctype html><html><body>
<h1>Confirm: {_esc(action)} on {len(job_ids)} sessions?</h1>
<form method="post" action="/action">
  <input type="hidden" name="csrf_token" value="{csrf_token}">
  <input type="hidden" name="action" value="{_esc(action)}">
  <input type="hidden" name="confirm" value="1">
  {hidden}
  <button type="submit">Yes, proceed</button>
</form>
<p><a href="/">Cancel</a></p>
</body></html>"""


def apply_action(ledger: Ledger, action: str, job_ids: list[str], *, tag_name: str | None) -> None:
    """Apply *action* to every job in *job_ids*. Refused on non-pending rows where
    that would be meaningless — the ledger's own guards (``WHERE status=
    'pending'``) make most of these naturally no-ops rather than errors."""
    for job_id in job_ids:
        job = ledger.get_job(job_id)
        if job is None:
            continue
        if action == "queue":
            ledger.rerun(job_id, reason="requeued from dashboard", requested_by="dashboard")
        elif action == "priority_top":
            ledger.priority_top(job_id)
        elif action == "priority_bottom":
            ledger.priority_bottom(job_id)
        elif action == "priority_bump":
            ledger.set_priority(job_id, bump=1)
        elif action == "priority_drop":
            ledger.set_priority(job_id, bump=-1)
        elif action == "skip":
            ledger.skip(job_id)
        elif action == "tag" and tag_name and job.session_name:
            ledger.add_tag(job.session_name, tag_name, added_by="dashboard")
        elif action == "untag" and tag_name and job.session_name:
            ledger.remove_tag(job.session_name, tag_name)


class _Response:
    """What a route handler hands back — the HTTP verb methods below only serialise it."""

    __slots__ = ("status", "content_type", "body", "location")

    def __init__(
        self,
        body: str,
        *,
        status: int = 200,
        content_type: str = "text/html; charset=utf-8",
        location: str | None = None,
    ) -> None:
        self.status = status
        self.content_type = content_type
        self.body = body
        self.location = location


def _route_get(
    path: str, query: dict[str, str], config: DashboardConfig, ledger_path: str, csrf_token: str
) -> _Response:
    with Ledger(ledger_path) as ledger:
        if path in ("/", ""):
            body = render_index(
                ledger,
                sort=query.get("sort", "priority"),
                status=query.get("status") or None,
                release=query.get("release") or None,
                tag=query.get("tag") or None,
                q=query.get("q") or None,
                csrf_token=csrf_token,
                allow_actions=config.allow_actions,
            ).replace("{refresh_s}", str(config.refresh_s))
            return _Response(body)
        if path.startswith("/log/"):
            return _route_get_log(ledger, path.removeprefix("/log/"))
        if path.startswith("/session/"):
            return _route_get_session(ledger, path.removeprefix("/session/"))
        return _Response("<p>Not found.</p>", status=404)


def _route_get_log(ledger: Ledger, job_id: str) -> _Response:
    """Serve a job's log when it is reachable from this host, and say where it is
    when it is not.

    ``log_uri`` names a location in the output store, which for a real campaign is
    S3 — and the dashboard deliberately runs without credentials (no auth, an
    SSH tunnel, a read-only ledger mount). Printing the URI is the honest degradation;
    fetching it would mean giving a read-only viewer the campaign's credentials.
    """
    from .stores import StoreConfigError, _uri_to_path

    job = ledger.get_job(job_id)
    if job is None or not job.log_uri:
        return _Response("<p>No log for this job.</p>", status=404)
    try:
        text = _uri_to_path(job.log_uri).read_text(encoding="utf-8", errors="replace")
    except (OSError, ValueError, StoreConfigError):
        return _Response(f"<p>Log published to <code>{_esc(job.log_uri)}</code>, not readable from here.</p>")
    return _Response(text, content_type="text/plain; charset=utf-8")


def _route_get_session(ledger: Ledger, name: str) -> _Response:
    jobs = ledger.list_jobs(session_name_like=name, order_by="run_count DESC")
    body = "<pre>" + html.escape("\n\n".join(j.model_dump_json(indent=2) for j in jobs)) + "</pre>"
    return _Response(f"<!doctype html><html><body><h1>{_esc(name)}</h1>{body}</body></html>")


def _route_post_action(
    form: dict[str, list[str]], config: DashboardConfig, ledger_path: str, csrf_token: str
) -> _Response:
    if not config.allow_actions:
        return _Response("<p>Actions are disabled (dashboard.allow_actions: false).</p>", status=405)

    token = form.get("csrf_token", [""])[0]
    if not secrets.compare_digest(token, csrf_token):
        return _Response("<p>Invalid or missing CSRF token.</p>", status=403)

    action = form.get("action", [""])[0]
    job_ids = form.get("job_id", [])
    tag_name = form.get("tag_name", [""])[0] or None
    confirmed = form.get("confirm", ["0"])[0] == "1"

    if action not in _ACTIONS or not job_ids:
        return _Response("<p>Nothing to do — no action or no rows selected.</p>", status=400)
    if len(job_ids) > config.confirm_threshold and not confirmed:
        return _Response(render_confirm(action, job_ids, csrf_token))

    with Ledger(ledger_path) as ledger:
        apply_action(ledger, action, job_ids, tag_name=tag_name)
    return _Response("", status=303, location="/")


def make_handler(config: DashboardConfig, ledger_path: str, csrf_token: str) -> type[BaseHTTPRequestHandler]:
    """Build a request-handler class bound to *ledger_path* and *csrf_token*.

    Routing lives in the free functions above; the handler itself only reads
    the request and serialises a :class:`_Response`. A fresh :class:`Ledger`
    per request keeps it stateless and safe under ``ThreadingHTTPServer`` —
    SQLite connections are not shared across threads (see ``ledger.py``).
    """

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            logger.info("%s - %s", self.address_string(), format % args)

        def do_GET(self) -> None:
            parts = urlsplit(self.path)
            query = {k: v[0] for k, v in parse_qs(parts.query).items()}
            self._send(_route_get(parts.path, query, config, ledger_path, csrf_token))

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body_raw = self.rfile.read(length).decode("utf-8")
            form = dict(parse_qs(body_raw))
            self._send(_route_post_action(form, config, ledger_path, csrf_token))

        def _send(self, resp: _Response) -> None:
            encoded = resp.body.encode("utf-8")
            self.send_response(resp.status)
            if resp.location:
                self.send_header("Location", resp.location)
            self.send_header("Content-Type", resp.content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            if encoded:
                self.wfile.write(encoded)

    return Handler


def serve(config: DashboardConfig, ledger_path: str) -> None:
    """Run the dashboard forever. Bind to ``127.0.0.1`` (the default) — there
    is no authentication, so never change ``dashboard.bind`` to ``0.0.0.0``."""
    csrf_token = secrets.token_urlsafe(32)
    handler_cls = make_handler(config, ledger_path, csrf_token)
    with ThreadingHTTPServer((config.bind, config.port), handler_cls) as httpd:
        logger.info(
            "Dashboard serving on http://%s:%d (allow_actions=%s)", config.bind, config.port, config.allow_actions
        )
        httpd.serve_forever()
