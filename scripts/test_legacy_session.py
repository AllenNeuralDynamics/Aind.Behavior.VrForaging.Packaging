"""Smoke-test script for legacy VR foraging sessions (v0.3 – v0.5).

Downloads public sessions from S3 into the integration-test cache if not
already present. Private sessions must already be cached locally.

Usage:
    uv run scripts/test_legacy_session.py           # run all configured sessions
    uv run scripts/test_legacy_session.py --id 716458_2024-05-13_09-03-55
    uv run scripts/test_legacy_session.py --local-path /path/to/session
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

CACHE_ROOT = Path(__file__).parent.parent / "tests" / "integration" / ".cache"
DEFAULT_EXCLUDES = ("**/*.mp4", "**/*.avi", "**/*.mkv")


@dataclass
class SessionConfig:
    session_id: str
    bucket: str
    expected_version_prefix: str  # e.g. "0.3", "0.4", "0.5" — for display only


SESSIONS: list[SessionConfig] = [
    SessionConfig(
        session_id="716458_2024-05-13_09-03-55",
        bucket="aind-open-data",
        expected_version_prefix="0.3",
    ),
    SessionConfig(
        session_id="behavior_754580_2024-12-18_09-28-16",
        bucket="aind-private-data-prod-o5171v",
        expected_version_prefix="0.4",
    ),
    SessionConfig(
        session_id="behavior_789919_2025-05-06_20-12-53",
        bucket="aind-private-data-prod-o5171v",
        expected_version_prefix="0.5",
    ),
]


# ---------------------------------------------------------------------------
# S3 download (public buckets only)
# ---------------------------------------------------------------------------


def _is_excluded(rel_key: str, patterns: tuple[str, ...]) -> bool:
    lowered = rel_key.lower()
    return any(PurePosixPath(lowered).match(p.lower()) for p in patterns)


def download_session(bucket: str, session_id: str, cache_root: Path) -> Path:
    """Download all non-excluded objects for *session_id* from a public S3 bucket."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    prefix = f"{session_id}/"
    local_root = cache_root / bucket / session_id

    if local_root.exists():
        log.info("Using cached session at %s", local_root)
        return local_root

    log.info("Downloading s3://%s/%s ...", bucket, prefix)
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED, connect_timeout=10, read_timeout=60))

    paginator = s3.get_paginator("list_objects_v2")
    objects: list[tuple[str, int]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            rel_key = key[len(prefix) :]
            if rel_key and not _is_excluded(rel_key, DEFAULT_EXCLUDES):
                objects.append((key, obj.get("Size", 0)))

    total_mb = sum(s for _, s in objects) / 1024 / 1024
    log.info("  %d objects (%.1f MB)", len(objects), total_mb)

    for i, (key, _) in enumerate(objects, 1):
        dest = cache_root / bucket / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(dest))
        if i % 20 == 0 or i == len(objects):
            print(f"  {i}/{len(objects)} files downloaded", end="\r", flush=True)
    print(flush=True)

    return local_root


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------


def _read_contract_version(session_path: Path) -> str:
    """Read the data-contract version from tasklogic_input.json."""
    tasklogic_path = session_path / "behavior" / "Logs" / "tasklogic_input.json"
    if not tasklogic_path.exists():
        raise FileNotFoundError(f"tasklogic_input.json not found at {tasklogic_path}")
    with open(tasklogic_path) as f:
        data = json.load(f)
    return data.get("schema_version") or data["version"]


def _resolve_loader_version(version: str) -> str:
    """Map a raw schema version to the nearest supported data-contract version."""
    import semver as _semver

    v = _semver.Version.parse(version)
    if v < _semver.Version.parse("0.4.0"):
        log.info("Version %s < 0.4.0; using v0_4_0 contract (compatible layout)", version)
        return "0.4.0"
    return version


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


def run(session_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (trial_table, velocity) DataFrames for the session at *session_path*."""
    from aind_behavior_vr_foraging.data_contract import dataset

    from aind_behavior_vr_foraging_packaging.pipeline import run_session

    version = _read_contract_version(session_path)
    loader_version = _resolve_loader_version(version)
    log.info("Contract version: %s (loader: %s)", version, loader_version)

    ds = dataset(session_path, version=loader_version)
    data = run_session(ds, OUTPUT_DIR / session_path.name)

    return data.get("trials", pd.DataFrame()), data.get("position_velocity", pd.DataFrame())


# ---------------------------------------------------------------------------
# Metrics + serialisation
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path(__file__).parent.parent / "results"


def compute_metrics(df: pd.DataFrame) -> dict:
    reward_sites = df[df["site_label"] == "RewardSite"]
    choices = df["has_choice"].fillna(False)
    rewards = df["has_reward"].fillna(False)

    total_reward_uL = df.loc[rewards, "reward_amount"].sum()
    avg_reward_delay = df.loc[rewards & choices, "reward_delay_duration"].dropna().mean()
    median_stop_duration = df["last_stop_duration"].dropna().median()

    per_patch: dict = {}
    for label, grp in reward_sites.groupby("patch_label"):
        g_choices = grp["has_choice"].fillna(False)
        g_rewards = grp["has_reward"].fillna(False)
        per_patch[str(label)] = {
            "n_sites": len(grp),
            "p_choice": round(float(g_choices.mean()), 3),
            "p_reward": round(float(g_rewards.mean()), 3),
            "p_reward_given_choice": round(float(g_rewards[g_choices].mean()) if g_choices.any() else float("nan"), 3),
        }

    return {
        "n_sites": len(df),
        "n_blocks": int(df["block_index"].nunique()),
        "n_patches": int(df["patch_index"].nunique()),
        "n_choices": int(choices.sum()),
        "n_rewards": int(rewards.sum()),
        "p_choice": round(float(choices.mean()), 3),
        "p_reward": round(float(rewards.mean()), 3),
        "p_reward_given_choice": round(float(rewards[choices].mean()) if choices.any() else float("nan"), 3),
        "total_reward_uL": round(float(total_reward_uL), 1),
        "mean_reward_delay_s": round(float(avg_reward_delay), 3) if not pd.isna(avg_reward_delay) else None,
        "median_stop_duration_s": round(float(median_stop_duration), 3) if not pd.isna(median_stop_duration) else None,
        "per_patch_label": per_patch,
    }


def save_parquet(session_id: str, df: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / f"{session_id}.parquet"
    df.to_parquet(out, index=False)
    log.info("Saved trial table → %s  (%d rows, %.1f KB)", out.name, len(df), out.stat().st_size / 1024)
    return out


def _fmt(v, fmt=".2f") -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return format(v, fmt)


def build_markdown(results: list[tuple[str, str, dict]]) -> str:
    """Build a Markdown report from a list of (session_id, version, metrics) tuples."""
    lines: list[str] = []
    lines.append("# Legacy VR Foraging — Trial Table Report\n")

    # ── Session overview table ──────────────────────────────────────────────
    lines.append("## Session Overview\n")
    lines.append(
        "| Session | Version | Sites | Blocks | Patches | Choices | Rewards | P(choice) | P(reward) | P(reward\\|choice) | Total reward (µL) | Mean reward delay (s) |"
    )
    lines.append(
        "|---------|---------|------:|-------:|--------:|--------:|--------:|----------:|----------:|------------------:|------------------:|----------------------:|"
    )
    for sid, ver, m in results:
        lines.append(
            f"| `{sid}` | v{ver}.x"
            f" | {m['n_sites']}"
            f" | {m['n_blocks']}"
            f" | {m['n_patches']}"
            f" | {m['n_choices']}"
            f" | {m['n_rewards']}"
            f" | {_fmt(m['p_choice'])}"
            f" | {_fmt(m['p_reward'])}"
            f" | {_fmt(m['p_reward_given_choice'])}"
            f" | {_fmt(m['total_reward_uL'], '.1f')}"
            f" | {_fmt(m['mean_reward_delay_s'], '.3f')} |"
        )
    lines.append("")

    # ── Per-session breakdowns ──────────────────────────────────────────────
    lines.append("## Per-Session Breakdown\n")
    for sid, ver, m in results:
        lines.append(f"### `{sid}` (v{ver}.x)\n")

        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Sites | {m['n_sites']} |")
        lines.append(f"| Blocks | {m['n_blocks']} |")
        lines.append(f"| Patches | {m['n_patches']} |")
        lines.append(f"| Choices | {m['n_choices']} (P={_fmt(m['p_choice'])}) |")
        lines.append(f"| Rewards | {m['n_rewards']} (P={_fmt(m['p_reward'])}) |")
        lines.append(f"| P(reward\\|choice) | {_fmt(m['p_reward_given_choice'])} |")
        lines.append(f"| Total reward | {_fmt(m['total_reward_uL'], '.1f')} µL |")
        lines.append(f"| Mean reward delay | {_fmt(m['mean_reward_delay_s'], '.3f')} s |")
        if m["median_stop_duration_s"] is not None:
            lines.append(f"| Median stop duration | {_fmt(m['median_stop_duration_s'], '.3f')} s |")
        lines.append("")

        if m["per_patch_label"]:
            lines.append("**Per patch label (RewardSite only)**\n")
            lines.append("| Patch label | n sites | P(choice) | P(reward) | P(reward\\|choice) |")
            lines.append("|-------------|--------:|----------:|----------:|------------------:|")
            for label, p in m["per_patch_label"].items():
                lines.append(
                    f"| {label}"
                    f" | {p['n_sites']}"
                    f" | {_fmt(p['p_choice'])}"
                    f" | {_fmt(p['p_reward'])}"
                    f" | {_fmt(p['p_reward_given_choice'])} |"
                )
            lines.append("")

    return "\n".join(lines)


def print_summary(session_id: str, df: pd.DataFrame, expected_version: str) -> None:
    m = compute_metrics(df)
    print(f"\n{'=' * 65}")
    print(f"Session : {session_id}  [expected v{expected_version}.x]")
    print(f"{'=' * 65}")
    print(f"  Sites              : {m['n_sites']}")
    print(f"  Blocks             : {m['n_blocks']}")
    print(f"  Patches            : {m['n_patches']}")
    print(f"  Choices            : {m['n_choices']}  (P={m['p_choice']:.2f})")
    print(f"  Rewards            : {m['n_rewards']}  (P={m['p_reward']:.2f})")
    print(f"  P(reward|choice)   : {m['p_reward_given_choice']:.2f}")
    print(f"  Total reward       : {m['total_reward_uL']:.1f} µL")
    if m["mean_reward_delay_s"] is not None:
        print(f"  Mean reward delay  : {m['mean_reward_delay_s']:.3f} s")
    if m["median_stop_duration_s"] is not None:
        print(f"  Median stop dur    : {m['median_stop_duration_s']:.3f} s")
    print()

    if m["per_patch_label"]:
        print("  Per-patch-label (RewardSite only):")
        for label, p in m["per_patch_label"].items():
            print(
                f"    {label:30s}  n={p['n_sites']:4d}"
                f"  P(choice)={p['p_choice']:.2f}"
                f"  P(reward)={p['p_reward']:.2f}"
                f"  P(reward|choice)={p['p_reward_given_choice']:.2f}"
            )
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", default=None, help="Run only the session with this ID.")
    parser.add_argument(
        "--local-path", type=Path, default=None, help="Run a single local path (implies --id from the path name)."
    )
    args = parser.parse_args()

    if args.local_path:
        session_path = args.local_path
        session_id = session_path.name
        trial_df, velocity_df = run(session_path)
        print_summary(session_id, trial_df, "?")
        if trial_df.empty:
            log.error("Trial table is empty.")
            sys.exit(1)
        return

    targets = [s for s in SESSIONS if args.id is None or s.session_id == args.id]
    if not targets:
        log.error("No session with id=%s found in SESSIONS list.", args.id)
        sys.exit(1)

    failed: list[str] = []
    report_entries: list[tuple[str, str, dict]] = []

    for cfg in targets:
        local_root = CACHE_ROOT / cfg.bucket / cfg.session_id
        if not local_root.exists():
            if cfg.bucket == "aind-open-data":
                local_root = download_session(cfg.bucket, cfg.session_id, CACHE_ROOT)
            else:
                log.error(
                    "Session %s not in cache (%s). Private bucket — download manually.",
                    cfg.session_id,
                    local_root,
                )
                failed.append(cfg.session_id)
                continue

        try:
            trial_df, velocity_df = run(local_root)
            m = compute_metrics(trial_df)
            print_summary(cfg.session_id, trial_df, cfg.expected_version_prefix)
            if trial_df.empty:
                raise ValueError("Trial table is empty")
            report_entries.append((cfg.session_id, cfg.expected_version_prefix, m))
            log.info("✓ %s — %d sites, %d velocity samples", cfg.session_id, len(trial_df), len(velocity_df))
        except Exception as exc:
            log.error("✗ %s FAILED: %s", cfg.session_id, exc)
            failed.append(cfg.session_id)

    if report_entries:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        md_path = OUTPUT_DIR / "legacy_report.md"
        md_path.write_text(build_markdown(report_entries), encoding="utf-8")
        log.info("Saved report → %s", md_path)

    print()
    if failed:
        log.error("FAILED sessions: %s", ", ".join(failed))
        sys.exit(1)
    log.info("All %d sessions processed successfully.", len(targets))


if __name__ == "__main__":
    main()
