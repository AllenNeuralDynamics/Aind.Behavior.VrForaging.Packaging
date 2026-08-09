"""Example: extract sites, velocity, licks, and sniffing to parquet.

Shows three usage patterns:

    uv run scripts/example_parquet_pipeline.py --session /data/my_session
"""

import argparse
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Pattern 1 — all streams in one call
# ---------------------------------------------------------------------------


def example_all_at_once(session_path: Path, output_dir: Path) -> None:
    """Run every processor and save all four parquets to output_dir."""
    from aind_behavior_vr_foraging.data_contract import dataset

    from aind_behavior_vr_foraging_packaging.session_pipeline import run_session

    ds = dataset(session_path)
    data = run_session(ds, output_dir)

    # Parquet files are on disk; DataFrames also returned in the dict:
    sites_df = data["sites"]  # one row per site
    vel_df = data["position_velocity"]  # position (cm) + velocity (cm/s)
    licks_df = data["licks"]  # is_lick_onset (bool)
    sniffing_df = data["sniffing"]  # voltage (V); attrs["sampling_rate_hz"]

    print(f"sites          : {len(sites_df):>6} rows")
    print(f"position_velocity: {len(vel_df):>6} rows")
    print(f"licks           : {len(licks_df):>6} rows")
    print(f"sniffing        : {len(sniffing_df):>6} rows  (fs={sniffing_df.attrs.get('sampling_rate_hz', '?'):.0f} Hz)")


# ---------------------------------------------------------------------------
# Pattern 2 — individual processor when you only need one stream
# ---------------------------------------------------------------------------


def example_single_stream(session_path: Path, output_dir: Path) -> None:
    """Compute and save only the sniffing signal and trial table."""
    from aind_behavior_vr_foraging.data_contract import dataset

    from aind_behavior_vr_foraging_packaging.processing import SniffingProcessor
    from aind_behavior_vr_foraging_packaging.session_pipeline import get_site_table_processor

    ds = dataset(session_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Trials table
    sites_df = get_site_table_processor(ds).compute()
    sites_df.to_parquet(output_dir / "sites.parquet", index=False)
    print(f"sites: {len(sites_df)} rows → {output_dir / 'sites.parquet'}")

    # Sniffing signal resampled to 1 kHz
    sniff_df = SniffingProcessor(ds, resampling_frequency_hz=1000.0).compute()
    sniff_df.to_parquet(output_dir / "sniffing.parquet")
    fs = sniff_df.attrs.get("sampling_rate_hz", "?")
    print(f"sniffing: {len(sniff_df)} rows @ {fs:.0f} Hz → {output_dir / 'sniffing.parquet'}")


# ---------------------------------------------------------------------------
# Pattern 3 — load back from disk
# ---------------------------------------------------------------------------


def example_load_from_parquet(output_dir: Path) -> None:
    """Read previously saved parquets back into DataFrames."""
    import pandas as pd

    sites = pd.read_parquet(output_dir / "sites.parquet")
    velocity = pd.read_parquet(output_dir / "position_velocity.parquet")
    licks = pd.read_parquet(output_dir / "licks.parquet")
    sniffing = pd.read_parquet(output_dir / "sniffing.parquet")

    print(f"\nLoaded from {output_dir}:")
    print(f"  sites            : {len(sites)} rows, {sites['has_reward'].sum()} rewarded")
    print(f"  position_velocity : {len(velocity)} rows")
    print(f"  licks             : {len(licks)} rows ({licks['is_lick_onset'].sum()} onsets)")
    print(f"  sniffing          : {len(sniffing)} rows")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=Path, required=True, help="Path to the session root directory.")
    parser.add_argument("--output", type=Path, default=None, help="Output directory (default: session/parquet/).")
    parser.add_argument(
        "--pattern", choices=["all", "single", "load"], default="all", help="Which usage pattern to demonstrate."
    )
    args = parser.parse_args()

    output_dir = args.output or (args.session / "parquet")

    if args.pattern == "all":
        print("=== Pattern 1: all streams at once ===")
        example_all_at_once(args.session, output_dir)
    elif args.pattern == "single":
        print("=== Pattern 2: individual processors ===")
        example_single_stream(args.session, output_dir)
    elif args.pattern == "load":
        print("=== Pattern 3: load from parquet ===")
        example_load_from_parquet(output_dir)


if __name__ == "__main__":
    main()
