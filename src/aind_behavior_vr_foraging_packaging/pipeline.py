"""Top-level pipeline factory.

Selects the correct processor set for a dataset version and returns it ready
to pass to ``NwbSession.run()``. Version dispatch is automatic: datasets with
schema version < 0.6.0 receive the legacy processor variants.

See ``scripts/example_parquet_pipeline.py`` for usage examples.
"""

import logging
from pathlib import Path

import pandas as pd
import semver
from contraqctor.contract import Dataset

from ._base import AbstractProcessor
from .processing import (
    LegacyPositionAndVelocityProcessor,
    LegacyTrialTableProcessor,
    LicksProcessor,
    PositionAndVelocityProcessor,
    SniffingProcessor,
    SoftwareEventsProcessor,
    TrialTableProcessor,
)

logger = logging.getLogger(__name__)

_LEGACY_VERSION_CUTOFF = semver.Version(major=0, minor=6, patch=0)


def create_processors(
    dataset: Dataset,
    *,
    raise_on_error: bool = False,
    sampling_rate_hz: float | None = 250.0,
) -> list[AbstractProcessor]:
    """Return the ordered processor list for *dataset*, dispatching on version.

    Parameters
    ----------
    dataset:
        The loaded contraqctor Dataset. Its ``.version`` attribute determines
        which processor variants are selected.
    raise_on_error:
        Passed through to every processor. When ``True``, any parsing anomaly
        raises; when ``False`` (default) it logs a warning and continues.
    sampling_rate_hz:
        Target downsampling rate for position/velocity. ``None`` keeps the
        native encoder resolution. Defaults to 250 Hz.

    Returns
    -------
    list[AbstractProcessor]
        Processors in the order they must be applied: processing module first,
        then trial table, then continuous streams.
    """
    version = semver.Version.parse(str(dataset.version))
    is_legacy = version < _LEGACY_VERSION_CUTOFF

    if is_legacy:
        logger.info("Dataset version %s < %s — using legacy processors.", version, _LEGACY_VERSION_CUTOFF)
        trial_table_cls = LegacyTrialTableProcessor
        pos_vel_cls = LegacyPositionAndVelocityProcessor
    else:
        logger.info("Dataset version %s — using current processors.", version)
        trial_table_cls = TrialTableProcessor
        pos_vel_cls = PositionAndVelocityProcessor

    return [
        trial_table_cls(dataset, raise_on_error=raise_on_error),
        pos_vel_cls(dataset, sampling_rate_hz=sampling_rate_hz, raise_on_error=raise_on_error),
        LicksProcessor(dataset, raise_on_error=raise_on_error),
        SniffingProcessor(dataset, raise_on_error=raise_on_error),
        SoftwareEventsProcessor(dataset, raise_on_error=raise_on_error),
    ]


def get_trial_table_processor(
    dataset: Dataset,
    *,
    raise_on_error: bool = False,
) -> TrialTableProcessor | LegacyTrialTableProcessor:
    """Return the correct trial-table processor for *dataset*'s version."""
    version = semver.Version.parse(str(dataset.version))
    cls = LegacyTrialTableProcessor if version < _LEGACY_VERSION_CUTOFF else TrialTableProcessor
    return cls(dataset, raise_on_error=raise_on_error)


def get_position_velocity_processor(
    dataset: Dataset,
    *,
    sampling_rate_hz: float | None = 250.0,
    raise_on_error: bool = False,
) -> PositionAndVelocityProcessor | LegacyPositionAndVelocityProcessor:
    """Return the correct position/velocity processor for *dataset*'s version."""
    version = semver.Version.parse(str(dataset.version))
    cls = LegacyPositionAndVelocityProcessor if version < _LEGACY_VERSION_CUTOFF else PositionAndVelocityProcessor
    return cls(dataset, sampling_rate_hz=sampling_rate_hz, raise_on_error=raise_on_error)


def run_session(
    dataset: Dataset,
    output_dir: Path,
    *,
    raise_on_error: bool = False,
    sampling_rate_hz: float | None = 250.0,
) -> dict[str, pd.DataFrame]:
    """Run all processors and save their outputs as parquet files.

    Each processor's ``output_name`` attribute determines its parquet filename,
    e.g. ``trials.parquet``, ``position_velocity.parquet``, etc.

    Parameters
    ----------
    dataset:
        Loaded contraqctor Dataset. Its version determines which processor
        variants are selected (legacy vs current).
    output_dir:
        Directory where parquet files are written. Created if absent.
    raise_on_error:
        Passed to all processors.
    sampling_rate_hz:
        Downsampling target for position/velocity. ``None`` = native resolution.

    Returns
    -------
    dict[str, pd.DataFrame]
        Computed DataFrames keyed by each processor's ``output_name``.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_data: dict[str, pd.DataFrame] = {}
    for proc in create_processors(dataset, raise_on_error=raise_on_error, sampling_rate_hz=sampling_rate_hz):
        name = proc.output_name
        logger.info("compute: %s → %s.parquet", proc.__class__.__name__, name)
        df = proc.compute()
        df.to_parquet(output_dir / f"{name}.parquet")
        all_data[name] = df
        logger.info("  saved %d rows", len(df))

    return all_data
