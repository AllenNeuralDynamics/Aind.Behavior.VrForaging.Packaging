"""What runs *inside* a processor container: one session, plus its sidecar.

``vr-foraging-server process`` is the container's command. It is a thin
wrapper over :func:`~aind_behavior_vr_foraging_packaging.pipeline.session.process_session`
— the packaging library does all the parsing and writing — that adds the one
thing the worker cannot get any other way: a record of *which* processor did
what, readable from outside the container after it exits.

The dataset is loaded here rather than inside ``process_session`` so that a
session which cannot be opened at all still produces a sidecar saying so. That
is the case most worth reporting, and the one where there is no dataset left to
ask for a version.
"""

import logging
from pathlib import Path
from typing import Sequence

from aind_behavior_vr_foraging_packaging.pipeline.session import process_session

from .sidecar import SIDECAR_NAME, SessionOutputMetadata, SidecarRecorder

logger = logging.getLogger(__name__)


def process_one_session(
    input_dir: Path,
    output_dir: Path,
    *,
    strict_parsing: bool = False,
    include: Sequence[str] = (),
    exclude: Sequence[str] = (),
    write_parquet: bool = True,
    write_nwb: bool = False,
) -> SessionOutputMetadata:
    """Process *input_dir* into *output_dir* and write ``output.metadata.json``.

    *input_dir* is one raw session root, and **its basename is the session's
    identity** — every table's ``session_id`` comes from it. The worker is what
    guarantees that (``Worker._resolve_mount``); this function just inherits it.

    Failures propagate. The sidecar is written on the way past, so the container
    exits nonzero *and* leaves behind a record naming the processor that broke —
    which is what :func:`~.runner.classify` reads to tell a bad session
    (``error_kind='data'``) from a bad run (``'code'``).

    Returns
    -------
    SessionOutputMetadata
        The record that was written. Callers inside the container mostly ignore
        it; it is returned so tests do not have to read the file back.
    """
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    recorder = SidecarRecorder(
        output_dir / SIDECAR_NAME,
        session_name=input_dir.name,
        parameters={
            "strict_parsing": strict_parsing,
            "include": list(include),
            "exclude": list(exclude),
            "write_parquet": write_parquet,
            "write_nwb": write_nwb,
        },
    )

    with recorder:
        from aind_behavior_vr_foraging.data_contract import dataset as load_dataset

        dataset = load_dataset(input_dir)
        recorder.dataset_loaded(str(dataset.version))

        process_session(
            dataset,
            output_dir,
            strict_parsing=strict_parsing,
            include=include,
            exclude=exclude,
            write_parquet=write_parquet,
            write_nwb=write_nwb,
            on_output=recorder.on_output,
            on_error=recorder.on_error,
        )
        if write_nwb:
            # process_session propagates an NWB failure, so reaching here means it
            # succeeded. A failure instead surfaces as status="error" with no
            # processor blamed — see SidecarRecorder._status.
            recorder.nwb_ok(output_dir / f"{input_dir.name}.nwb.zarr")

    return recorder.build()
