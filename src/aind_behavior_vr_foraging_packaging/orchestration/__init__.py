"""Orchestration layer for running :mod:`aind_behavior_vr_foraging_packaging` at scale.

Ledger, discovery, stores, worker and dashboard around the per-session
processor. Where :mod:`~aind_behavior_vr_foraging_packaging.pipeline` runs
sessions in-process, this runs each one as an ephemeral container and keeps
track of what has been done.

Requires the ``orchestration`` optional dependency group (``pip install
aind-behavior-vr-foraging-packaging[orchestration]``). Nothing in the base
package or the ``vr-foraging-packaging`` CLI depends on this subpackage — that
is what keeps the processor image slim.
"""
