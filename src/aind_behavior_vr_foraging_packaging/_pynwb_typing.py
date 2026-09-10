"""Type-checking-only aliases for pynwb classes built dynamically at runtime.

pynwb took one look at Python's type system, said "cute," and built its own object
model out of YAML specs and runtime metaclass wizardry instead. Every ``NWBFile`` and
``ProcessingModule`` is basically a Mr. Potato Head assembled at import time: the
attributes are real, they're just allergic to being declared anywhere ``ty`` can see
them.

The fix: alias each name to ``Any`` for ``ty`` only (via ``TYPE_CHECKING``), so every
attribute access type-checks, while the real pynwb class is still used at runtime.
``NWBFile`` is never constructed in our own code (it always comes from
``aind_nwb_utils.create_base_nwb_file``), so a bare `= Any` alias is enough.
``ProcessingModule`` *is* constructed directly (see ``_licks.py``, ``_sniffing.py``,
``_position_and_velocity.py``), and a bare `= Any` alias would make it the
`typing.Any` special form, which isn't callable -- so it gets a `TYPE_CHECKING`-only
stub function instead, mirroring what a `.pyi` stub would declare: same idea, still
callable, still typed to return `Any`.
"""

import typing as ty

if ty.TYPE_CHECKING:
    NWBFile = ty.Any

    def ProcessingModule(*args: ty.Any, **kwargs: ty.Any) -> ty.Any: ...
else:
    from pynwb import NWBFile
    from pynwb.base import ProcessingModule

__all__ = ["NWBFile", "ProcessingModule"]
