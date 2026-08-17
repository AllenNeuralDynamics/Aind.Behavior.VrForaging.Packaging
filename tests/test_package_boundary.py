"""The published package must not depend on the unpublished one.

`orchestration/` is a separate distribution that is never uploaded to PyPI. If
anything under `src/` imports it, the published wheel becomes installable but
broken for everyone outside this repo — and the failure appears at *their* import
time, not in our CI, which has both packages on the path.

A grep, deliberately, rather than an import experiment: it catches a lazy import
inside a function body, which is exactly where this mistake would hide.
"""

import ast
from pathlib import Path

import pytest

_PUBLISHED = Path(__file__).resolve().parents[1] / "src" / "aind_behavior_vr_foraging_packaging"
_FORBIDDEN = "aind_behavior_vr_foraging_orchestration"


def _imported_modules(source: str) -> set[str]:
    """Every module named by an import anywhere in *source*, including inside
    functions and `if TYPE_CHECKING` blocks."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


@pytest.mark.parametrize(
    "path",
    sorted(_PUBLISHED.rglob("*.py")),
    ids=lambda p: str(p.relative_to(_PUBLISHED)),
)
def test_published_package_does_not_import_the_orchestration_package(path: Path):
    offenders = {m for m in _imported_modules(path.read_text(encoding="utf-8")) if m.startswith(_FORBIDDEN)}
    assert not offenders, (
        f"{path.relative_to(_PUBLISHED)} imports {sorted(offenders)}. The dependency runs one way only: "
        "orchestration -> packaging. Pass a callback in (see process_session's on_output/on_error) "
        "rather than importing back out."
    )
