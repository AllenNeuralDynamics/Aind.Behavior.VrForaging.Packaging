# Centralize Packaging Provenance Metadata Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace scattered, duplicated provenance stamping with a single `PackagingProvenance` frozen dataclass that propagates the same keys to parquet attrs and NWB automatically.

**Architecture:** A new `_provenance.py` module holds `PackagingProvenance` — a frozen dataclass that is the only place where provenance key names and values are defined. `AbstractProcessor.compute()` and `NwbSession._create_nwb_file()` both call `PackagingProvenance.build(dataset)` and delegate stamping to it. `_write_parquet()` in `pipeline.py` needs no change — it already promotes all `df.attrs` to parquet schema metadata.

**Tech Stack:** Python `dataclasses` (stdlib), `importlib.metadata` (stdlib), `json` (stdlib), `aind_behavior_vr_foraging` (existing dep), `contraqctor` (existing dep), `pytest` (test runner)

---

## Background: what exists today and what's wrong

| Site | Keys written | Problem |
|---|---|---|
| `_base.py:AbstractProcessor.compute()` L74–80 | `packaging_version`, `data_contract_version`, `dataset_version`, `processor` | Key names are string literals; inline `importlib.metadata` import |
| `nwb_file/__init__.py:NwbSession._create_nwb_file()` L82 | only `dataset_version` | Missing `packaging_version` and `data_contract_version`; duplicate version logic |
| `nwb_file/__init__.py:NwbSession.dataset_version` L41–42 | — | Duplicates `AbstractProcessor.dataset_version`; exists only to feed `_create_nwb_file` |

PR #44 (Arjun, branch `39-consider-using-function-from-aind-nwb-utils-to-create-nwb-file`) moves NWB construction to `create_base_nwb_file` from `aind-nwb-utils` and puts `dataset_version` in the NWB `notes` field. This plan is designed to be compatible: after PR #44 merges, only Task 3 needs a small follow-up adjustment (noted inline).

---

## Task 1: Create `_provenance.py` with full test coverage (TDD)

**Files:**
- Create: `src/aind_behavior_vr_foraging_packaging/_provenance.py`
- Create: `tests/test_provenance.py`

### Step 1: Write the failing tests

Create `tests/test_provenance.py`:

```python
"""Tests for PackagingProvenance — the central provenance metadata object."""

import dataclasses
import json

import pytest

from aind_behavior_vr_foraging_packaging._provenance import PackagingProvenance


class _FakeDataset:
    """Minimal stand-in for contraqctor.contract.Dataset."""

    version = "0.7.1"


# ── build ────────────────────────────────────────────────────────────────────


def test_build_returns_provenance_instance():
    prov = PackagingProvenance.build(_FakeDataset())
    assert isinstance(prov, PackagingProvenance)


def test_build_dataset_version_matches_dataset():
    prov = PackagingProvenance.build(_FakeDataset())
    assert prov.dataset_version == "0.7.1"


def test_build_packaging_version_is_non_empty_string():
    prov = PackagingProvenance.build(_FakeDataset())
    assert isinstance(prov.packaging_version, str) and prov.packaging_version


def test_build_data_contract_version_is_non_empty_string():
    prov = PackagingProvenance.build(_FakeDataset())
    assert isinstance(prov.data_contract_version, str) and prov.data_contract_version


def test_provenance_is_immutable():
    prov = PackagingProvenance.build(_FakeDataset())
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        prov.dataset_version = "evil"  # type: ignore[misc]


# ── as_dict ──────────────────────────────────────────────────────────────────


def test_as_dict_contains_all_three_keys():
    prov = PackagingProvenance.build(_FakeDataset())
    d = prov.as_dict()
    assert set(d.keys()) == {"packaging_version", "data_contract_version", "dataset_version"}


def test_as_dict_values_are_all_strings():
    prov = PackagingProvenance.build(_FakeDataset())
    for v in prov.as_dict().values():
        assert isinstance(v, str)


# ── stamp_df_attrs ────────────────────────────────────────────────────────────


def test_stamp_df_attrs_sets_provenance_keys():
    import pandas as pd

    prov = PackagingProvenance.build(_FakeDataset())
    df = pd.DataFrame({"x": [1, 2]})
    prov.stamp_df_attrs(df)
    for key in ("packaging_version", "data_contract_version", "dataset_version"):
        assert key in df.attrs


def test_stamp_df_attrs_does_not_overwrite_existing():
    import pandas as pd

    prov = PackagingProvenance.build(_FakeDataset())
    df = pd.DataFrame()
    df.attrs["packaging_version"] = "pinned"
    prov.stamp_df_attrs(df)
    assert df.attrs["packaging_version"] == "pinned"


def test_stamp_df_attrs_with_processor_name():
    import pandas as pd

    prov = PackagingProvenance.build(_FakeDataset())
    df = pd.DataFrame()
    prov.stamp_df_attrs(df, processor_name="MyProcessor")
    assert df.attrs.get("processor") == "MyProcessor"


def test_stamp_df_attrs_without_processor_name_leaves_key_absent():
    import pandas as pd

    prov = PackagingProvenance.build(_FakeDataset())
    df = pd.DataFrame()
    prov.stamp_df_attrs(df)
    assert "processor" not in df.attrs


# ── to_nwb_notes ─────────────────────────────────────────────────────────────


def test_to_nwb_notes_is_valid_json():
    prov = PackagingProvenance.build(_FakeDataset())
    notes = prov.to_nwb_notes()
    parsed = json.loads(notes)  # must not raise
    assert isinstance(parsed, dict)


def test_to_nwb_notes_contains_all_keys():
    prov = PackagingProvenance.build(_FakeDataset())
    parsed = json.loads(prov.to_nwb_notes())
    assert set(parsed.keys()) == {"packaging_version", "data_contract_version", "dataset_version"}


def test_to_nwb_notes_dataset_version_matches_dataset():
    prov = PackagingProvenance.build(_FakeDataset())
    parsed = json.loads(prov.to_nwb_notes())
    assert parsed["dataset_version"] == "0.7.1"
```

### Step 2: Run to confirm they all fail (module doesn't exist yet)

```
pytest tests/test_provenance.py -v
```

Expected: `ModuleNotFoundError: No module named '...._provenance'`

### Step 3: Write the implementation

Create `src/aind_behavior_vr_foraging_packaging/_provenance.py`:

```python
"""Centralized packaging provenance metadata.

``PackagingProvenance`` is the single source of truth for the keys and values
written to every output format (parquet attrs and NWB). Add a new field here
and both outputs pick it up with no further changes.
"""

from __future__ import annotations

import dataclasses
import importlib.metadata
import json
import typing as ty

import aind_behavior_vr_foraging

if ty.TYPE_CHECKING:
    import pandas as pd
    from contraqctor.contract import Dataset


_PACKAGING_PKG = "aind-behavior-vr-foraging-packaging"


@dataclasses.dataclass(frozen=True)
class PackagingProvenance:
    """Immutable snapshot of packaging provenance for one session.

    Fields
    ------
    packaging_version:
        Version of ``aind-behavior-vr-foraging-packaging`` (this library).
    data_contract_version:
        Version of ``aind-behavior-vr-foraging`` that defines the behavioral
        data schema (the "parser" version).
    dataset_version:
        Version recorded in the session's ``tasklogic_input.json``
        (the "dataset" version).
    """

    packaging_version: str
    data_contract_version: str
    dataset_version: str

    # ── factories ────────────────────────────────────────────────────────────

    @classmethod
    def build(cls, dataset: "Dataset") -> "PackagingProvenance":
        """Build provenance from a loaded contraqctor ``Dataset``.

        This is the only place where version values are computed — both
        :class:`AbstractProcessor` and :class:`NwbSession` call this.
        """
        return cls(
            packaging_version=importlib.metadata.version(_PACKAGING_PKG),
            data_contract_version=aind_behavior_vr_foraging.__semver__,
            dataset_version=str(dataset.version),
        )

    # ── adapters ─────────────────────────────────────────────────────────────

    def as_dict(self) -> dict[str, str]:
        """Return a flat ``str → str`` dict.

        Safe for use as parquet schema metadata or ``df.attrs`` values.
        """
        return dataclasses.asdict(self)

    def stamp_df_attrs(
        self,
        df: "pd.DataFrame",
        *,
        processor_name: str | None = None,
    ) -> None:
        """Write provenance keys into ``df.attrs`` non-destructively.

        Keys already present (e.g. ``sampling_rate_hz`` set by ``_compute``)
        are preserved via ``setdefault``.

        Parameters
        ----------
        df:
            DataFrame whose ``attrs`` will be updated in-place.
        processor_name:
            When provided, also stamps ``df.attrs["processor"]``.
        """
        for k, v in self.as_dict().items():
            df.attrs.setdefault(k, v)
        if processor_name is not None:
            df.attrs.setdefault("processor", processor_name)

    def to_nwb_notes(self) -> str:
        """Serialize all provenance keys as a compact JSON string.

        Intended for the ``notes`` field of an ``NWBFile``. Consumers can
        recover the dict with ``json.loads(nwb_file.notes)``.
        """
        return json.dumps(self.as_dict())
```

### Step 4: Run tests — all must pass

```
pytest tests/test_provenance.py -v
```

Expected: all 15 tests PASS, no warnings about provenance keys.

### Step 5: Commit

```
git add src/aind_behavior_vr_foraging_packaging/_provenance.py tests/test_provenance.py
git commit -m "feat: add PackagingProvenance — central provenance metadata dataclass"
```

---

## Task 2: Wire `AbstractProcessor.compute()` to use `PackagingProvenance`

**Files:**
- Modify: `src/aind_behavior_vr_foraging_packaging/_base.py` (lines 61–81)
- Verify: `tests/test_abstract_processor.py` (no changes needed — existing assertions must still pass)

### Step 1: Confirm existing tests pass on unmodified code

```
pytest tests/test_abstract_processor.py -v
```

Expected: PASS (establishes baseline).

### Step 2: Edit `_base.py`

Replace the entire `compute` method (lines 61–81) and remove the now-redundant `parser_version` property (lines 42–44) and `dataset_version` property (lines 38–40) **only if** they are not used anywhere else in the class.

> **Check first:** Run `grep -n "parser_version\|dataset_version" src/aind_behavior_vr_foraging_packaging/_base.py` — confirm neither property is referenced outside `compute()`. They are only used there; the properties can be removed.

Also remove the `import aind_behavior_vr_foraging` at line 5 **only if** nothing else in `_base.py` uses it after this change (check with the grep above). It won't be needed after refactoring.

New `_base.py` — full file after changes:

```python
import abc
import re
import typing as ty

import pandas as pd
from contraqctor.contract import Dataset

from ._provenance import PackagingProvenance


def _class_name_to_snake(name: str) -> str:
    """Convert a CamelCase class name to snake_case, e.g. ``LicksProcessor`` → ``licks_processor``."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


class AbstractProcessor(abc.ABC):
    #: Override in subclasses to set a canonical parquet filename stem (e.g. ``"trials"``).
    #: When ``None`` (the default), ``output_name`` falls back to a snake_case of the class name.
    __output_name__: ty.ClassVar[str | None] = None

    @property
    def output_name(self) -> str:
        """Canonical name used as the parquet filename stem.

        Returns ``__output_name__`` if defined on the class, otherwise a
        snake_case of the class name (e.g. ``LicksProcessor`` → ``licks_processor``).
        """
        return self.__class__.__output_name__ or _class_name_to_snake(type(self).__name__)

    def __init__(self, dataset: Dataset, *, raise_on_error: bool = False) -> None:
        self._dataset = dataset
        self._raise_on_error = raise_on_error

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @abc.abstractmethod
    def _compute(self) -> pd.DataFrame:
        """Compute this processor's output as a DataFrame.

        Subclasses implement this method. Callers should use :meth:`compute`,
        which wraps ``_compute`` and stamps provenance metadata into ``df.attrs``.
        """
        raise NotImplementedError

    def compute(self) -> pd.DataFrame:
        """Return the processor's output DataFrame with provenance metadata in attrs.

        Calls :meth:`_compute`, then stamps ``df.attrs`` via
        :class:`~aind_behavior_vr_foraging_packaging._provenance.PackagingProvenance`
        with:

        - ``packaging_version``: version of this package
        - ``data_contract_version``: version of ``aind-behavior-vr-foraging``
        - ``dataset_version``: version recorded in the session's ``tasklogic_input.json``
        - ``processor``: this processor's class name

        Attrs already set by ``_compute`` (e.g. ``sampling_rate_hz`` from
        :class:`SniffingProcessor`) are preserved.
        """
        df = self._compute()
        PackagingProvenance.build(self._dataset).stamp_df_attrs(df, processor_name=type(self).__name__)
        return df

    def nwbize(self, nwb_file: ty.Any) -> ty.Any:
        """Write this processor's output to *nwb_file* and return it.

        Default implementation is a no-op. Override in subclasses that have
        an NWB representation. May call ``compute()`` internally; the two
        methods are intentionally independent (no shared state).
        """
        return nwb_file

    def with_raise_errors(self, raise_on_error: bool = True) -> ty.Self:
        self._raise_on_error = raise_on_error
        return self

    @property
    def raise_on_error(self) -> bool:
        return self._raise_on_error
```

> **Note on `semver` import:** The original file imported `semver` for the `dataset_version`/`parser_version` properties, which are removed. Verify `semver` is not used elsewhere in `_base.py` and remove that import too if unused.

### Step 3: Run the existing abstract processor tests

```
pytest tests/test_abstract_processor.py -v
```

Expected: all PASS. The tests assert the same 4 keys (`packaging_version`, `data_contract_version`, `dataset_version`, `processor`) — they should still hold because `stamp_df_attrs` sets exactly those keys.

### Step 4: Run the full test suite (except integration)

```
pytest tests/ --ignore=tests/integration -v
```

Expected: all PASS.

### Step 5: Commit

```
git add src/aind_behavior_vr_foraging_packaging/_base.py
git commit -m "refactor: use PackagingProvenance in AbstractProcessor.compute()"
```

---

## Task 3: Wire `NwbSession` to use `PackagingProvenance`

**Files:**
- Modify: `src/aind_behavior_vr_foraging_packaging/nwb_file/__init__.py`
- Modify: `tests/test_nwb_session.py` (add provenance assertions)

### Step 1: Write new failing tests in `test_nwb_session.py`

Open `tests/test_nwb_session.py` and add these tests alongside the existing ones (do not remove any existing test):

```python
import json


def test_nwb_file_notes_is_valid_json(nwb_session_with_local_schema):
    """NWB notes field must hold parseable JSON provenance."""
    nwb = nwb_session_with_local_schema.process()
    parsed = json.loads(nwb.notes)
    assert isinstance(parsed, dict)


def test_nwb_file_notes_contains_all_provenance_keys(nwb_session_with_local_schema):
    """All three provenance keys must be present in NWB notes."""
    nwb = nwb_session_with_local_schema.process()
    parsed = json.loads(nwb.notes)
    assert "packaging_version" in parsed
    assert "data_contract_version" in parsed
    assert "dataset_version" in parsed


def test_nwb_notes_dataset_version_matches_dataset(nwb_session_with_local_schema):
    """dataset_version in NWB notes must match the dataset's actual version."""
    session = nwb_session_with_local_schema
    nwb = session.process()
    parsed = json.loads(nwb.notes)
    assert parsed["dataset_version"] == str(session.dataset.version)
```

> **Fixture name:** Look at the existing tests in `test_nwb_session.py` to confirm the correct fixture name for a local-schema session. Use whatever the existing tests use (likely something like `nwb_session` or a fixture from `conftest.py`).

### Step 2: Run the new tests to confirm they fail

```
pytest tests/test_nwb_session.py::test_nwb_file_notes_is_valid_json \
       tests/test_nwb_session.py::test_nwb_file_notes_contains_all_provenance_keys \
       tests/test_nwb_session.py::test_nwb_notes_dataset_version_matches_dataset -v
```

Expected: FAIL — `nwb.notes` is `None` or does not contain JSON provenance.

### Step 3: Edit `nwb_file/__init__.py`

Two changes:

**a) Delete `NwbSession.dataset_version` property** (lines 40–42) — it was only there to feed `_create_nwb_file` and is now replaced by `PackagingProvenance.build`.

**b) Rewrite `_create_nwb_file`** to use `PackagingProvenance`:

```python
def _create_nwb_file(self) -> NdxEventsNWBFile:
    from ._provenance import PackagingProvenance  # local import avoids circular risk

    # Wait — _provenance is a sibling of nwb_file, not a child.
    # Use the package-level import path:
    from .._provenance import PackagingProvenance

    prov = PackagingProvenance.build(self._dataset)
    nwb_file = NdxEventsNWBFile(
        session_id=self.aind_data_schema.data_description.name,
        session_description=f"Dataset version: {prov.dataset_version}",
        session_start_time=self.aind_data_schema.acquisition.acquisition_start_time,
        identifier=self.aind_data_schema.data_description.subject_id,
        subject=get_subject_nwb_object(
            self.aind_data_schema.data_description.model_dump(mode="json"),
            self.aind_data_schema.subject.model_dump(mode="json"),
        ),
        notes=prov.to_nwb_notes(),
    )
    return nwb_file
```

> **`session_description`:** Kept as-is for now to avoid breaking existing test assertions. If Arjun's PR #44 is merged before this task runs, `_create_nwb_file` will be replaced by `create_base_nwb_file(...)` — in that case, pass `notes=prov.to_nwb_notes()` as a kwarg to that function instead, and drop `session_description` entirely from this method.

Also remove the `import semver` at line 8 if it is no longer referenced anywhere in the file after deleting `dataset_version`.

### Step 4: Run the new tests — all must pass

```
pytest tests/test_nwb_session.py -v
```

Expected: all PASS including the three new provenance tests.

### Step 5: Run the full test suite (except integration)

```
pytest tests/ --ignore=tests/integration -v
```

Expected: all PASS, zero regressions.

### Step 6: Commit

```
git add src/aind_behavior_vr_foraging_packaging/nwb_file/__init__.py \
        tests/test_nwb_session.py
git commit -m "refactor: use PackagingProvenance in NwbSession; drop duplicate dataset_version property"
```

---

## Task 4: Update OKF documentation

**Files:**
- Modify: `docs/knowledge/architecture/data-contract-and-versioning.md`

### Step 1: Edit the doc

In the "Where the versions land in the outputs" section (added by PR #44), update the "Written by" column to reference `PackagingProvenance`:

```markdown
## Where the versions land in the outputs

Both outputs carry the same three keys under the same names, computed once by
`PackagingProvenance.build(dataset)` in `_provenance.py`:

| Key | Parquet | NWB |
|---|---|---|
| `packaging_version` | `df.attrs` + parquet schema metadata | `nwb.notes` (JSON) |
| `data_contract_version` | `df.attrs` + parquet schema metadata | `nwb.notes` (JSON) |
| `dataset_version` | `df.attrs` + parquet schema metadata | `nwb.notes` (JSON) |
| `processor` | `df.attrs` + parquet schema metadata only | — (per-processor, not session-level) |

`AbstractProcessor.compute()` calls `PackagingProvenance.build(self._dataset).stamp_df_attrs(df, processor_name=...)`.
`NwbSession._create_nwb_file()` calls `PackagingProvenance.build(self._dataset).to_nwb_notes()`.

To add a new provenance field: add it to `PackagingProvenance` in `_provenance.py`. Both outputs pick it up automatically.
```

Also update the `timestamp:` frontmatter field to today's date.

### Step 2: Commit

```
git add docs/knowledge/architecture/data-contract-and-versioning.md
git commit -m "docs: update provenance section to reference PackagingProvenance"
```

---

## Verification checklist (run after all tasks)

```
# Full unit test suite
pytest tests/ --ignore=tests/integration -v

# Confirm the three provenance keys still reach the parquet schema metadata
pytest tests/test_pipeline.py -v

# Confirm NWB carries all three keys in notes
pytest tests/test_nwb_session.py -v -k "provenance"

# No import of semver left in _base.py (should return nothing)
grep -n "semver" src/aind_behavior_vr_foraging_packaging/_base.py

# No inline importlib.metadata.version call left in _base.py (should return nothing)
grep -n "importlib" src/aind_behavior_vr_foraging_packaging/_base.py

# No dataset_version property on NwbSession (should return nothing)
grep -n "def dataset_version" src/aind_behavior_vr_foraging_packaging/nwb_file/__init__.py
```

---

## Interaction with PR #44 (Arjun)

If PR #44 merges **before** this plan is executed:
- Task 3, Step 3: replace the `NdxEventsNWBFile(...)` constructor call with `create_base_nwb_file(...)` as Arjun's PR does, and pass `notes=prov.to_nwb_notes()` as a keyword argument.
- Remove the `from ndx_events import NdxEventsNWBFile` import if Arjun's PR hasn't already.
- All other tasks are unaffected.

If this plan merges **before** PR #44:
- PR #44 will need a small update: remove its `notes=f"dataset version: ..."` string and replace with `notes=PackagingProvenance.build(self._dataset).to_nwb_notes()`.
