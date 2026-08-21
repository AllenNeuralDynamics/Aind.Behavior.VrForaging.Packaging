"""Provenance metadata for a packaging run.

:class:`PackagingProvenance` is the single source of truth for the version keys
written to every output (parquet ``df.attrs`` and NWB ``was_generated_by``).
Adding a new metadata field means editing only this file.
"""

import importlib.metadata

import aind_behavior_vr_foraging
import semver
from contraqctor.contract import Dataset
from pydantic import BaseModel, ConfigDict

_PACKAGING_PKG = "aind-behavior-vr-foraging-packaging"


class PackagingProvenance(BaseModel):
    """Immutable provenance snapshot for one packaging run.

    All version strings are validated as semver-compatible on construction, so
    any call site is guaranteed a well-formed object.

    Attributes
    ----------
    packaging_version:
        Version of this package (``aind-behavior-vr-foraging-packaging``).
    data_contract_version:
        Version of ``aind-behavior-vr-foraging`` (the behavioural schema library).
    dataset_version:
        Version recorded in the session's ``tasklogic_input.json``.
    """

    model_config = ConfigDict(frozen=True)

    packaging_version: str
    data_contract_version: str
    dataset_version: str

    @property
    def dataset_semver(self) -> semver.Version:
        """Dataset version as a parsed :class:`semver.Version`."""
        return semver.Version.parse(self.dataset_version)

    @property
    def data_contract_semver(self) -> semver.Version:
        """Data-contract (parser) version as a parsed :class:`semver.Version`."""
        return semver.Version.parse(self.data_contract_version)

    @classmethod
    def build(cls, dataset: Dataset) -> "PackagingProvenance":
        """Construct a :class:`PackagingProvenance` from a live dataset."""
        return cls(
            packaging_version=importlib.metadata.version(_PACKAGING_PKG),
            data_contract_version=aind_behavior_vr_foraging.__semver__,
            dataset_version=str(dataset.version),
        )
