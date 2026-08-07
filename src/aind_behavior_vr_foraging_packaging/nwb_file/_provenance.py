"""Machine-readable pipeline provenance, carried in the NWB file's ``lab_meta_data``.

The fields mirror the keys ``AbstractProcessor.compute`` stamps into ``df.attrs``,
so the parquet and NWB outputs of a session report identical provenance.

The neurodata type has to be spec'd: ``add_lab_meta_data`` accepts a plain
``LabMetaData`` subclass carrying extra attributes and writes it without complaint,
but hdmf drops any attribute it has no spec for — the values silently vanish on
read-back. The spec below is built at runtime and cached into every file written,
so a reader gets the attributes back without this package installed.
"""

import functools
import tempfile
from pathlib import Path
from typing import Any

from hdmf.spec import AttributeSpec, GroupSpec, NamespaceBuilder
from pynwb import NWBFile, get_class, load_namespaces

NAMESPACE = "vr-foraging-packaging"
NAMESPACE_VERSION = "0.1.0"
NEURODATA_TYPE = "PackagingProvenance"

#: Key under which the container is mounted on ``nwb.lab_meta_data``.
LAB_META_DATA_KEY = "provenance"

#: Attribute name → doc. Mirrors ``AbstractProcessor.compute``'s ``df.attrs`` keys.
FIELDS = {
    "dataset_version": "Version of dataset.",
    "packaging_version": "Version of aind-behavior-vr-foraging-packaging that wrote this file.",
    "parser_version": "Version of aind-behavior-vr-foraging used to parse the session.",
}


@functools.cache
def provenance_type() -> type[Any]:
    """Return the ``PackagingProvenance`` class, registering its namespace on first call.

    Cached: the namespace can only be loaded once per process, and the generated
    class is stable for the lifetime of the interpreter.
    """
    spec = GroupSpec(
        doc="Provenance of the packaging pipeline that produced this file.",
        data_type_def=NEURODATA_TYPE,
        data_type_inc="LabMetaData",
        attributes=[AttributeSpec(name=name, doc=doc, dtype="text") for name, doc in FIELDS.items()],
    )

    builder = NamespaceBuilder(
        doc=f"Provenance types for {NAMESPACE}",
        name=NAMESPACE,
        version=NAMESPACE_VERSION,
    )
    builder.include_namespace("core")
    extensions_name = f"{NAMESPACE}.extensions.yaml"
    builder.add_spec(extensions_name, spec)

    # NamespaceBuilder only exports to disk and load_namespaces only reads from disk,
    # so the yamls are a means of registration rather than an artifact worth keeping.
    # load_namespaces parses them into the type map eagerly, so the directory can go.
    namespace_name = f"{NAMESPACE}.namespace.yaml"
    with tempfile.TemporaryDirectory(prefix=f"{NAMESPACE}-spec-") as outdir:
        builder.export(namespace_name, outdir=outdir)
        load_namespaces(str(Path(outdir) / namespace_name))

    return get_class(NEURODATA_TYPE, NAMESPACE)


def add_provenance(
    nwb_file: NWBFile,
    *,
    dataset_version: str,
    packaging_version: str,
    parser_version: str,
) -> NWBFile:
    """Attach provenance to *nwb_file* under ``lab_meta_data[LAB_META_DATA_KEY]`` and return it."""
    nwb_file.add_lab_meta_data(
        provenance_type()(
            name=LAB_META_DATA_KEY,
            dataset_version=dataset_version,
            packaging_version=packaging_version,
            parser_version=parser_version,
        )
    )
    return nwb_file
