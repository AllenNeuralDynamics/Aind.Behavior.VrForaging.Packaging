# Processors

Every processor subclasses `AbstractProcessor` and implements two methods:

- `_compute()` — returns a `pandas.DataFrame` with one row per output unit.
- `nwbize(nwb)` — populates an `NWBFile` with the same data (optional).

`compute()` wraps `_compute()` and stamps provenance (`packaging_version`,
`data_contract_version`, `dataset_version`, `processor`) into `df.attrs`.

---

## AbstractProcessor

::: aind_behavior_vr_foraging_packaging._base.AbstractProcessor

---

## PackagingProvenance

::: aind_behavior_vr_foraging_packaging._provenance.PackagingProvenance

---

## SiteTableProcessor

::: aind_behavior_vr_foraging_packaging.processing.SiteTableProcessor

---

## PositionAndVelocityProcessor

::: aind_behavior_vr_foraging_packaging.processing.PositionAndVelocityProcessor

---

## LicksProcessor

::: aind_behavior_vr_foraging_packaging.processing.LicksProcessor

---

## SniffingProcessor

::: aind_behavior_vr_foraging_packaging.processing.SniffingProcessor

---

## SoftwareEventsProcessor

::: aind_behavior_vr_foraging_packaging.processing.SoftwareEventsProcessor

---

## EventsProcessor

::: aind_behavior_vr_foraging_packaging.processing.EventsProcessor

---

## SessionMetadataProcessor

::: aind_behavior_vr_foraging_packaging.processing.SessionMetadataProcessor

---

## Legacy processors

These are selected automatically when the dataset version is `< 0.6.0`.

::: aind_behavior_vr_foraging_packaging.processing.LegacySiteTableProcessor

::: aind_behavior_vr_foraging_packaging.processing.LegacyPositionAndVelocityProcessor
