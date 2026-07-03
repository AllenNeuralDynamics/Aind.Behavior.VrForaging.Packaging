# Knowledge Bundle — aind-behavior-vr-foraging-packaging

This is an [Open Knowledge Format (OKF)](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
bundle: a directory tree of markdown files that captures the architecture,
testing harness, and conventions of this library so humans and agents can
orient quickly and keep the project moving in a consistent direction.

Read [overview.md](overview.md) first, then descend into whichever group is
relevant. Every concept file carries YAML frontmatter (`type`, `title`,
`description`, …); reserved files (`index.md`, `log.md`) do not.

## Contents

- [overview.md](overview.md) — What this library is, the end-to-end dataflow, and the vocabulary (sites/patches/blocks).
- [architecture/](architecture/index.md) — How the code is structured: the processor abstraction, pipeline, trial table, streams, NWB packaging, and versioning.
- [testing/](testing/index.md) — The two-tier test harness: fast unit tests and S3-backed integration tests.
- [conventions/](conventions/index.md) — Tooling, code style, CI/CD, and release mechanics that every contribution must respect.

## How to keep this bundle current

When you change the system, update the matching concept file and add a dated
entry to [log.md](log.md). New concept files need frontmatter with a non-empty
`type`. See the `open-knowledge-format` skill for the authoring rules.
