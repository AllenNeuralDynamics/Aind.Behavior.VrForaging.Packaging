# aind-behavior-vr-foraging-packaging

Parses and packages AIND VR Foraging behavioural sessions into tabular (parquet) and NWB outputs.

```bash
pip install aind-behavior-vr-foraging-packaging
vr-foraging-packaging session --input-dir <raw-session> --output-dir <out>
```

One processor per output table, a thin pipeline layer that dispatches on dataset version, and three tiers built on each other: one session, many sessions, and a CLI.

- **Docs:** <https://allenneuraldynamics.github.io/Aind.Behavior.VrForaging.Packaging/>
- **Source, contributing, and the full README:** <https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging>

Running 4700 of these — job ledger, discovery, container worker, dashboard — is the
`server/` distribution in the same repo, which is never published.
