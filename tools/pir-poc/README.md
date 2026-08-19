# DefraDB PIR POC

This isolated POC demonstrates private retrieval over immutable Defra-shaped
byte tables without changing DefraDB storage, CRDT, transaction, ACP, or query
planner code. It contains correctness tests, reproducible benchmarks, artifact
adapters, and production-boundary notes. It is not audited cryptography.

The authoritative measured comparison is [COMPARISON.md](COMPARISON.md).

## Current result

The primary objective is minimum **aggregate server work per complete useful
private result**. Client traffic and memory must remain practical, but are not
the optimization target.

| Query shape | Current POC choice |
|---|---|
| Cold, one/few queries, exactly two non-colluding servers | Official finite-differences PIR over exact pages when 8x storage and a 5.36 MiB answer are acceptable |
| Cold with 3+ servers, low storage/traffic, or the simplest path | Public exact MPHF ordinal plus replicated Dense XOR over a fixed inline projection |
| Several simultaneous cold queries | Shared-row Dense traversal; ephemeral Four-Russians for larger admitted batches |
| Warm client making many sequential queries | Generation-bound two-server SinglePass, normally `Q=2` |
| Live subscriptions | Two-server Compact DPF; computational target privacy under its AES-based PRG/DPF construction |
| Weaker candidate-set privacy | Ordinary indexed lookup of 100 visible decoys |

Public coarse time windows are optional routing leakage, not a requirement.
Large or variable results can use private locators followed by a padded private
document batch, but the identical-workload benchmark strongly favors bounded
inline projections when the application can define one.

## Important implementation boundary

The selected paths above are in-process research modules and benchmark
commands. The existing `build` / `serve` / `query` commands still exercise the
older `Snapshot::build_paged` layout through `dense::ParallelEvaluator`. They
are transport and integration smoke tests, **not** the production endpoint for
exact MPHF, finite-differences, adaptive batching, or SinglePass.

Production should keep PIR as an optional sidecar serving index:

```text
authorized deterministic Defra export
  -> immutable projection generation + signed manifest
  -> separately operated PIR replicas
  -> bounded, authenticated query endpoint
```

Defra's committed update event can independently feed the Compact-DPF live
sidecar. The production design and remaining integration gates are in
[PRODUCTION.md](PRODUCTION.md).

## Quick commands

From the repository root:

```bash
cargo test -p pir-poc --all-targets
cargo run -p pir-poc --release -- demo
cargo run -p pir-poc --release -- singlepass-demo
cargo run -p pir-poc --release -- subscription-demo
```

Primary benchmark commands:

```bash
cargo run -p pir-poc --release -- bench-mphf quick
cargo run -p pir-poc --release -- bench-dense-batch quick
cargo run -p pir-poc --release -- bench-warm-stateful quick
cargo run -p pir-poc --release -- bench-subscription-batches quick
cargo run -p pir-poc --release -- bench-end-to-end quick
cargo run -p pir-poc --release -- bench-production-scale quick preflight
```

Use `full` only for decision runs on an otherwise idle machine. Unknown profile
names are rejected. Phase-scoped Linux hardware counters are collected by
`scripts/bench-perf.sh`; unsupported energy and DRAM counters remain explicitly
unmeasured.

The legacy HTTP smoke demo is:

```bash
cargo run -p pir-poc -- build input.json snapshot collection tag cid
cargo run -p pir-poc -- serve snapshot 127.0.0.1:8787
cargo run -p pir-poc -- query tag-value http://127.0.0.1:8787 http://replica:8787
```

Do not use that command sequence as evidence that the selected layouts are
served over HTTP.

## Reproducing external artifacts

Adapters pin upstream revisions, export the same populated 262,144 x 96-byte
page corpus, check exact reconstruction, and keep build/setup/online work
separate. The current exporter records BLAKE3 and SHA-256; external runners
recompute SHA-256 before invoking upstream code.

```bash
tools/pir-poc/research/run-simplepir-defra.sh
tools/pir-poc/research/run-ypir-defra.sh
tools/pir-poc/research/run-finite-diffs-defra.sh
tools/pir-poc/research/run-inspire-defra.sh
```

InsPIRe is intentionally blocked on hosts without its required AVX-512
features. Artifact pins, qualification, correctness gates, and resource limits
are recorded in [ARTIFACTS.md](ARTIFACTS.md).

## Security and deployment limits

- Replicated Dense provides information-theoretic selector privacy only while
  at least one of the `n` replicas does not collude; all `n` answers are
  required in this construction.
- SinglePass has exactly two asymmetric roles. Its mutable state, prepared
  queries, and answers are bound to one immutable generation and state must not
  be rolled back after an ambiguous request.
- Compact DPF is computational, two-party privacy. It evaluates every active
  subscription and returns fixed output for each subscription/event.
- PIR hides a selector; it does not grant access. Plaintext tables must contain
  only data authorized to every client in that artifact. Use cohort artifacts
  or client-held AEAD keys for private projections.
- Signed manifests authenticate a generation, not arbitrary malicious XOR
  answers. The POC assumes semi-honest servers and does not provide verifiable
  PIR, Byzantine availability, or malicious-server correctness.
- Exact MPHF metadata currently uses same-build `epserde`; replace it with a
  stable, safe, authenticated, size-bounded format before production/mobile
  distribution.
- Portable-client checks are build/resource envelopes, not phone performance
  results. A client-only crate and named ARM device measurements remain needed.

## Documentation map

- [COMPARISON.md](COMPARISON.md): measured decision ledger and exclusions
- [EXPLORATION.md](EXPLORATION.md): complete research/optimization program
- [PRODUCTION.md](PRODUCTION.md): Defra integration, authorization, serving,
  and failure boundaries
- [PORTABLE_READINESS.md](PORTABLE_READINESS.md): mobile/resource gates
- [HARDWARE_COUNTERS.md](HARDWARE_COUNTERS.md): phase-scoped perf evidence
- [WARM_STATEFUL.md](WARM_STATEFUL.md): Dense versus SinglePass lifecycle
- [RIBBON_BENCHMARK.md](RIBBON_BENCHMARK.md): Ribbon/Fuse/MPHF comparison
- [ARTIFACTS.md](ARTIFACTS.md): upstream pins and reproducibility status
