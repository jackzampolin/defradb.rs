# SimplePIR / DoublePIR common-corpus adapter

This adapter runs the official
[`ahenzinger/simplepir`](https://github.com/ahenzinger/simplepir) revision
`e9020b03bf2872c75b8954e749e32408b5db87ed` on the exact populated page corpus
used by the Defra PIR layout benchmarks. Upstream source is neither vendored nor
patched. The runner clones the pinned revision into ignored `target/` scratch
space and copies `main.go` into it as a new, out-of-tree command.

Run from WSL/Linux because the official artifact uses cgo and GCC:

```bash
cd /mnt/c/src/defradb.rs
bash tools/pir-poc/research/run-simplepir-defra.sh
```

Use `--skip-smoke` only after the pinned official correctness tests have passed
on the same runner. `--reuse-corpus` skips Cargo and uses an already exported
`pages.bin` plus `manifest.json`; the adapter still checks the selected page.
`DEFRA_PIR_SAMPLES`, `DEFRA_PIR_ARTIFACT_DIR`,
`DEFRA_PIR_CORPUS_DIR`, and `DEFRA_PIR_RESULT_DIR` override the defaults.

## Exact 96-byte result mapping

The official public API reconstructs values no wider than a Go `uint64`. The
adapter therefore splits each page into 24 little-endian 32-bit lanes, lays
those lanes out as 24 equal row bands, and uses the artifact's batch API. The
24 answer subcomputations collectively cover the packed database once and the
client rejoins the lanes into the original 96 bytes. Every timed sample checks
the entire page against `pages.bin`.

The adapter reports all expansions separately:

- 25,165,824 raw useful bytes for the standard 262,144-page run;
- parameter-alignment padding in pages per lane;
- the upstream squished matrix allocation;
- protocol server state;
- DB-specific client hint and public-matrix seed/state.

Build, hint setup, client setup, query generation, server answering, and
reconstruction are individually timed. They must not be added as if they were
jointly timed or shared the same amortization horizon.

## Fuse qualification

The official query routine creates one arithmetic point query. Adding four
points would return an arithmetic sum, whereas Defra's Fuse-4 cells require a
bytewise XOR. Four independent cell PIRs would perform a different workload,
so this adapter does not claim a one-query Fuse composition.
