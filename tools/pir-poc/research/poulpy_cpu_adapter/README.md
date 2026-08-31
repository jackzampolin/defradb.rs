# Poulpy InsPIRe2 CPU adapter

This adapter compares pinned
[`poulpy-fhe/poulpy-pir`](https://github.com/poulpy-fhe/poulpy-pir) commit
`533081a74301c8ba6ddd5e1dfc0c9daa6e3e75ef` with the Defra CPU Dense control.
It patches a copied upstream example rather than changing the library.

The common workload is `2^23` entries, 120 useful bytes and 128 physical bytes.
The deterministic first 120 bytes match the GPU adapters; the final eight
bytes are zero. The runner uses the supported AVX2/FMA backend and pinned Rust
nightly `2026-05-14`:

```bash
bash tools/pir-poc/research/run-poulpy-cpu-defra.sh full
```

Set `DEFRA_POULPY_BATCHES="1 2 4 8 16 32"` to expand the default `1 8 32`
batch set. Results are written beneath
`target/pir-research-results/poulpy-pir-533081a74301c8ba6ddd5e1dfc0c9daa6e3e75ef/`.

The JSON reports server wall time and summed measured phase work separately.
The latter can exceed wall time when independent work runs in parallel. Neither
number includes network, queueing, keyword-to-ordinal lookup, or the Rust Dense
process; run `research cpu-snapshot full` separately on the same idle host.

The adapter is a comparison harness, not production integration. It does not
vendor Poulpy, change DefraDB, or claim that the AVX2 result represents an
AVX-512 server.
