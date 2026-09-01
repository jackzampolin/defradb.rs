# External PIR artifact reproduction ledger

This ledger distinguishes upstream artifact results from the Rust POC's own
implementations. External repositories are cloned into ignored scratch space;
their source is not vendored into DefraDB.

## Runner

- AMD Ryzen 7 3700X, 8 cores / 16 threads, AVX2 but no AVX-512.
- WSL2 Ubuntu, Rust 1.97.0 for unpinned projects, Go 1.26.0.
- WSL2 currently exposes approximately 8 GiB RAM.
- NVIDIA RTX 2070 SUPER is visible through CUDA `/dev/dxg`; the isolated CUDA
  12.4/GCC 13 toolchain executes CUDA artifacts, while Vulkan remains absent.
  CPU and GPU artifact results are therefore separate.

## Revisions and status

| Artifact | Revision | Status on this runner |
|---|---|---|
| finite-diffs-pir | `4574a4f8c52eeda165e110cbb64f834397d7c049` | Small 1-byte and 10-byte end-to-end PIR tests pass. Paper-scale tests require far more than 8 GiB. |
| SimplePIR/DoublePIR | `e9020b03bf2872c75b8954e749e32408b5db87ed` | Simple, compressed, long-row, and batch correctness tests pass. |
| YPIR | `b9801521301f34502496d694b2ac034857104ebc` (`artifact-evaluation`, Zenodo 13117988) | Both official end-to-end tests pass on the artifact's scalar/non-explicit AVX2 path. The common corpus passes four exact reconstructions; local scalar server p50 is 80.655 ms. |
| ChalametPIR | `448698f7c314fd4eb36e889f6a6ec7fba64db03d` | Common Fuse/matrix suite passes 34/34 after the dependency-resolution qualification below. Client/common crates compile for `wasm32-wasip1`. |
| MPC4J Practical Keyword PIR | `178da1b07e8aa011bce9ffbd921e8a1d24477f0b` (`v1.1.3-beta`, Zenodo 14722434) | Exact tag builds. PGM index tests pass 4/4; requested `SIMPLE_NAIVE`, `SIMPLE_BIN`, `PGM_INDEX`, and `CHALAMET` correctness cases pass individual and batch paths. The upstream parameter class is deliberately ignored. Common-corpus performance is not yet admitted. |
| InsPIRe | Zenodo `17361471`, `artifact-final.zip` MD5 `bfa9edb2d8403f0dc20830fb40608b78` | The official archive (not the third-party `inspire-rs` crate) requires AVX-512. Its checked corpus adapter is ready, but correctness/performance is blocked on this AVX2 host. |
| Poulpy InsPIRe2 CPU | `533081a74301c8ba6ddd5e1dfc0c9daa6e3e75ef` | The AVX2/FMA backend reconstructs the common `2^23 x 120 B` corpus at batches 1/8/32. It is slower than same-host Dense wall time, uses 5.71--6.87 GiB peak RSS, and spends 30.5--36.1 s offline. |
| InsPIRe GPU | `c14d1d84a425cdaa9f86ed09465b09c9c9802f13` | The Ethereum-oriented CUDA server and CPU client build on the RTX 2070 SUPER, pass 9/9 upstream tests, and complete five alternating 1 GiB repetitions. The 4 GiB state is capacity-blocked on the 8 GiB card. This is distinct from the official CPU artifact. |
| GPU-DPF | `ce23a06af884ee54300b5bc5fd5350e445f10b0b` | Pinned ChaCha12 DPF expansion/fused reduction compiles with CUDA 12.4 and passes every 120-byte snapshot and packed live reconstruction through the 4 GiB local table limit. Archived upstream POC; not production-vetted. |

The current exporter records both BLAKE3 and SHA-256, and each external runner
recomputes SHA-256 before handing `pages.bin` to an upstream artifact. The
SimplePIR and YPIR measurements below predate that post-run guard; their exact
selected-page reconstruction and recorded BLAKE3 identify the corpus used, but
the heavy measurements were not rerun merely to relabel provenance.

## Commands reproduced

Finite differences:

```bash
cd finite-diffs-pir/pir
go test -run '^(TestPIRSmall1|TestPIRSmall10)$' -v
```

SimplePIR:

```bash
cd simplepir/pir
go test -run '^(TestSimplePir|TestSimplePirCompressed|TestSimplePirLongRow|TestSimplePirBatch)$' -v
```

The small official SimplePIR tests reported approximately 4.0-4.5 GB/s for
single queries and 12.2 GB/s for the small batch test on this runner. These are
artifact smoke measurements over the artifact's own tiny generated database,
not Defra-corpus comparisons and not stable paper reproductions.

YPIR official correctness and common corpus:

```bash
bash tools/pir-poc/research/run-ypir-defra.sh
```

The runner pins the paper artifact rather than moving `main`, runs the two
official end-to-end tests, and uses the exact populated Defra corpus. Results
and physical-record qualification are in
[`ypir_adapter/RESULTS.md`](ypir_adapter/RESULTS.md).

InsPIRe official archive gate:

```bash
bash tools/pir-poc/research/run-inspire-defra.sh
```

On this runner it verifies the permanent archive and checked input patch, then
writes `BLOCKED.txt` and exits before building because AVX-512F is absent.

InsPIRe same-GPU server comparison:

```bash
bash tools/pir-poc/research/run-inspire-gpu-defra.sh full
```

This runner targets the local CUDA compute capability, preserves the upstream
cryptographic implementation, checks the upstream tests, and reports client
cold start, first online answer, preprocessing and batches 1 through 32.

Poulpy InsPIRe2 same-CPU comparison:

```bash
bash tools/pir-poc/research/run-poulpy-cpu-defra.sh full
```

The runner pins Rust nightly `2026-05-14`, verifies AVX2/FMA, patches only a
new upstream example, uses 128 physical bytes with the last eight bytes zero,
and emits one JSON file for each requested batch.

GPU-DPF plus same-GPU Dense and packed-presence controls:

```bash
bash tools/pir-poc/research/run-gpu-pir-defra.sh full
```

The runner clones the exact commit outside the repository and writes JSON under
`target/pir-research-results/`. See
[`gpu_dpf_adapter/README.md`](gpu_dpf_adapter/README.md) for
the glibc/CUDA compatibility qualification and timed boundaries.

Chalamet common and WASI-compatible client:

```bash
cd ChalametPIR
cargo test --profile test-release -p chalametpir_common
cargo check -p chalametpir_common -p chalametpir_client \
  --target wasm32-wasip1 --features wasm --no-default-features
```

MPC4J Practical Keyword PIR:

```bash
mvn -pl mpc4j-s2pc-pir -am -DskipTests package
mvn -pl mpc4j-common-structure -Dmaven.test.skip=false \
  -Dtest=LongApproxPgmIndexTest test
mvn -pl mpc4j-s2pc-pir -Dmaven.test.skip=false \
  -Dtest=SimpleCpKsPirParamsTest test
mvn -pl mpc4j-s2pc-pir -Dmaven.test.skip=false \
  -Dtest='CpKsPirTest#testDefault*' test
```

The last command has a suite-level failure only because five unrelated
PAI/ALPR parameters load the absent native tool. The four pure-Java artifact
configurations pass. Exact results, analytical 262,144 x 96-byte geometry,
and the prepared two-process runner are in
[`kpir_artifact/README.md`](kpir_artifact/README.md).

## Reproduction qualifications

### Chalamet dependency resolution

The checked-out Chalamet revision has no lockfile and pins optional `vulkano`
`0.35.1`, which is yanked from crates.io. Cargo resolves the whole workspace
even when only the common crate is selected. The scratch checkout was changed
to `vulkano = 0.35.2` solely to resolve and run the CPU/common tests. No
performance result from that modified checkout is presented as an exact
upstream result.

### YPIR and InsPIRe CPU requirements

The exact YPIR paper artifact has a scalar/non-explicit K=1 path. It compiled
unchanged and passed both official end-to-end tests on this AVX2 host. Its
80.655 ms common-corpus server result is valid same-host evidence, but not a
reproduction of the paper's AVX-512 number. The run also measured 741,573,720
bytes of serialized offline server state and a 2,084,840 KiB process peak; all
watchdog phases stayed below 5 GiB with zero swap.

The official InsPIRe Zenodo source imports and invokes AVX-512 intrinsics
unconditionally and its README requires AVX-512. Running it unchanged on a
pinned AVX-512 machine is the next valid artifact step. An AVX2/scalar port
would be a new Defra implementation and must not be labeled an official
artifact result. Until then, InsPIRe paper numbers remain background evidence
and do not enter the local Pareto table.

### MPC4J test selection and native boundary

MPC4J's root POM defaults `maven.test.skip=true`; every command that claims a
test result must explicitly set `-Dmaven.test.skip=false`. Its parameter
printer is itself annotated `@Ignore`. The shared keyword-PIR test also mixes
the four pure-Java artifact configurations with five PAI/ALPR configurations
that need `mpc4j-native-tool`. The ledger records outcomes per configuration
instead of converting either zero executed tests or unrelated JNI failures
into a misleading artifact verdict.

The official main program can match the Defra dimensions and value width, but
it generates random values and decimal-string keys. Until an adapter consumes
the identical encoded pages and produces the common aggregate-work schema,
its paper claims and future synthetic local timings stay outside the Defra
Pareto table.

## Common-corpus gate

Passing an upstream test is only the first gate. A protocol enters the Defra
comparison after it consumes the same immutable encoded page corpus and emits
the aggregate-work schema used by Dense, MPHF, Fuse, subset-XOR, SinglePass,
finite differences, decoys, and subscriptions.
