# Portable feasibility and production gates

The selected POC paths are small enough to be phone-compatible by deterministic
memory and network ceilings, but no phone CPU or energy claim is made yet.
[`portable_gates.rs`](src/portable_gates.rs) makes that distinction executable:
it reports exact protocol payload/operation budgets, while CPU milliseconds stay
`not_measured` unless a named target device supplies them.

The default compatibility envelope is deliberately generous rather than a
performance target:

| Resource | Per-client limit |
|---|---:|
| Persistent state | 64 MiB |
| Peak transient owned payload | 128 MiB |
| Setup download | 64 MiB |
| Online upload | 1 MiB |
| Online download | 1 MiB |
| Named-device setup CPU | 10 s |
| Named-device online CPU | 1 s |
| Dense batch | 16 queries |
| SinglePass partition `Q` | 32 |
| Live subscriptions/server | 100,000 |
| Live event batch | 1,024 |

A production deployment should tighten these limits after measuring its oldest
supported device. Desktop clients can use a different declared policy; they do
not bypass admission control.

Generate the deterministic JSON report for an authenticated generation with:

```sh
cargo run -p pir-poc --release --example portable-gates -- MPHF_PUBLIC_METADATA_BYTES 2
```

The final argument is SinglePass partition `Q`. This command intentionally does
not populate CPU milliseconds; named-device measurements must be injected by a
device harness and missing values remain `not_measured`.

## Canonical deterministic client budgets

These values use the 1,048,576-document, 262,144-row, 96-byte exact-MPHF corpus,
two servers, the observed 98,534-byte PtrHash public artifact, SinglePass `Q=2`,
and a 4,194,304-bucket live domain. PtrHash serialization is build-specific, so
the executable report takes the authenticated artifact size as an input rather
than hard-coding it.

| Path | Persistent | Conservative transient | Setup download | Online upload | Online download |
|---|---:|---:|---:|---:|---:|
| Cold MPHF Dense | 98,534 B | 65,824 B | 98,534 B | 65,536 B | 192 B |
| Warm SinglePass `Q=2` | 14,778,662 B | 39,944,486 B | 25,264,358 B | 80 B | 448 B |
| Live packed presence, 65,536 buckets (research adapter) | selector seed + cursor | about 16 KiB while generating both shares | 0 B | 16,384 B once | 2 B/epoch before framing |
| Live Compact DPF | 0 B cryptographic client key state | 908 B | 0 B | 844 B registration | 64 B/event |

SinglePass's transient upper bound assumes the full 25,165,824-byte table and
finished state coexist. A streaming implementation can reduce that peak, but the
gate does not credit an optimization that has not been implemented and measured.
Its setup download also means every client must be authorized to receive the
whole locator projection.

Deterministic CPU work is reported as protocol units rather than invented
milliseconds:

- Cold Dense performs one MPHF lookup, generates and XORs one 32 KiB random
  selector share, and combines one 96-byte response pair.
- Warm SinglePass consumes every 25,165,824 table byte during setup, initializes
  forward/inverse permutation positions, and reports reconstruction plus
  show-and-shuffle byte-XOR work per query.
- Live packed presence generates two reusable 8 KiB selector shares once and
  combines one bit from each replica at every public epoch.
- Live Compact DPF generates a depth-22 key and combines one 16-byte result pair.

Instruction count, AES/PRG throughput, PtrHash behavior, allocator/RSS, battery,
and thermal throttling are target-specific. A CPU gate passes only when the
caller supplies finite measurements and a non-empty target-device name.

## Build gates

Run the Linux/installed-target gate with:

```sh
tools/pir-poc/scripts/check-portable.sh
```

Set `PIR_REQUIRE_PORTABLE_TARGETS=1` in CI to fail when a listed target is not
installed. On Windows, run:

```powershell
tools\pir-poc\scripts\check-portable.ps1
```

The scripts use `cargo check` and label the evidence `build-only`. A green build
does not prove mobile latency, peak RSS, energy, networking, or background-task
behavior. The current environment has Linux x86-64 and WASI targets installed;
Windows MSVC is installed separately, while Android/iOS toolchains are absent.
The Windows script defaults to the already-installed pinned Rust 1.91 toolchain
(`PIR_RUST_TOOLCHAIN` overrides it); the machine's default Rust 1.73 cannot parse
the workspace's Cargo.lock v4. The exercised build matrix on this commit is:

| Target | Result | Exact blocker |
|---|---|---|
| `x86_64-unknown-linux-gnu` | Pass | None; build-only evidence |
| `wasm32-wasip1` | Fail | Transitive `sucds 0.8.3` emits ``compile_error!("target_pointer_width must be 64")``; WASI is 32-bit |
| `x86_64-pc-windows-msvc` with Rust 1.91 | Fail | `sha2-asm 0.6.4` passes `sha512_x64.S` to MSVC `cl`, which ignores it; link then fails with `LNK1181` for the missing object |
| `aarch64-linux-android` | Unavailable | Target and Android NDK are not installed |
| `aarch64-apple-ios` | Unavailable | Requires a macOS/Xcode runner; unavailable on this host |

The WASI and Windows results above came from the research/portable-gate build,
which pulls historical native index code and the optional DefraDB integration
demo. The default POC no longer depends on DefraDB crates, but it still combines
portable client algebra, HTTP/OHTTP clients, server evaluators and Rayon in one
crate. Neither failure is evidence that the selected client-side PIR algebra is
intrinsically incompatible with the target.

The production fix is a client-only crate containing authenticated manifest and
MPHF loading, Dense share generation/combine, generation-bound SinglePass state,
Compact DPF registration/combine, and portable error types—without DefraDB node,
server evaluators, or native storage dependencies. Android needs a pinned NDK and
device runner; iOS needs macOS/Xcode and a device runner.

## Robustness gates

The isolated tests cover:

- wrong Dense share lengths, invalid server counts, oversized batches, and
  upload/download overflow;
- invalid SinglePass `Q`, oversized setup/state, stale generation before state
  mutation, and the core one-in-flight constraint;
- Compact DPF wrong magic, party, flags, length, domain-derived key size,
  subscription capacity, event batch, and output allocation;
- absent MPHF tags, corrupted/truncated metadata (in the MPHF module), and
  128-bit fingerprint rejection after private retrieval.

These helpers must run before allocation or queueing in the production sidecar.
They do not replace authenticated principals, per-principal rate limits, maximum
queue dwell time, cancellation, backpressure, or load tests of rejection paths.

## Production readiness checklist

- [ ] Split and publish a client-only portable crate.
- [x] Bind SinglePass client state, prepared/server queries, and answers to one
  generation and reject mismatches before state mutation.
- [ ] Authenticate every immutable manifest and bind its MPHF metadata/rows to
  the generation checked by the protocol API; reject rollback.
- [ ] Enforce all dimension, batch, state, subscription, event, output, queue,
  and per-principal rate limits before allocation/evaluation.
- [ ] Replace unsafe/build-specific PtrHash `epserde` bytes with a stable, safe,
  bounded format or a tightly authenticated pinned loader.
- [ ] Use reviewed cryptography and randomness; review DPF constant-time and
  ARMv8 AES behavior; define secret/state erasure requirements.
- [ ] Implement atomic SinglePass persistence and ambiguous-failure recovery;
  never reuse rolled-back state after a query might have been observed.
- [ ] Pad live notification identifiers/shares and release on a fixed schedule,
  or provide a separately proven private aggregation protocol.
- [ ] Run Linux, Windows, WASI, Android, and iOS CI gates where supported; archive
  toolchain/dependency versions.
- [ ] Measure cold/warm/live CPU, peak RSS, allocations, battery, thermal
  behavior, Wi-Fi/cellular transfer, retries, and background execution on the
  oldest supported ARM Android and iPhone devices.
- [ ] Keep SinglePass generations inside one authorization cohort; use another
  path if clients may not see the entire locator projection.
- [ ] Add coverage-guided fuzzing for metadata, keys, queries, answers, and state
  journals, plus crash/failure injection around persistence and generation swap.
- [ ] Add malicious-server verification/committed PIR or a proven threshold
  construction before claiming Byzantine correctness or partial-answer
  availability. Extra replicas alone do not supply either property.
- [ ] Emit privacy-reviewed aggregate work/admission/generation/drop metrics and
  never log tags, selectors, DPF keys, result shares, or plaintext results.
