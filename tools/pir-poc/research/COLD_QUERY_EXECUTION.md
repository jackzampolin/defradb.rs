# Cold search execution ledger

Bounded experiment pass finished, 2026-09-05. See
[results and all 24 dispositions](COLD_QUERY_RESULTS.md). Feasible implementations
and screening stages were exercised; this does not claim 24 complete protocols
or every named variant was implemented.

The user clarified that cold means ad hoc Shinzo/Mizu tag or tree-value searches,
including historical catch-up, rather than registered live alerts. Physical row
retrieval is only a backend diagnostic. Fresh-client state is a separate axis.

## Implemented in this campaign

- `benchmarks/cold_search.py`: isolated spawned clients, persistent native
  replicas, direct client-to-replica pipe descriptors, public metadata RPC,
  separately charged per-client preparation and global publication. Complete
  synthetic tag searches include collisions, duplicate matches, fixed
  continuation schedules and absence. Tree proxy uses authenticated SHA-256
  predecessor paths, not canonical Mizu Poseidon witnesses.
- Binary and JSON tag pages use identical buckets and predicates. Binary
  Patricia variants compress unary bit paths and pad every query to the public
  maximum depth. Old posting/hash/radix/authenticated layouts also run with fresh
  clients and persistent servers.
- `benchmarks/finite_store.go`: actual official finite-differences encoding in
  two persistent native processes. Python client uses fresh cryptographic
  randomness, published combinatorial index encoding and full recovery. Encoded
  storage is limited to 256 MiB per replica before allocation.
- `benchmarks/finite_cold_test.go`: separate same-process diagnostic that builds
  both real encodings and verifies all tag matches and payload bytes. The first
  16-slot attempt overflowed at 16,384 source rows; retained log. A 32-slot
  variant passed all eight 256/1024/4096/16384-row by 32/96-byte cases (128
  complete answers). This is not a canonical product benchmark.
- `run_sandwich_cold.py`: complete synthetic tag searches through the official
  GPU HTTP server. Each continuation uses a fresh native client (conservatively
  repeating setup). Four-client smoke verified complete results and absence.
  CPU and GPU timing are separate. Current HTTP byte accounting uses payloads
  and a conservative header allowance; no production proofs.
- `test_cold_search.py`: collision/continuation/absence oracle, fixed compressed
  bit-tree schedule, and actual fresh-client process/setup tests pass.

Early gateway-relayed smoke runs are diagnostics only. Repeated comparison
campaigns use direct descriptors so the coordinator never forwards query shares.

## Campaigns and source provenance

- `target/pir-cold-search-screen-v1`: completed; five alternating repetitions,
  Dense/SinglePass binary/JSON tag cases up to 65,536 rows plus private tree
  variants. Preserves raw results, failures, matrix, hashes and outcomes.
- `target/pir-cold-finite-screen-v1`: completed, five paired
  repetitions of the same complete tag layouts with Dense and finite differences.
- `target/pir-cold-sandwich-tags-smoke-v3`: successful HTTP GPU smoke.
- `target/pir-cold-finite-v2.json`: real-encoding same-process diagnostic.

External sources and logs are under `/root/pir-cold-artifacts` in WSL:

| Artifact | Pinned revision | Status |
|---|---|---|
| SandwichPIR | `8a468f8831edf63991a572295ed73ede4ada73ba` | CPU and sm_75 GPU builds work; native HTTP client/server work |
| finite-diffs-pir | `4574a4f8c52eeda165e110cbb64f834397d7c049` | Official real encoding tests and complete-search adapter work |
| HintlessPIR | `812babf1a742d08b303bf2c603dafe5916955773` | Ten upstream tests pass; optimized complete-tag pilot verified and failed download cap |
| ZipPIR | `a2ffee01accd20c51dbbc69fd2bf9de12f79c0b5` | Full admission/correctness run with CPU counters; first-client CPU is prohibitive at tested parameters |

Sandwich GPU build uses CUTLASS v3.5.1 with the author's no-saturation patch,
CUDA 12.4, and GCC 13 through NVCC_PREPEND_FLAGS. Hintless tests use Bazel 7.4.1
with `--enable_workspace=false`. Its default debug test timings are not
performance measurements.

## Literature gates updated from full papers

Full PDFs for CHOO-PIR, Barely Doubly-Efficient SimplePIR, practical DEPIR 2026,
and low-storage secret-key DEPIR were retrieved and extracted locally.

- CHOO-PIR SS: Fig. 5 streams both shared hint tables and privately marks used
  hints; includes refresh and table growth. Table 2's approximately 128/263 MB
  online communication examples are outside the current client budget. Smaller
  parameters and the FHE variant remain to be screened. Do not substitute an
  unproved hint-sharing variant of SinglePass for its underlying protocol.
- Barely Doubly-Efficient SimplePIR: Fig. 2 sends the entire H matrix in the
  answer. A CRS lets H be globally precomputed, while Williams/CRT preprocessing
  accelerates matrix-vector multiplication. Concrete H bytes and ring/table
  parameters must be screened before a full construction.
- Practical DEPIR 2026: Table 4 evaluates batches of thousands, including
  0.39 GB state and a ten-second batch at its smallest reported size. The paper
  describes an implementation, but no code URL was found in the extracted text.
  Do not call its amortized milliseconds a singleton measurement.
- Secret-key DEPIR: full paper links FastSecretStatePIR, but both GitHub page
  and API return 404. The stalled clone was stopped, without removing its log.
  The paper discusses distributing the client role with a key server; this
  requires a real secure computation implementation and full helper accounting.
  Its large encoding-time estimates are not measured build results.

## Completed extensions and final checks

- `pir-cold-directory-v1`, `pir-cold-extensions-v1`, `pir-cold-large-frontier-v1`
  and `pir-cold-bit64-v1`: five-repeat directories, XOR retrieval, private bit
  owners, packed wavelets and Patricia controls; up to 262,144 hashed tags.
- `pir-cold-reuse-v1`: actual 1/2/4/16-query clients, with admission charged.
- `pir-cold-sorted-bits-v1`: sorted placement and a single private 8/12/16-bit
  prefix owner; exact full-tag filtering, padded candidate blocks and controls.
- `pir-cold-canonical-v1`: unchanged Poseidon witnesses retrieved privately and
  verified against their original root, including tamper/wrong-root rejection.
- `pir-cold-finite-frontier-v1`: real explicit-M/D encodings up to 4.97 GB total,
  matched Dense controls, and separate many-server cost calculations. The exact
  in-place Boolean zeta encoder matches the reference byte-for-byte in all nine
  parameter/width test combinations. Its larger storage did not beat the best
  complete-search Dense layout.
- `pir-cold-ramen-tags-v1`: actual persistent three-party Ramen, four fresh clients.
- `pir-cold-maintenance-v1`: real base/delta operations and compactions, plus
  separate shared-proof topology checks with scattered physical positions.
- `pir-cold-frontiers-v1`: H-size and exact CRT kernel screens, plus published
  storage/communication/hardware gates.
- `pir-cold-gpu-v2-*`: five repeats with isolated complete clients fetching public
  navigation over HTTP, 1/8/32/128 arrivals, public windows and spacing. All 2,695
  returned answers verified and passed the stated isolated-client cap checks.
- `pir-cold-dense-batch-v1`: 80 complete-query batch kernel runs, including
  GPU-matched 2,048-byte pages. The driver is a trusted synthetic client/oracle.

The primary IPC measurement files contain 416 result-bearing configurations and
17,716 verified answers, including cap-failing configurations and memory-only
qualification repeats. Failed preflights remain in raw logs and are not zero work.
Ten final Python tests pass, as do canonical negative checks and the Go encoder
equivalence tests. Cost-formula extensions are checked against the author's DP
table; their parameters are numerical candidates, not a new security proof.

The initial `ru_maxrss` included a publisher high-water mark inherited before
exec. The probe reproduced 150 MB inherited versus 16 MB current-image peak.
Clients now use `VmHWM`; old configurations were rerun for memory qualification.
Original CPU samples remain untouched. GPU CPU upper bounds account for printed
counter precision. Later campaigns execute frozen Python source snapshots.

## Explicit unimplemented or gated variants

The result ledger distinguishes full block Ramen, smaller/FHE CHOO variants,
full Williams/LWE DEPIR, new practical/secret-key DEPIR ports, every additional
HE artifact, a physical remote cluster/SSD/energy frontier, live-root production
maintenance, relaxed-correctness protocols and trusted-hardware deployments.
They are not silently treated as implemented, benchmarked or ruled out.

No serving default or production protocol has changed.
