# Findings from the six private index compositions

All six compositions were implemented and exercised. The retained campaigns
contain **629 successful runs and 66,668 verified complete answers**, with no
retained case failures. Correctness is separate from admission: several runs
exceeded client caps. The optional larger Ramen radix sweep was deliberately
stopped after its measured scalar-access cost was far above the controls; the
in-flight case finished and its result was retained. This is not a claim that
every possible parameter combination was executed.

The final regression run passed **22 tests**, including the actual compiled
Ramen/native-store tests, without skips. See the
[implementation and reproduction guide](PRIVATE_INDEX_COMPOSITIONS.md),
[full measurements](PRIVATE_INDEX_MEASUREMENTS.md), and
[machine-readable comparisons](PRIVATE_INDEX_MEASUREMENTS.csv).

## Strongest positive result: stable inline posting pages

Workload: 65,536 source rows, two matching rows per key, 32-byte source payloads,
fixed-width inline result pages, one client, immutable generation, **4,096 actual
queries after setup**, three independent repetitions. Both controls use compiled
store processes and the same complete-answer layout.

| Measured resource | Dense | Streamed SinglePass |
|---|---:|---:|
| Total CPU per answer, median across runs | 2.53 ms | 0.63 ms |
| Aggregate online server CPU, mean | 2.18 ms | 0.21 ms |
| Online client CPU, p95 | 0.39 ms | 0.22 ms |
| Maximum upload per complete answer | 32,854 B | 142 B |
| Maximum download per complete answer | 894 B | 2,975 B |
| Client caps | Pass | Pass |

The total includes all server processes plus index construction and the entire
setup controller, including client hint setup. **Approximately 75% less total
CPU** survives this conservative accounting. In a representative SinglePass run,
required backend/navigation state was 3,784,964 bytes and controller peak RSS was
115,490,816 bytes. Streaming the public setup download in 1,024-row chunks removed
the transient-RSS failure seen in the earlier whole-download prototype.

At 16,384 source rows and the same 4,096-query reuse, totals were approximately
0.37 ms Dense versus 0.28 ms SinglePass. Earlier 128-query whole-download runs
lost on lifecycle total despite lower online work. The conclusion is that
preprocessed inline pages merit further integration for sufficiently stable,
reused generations—not that setup or updates are free. There is no automatic
production configuration change, and these JSON-based composition measurements
do not replace the earlier optimized native POC measurements.

## Bit navigation also improved

For 1,024 rows with scattered 32-bit keys, four-bit radix steps and compiled
Dense private access, a fixed early stop left a bounded leaf bucket to examine:

| Remaining bits handled within the private leaf | Total CPU/answer | Online server CPU |
|---|---:|---:|
| 0: full radix traversal | 5.27 ms | 1.57 ms |
| 8 | 3.66 ms | 1.11 ms |
| 16 | 2.27 ms | 0.72 ms |

These are three-repeat measurements with 16 complete queries per run, including
construction/publication. Stopping at 16 remaining bits reduced total CPU by
about **57% against full radix traversal**. It reduced both node count and
private accesses on this dataset. This does not establish superiority over an
inline exact-key index, and large or skewed leaf populations can reverse the
tradeoff. Leaf payload/metadata is disclosed to the honest client; access
addresses remain private.

## What each family established

| Family | Outcome |
|---|---|
| Radix + private memory | Correct with Dense, Path ORAM and real three-party Ramen. Early-stop leaves are a useful measured improvement. Scalar Ramen remains expensive in this adapter. |
| Two-choice hash + private buckets | Correct fixed three-bucket retrieval, including absent keys. No general advantage over direct inline pages in the bounded-integer workloads tested. |
| Hierarchical compressed bitmap + MPC | Correct private block retrieval, three-party intersection and padded private payload retrieval. Additional accesses/roles did not produce a new winner in the tested cases. |
| Wavelet + private rank | Correct counts and bounded range reporting. Many private rank accesses dominate; some Path cases exceeded the online wire cap. |
| Inline postings + preprocessed PIR | Best positive complete-answer result above, once generation reuse amortizes setup and streaming bounds transient memory. |
| Authenticated ordered tree + maintenance | Correct current-root verification, tamper rejection, predecessor/absence, and incremental changed paths. The Path adapter did not beat its Dense control in the tested maintenance lane. SHA-256 prototype, not production Poseidon proofs. |

Payload and indexed-key changes were benchmarked for all six families. The
authenticated lane additionally tested deletion and reserved-slot insertion,
including Ramen. Other families charge full generation rebuilds; posting hints
are refreshed with the new generation. Updates and all helper processes are
included in the recorded lifecycle.

## Limits and next engineering priorities

- Keep the existing serving default. A local prototype win on stable posting
  pages is evidence for integration work, not a universal replacement policy.
- For bit indexes, prioritize bounded leaf layouts and separating narrow routing
  nodes from wide results. The current common-width padded JSON layout can waste
  space and private-access work.
- For Ramen, vector/block-oriented access and compact binary records are needed
  before another large sweep is justified. The 256-row full-radix pilot consumed
  roughly 5.6 seconds of aggregate online server CPU per answer in one run.
- Multi-client state ownership, real network deployments, malicious security,
  production proof encoding, crash recovery for these new compositions, and
  larger update workloads remain integration work. Existing standalone suite
  recovery tests are not a recovery guarantee for these new indexes.
- Several exploratory campaigns overlapped on this host. Repeated process CPU
  measurements support these local findings; wall-clock latency, fleet energy
  and production throughput are not inferred from them.
