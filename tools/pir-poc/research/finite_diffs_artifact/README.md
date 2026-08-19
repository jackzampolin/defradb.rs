# Henzinger–Ragavan finite-differences artifact gate

This gate covers the official reference implementation for *Two-Server Private
Information Retrieval in Sublinear Time and Quasilinear Space* (EUROCRYPT
2026). It is a separate check from the native Rust spike in
`src/finite_differences.rs`.

## Exact pins

- Repository: <https://github.com/ahenzinger/finite-diffs-pir>
- Commit: `4574a4f8c52eeda165e110cbb64f834397d7c049` (upstream `main` on
  2026-08-18; commit timestamp 2026-05-01)
- Full paper: IACR ePrint 2025/2008, revision dated 2026-07-02
- Downloaded paper SHA-256:
  `853982b87dbd89519e76203d8de7a5d13e5cd4ef6be2d6e58614fefb3a7781e3`

The repository commit predates the latest paper revision, so the code pin and
paper pin are recorded independently. The code labels itself a research
prototype and its `go.mod` currently selects Go 1.24.9; upstream says its
measurements used Go 1.22.2, GCC 13.3.0, and an AWS `r7a.metal-48xl` with 1.5 TB
RAM.

## What was reproduced

The official real-data suite was first attempted without its two `FakePIR`
cost tests. All seven `TestEncoding*` tests and PIR tests through
`TestPIRMed1024` passed. `TestPIRMed10240` then exhausted the 7.7 GiB WSL VM
while building its table, so the process was killed before a correctness
result. The exact bounded suite was rerun cleanly: 13 tests passed in 3.289 s.

This is a resource blocker, not a failed assertion. `RunPIRWithParams` disables
Go garbage collection before calling `EncodeDatabase`. For
`TestPIRMed10240`, the parameters are 7,168 records, 10,240 bytes per record,
`m=16`, and `D=9`. The encoder rebuilds term and coefficient slices once per
record byte, so temporary allocations accumulate while GC is disabled. The
encoded output alone is about 640 MiB, but it is not a useful peak-memory bound.
The two `FakePIRBig*` cases are also intentionally excluded: upstream describes
them as cost-reporting tests over random encoded tables, and the README's full
suite example reports 91 GiB of Go `Sys` memory.

`run-finite-diffs-defra.sh` therefore:

1. always emits allocation-free analytical accounting first;
2. requires at least 5 GiB `MemAvailable` before any Go work;
3. runs only the named bounded correctness tests;
4. monitors the entire process group's RSS and terminates it at 2 GiB for the
   bounded suite or 4 GiB for the common-corpus adapter; and
5. never invokes `systemd-run` or a cgroup API.

No large test should be re-enabled on this host.

## Common-corpus mapping and checked analytical costs

The mapping is exact: each populated 96-byte Defra page is one artifact record.
There are 262,144 records (25,165,824 raw bytes), and the selected page is
checked byte-for-byte after `Recover`. This avoids inflating or subdividing the
useful result.

The pinned artifact's `PickParams(262144, 96, 0.5)` gives `m=21`, `D=11`, and a
capacity of 352,716 records. Each encoded truth table contains `2^21` 96-byte
records. A server returns the Hamming ball through radius five:

| Quantity | Per server | Aggregate across two servers |
|---|---:|---:|
| Encoded table | 201,326,592 B | 402,653,184 B |
| Logical record probes/query | 27,896 | 55,792 |
| Logical bytes fetched/query | 2,678,016 B | 5,356,032 B |
| Answer/download | 2,678,016 B | 5,356,032 B |
| Artifact query representation | 8 B | 16 B |

The primary Dense payload-work denominator is 25,165,824 aggregate expected
bytes: each of the two uniformly random Dense shares selects half the rows, so
the two servers XOR one table's worth of payload in expectation. Finite
differences therefore fetches **4.699× less selected payload**, not 9.397×.
Two Dense servers still walk two full row address spaces (50,331,648 bytes of
addressable rows); finite differences is 9.397× smaller only under that
secondary full-traversal denominator. Dense streams rows while finite
differences performs random probes, so neither byte ratio is an elapsed-time
ratio. The price is 8× storage per replica and a 5.11 MiB response. These are
deterministic algorithmic counts, not physical memory-traffic measurements.
The official artifact's single padded table also does not implement the
decreasing chunk decomposition used to prove Corollary 3.3.

Run the safe accounting without Go or C:

```bash
tools/pir-poc/research/run-finite-diffs-defra.sh --analysis-only
```

After reserving an exclusive artifact window, run bounded correctness and the
adapter:

```bash
tools/pir-poc/research/run-finite-diffs-defra.sh
```

The adapter calls the upstream exported implementation without changing its
cryptographic code. It records preprocessing time, sampled process RSS, client
query/recovery time, each server's answer time, their sum, exact traffic, exact
storage, and correctness. Direct calls exclude networking, serialization
framing, TLS, filesystems, and energy.

## Guarded common-corpus result

The one approved common-corpus attempt completed below the 4 GiB process-group
guard with no swap. It used Go 1.26.0, the artifact's generic C lookup path for
96-byte records, one database encoding, no online warmup, and three
correctness-checked online trials.

The measured run used the manifest's corpus identity. The adapter now also
recomputes and verifies that BLAKE3 digest before invoking `EncodeDatabase`,
using the separately pinned `github.com/zeebo/blake3` v0.2.4 dependency in its
isolated adapter module. This post-run hardening was statically validated; the
allocation-heavy measurement was not rerun.

| Phase/metric | Local artifact result |
|---|---:|
| Upstream `EncodeDatabase` | 39,777.382 ms |
| Peak adapter RSS during encoding (1 ms samples) | 691,642,368 B |
| Client `Query` p50 / p95 | 0.018 / 0.026 ms |
| Server 0 `Answer` p50 / p95 | 1.441 / 1.652 ms |
| Server 1 `Answer` p50 / p95 | 1.888 / 2.099 ms |
| **Summed two-server `Answer` p50 / p95** | **3.329 / 3.752 ms** |
| Client `Recover` p50 / p95 | 0.208 / 0.413 ms |
| Correct page recovery | 3 / 3 trials |

The aggregate server samples were 3.752, 3.329, and 2.263 ms. With only three
unwarmed trials, p95 is effectively the maximum and this is a screening result,
not a stable throughput claim. The decline across the three samples also shows
why it must not be presented as a warmed steady-state distribution. Exact
deterministic bytes and probes are stronger evidence than these preliminary
timings.

The result is stored outside Git at
`target/pir-research-results/finite-diffs-4574a4f8c52eeda165e110cbb64f834397d7c049/common-corpus.json`;
the adjacent log retains phase markers so a future guard or timeout failure is
diagnosable.

## Accounting audit

The paper's definitions matter:

- Definition 2.4 calls the length of one encoded database `DB'` “server
  storage.” It is naturally the per-replica size. A deployment with two
  independently operated replicas physically stores twice that amount, which
  this gate reports separately.
- Definition 2.5 defines server time/work as expected RAM probes **summed over
  all servers**. The upstream benchmark times one `Answer` call and labels the
  answer length for one server. This gate instead reports both server times and
  their sum. Parallel wall time is not aggregate work.
- Definition 2.6 defines communication as every query and every answer summed
  over all servers. The prototype passes Go integers in-process and does not
  define a wire format. We report its amd64 representation (two 8-byte values),
  the six-byte logical minimum for two packed 21-bit points, and no fabricated
  framing cost.
- With 96-byte records, each logical probe copies 96 bytes. Both probe count and
  byte count are reported; neither is mislabeled as hardware cache-line traffic.

## Security and the many-server theorem

The implementation is the binary two-server construction from Section 3. A
query is `(r, r+p)` over `F_2^m`. Either marginal is uniform, giving perfect
privacy against either one semi-honest server (`s=2`, `t=1`). If the servers
collude, XORing the two points reveals `p` and therefore the target ordinal.

The paper's Theorem 5.3 is broader, but it is not “add one more copy” of this
implementation. It uses a prime field `q >= s`, a degree-`t` sharing curve,
individual degree `d`, homogeneous polynomial slices, finite-difference
reconstruction of Hasse derivatives, and Hermite interpolation. It supports
`s >= 2` and `t <= s-1` under its parameter condition. None of that general
construction is present in the official Go/C artifact. In particular, this
gate is not evidence for a three-server implementation or for privacy against
two colluding servers.

This distinction also prevents a misleading reading of the paper's
many-server asymptotics. The particularly favorable `s`-server corollary sets
`t=1`; adding servers there improves work while protecting only against one
server at a time. The project's “three servers, privacy if at least one does
not collude” goal instead requires `s=3, t=2`. Theorem 5.3 permits analyzing
that setting subject to its parameter condition, but its query curve,
finite-field tables, answers, and Hermite reconstruction must be implemented
and benchmarked separately. The binary artifact cannot supply those shares.

## Production verdict

This construction is a serious cold-query frontier candidate when aggregate
server memory work dominates: on the common corpus it fetches about 21.3% as
many selected payload bytes as aggregate Dense in expectation (or addresses
10.6% as many bytes as two full Dense table traversals), and the guarded
official artifact recovered the exact page. It is not currently the default
because:

- immutable preprocessed storage is 8× the compact corpus on every replica;
- every 96-byte page query downloads 5.11 MiB before transport framing;
- rebuilding the reference encoder is allocation-heavy;
- the code is explicitly a research prototype with no authenticated artifact
  format, generation gate, network protocol, malicious-server verification, or
  implemented many-server path; and
- the first local timing has only three unwarmed samples and no network.

The next production-oriented experiment, if the guarded adapter passes, is an
immutable file/mmap encoder with bounded streaming preprocessing and the same
logical query/answer API. That can reduce build RSS and duplicate heap copies;
it cannot remove the 8× persisted table or 5.11 MiB information-theoretic
answer without changing the protocol/security lane.
