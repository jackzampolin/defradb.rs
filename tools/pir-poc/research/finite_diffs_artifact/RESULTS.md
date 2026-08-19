# Finite-differences common-corpus result

Status: correctness-checked local measurement of the pinned official artifact,
not a paper performance claim.

## Reproduction identity

- Artifact revision: `4574a4f8c52eeda165e110cbb64f834397d7c049`
- Corpus BLAKE3:
  `5e299f633d8b1685d933d9f0bbe52fd219a5f78b22f8f1e2dee6803c6c72d03e`
- Corpus: 262,144 records × 96 bytes = 25,165,824 bytes
- Mapping: one exact populated Defra tag page per artifact record
- Parameters: `m=21`, `D=11`, Hamming-ball radius 5
- Runtime: Go 1.26.0; upstream Go/C code; generic 96-byte C copy kernel
- Run discipline: one encoding, no online warmup, three trials, 4 GiB
  process-group RSS guard, abort on any swap, 10-minute timeout

The measured run used the manifest corpus identity. Afterward, the adapter was
hardened to recompute that BLAKE3 digest before encoding via pinned
`github.com/zeebo/blake3` v0.2.4. That check was statically validated without
rerunning the allocation-heavy measurement.

## Result

| Metric | Value | Scope |
|---|---:|---|
| Correctness | 3/3 | Recovered page equals manifest page byte-for-byte |
| Build | 39,777.382 ms | One `EncodeDatabase` |
| Peak build RSS | 691,642,368 B | Adapter `/proc/self/status`, 1 ms sampling |
| Client query p50 | 0.018165 ms | Generates both shares and client state |
| Aggregate server p50 | 3.328731 ms | Sequential elapsed sum of both `Answer` calls |
| Aggregate server p95 | 3.751534 ms | Maximum of only three unwarmed trials |
| Client recover p50 | 0.207579 ms | Reconstructs the 96-byte page |
| Upload | 16 B | Two 64-bit artifact query integers, no framing |
| Download | 5,356,032 B | Sum of both answer vectors |
| Logical server reads | 55,792 records / 5,356,032 B | Sum over both servers |
| Encoded storage | 201,326,592 B/server | Paper Definition 2.4 / per replica |
| Aggregate deployed storage | 402,653,184 B | Two full replicas |

The three aggregate server samples were 3.751534, 3.328731, and 2.262768 ms.
They are not a warmed distribution. Direct calls omit wire serialization,
networking, TLS, filesystem service, and energy.

## Interpretation

Each uniformly random Dense share selects half the rows. Across two servers,
Dense therefore XORs 25,165,824 payload bytes in expectation on this corpus.
The official finite-differences artifact fetches 5,356,032 bytes: a
deterministic **4.699× reduction in the primary selected-payload work metric**.
Dense also traverses the complete row address space at both replicas, or
50,331,648 addressable bytes; 9.397× is retained only as that secondary
full-traversal comparison. Dense's streaming accesses and finite differences'
random probes have different costs, so these are not latency ratios. Finite
differences costs 8× storage per replica and 5.11 MiB of download for one useful
96-byte page.

The result proves only the implemented two-server binary protocol's behavior:
perfect privacy against either individual semi-honest server (`t=1`). Both
servers together recover the target. The paper's general `s`-server theorem is
a distinct, unimplemented q-ary protocol and is not validated by this run.
In particular, its favorable one-private (`t=1`) multi-server corollary does
not satisfy the desired three-server, two-collusion (`s=3, t=2`) threat model.
