# YPIR common-corpus result

Status: correctness-checked local artifact measurement on the official AVX2
scalar/non-explicit path. This is not an AVX-512 paper-result reproduction and
is not security-equivalent to replicated information-theoretic PIR.

## Pin and gates

- Official USENIX artifact revision:
  `b9801521301f34502496d694b2ac034857104ebc`.
- Annotated tag: `artifact-evaluation`.
- Permanent archive: [Zenodo 13117988](https://doi.org/10.5281/zenodo.13117988),
  MD5 `7a1836864bd54fd3288c7a916619b2da`.
- `scheme::test::test_ypir_basic`: passed.
- `scheme::test::test_ypir_simplepir_basic`: passed.
- Common corpus: four full-page reconstructions passed (one excluded warmup
  plus three measured samples), including the final logical page in the
  incomplete last physical row.

Every phase ran below the 5 GiB process-tree watchdog ceiling and used zero
swap. The larger official YPIR+SP correctness test peaked at 3,800,297,472
bytes of aggregate sampled RSS. The common-corpus process had a 2,084,840 KiB
maximum RSS from `/usr/bin/time`; the one-second watchdog sampled a lower
1,532,440,576-byte peak, so the former is the retained process peak.

## Exact workload and mapping

- Raw corpus: 262,144 x 96-byte pages = 25,165,824 useful bytes.
- Physical mapping: 70 pages per YPIR+SP row.
- Populated rows: 3,745, padded to 4,096.
- Useful row: 6,720 bytes; decoded physical row: 7,168 bytes.
- Encoded plaintext table: 29,360,128 bytes.
- Upstream `u16` database allocation: 33,554,432 bytes.
- Database encoding/padding above useful corpus: 4,194,304 bytes.

The grouping is required because YPIR+SP rejects records below `2048 * 14`
bits. Seventy is the minimum-table valid mapping: unlike 64, it is exactly
14-bit aligned and cannot lose trailing input bits. The server privately
returns a whole physical row and the client selects one 96-byte page locally.

## Separately timed phases

| Phase | Result |
|---|---:|
| Corpus load + upstream server layout | 579.803 ms |
| Database-dependent server preprocessing | 2,863.563 ms |
| Client query generation, p50 | 62.311 ms |
| Online server answer, p50 | 80.655 ms |
| Client response recovery, p50 | 5.332 ms |
| Online upload | 573,440 B |
| Online download | 24,576 B |
| Offline client hint | 0 B |
| Serialized offline server state | 741,573,720 B |

The three online server samples were 84.419, 78.409, and 80.655 ms. The query
samples were 62.311, 62.652, and 61.511 ms; recovery samples were 5.407, 5.330,
and 5.332 ms.

The serialized state measurement creates a transient byte buffer and therefore
contributes to the process RSS peak. It nevertheless reveals a real deployment
cost: removing the client hint is achieved with roughly 742 MB of
database-dependent server preprocessing state for this physical layout.

## Same-host SimplePIR comparison

This comparison is within the single-server computational lane and uses the
existing correctness-checked SimplePIR common-corpus result. It is not a claim
of identical protocol assumptions or vectorization.

| Metric | SimplePIR | YPIR+SP AVX2 | Change |
|---|---:|---:|---:|
| Server online p50 | 3.499 ms | 80.655 ms | 23.05x / +2,205% |
| Client query + recovery | 185.792 ms | 67.643 ms | -63.6% |
| Upload | 481,824 B | 573,440 B | +19.0% |
| Download | 20,352 B | 24,576 B | +20.8% |
| Client hint | 20,840,448 B | 0 B | removed |
| DB + serialized protocol state | 34,048,896 B | 775,128,152 B | 22.77x |

On this AVX2 runner, YPIR is not the server-work winner. Its useful trade is a
hintless client with substantially lower online client CPU, paid for by much
higher scalar server time and server state. An AVX-512 run could change the
server-time conclusion and must be recorded as a separate hardware result; it
cannot erase the measured state and physical-row expansion.
