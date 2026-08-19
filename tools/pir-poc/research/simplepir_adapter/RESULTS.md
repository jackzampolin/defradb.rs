# SimplePIR and DoublePIR common-corpus result

Status: correctness-checked local artifact measurement, not a paper result and
not directly comparable to replicated information-theoretic PIR security.

## Workload and runner

- Official upstream revision: `e9020b03bf2872c75b8954e749e32408b5db87ed`.
- Corpus: 1,048,576 documents encoded into 262,144 populated tag pages.
- Useful row: 96 bytes; total raw useful corpus: 25,165,824 bytes.
- Query result: the complete selected 96-byte page, checked byte-for-byte on
  every sample.
- Mapping: 24 little-endian 32-bit lanes answered with the official batch API.
- Runner: Ryzen 7 3700X, WSL2, Go 1.26.0, GCC/cgo, one upstream server thread.
- Online values below are medians of three samples. Network, TLS, hardware byte
  counters, and energy are excluded.

The official smoke suite passed `TestSimplePir`, compressed, long-row and batch
variants, plus `TestDoublePirLongRowCompressed`. The common-corpus adapter then
passed full-page reconstruction for both protocols.

## Expansion and traffic

All values are bytes. Raw bytes, parameter padding, matrices, hints, and state
are deliberately kept separate.

| Metric | SimplePIR | DoublePIR | Evidence |
|---|---:|---:|---|
| Raw useful page corpus | 25,165,824 | 25,165,824 | deterministic |
| Batch-alignment padding | 365,760 | 0 | deterministic |
| Unsquished `p.L × p.M × 4` matrix | 102,126,336 | 100,663,296 | deterministic |
| Squished online database | 34,048,896 | 33,555,456 | deterministic |
| Additional server protocol state | 0 | 2,490,368 | deterministic |
| DB-specific client hint | 20,840,448 | 67,108,864 | deterministic |
| Persistent client hint + seed | 20,840,464 | 67,108,880 | deterministic |
| Decompressed public matrices in client memory | 20,553,728 | 268,828,672 | deterministic |
| Online upload | 481,824 | 6,300,864 | deterministic |
| Online download | 20,352 | 1,639,936 | deterministic |

Point-in-time Go heap allocation after setup was 223,681,136 bytes for the
combined SimplePIR client/server process and 797,273,712 bytes for DoublePIR.
These are diagnostics, not isolated client memory or peak RSS.

## Separately timed phases

These component measurements have different amortization horizons. They are
not summed into a jointly timed latency.

| Phase | SimplePIR | DoublePIR |
|---|---:|---:|
| Raw-to-lane transform | 48.660 ms | 45.843 ms |
| Upstream database encoding | 551.502 ms | 567.272 ms |
| Compressed shared-matrix initialization | 704.952 ms | 8,131.892 ms |
| DB-specific hint setup | 9,325.496 ms | 7,051.695 ms |
| Client public-matrix decompression/setup | 618.323 ms | 8,267.960 ms |
| Client online query generation, p50 | 151.002 ms | 1,745.184 ms |
| Server online answer, p50 | 3.499 ms | 7.954 ms |
| Client reconstruction, p50 | 34.790 ms | 855.689 ms |

An immediately preceding three-sample pass measured 3.172 ms SimplePIR and
11.606 ms DoublePIR server p50. The 3.17–3.50 ms and 7.95–11.61 ms observed
ranges show why more isolated samples and hardware counters are required before
placing either number in the final Pareto table.

## Interpretation

For this 25.17 MiB useful corpus, SimplePIR is the stronger of the two upstream
artifacts on every measured online client/server and communication metric.
Its roughly 20 MiB hint, 20 MiB decompressed public matrix, 0.46 MiB upload,
20 KiB response, and sub-second client setup are compatible with a modern
phone-class device, subject to a real ARM64/mobile measurement. DoublePIR's
smaller-hint goal does not materialize for this 96-byte, 24-lane mapping: it has
a 64 MiB hint and roughly 256 MiB of decompressed public matrices.

The server time is attractive, but this remains a different security line from
the main `n-1`-collusion target: it is one-server computational PIR under LWE,
not replicated information-theoretic PIR. The common result and workload make
performance comparisons possible; the accounting gate must still block a
direct security-equivalent winner claim.

Fuse-4 is not composed into one upstream query. The official API produces an
arithmetic point query, while the current Fuse layout requires bytewise XOR of
four cells. Four independent cell PIRs would change server work and is retained
as a separately modeled experiment rather than mislabeled as one scan.
