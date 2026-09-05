# Six private index compositions

This suite implements the six follow-up experiments from the bit-index design
discussion. It is isolated research code; production serving defaults are not
changed. The objective is aggregate work, not latency per machine.

## Implementations

| Composition | Implemented variants | Complete answer |
|---|---|---|
| Radix | 1/2/4/8-bit digits; full traversal; fixed early stop into leaf buckets; contiguous and scattered 32-bit keys | Matching IDs and inline payloads; fixed branch/leaf access count for hits and misses |
| Hash | Two-choice bounded buckets with public construction salt retries and a padded overflow bucket | Always privately read both candidate buckets and overflow; verify full keys, deduplicate and return all matching inline payloads |
| Hierarchical compressed bitmap | Private primary-key block directory; private block pages; adaptive array/run/bitmap encoding; secondary bitplanes; three-party ABY3 AND | Private payload read for every padded output slot; primary directory fanout padded to the maximum |
| Wavelet | Wavelet matrix with sampled rank blocks; several block sizes; range count and bounded reporting | Four private rank accesses per field bit for two thresholds; reporting adds a fixed number of covering-array reads |
| Inline postings | One fixed-width covering result page per key in a bounded integer domain | Full matching IDs and payloads, with Dense, Path ORAM or streamed SinglePass setup |
| Authenticated ordered tree | Merkle ordered segment tree; membership/predecessor/lower absence; incremental value, key, deletion and reserved-slot insertion | Two fixed verified paths under a trusted current root; authenticated choice of predecessor; stale-root/tampering checks |

The hash prototype is two-choice bucketed hashing, not a cuckoo-relocation
implementation. The bitmap prototype is client-assisted: the client can see
visited block metadata and supplies fresh shares to the server-side AND. It
does not claim server-only candidate processing or symmetric database privacy.
The wavelet prototype searches the complete column; it does not implement every
subsequence/rank/select/quantile variant. Authenticated records use SHA-256, not
the production Poseidon witness schema. Updates in this lane use a fixed key
universe; this is not a dynamic multiwriter B-tree.

## Private memory and trust

- `dense`: existing two-replica Dense selection over actual TCP endpoints.
- `path`: existing encrypted Path ORAM, Z=5, one honest serialized owner;
  position map, stash, full read/write paths and setup are included.
- `singlepass`: Python port of the repository's show-and-shuffle algorithm.
  Every source row is downloaded once during metered setup in public 1,024-row
  chunks; fresh cryptographic permutations and query randomness are used.
  Updates invalidate hints and require a charged generation rebuild/refresh.
- `ramen`: the actual [Ramen artifact](https://github.com/AarhusCrypto/Ramen),
  revision `e39e55625fea803c8d369f31988e7cbe8d656c7a`, with three independent
  processes and TCP peer channels. The client freshly additive-shares every
  database cell, address, operation and replacement. Nodes are split into
  15-byte field limbs; **every limb access and automatic epoch rebuild runs**.
  The artifact assumes at most one passive corruption.
- `dense-native`, `path-native`, `singlepass-native`: compiled Rust store roles
  with a set-bit XOR scan control. The index navigation and client algorithms
  remain Python. Client/store transport uses metered JSON-line pipes; Ramen
  peers use TCP. These are local comparisons, not WAN latency predictions.

Ramen's original dependency constraint admits an incompatible newer derive
macro. The checked-in artifact lockfile pins `bincode_derive` to `2.0.0-rc.2`.
The bridge and lockfile do not replace the Ramen protocol. Build concurrency is
bounded to two jobs to avoid exhausting the local WSL environment.

All index records use padded JSON in this first composition experiment.
Encoding, padding, limb expansion and transport costs are charged. Binary
layouts, vector-valued DORAM, recursive client maps and malicious-security
hardening remain separate engineering tasks. The optional 256-row Ramen radix
pilot was measured; larger optional Ramen radix sweeps were stopped after its
scalar-access cost was far above the compiled controls. This is an adapter
frontier, not an impossibility result for distributed private indexes.

## Accounting

Each run records actual role PIDs, full process CPU, operation CPU, application
wire bytes, inter-server bytes, storage and role RSS. Query CPU is summed across
all participating roles **before** computing percentiles. The bitmap lane
includes the additional three MPC processes. Query correctness is checked
against an independent oracle outside the timed query.

The conservative total in the report is:

```
(all server process CPU + index-build/setup/update controller CPU)
/ verified complete answers
```

The lifecycle controller term includes client setup as well as publication and
index construction. This intentionally overcharges client setup in the primary
total rather than treating it as free server work. Online client CPU is separate;
the all-participant total is also recorded. Startup, replaced generations and
refresh work are retained. Updates expose their schedule and writer addresses.
Non-authenticated indexes rebuild; the authenticated tree writes only changed
paths. Key changes remove the old authenticated slot and install the new slot.

The honest query routine receives a metadata-only index view, never the source
table. The benchmark process also contains the publisher and correctness oracle;
its 128 MiB transient-RSS check conservatively includes those retained copies and
instrumentation. Backend state plus navigation metadata has a 64 MiB persistent
cap. Each complete query has 1 MiB upload/download and 1 second client CPU caps;
setup has a 64 MiB download and 10 second controller CPU cap. A correct run that
exceeds a cap is reported separately and is not an admitted winner.

Old Ramen bridge online phase samples ended before reply serialization. The
report marks those samples `~`; their full process totals still count that work.
The final bridge records completed phases including serialization. Retained
screening campaigns may overlap on the local host, so their wall times are not
used to rank designs. CPU results are local prototype evidence, not extrapolated
production throughput or a cryptographic audit.

## Reproduce

Use Linux/WSL and the existing pinned Python requirements. From repository root:

```bash
python3 tools/pir-poc/research/prepare_index_compositions.py \
  target/private-index-artifacts --target-dir /root/private-index-build

PIR_INDEX_BRIDGE=/root/private-index-build/release/examples/private_index_bridge \
  python3 -m unittest discover -s tools/pir-poc/research -p 'test_*.py' -v

python3 tools/pir-poc/research/run_index_compositions.py \
  --profile all --repeats 3 --output target/private-index-all \
  --ramen-binary /root/private-index-build/release/examples/private_index_bridge

python3 tools/pir-poc/research/report_index_compositions.py \
  target/private-index-all --output target/private-index-report.md
```

Output directories must be new. Profiles are `smoke`, `screen`, `native`,
`ramen`, `extras`, `frontier`, `warm`, `maintenance`, and `all`. `screen` is the
broader Python 256/1,024/4,096-row sweep. `warm` executes 4,096 queries after
setup, rather than projecting an amortized benefit. `--family`, `--backend`,
`--matrix`, `--dry-run`, and `--timeout` support bounded/adaptive runs. Timed-out
cases terminate only their own process group. All result/failure files remain.

The test suite includes exhaustive small-domain answers, absence, duplicate
postings, bounded overflow rejection, variable radix cutoffs, arbitrary range
boundaries, compression roundtrips, fixed access schedules, tampering, stale
roots, updates and repeated real private accesses. Compiled artifact tests cross
many Ramen epochs, include non-power-of-two row counts and 15-byte limb boundaries,
and verify writes. Set `PIR_INDEX_BRIDGE` to execute those tests instead of skipping
them. See [measurements](PRIVATE_INDEX_MEASUREMENTS.md) and its adjacent CSV/JSON.
