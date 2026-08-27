# Encrypted exact search

Searchable encryption can materially reduce server work, but it solves a
different threat model from strict PIR. This POC implements the smallest useful
variant for immutable exact lookups:

```text
trusted exporter
  -> token = keyed BLAKE3(generation, exact key)
  -> value = AES-256-GCM(fixed projection)
  -> shuffled token/ciphertext hash index

client sends 32-byte token
  -> untrusted sidecar performs one hash lookup
  -> client authenticates and decrypts the fixed projection
```

Run a resident index with up to one million rows:

```bash
cargo run -p pir-poc --release -- encrypted-search 1000
cargo run -p pir-poc --release -- encrypted-search 1000000
```

The implementation rejects larger resident executions instead of pretending a
one-billion-row `HashMap` fits on the development machine. The scale table
below uses exact protocol geometry for one billion rows.

## Scale comparison

Each of the two Dense replicas traverses every row position. Roughly half the
bits in each random XOR share are set, giving `N` expected aggregate payload
XORs but `2N` row-position visits. The decoy comparator reads 100 visible
candidates. Blind search reads one token entry. Response counts are fixed rows:
two shares for Dense, 100 rows for decoys and one encrypted row for blind
search.

| Rows | Dense positions visited | Dense payload XORs | 100-decoy rows | Blind-index rows | Dense XORs / decoy | Dense upload | Blind upload | Current JSON locator | 2.4-bit MPHF | Min blind index |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1K | 2,000 | 1,000 | 100 | 1 | 10x | 250 B | 32 B | 145 KB | 300 B | 69 KB |
| 1M | 2,000,000 | 1,000,000 | 100 | 1 | 10,000x | 250 KB | 32 B | 145 MB | 300 KB | 69 MB |
| 1B | 2,000,000,000 | 1,000,000,000 | 100 | 1 | 10,000,000x | 250 MB | 32 B | 145 GB | 300 MB | 69 GB |

The minimum blind-index size is the raw 32-byte token plus an AES-GCM envelope
around an 8-byte value: 69 bytes per row. It excludes hash-table allocation,
load-factor slack and allocator overhead. If the encrypted value is an inline
projection rather than an 8-byte locator, raw storage becomes approximately
`row_size + 61` bytes per record.

The current canonical JSON directory estimate comes from the gallery's
measured approximately 145 bytes per row. It is safe to parse but not viable at
one billion rows. The 2.4-bit MPHF number is the compact research assumption;
300 MB may be reasonable on a computer but is a heavy cold-client prerequisite
for a phone. Blind search needs no client locator download.

## Executed blind-index results

Both runs use an 8-byte encrypted result. Timings are medians of 101 resident
lookups and exclude transport. Raw bytes exclude `HashMap` overhead.

| Rows | Build | Raw entries | Client token | Server lookup/copy | Client decrypt | Upload | Download |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1K | 0.963 ms | 69 KB | 0.2 us | 0.1 us | 0.6 us | 32 B | 37 B |
| 1M | 1,471.9 ms | 69 MB | 0.2 us | 0.1 us | 0.5 us | 32 B | 37 B |
| 1B | not resident-executed | at least 69 GB | exact geometry only | exact geometry only | exact geometry only | 32 B | 37 B |

The sub-microsecond lookup values are at the benchmark timer's practical
resolution; their meaning is that ordinary lookup and an 8-byte copy are tiny,
not that all deployments will produce exactly 0.1 us latency.

## Leakage

The speedup is purchased by revealing information that PIR hides:

- the same key emits the same token within a generation, revealing repeated
  searches;
- the server observes which encrypted entry is returned, revealing access and
  overlap patterns;
- response size reveals result volume unless every page is padded;
- insert/update timing can correlate encrypted entries with public events;
- compromise of the search key enables offline mapping of the complete index;
- if the serving provider also built the index from plaintext, it already knows
  the token-to-key mapping and query-content privacy is lost.

These leakages are known to be exploitable. Searchable-encryption research
explicitly models search, access, overlap and volume leakage, and practical
query-recovery attacks remain possible even when some patterns are hidden:

- [SEAL: Attack Mitigation for Encrypted Databases via Adjustable Leakage](https://www.usenix.org/system/files/sec20-demertzis.pdf)
- [Hiding the Access Pattern is Not Enough](https://www.usenix.org/system/files/sec21summer_oya.pdf)
- [Leakage-Abuse Attacks Against Structured Encryption for SQL](https://www.usenix.org/conference/usenixsecurity24/presentation/hoover)

## Where it fits

The blind index is promising for high-entropy, usually one-shot exact lookups:

- Mizu nullifier -> witness;
- Shinzo transaction hash -> receipt/attestation;
- DefraDB high-entropy document ID -> authorization-specific projection.

It is less convincing for popular, repeated or low-entropy keywords such as
contract names, status enums and routing prefixes. Frequency and auxiliary
data can identify them even when the keyed token itself resists guessing.

The necessary deployment boundary is stronger than the PIR sidecar boundary:

```text
trusted/non-colluding exporter holds search + data keys
  -> exports shuffled encrypted fixed pages
  -> query server never sees plaintext or keys
```

If a DefraDB provider sees the plaintext collection and the search key, blind
tokens add encryption-at-rest but do not hide what that provider is queried
for.

## Recommended tiers

| Tier | Server work | Main leakage | Suggested use |
|---|---|---|---|
| Strict PIR | Linear scan/layout traversal | Public table/window shape | Sensitive repeated queries and providers that know plaintext |
| Blind encrypted index | One lookup + row copy | Search, access, volume and update patterns | Independent encrypted sidecar; high-entropy, mostly one-shot keys |
| Blind index + 100 encrypted decoys | 100 lookups/rows | Candidate tokens and intersections | Middle tier when bandwidth permits and token meanings are hidden from the server |
| Plain 100 decoys | 100 lookups/rows | Candidate plaintexts and intersections | Operational fallback, not a strict privacy guarantee |

Encryption is also orthogonal to PIR: Dense/Fuse tables can store these fixed
encrypted projections inline. That preserves strict query privacy and protects
the sidecar at rest, but it does not reduce the PIR scan. A compact encrypted
locator page plus PIR is useful only when the locator response is padded and
the associated projection is returned inline; a later direct document fetch
would otherwise reveal the access that the first PIR lookup hid.

Two useful mitigations can be layered onto blind search without changing the
index semantics:

1. rotate search keys and rebuild/shuffle at immutable generation boundaries,
   limiting cross-generation linkability;
2. use fixed page classes, batched real/fake requests and frequency smoothing.

[PANCAKE](https://www.usenix.org/conference/usenixsecurity20/presentation/grubbs)
demonstrates that selective replication, fake accesses and batching can protect
an encrypted key-value store from passive access-frequency analysis with much
less overhead than Path ORAM, but it requires a proxy, workload-distribution
estimates and continuous cover traffic. It is an optional privacy tier, not a
drop-in replacement for PIR.

Another useful option for small indexes is downloading authenticated,
size-locked encrypted index partitions and searching locally. The
[size-locked index work](https://www.usenix.org/conference/usenixsecurity21/presentation/xu-min)
shows why encoded index length itself must be padded and how partitioning trades
bandwidth for explicit leakage. This is attractive for small per-window routing
or enum indexes, not a billion-entry global hash space.
