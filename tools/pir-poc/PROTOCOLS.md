# Protocols

How the POC's protocols work. [DECISIONS.md](DECISIONS.md) chooses between them;
[BENCHMARKS.md](BENCHMARKS.md) contains costs.

## Dense XOR

The client represents its chosen row as a bit vector, then splits that vector
into random XOR shares. Each replica sees only its share and XORs the rows
selected by it. Combining all replies cancels the random selections and leaves
the requested row.

Selection privacy is information-theoretic while at least one replica remains
non-colluding. The construction supports additional replicas, but every reply
is required; a third server does not provide fault tolerance. Upload and server
traversal grow with table size. It needs no full-table client preload, although
keyword-to-row metadata may still be needed.

For a fixed replica count, a single-row query's scan cost grows roughly with
row count times encoded row width. A known-block receipt table can therefore
be much cheaper than a multi-block payload table using exactly the same Dense
protocol. See the [table footprints](BENCHMARKS.md#why-receipt-retrieval-is-cheaper).

## Packed-presence Dense

Think of this as **private block alerts using Dense XOR**. Instead of retrieving
a document, the wallet privately asks: "Did anything for my routing tag appear
in this block?"

For example:

1. The wallet registers random Dense selector shares for its routing bucket,
   one share with each server. No individual server learns the selected bucket.
2. During each block, the servers mark which buckets received events in a
   yes/no table. Multiple events in a bucket still produce just one "yes".
3. At block close, each server evaluates the registered shares against that
   table, once per subscriber. The wallet combines the replies to learn yes/no
   for its bucket.
4. If yes, the wallet makes a separate private query for the encrypted actions.

**Packed** means the yes/no table uses one bit per bucket: eight buckets fit
in one byte. **Presence** means the result says whether an event appeared,
not how many events appeared or what they contained.

This is not a different, faster cryptographic construction. Compared with
Dense over full payload or histogram rows, it retrieves less information from
a smaller representation. Specializing ordinary Dense to this bitmap gives
packed-presence Dense; it cannot replace document retrieval.

Compared with immediate per-event DPF, another saving comes from combining
events into one table before answering subscriptions. Servers still evaluate
every subscriber each block. Registered selectors avoid repeated uploads but
require retained server state; the fastest benchmark also needs a ready batch
and GPU-resident selectors. See [live costs](BENCHMARKS.md#live-costs).

The tradeoffs are waiting for block close and retaining the selector shares.
Hash-bucket collisions can cause false alerts, resolved after retrieval.
Like Dense, it supports additional replicas, requires all replies, and hides
the selected bucket while at least one replica remains non-colluding.
Hit-only follow-up traffic can reveal timing; see [PRIVACY.md](PRIVACY.md).

## Compact DPF and GPU-DPF

A distributed point function splits a selector into small cryptographic keys.
Evaluating and combining the keys yields a nonzero value at the target position
and zero elsewhere. Neither party's key reveals the target under the
construction's cryptographic assumptions and non-collusion requirement.

For a snapshot, servers expand/evaluate the selector across the table. For an
immediate alert, each server evaluates its registered key at the new event's
bucket. Small keys save upload but add cryptographic work.

The served Compact-DPF implementation uses an AES-based generator. The research
GPU-DPF adapter is a separate implementation; GPU acceleration is on the server.
Both are two-party here. Deploying pairwise copies on three servers does not
give privacy against two colluding servers.

## SinglePass

During setup, the client reads an immutable generation and constructs local
parity hints and permutation state. Later queries send selected positions to
two servers; replies and the hints recover the target without full online scans.

The saving depends on amortizing preload and preserving evolving client state.
Queries update that state and must complete consistently; rolling back after a
possibly delivered request can break privacy. A new generation requires refresh.
This POC has exactly two server roles.

## InsPIRe

The client encrypts its selector. A single server performs encrypted algebra
over a prepared database representation, and the client decrypts the response.

Privacy relies on computational cryptographic assumptions rather than separate
non-colluding providers. Server preprocessing, expanded state and encrypted
evaluation replace Dense's simple XOR work. The GPU variant accelerates the
server, not the wallet; client query generation still has a cost.

## Blind encrypted search

An independent exporter turns an exact key into a secret keyed search token
and encrypts the associated value. The client sends that token; the serving
index performs an ordinary lookup and returns ciphertext.

This is not PIR: repeated tokens, selected entries and update correlations
remain visible. If the provider knows the plaintext-to-token mapping, the token
does not hide the query from it. Encryption of values and privacy of selection
are separate properties.

## 100 decoys

The client sends its real key with unrelated candidates. The server returns all
candidate rows; the client keeps its target and ignores the others.

No cryptographic selection secrecy is provided. The server sees the candidate
set, and popularity or repeated-query intersections can reveal the target.
Server work is indexed reads rather than a private full-table traversal.

## Other explored constructions

| Construction | What it does | Added requirement |
|---|---|---|
| Finite-differences PIR | Preprocesses encoded data so algebraic combinations of server answers recover a selected entry with less online work in the tested setting. | Expanded stored representation and communication; evaluated artifact is two-server. |
| ChalametPIR | Single-server computational PIR using LWE and a client hint. | Client setup/state and query computation; it is not a lightweight proxy lookup. |
| Path ORAM | Hides a sequence of reads/writes by accessing and reshuffling tree paths. | Client position state/stash and interactive reads/writes. |
| TEE + ORAM | Runs computation in an attested enclave, using ORAM to obscure memory access. | Hardware/attestation trust plus side-channel and ORAM constraints. |
| RAID-PIR | Distributes encoded storage/query work across multiple servers. | A different replication/collusion tradeoff; not the same as adding a Dense replica. |

## Layouts and composition—not additional privacy protocols

| Technique | Meaning |
|---|---|
| Compact ordinal / MPHF | Maps populated keys to compact row positions, avoiding empty rows. Public mapping metadata must be acceptable to expose. |
| Binary Fuse | Encodes a keyed value across a small set of cells whose XOR reconstructs it. PIR privately retrieves that combination. |
| Packed pages / inline projection | Stores bounded useful result fields together, reducing unnecessary scans or follow-up fetches. |
| Two-stage retrieval | Privately retrieves a locator/directory entry, then privately retrieves its data. Both stages must preserve the intended secrecy. |
| Shared traversal / Four-Russians batching | Reuses row traversal or temporary XOR combinations across ready requests; does not change Dense's trust model. |

PIR does not hide IP addresses or traffic timing. OHTTP, Tor and padding are
separate layers described in [PRIVACY.md](PRIVACY.md).
