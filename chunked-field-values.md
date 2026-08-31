# Chunked Field Values

Distributing byte arrays larger than any single node can hold in memory, over
the Iroh transport, using a chunk list plus streaming continuation.

| | |
|---|---|
| Status | Draft for review |
| Runtimes | Go and Rust |
| Scope | Distribution and memory bounds. Not retention, not lifecycle. |

---

## 1. Problem

A field value is carried inline in the CRDT delta block. Nothing splits it, and
every transfer path frames it as one message, so peak memory on both sender and
receiver scales with the value rather than with a bounded unit. Above a few tens
of megabytes there is no path at all, and well below that there is no path a
constrained node can walk.

The Iroh lane makes this concrete. Its CAR transfer reads a length prefix and
then allocates the whole payload:

```rust
let mut payload = vec![0u8; len];
recv.read_exact(&mut payload).await?;
```

capped by `MAX_CAR_SIZE = 64 MiB`, with the sender serialising the entire
response through `cbor::to_vec` before writing. QUIC underneath is already a
stream; the framing on top of it is not. A browser node over OPFS or an embedded
node cannot take a 64 MiB allocation per in-flight transfer, and no allocation
cap makes a 1 GB value fit.

### Requirements

- **R1** Peak memory per transfer is bounded by a fixed chunk, independent of value size, on both sides.
- **R2** A value of any size transfers to completion.
- **R3** An interrupted transfer resumes without refetching what arrived.
- **R4** Every unit is verified on arrival, before it is persisted.
- **R5** A node may hold a document without holding the value.
- **R6** Values below the threshold are byte-identical to today: same CIDs, same DocIDs.

---

## 2. Three layers

Three mechanisms keep getting conflated. Only the middle one is a wire format.

| Layer | Does | Defines CIDs? | Configurable? |
|---|---|---|---|
| **A** Value at rest | Splits a stored value across key-value entries so the backend limit stops binding. This is `corekv/chunk`. | No | Yes, per node |
| **B** Content structure | Splits a value into a DAG of content-addressed blocks. This is what `lens/host-go/store/block.go` does for WASM. | **Yes** | **No** |
| **C** Transport | Moves those blocks within a bounded memory budget, resumably. | No | Per transport |

Layer B is the chunk list. Once it exists, Layer A largely stops mattering,
because every block is small enough for any backend. Layer C is where the
constrained-environment requirement actually lives, and it is free to differ per
transport.

On Iroh: content identity is Layer B and must be identical across transports, so
the transport cannot define it. But Iroh is the better Layer C, because a QUIC
stream is already a continuation. The Rust tree depended on `iroh-blobs`,
dropped it, and nothing replaced its function; `blake3` survives on that lane
only for topic hashing.

---

## 3. Chunk list

A value is encoded exactly as today. At or below `INLINE_THRESHOLD` the delta
carries it inline and nothing changes. Above it, the bytes move into a chunk DAG
and the delta carries a descriptor plus a link. The decision is on encoded
length alone, never on field kind, so it covers blobs, oversized strings,
arrays, JSON, and vectors through one mechanism.

```
FieldBlock (dag-cbor)                    inline form, unchanged
  delta: Lww { fieldName, priority, collectionVersionID,
               data: <encoded value> }

FieldBlock (dag-cbor)                    chunked form
  delta: Lww { fieldName, priority, collectionVersionID,
               data: <descriptor CBOR> }
  links: [ DAGLink { name: "value", cid: <ChunkRoot> } ]

ChunkRoot (dag-cbor)   { v, len, alg, chunks: [Cid, ...] }
ChunkLeaf (raw, 0x55)  <CHUNK_SIZE bytes; final leaf short>
```

This mirrors the shape already shipping for Lens WASM, where `LensBlock` is
either inline `WasmBytes` or a `Chunks` link list, selected by size.

> **The link must be a real link.** Link extraction runs over the whole
> dag-cbor structure and finds any CID encoded as a CBOR link. The delta's
> `data` is an opaque byte string, so a CID inside it is invisible to the DAG
> walker, to block collection, to missing-link discovery, and to access-grant
> computation. A CID buried in `data` yields a value whose chunks silently never
> replicate.

Two consequences fall out rather than needing work. The materialised datastore
entry is written from the delta's `data` field verbatim, so once `data` is a
descriptor the second copy is a descriptor too, and the value stops being stored
twice. For the same reason the equal-priority LWW tie-break, which today loads
the entire current value to compare lexicographically, compares descriptors
instead. That is a real semantic change: tie-break ordering moves from content
order to descriptor order, so both runtimes must adopt it together.

Version 1 uses one level: a root holding a flat leaf list, capping a value in
the tens of gigabytes. The root's `v` reserves a balanced-tree encoding beyond
that.

---

## 4. Invariants

**I1. Chunking parameters are protocol constants.** `CHUNK_SIZE` and
`INLINE_THRESHOLD` are compiled into both runtimes and versioned through the
descriptor, never node options. Two nodes chunking the same bytes differently
produce different leaf CIDs, a different root, and a different field-block CID.
For a CRDT register that is two distinct values for one logical write.

> **This already exists in lens, where it costs less.** Lens exposes chunk size
> as an option: Go defaults to 3 MiB, the Rust path uses 256 KB, and both
> runtimes create lens blocks. Any WASM between those sizes gets a different CID
> per runtime, and definitions reference the transform by CID. Only constants
> have been compared, so this needs a test rather than a bug report, but it is
> the concrete argument for I1.

**I2. Below the threshold, bytes are identical to today.** Same delta bytes,
same block CID, same DocID. Enforced by a fixture round-trip generated before
the change, in both runtimes.

**I3. One chunk resident.** No path materialises a whole value: not ingest, not
hashing, not the merge path, not transfer, not read. This is the requirement
constrained nodes impose on everything else in this document.

---

## 5. Type and interface definitions

Normative: the CBOR encodings of the descriptor, chunk root, and stream frame,
plus the constants. Both runtimes must produce identical bytes. Non-normative:
the API shapes, which follow each language's idiom and need not mirror each
other.

### Shared constants and encodings

```
CHUNK_SIZE        = 1 MiB          leaf payload, exact except final leaf
INLINE_THRESHOLD  = 1 MiB          encoded length at or below stays inline
DESCRIPTOR_V1     = 1
HASH_ALG_SHA2_256 = 1

descriptor := { "v": uint, "len": uint, "alg": uint, "root": Cid }
chunk root := { "v": uint, "len": uint, "alg": uint, "chunks": [Cid] }
frame      := { "cid": Cid, "idx": uint, "bytes": bytes }
```

Canonical CBOR key ordering already used for blocks applies unchanged.

### Rust

```rust
pub struct ValueDescriptor {
    pub version: u8,
    pub len: u64,
    pub alg: HashAlg,
    pub root: Cid,
}

pub enum FieldPayload {
    Inline(Bytes),
    Chunked(ValueDescriptor),
}

pub enum Residency {
    Resident,
    Partial { have: u64, total: u64 },
    Absent,
}

#[async_trait]
pub trait PayloadStore: Send + Sync {
    async fn put_leaf(&self, cid: &Cid, data: &[u8]) -> Result<()>;
    async fn get_leaf(&self, cid: &Cid) -> Result<Option<Bytes>>;
    async fn residency(&self, d: &ValueDescriptor) -> Result<Residency>;
    /// Leaf indices still missing, for resume.
    async fn missing(&self, d: &ValueDescriptor) -> Result<Vec<u64>>;
}

#[async_trait]
pub trait ValueChunker: Send + Sync {
    /// Consumes src one chunk at a time; never buffers the whole value.
    async fn write(
        &self,
        src: Pin<Box<dyn AsyncRead + Send>>,
    ) -> Result<ValueDescriptor>;

    /// Yields leaves in order, verifying each before it is emitted.
    fn open(
        &self,
        d: &ValueDescriptor,
        range: ByteRange,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

    async fn verify(&self, d: &ValueDescriptor) -> Result<bool>;
}

/// Layer C. One call moves a whole value; memory stays at one chunk.
#[async_trait]
pub trait ValueTransport: Send + Sync {
    /// Serve leaves for `d`, skipping indices the peer already holds.
    async fn serve(
        &self,
        d: &ValueDescriptor,
        skip: &[u64],
        sink: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()>;

    /// Fetch leaves for `d`, persisting and verifying each as it lands.
    /// Resumes from `store.missing(d)`; returns bytes actually transferred.
    async fn fetch(
        &self,
        d: &ValueDescriptor,
        from: &PeerId,
    ) -> Result<u64>;
}
```

### Go

```go
type ValueDescriptor struct {
    Version uint8
    Len     uint64
    Alg     HashAlg
    Root    cid.Cid
}

type FieldPayload struct {
    Inline     []byte           // nil if chunked
    Descriptor *ValueDescriptor // nil if inline
}

type Residency struct {
    State ResidencyState
    Have  uint64
    Total uint64
}

type PayloadStore interface {
    PutLeaf(ctx context.Context, c cid.Cid, data []byte) error
    GetLeaf(ctx context.Context, c cid.Cid) ([]byte, error)
    Residency(ctx context.Context, d ValueDescriptor) (Residency, error)
    // Leaf indices still missing, for resume.
    Missing(ctx context.Context, d ValueDescriptor) ([]uint64, error)
}

type ValueChunker interface {
    // Consumes src one chunk at a time; never buffers the whole value.
    Write(ctx context.Context, src io.Reader) (ValueDescriptor, error)

    // Verifies each leaf as it is read.
    Open(ctx context.Context, d ValueDescriptor, r ByteRange) (io.ReadCloser, error)

    Verify(ctx context.Context, d ValueDescriptor) (bool, error)
}

type ValueTransport interface {
    // Serve leaves for d, skipping indices the peer already holds.
    Serve(ctx context.Context, d ValueDescriptor, skip []uint64, w io.Writer) error

    // Fetch leaves for d, persisting and verifying each as it lands.
    // Resumes from Missing(d); returns bytes actually transferred.
    Fetch(ctx context.Context, d ValueDescriptor, from PeerID) (uint64, error)
}
```

### Contract notes

- `write` must not buffer beyond one chunk and must be safe to abort: partial
  leaves are unreferenced blocks, collected by the same GC that handles orphans.
- `open` and `fetch` verify each leaf against its CID before it is emitted or
  persisted. A mismatch is an error, never a short read.
- `serve` writes frames onto an already-open stream and must not build a
  response in memory.
- `fetch` is idempotent: calling it on a partially transferred value moves only
  what is missing.
- `residency` is node-local and never participates in convergence.
- Access filtering applies per leaf, not once per request.

### Conformance

A shared fixture set is the contract, not the signatures: for a fixed input
corpus, the expected leaf CIDs, root CID, descriptor bytes, and resulting
field-block CID. Both runtimes run it. The I2 fixtures, generated before the
change, live in the same set.

---

## 6. Streaming continuation

Once the value is a chunk list, resumability is mostly free: the root names
every leaf, so a receiver knows the full CID set up front, can request leaves in
any order, and re-requests only what is missing. There is no per-level round
trip left for a bulk archive format to amortise.

What is not free is the memory bound, and that is where the work is.

**Iroh lane.** The QUIC stream is the continuation. A `/defra-iroh/value/0.1`
ALPN carries the descriptor, then the peer's already-held indices, then a
sequence of frames. The sender reads one leaf from the store and writes one
frame; the receiver reads one frame, verifies it against its CID, persists it,
and drops it. Peak memory is one chunk on both sides, whatever the value size.
Resume is reopening the stream with a new skip set. No cursor protocol, no
archive format, no allocation proportional to the response.

**libp2p lane.** Request and response are message-framed, so the same property
needs the skip set carried explicitly per request and a bounded number of leaves
per response. This is strictly worse and is the reason to prefer Iroh for large
values.

Independently of this design, the existing collector has a bug worth fixing: a
block larger than the response budget is skipped and therefore permanently
unservable, and reaching the budget abandons the rest of the walk, dropping
unrelated siblings.

---

## 7. Constrained environments

Peak memory today is set by the transfer framing, not by the value. On the Iroh
lane that is a 64 MiB allocation per in-flight CAR, on the sender and again on
the receiver.

| Concurrent transfers | Today, 64 MiB frame | Streaming, 1 MiB chunk |
|---:|---:|---:|
| 1 | 64 MiB | 1 MiB |
| 4 | 256 MiB | 4 MiB |
| 16 | 1 GiB | 16 MiB |

The important column is neither: it is that the streaming figure does not move
when the value grows. A browser node over OPFS, a mobile embedded node, and a
server node run the same code path with the same ceiling, and differ only in how
many transfers they admit at once.

This is also what makes R5 workable. A constrained node can hold documents,
serve queries over their metadata, and fetch a value only when something asks
for it, because holding the descriptor costs a few dozen bytes.

---

## 8. Distribution cost

A 1 GB value at 1 MiB chunks is 1024 leaves plus a root. It does not transfer
today at all: the Iroh CAR cap is 64 MiB and the libp2p one is 16 MiB. Under
this design it is one stream carrying 1025 frames, at one chunk of memory.

Fan-out is the other half. Per-node egress under naive push to every subscribed
peer, at the capture rates under discussion:

| Rate | N = 3 | N = 10 | N = 50 |
|---|---:|---:|---:|
| 200 MB/s, value pushed | 400 MB/s | 1.8 GB/s | 9.8 GB/s |
| 20 MB/s, value pushed | 40 MB/s | 180 MB/s | 980 MB/s |
| Descriptor only, 200 MB/s in 3.4 MB segments | 35 KB/s | 159 KB/s | 865 KB/s |

Pushing values closes at no useful N at the high rate. Sending the descriptor
with the document and letting peers fetch on demand is bounded by document count
rather than by payload size. Since both use the same blocks and the same CIDs,
this is a per-node policy, not a wire change: a server peer can prefetch, a
phone can wait until something reads.

---

## 9. Compatibility and phases

DocIDs derive from the genesis composite CID, and the composite links field
blocks. A chunked field block has different bytes, so a different CID, so a
different composite, so a different DocID. I2 bounds this to values that cannot
exist today, so no deployed document is affected and no migration runs.

| # | Phase | Wire | Gate |
|---:|---|---|---|
| 1 | Collector bug: oversize block served alone, walk advances instead of terminating | none | Oversize block served; siblings kept |
| 2 | Bytes for Blob | **yes** | Cross-runtime identity preserved |
| 3 | Chunk list, descriptor, value link, fixed constants, descriptor tie-break | **yes** | Conformance and I2 fixtures green in both runtimes |
| 4 | Iroh value ALPN with frame streaming and skip set | additive | 1 GB value transfers at one chunk of memory; resumes after interruption |
| 5 | Streaming ingest and read; typed embedded surface | additive | Write and read a 1 GB value with bounded memory |
| 6 | libp2p lane parity | additive | Same transfer over libp2p within a bounded per-response leaf count |

Phases 2 and 3 both change the delta block and should land together,
renegotiating compatibility once. Phase 1 is independent and can go now.

---

## 10. Evidence and open questions

| Claim | Confidence |
|---|---|
| Iroh CAR allocates the whole payload; `MAX_CAR_SIZE` is 64 MiB | Verified in source |
| Sender serialises the whole response before writing | Verified in source |
| Composite head is what gossips; field values are in linked blocks | Verified in source |
| Datastore value is the delta's `data` field verbatim | Verified in source |
| Equal-priority tie-break loads the whole current value | Verified in source |
| libp2p CAR budget 16 MiB / 10k blocks; oversize block unservable | Verified in source |
| Blob is hex text in both runtimes | Verified in source |
| Lens CID divergence, 3 MiB against 256 KB | Constants differ and both runtimes build lens blocks; no cross-runtime test run |
| Memory table | Arithmetic over verified constants |
| Fan-out table | Arithmetic over quoted rates |
| Sustained throughput at these rates | **Not measured.** The existing benchmark uses an in-memory store with 16-byte and 1 KB values |

### Open questions

1. **Constants.** Agreement that chunk size and threshold are protocol constants
   rather than options, and whether lens is retrofitted.
2. **Chunk size.** Proposed 1 MiB, threshold equal to it. Permanent once shipped.
3. **Tie-break.** Confirm descriptor comparison replaces value comparison in both
   runtimes simultaneously.
4. **Iroh blob layer.** Whether to adopt `iroh-blobs` verified range streaming
   for Layer C instead of a hand-written value ALPN. It gives BLAKE3 range
   proofs and resume, at the cost of a second hash function alongside the CID's.
5. **Per-leaf access control.** Leaves are raw blocks with no field context.
   Whether they inherit the field block's policy, and how that is enforced on a
   stream rather than per request.
6. **Prefetch policy.** What decides whether a peer pulls a value eagerly, given
   it is per-node and not part of convergence.

---

Draft for review across the Go and Rust runtimes. Section 10 records what is
verified, derived, and unmeasured; no figure here is a measurement unless it
says so.
