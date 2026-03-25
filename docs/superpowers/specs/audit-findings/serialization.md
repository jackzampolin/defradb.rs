# Serialization & Zero-Copy Audit Findings

## Summary
- Total findings: 16
- Critical: 0 | High: 5 | Medium: 7 | Low: 4

## Findings

### Finding 1
- **severity:** high
- **category:** anti-pattern
- **crate:** blockstore
- **file:** crates/blockstore/src/lib.rs
- **line:** 190-191
- **pattern:** hot-path-clone
- **description:** `Blockstore::get()` clones the cached `Vec<u8>` on every cache hit (`data.clone()` on line 191). This is the primary block read path -- every document query, merge, and P2P sync operation hits this. The LRU cache stores `Vec<u8>` so each clone is an O(n) copy of the full block. For a 4 KB block, this is 4 KB of allocation+copy per read.
- **training_ref:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Change the cache type from `LruCache<Cid, Vec<u8>>` to `LruCache<Cid, bytes::Bytes>`. `Bytes::clone()` is O(1) refcount increment. The `get()` return type can stay `Vec<u8>` at the trait boundary (call `.to_vec()` only when the caller actually needs mutation), or better yet, change the `Blockstore` trait to return `Bytes`. The `put` path already has `data.to_vec()` which would become `Bytes::copy_from_slice(data)`.

### Finding 2
- **severity:** high
- **category:** anti-pattern
- **crate:** blockstore
- **file:** crates/blockstore/src/lib.rs
- **line:** 244, 277
- **pattern:** hot-path-to-vec
- **description:** `Blockstore::put()` (line 244) and `put_many()` (line 277) call `data.to_vec()` to populate the write-through cache. In `put_many`, the block bytes are copied into `written: Vec<(Cid, Vec<u8>)>` and then moved into the cache, but the initial copy from the `&[u8]` parameter is unavoidable with `Vec<u8>`. With `Bytes`, if the caller already has `Bytes`, no copy is needed.
- **training_ref:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Same as Finding 1: switch cache to `Bytes`. Accept `impl Into<Bytes>` in `put` so callers that already have `Bytes` (e.g., P2P bitswap) avoid the copy entirely.

### Finding 3
- **severity:** high
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/sync/broadcaster.rs
- **line:** 79-83
- **pattern:** hot-path-clone
- **description:** `broadcast_update()` clones the entire `PushLogBroadcast` twice -- once for the document topic publish and once for the collection topic. `PushLogBroadcast` contains `block: Vec<u8>` (the full IPLD block bytes) and `cid: Vec<u8>`. For a typical 4 KB block, this is ~8 KB of deep copies per broadcast. Every local document write triggers this.
- **training_ref:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Change `PushLogBroadcast.block` and `.cid` from `Vec<u8>` to `bytes::Bytes`. Clone becomes O(1). Alternatively, pass the broadcast by `Arc` to the transport layer so both publishes share the same allocation.

### Finding 4
- **severity:** high
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/sync/coordinator/broadcast.rs
- **line:** 108-113
- **pattern:** hot-path-clone
- **description:** In `push_dag_to_replicators`, for each replicator peer, each DAG block's data is cloned into a new `PushLogRequest` (`block_data.clone()` on line 113). If there are N replicators and M blocks in a DAG, this produces N*M copies of each block's bytes. For a 10-block DAG with 3 replicators, that is 30 deep copies of block data.
- **training_ref:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Store DAG block data as `Arc<Vec<u8>>` or `bytes::Bytes` in the `dag_blocks` vector so cloning into each `PushLogRequest` is O(1). The `PushLogRequest.block` field should also be `Bytes`.

### Finding 5
- **severity:** high
- **category:** anti-pattern
- **crate:** events
- **file:** crates/events/src/event.rs
- **line:** 117
- **pattern:** vec-u8-to-bytes
- **description:** The `Update` event struct carries `block: Vec<u8>` which is deep-copied every time the event is broadcast to subscribers (`msg.clone()` in `channel_bus.rs:132`). The event bus fans out to multiple subscribers (HTTP SSE, P2P sync, merge processor). Each subscriber gets a full copy of the block bytes. This is the primary event delivery path for every document mutation.
- **training_ref:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Change `Update.block` from `Vec<u8>` to `bytes::Bytes`. All subscriber clones become O(1). The `Update::new()` constructor can accept `impl Into<Bytes>`.

### Finding 6
- **severity:** medium
- **category:** improvement
- **crate:** p2p
- **file:** crates/p2p/src/message/pushlog.rs
- **line:** 283-303
- **pattern:** hot-path-clone
- **description:** `PushLogBroadcast::from_request()` deep-clones every field including `block: Vec<u8>` and `cid: Vec<u8>`. Similarly, `to_request()` deep-clones everything back. These conversions happen in the P2P hot path (every incoming/outgoing message). With `Bytes` fields, these conversions would be O(1).
- **training_ref:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Use `Bytes` for `block` and `cid` fields in both `PushLogRequest` and `PushLogBroadcast`. The `from_request`/`to_request` conversions become trivially cheap.

### Finding 7
- **severity:** medium
- **category:** improvement
- **crate:** blockstore
- **file:** crates/blockstore/src/verify.rs
- **line:** 32
- **pattern:** unnecessary-alloc
- **description:** `verify_block_cid` allocates a `Vec<u8>` for the SHA-256 digest (`hasher.finalize().to_vec()`) solely to compare it with the CID's digest bytes. The `finalize()` output is a fixed-size `[u8; 32]` array (GenericArray) that can be compared directly against the slice without heap allocation.
- **training_ref:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Replace `let computed: Vec<u8> = hasher.finalize().to_vec()` with `let computed = hasher.finalize()` and compare with `mh.digest() != computed.as_slice()`. The same pattern exists in `lib.rs:153-158`.

### Finding 8
- **severity:** medium
- **category:** improvement
- **crate:** blockstore
- **file:** crates/blockstore/src/lib.rs
- **line:** 153-158
- **pattern:** unnecessary-alloc
- **description:** Same as Finding 7. `verify_hash()` in the main blockstore file allocates a `Vec<u8>` for the SHA-256 digest just to compare it. This runs on every block read when `hash_on_read` is enabled.
- **training_ref:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Use `hasher.finalize()` directly and compare the `GenericArray` as a slice. Saves one heap allocation per verified block read.

### Finding 9
- **severity:** medium
- **category:** improvement
- **crate:** crdt
- **file:** crates/crdt/src/composite.rs
- **line:** 341, 387
- **pattern:** hot-path-to-vec
- **description:** Counter merge operations in `CompositeDAG::apply_field_delta` create `new_value_bytes: Vec<u8>` from fixed-size `to_be_bytes().to_vec()` (8-byte value). This allocates a heap `Vec` for exactly 8 bytes that is immediately written to storage. This runs per counter field per document merge.
- **training_ref:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Use a stack-allocated `[u8; 8]` array instead. `rw.set()` accepts `&[u8]` so `&value.to_be_bytes()` works directly without any allocation.

### Finding 10
- **severity:** medium
- **category:** improvement
- **crate:** crdt
- **file:** crates/crdt/src/counter.rs
- **line:** 74, 111
- **pattern:** unnecessary-alloc
- **description:** `CounterDelta::new_int64` and `new_float64` store increment values as `data: Vec<u8>` via `increment.to_be_bytes().to_vec()`. This always allocates 8 bytes on the heap. The data is later read back with `decode_int64`/`decode_float64`. A fixed-size `[u8; 8]` would avoid the allocation entirely.
- **training_ref:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Change `CounterDelta.data` to `[u8; 8]` instead of `Vec<u8>`. This eliminates heap allocation for every counter delta creation. The serde `with = "serde_bytes"` attribute works with fixed-size arrays too.

### Finding 11
- **severity:** medium
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/message/pushlog.rs
- **line:** 16
- **pattern:** serde-flatten-cbor
- **description:** `PushLogRequest` uses `#[serde(flatten)]` on the `metadata` field. The code comments on `PushLogReply` (line 119-122) explicitly note that `#[serde(flatten)]` is NOT used because "serde_cbor produces indefinite-length maps when flatten is used... causing signature verification to fail." Yet `PushLogRequest` still uses flatten. This inconsistency may cause subtle signature issues. Additionally, `#[serde(flatten)]` has a known performance cost -- serde buffers the entire struct into an intermediate map representation, adding allocation overhead per message.
- **training_ref:** rust-patterns-book ch11 "Common serde Attributes"
- **suggested_fix:** Remove `#[serde(flatten)]` from `PushLogRequest` (and `DocSyncRequest`, `BranchableSyncRequest`, `SEKeyRequest`) and duplicate the metadata fields directly, matching the pattern used by `PushLogReply`. This fixes both the performance overhead and potential wire compatibility issues.

### Finding 12
- **severity:** medium
- **category:** improvement
- **crate:** document
- **file:** crates/document/src/encoding.rs
- **line:** 119, 125, 133, 153, 212, 213, 223, 257, 261, 339, 349, 401, 410, 474, 490, 629, 715, 728
- **pattern:** encoding-string-clone
- **description:** `normal_value_to_json` and `normal_value_to_cbor` clone String and Vec values extensively when converting `NormalValue` to JSON/CBOR representation. For example, `NormalValue::String(s) => Ok(ciborium::Value::Text(s.clone()))` clones every string field during document encoding. This runs once per field per document during CBOR encoding (docID generation) and JSON serialization (query results). For a document with 10 string fields, that is 10 string allocations.
- **training_ref:** rust-patterns-book ch11 "Zero-Copy Deserialization"
- **suggested_fix:** Consider taking `NormalValue` by value (`fn normal_value_to_cbor(value: NormalValue)`) instead of by reference in the encoding paths where the source value is not needed afterward. This enables moving strings/vecs into the target representation without cloning. The by-reference signature forces clones that are often unnecessary.

### Finding 13
- **severity:** low
- **category:** improvement
- **crate:** crdt
- **file:** crates/crdt/src/priority.rs
- **line:** 15-19
- **pattern:** unnecessary-alloc
- **description:** `encode_priority()` allocates a new `Vec<u8>` for each priority encoding. Priority encoding is called per CRDT merge operation (every field write). The varint output is at most 10 bytes, so a stack-allocated `[u8; 10]` with a length marker would avoid the heap allocation.
- **training_ref:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Return a fixed-size array or use `SmallVec<[u8; 10]>` to keep the encoding on the stack. Alternatively, write directly into the storage write buffer if the API supports it.

### Finding 14
- **severity:** low
- **category:** improvement
- **crate:** p2p
- **file:** crates/p2p/src/bitswap/store.rs
- **line:** 62-66
- **pattern:** unnecessary-read
- **description:** `BitswapStoreAdapter::get_size()` fetches the full block data (`self.blockstore.get(cid)`) just to call `.len()` on it. The blockstore has a dedicated `get_size()` method that avoids reading the entire block.
- **training_ref:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Use `self.blockstore.get_size(cid)` instead of `self.blockstore.get(cid).map(|data| data.len())`. This avoids reading and potentially allocating the full block data just to measure its size.

### Finding 15
- **severity:** low
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/types.rs
- **line:** 165
- **pattern:** repr-c-alignment
- **description:** `NodeInitOptions` is `#[repr(C)]` with mixed field types (pointers, `c_int`, `u16`, `usize`, `u32`, `f64`). The `iroh_bind_port: u16` field at line 208 sits between pointer-sized fields, which will cause 6 bytes of padding on 64-bit platforms. While correct, the struct could be reordered to minimize padding (pointers first, then u64/usize, then u32, then u16, then c_int). However, since this is an FFI struct matching Go's layout, reordering requires coordinating with the Go side.
- **training_ref:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** No action needed for correctness. If performance matters (it doesn't -- this is init-only), reorder fields to minimize padding. Document the intentional layout choice.

### Finding 16
- **severity:** low
- **category:** improvement
- **crate:** p2p
- **file:** crates/p2p/src/message/metadata.rs
- **line:** 48-52
- **pattern:** unnecessary-alloc
- **description:** `MetaData::new()` and `set_version()` call `MESSAGE_VERSION.to_string()` which allocates a new String. `MESSAGE_VERSION` is a static `&str`. Since this is called once per P2P message construction, the allocation is minor but could be avoided with a `Cow<'static, str>` version field.
- **training_ref:** rust-patterns-book ch11 "Zero-Copy Deserialization"
- **suggested_fix:** This is low priority since message construction is not the bottleneck. If optimizing, change `version: String` to `Cow<'static, str>` and initialize with `Cow::Borrowed(MESSAGE_VERSION)`. However, the serde CBOR compatibility constraints may make this impractical.
