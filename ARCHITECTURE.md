# Architecture

## Principle

**The Go implementation is the source of truth for DefraDB architecture and behavior.**

This document only describes what's different in the Rust implementation.

## Core Vision

defradb.rs aims to be a **fully interoperable Rust implementation** targeting embedded, edge, and WASM deployments. See [Issue #18](https://github.com/sourcenetwork/defradb.rs/issues/18) for the full roadmap.

## Crate Organization

```
defra-core       # Core types, traits, errors (no dependencies)
    ↓
crdt             # CRDT implementations (depends: defra-core)
storage          # Multi-store with pluggable backends (depends: defra-core)
crypto           # Signing/encryption (depends: defra-core)
schema           # Schema validation (depends: defra-core)
    ↓
blockstore       # IPLD blocks (depends: storage)
    ↓
query            # Query planner (depends: schema, storage, crdt)
p2p              # libp2p networking (depends: crypto, blockstore, crdt)
```

## Key Differences from Go

### Storage Backend
- **Go**: LevelDB (migrating for WASM compatibility)
- **Rust**: redb (primary), rusty-leveldb (Go interop)
- **Reason**: Pure Rust, WASM-compatible, no C/C++ dependencies
- Both use single-writer model matching Go's transaction semantics

### HTTP Server
- **Go**: Chi
- **Rust**: Axum
- **Reason**: Type-safe routing, tower middleware ecosystem

### Async Runtime
- **Go**: Native goroutines
- **Rust**: Tokio
- **Reason**: De facto standard for async Rust

## Wire Compatibility

Must match Go implementation:
- IPLD block format (CBOR)
- CID generation (SHA-256 + multibase)
- P2P message structure (libp2p protobuf)
- GraphQL schema format

## Testing Strategy

1. **Unit tests**: Per-crate, test internal correctness
2. **Property tests**: Verify CRDT invariants (commutativity, convergence)
3. **Integration tests**: Cross-crate workflows
4. **Compatibility tests**: Verify wire compatibility with Go (future)

## For Detailed Architecture

See [DefraDB (Go) documentation](https://github.com/sourcenetwork/defradb) for:
- CRDT algorithms
- Multi-store architecture
- Query planning
- P2P protocols
- Block structure
- Security model
