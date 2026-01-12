# DefraDB.rs

A Rust implementation of DefraDB - a content-addressed, multi-node database built on Merkle-CRDTs and IPLD.

## Project Status

🚧 **Early Exploration Phase** - This is an experimental Rust reimplementation of [DefraDB](https://github.com/sourcenetwork/defradb).

The goal is to create a Rust version that can pass DefraDB's integration test suite, providing compatibility with the Go implementation while leveraging Rust's performance and safety guarantees.

## What is DefraDB?

DefraDB is a sophisticated distributed database with:

- **Content-addressed storage** using IPLD (InterPlanetary Linked Data)
- **Merkle-CRDT** data structures for conflict-free distributed updates
- **P2P synchronization** via libp2p for multi-node collaboration
- **GraphQL API** for intuitive querying and mutations
- **Document-level encryption** with searchable encryption capabilities
- **Access control** with Relationship-Based Access Control (ReBAC)

## Architecture Overview

DefraDB.rs is organized as a Cargo workspace with the following crates:

```
defradb.rs/
├── crates/
│   ├── defra-core/      # Core types, traits, and interfaces
│   ├── crdt/            # CRDT implementations (LWW, Counter, Composite)
│   ├── storage/         # Multi-store architecture (datastore, headstore)
│   ├── blockstore/      # IPLD content-addressed block storage
│   ├── schema/          # Schema definition, validation, type system
│   ├── query/           # Query planner, execution engine
│   ├── p2p/             # P2P networking, replication, sync
│   ├── crypto/          # Cryptographic operations (signing, encryption)
│   ├── http/            # HTTP/GraphQL API server
│   └── cli/             # Command-line interface
```

## Key Components

### 1. CRDT System (🔴 Critical)
Implements Merkle-CRDT data structures with delta-state merging:
- **LWW Register**: Last-Write-Wins conflict resolution
- **Counter**: Increment/decrement operations
- **Composite CRDT**: Document-level merging with field-level CRDTs

### 2. Block Storage (🔴 Critical)
IPLD-based content-addressed storage:
- Content Identifiers (CIDs) using multihash
- Block linking and DAG traversal
- Integration with encryption layer

### 3. Multi-Store Architecture (🔴 Critical)
Transactional key-value storage with namespacing:
- **Blockstore**: IPLD blocks
- **Datastore**: Materialized document state
- **Headstore**: Document heads (latest CIDs)
- **Systemstore**: Schema metadata

### 4. Query Engine (🟠 High Priority)
GraphQL-based query processing:
- Schema-driven query parsing
- Query planning and optimization
- Filter, sort, aggregate operations
- Document fetcher with index support

### 5. P2P Networking (🟠 High Priority)
libp2p-based distributed synchronization:
- Pubsub for broadcast updates
- DAG sync for missing blocks
- Replicator for targeted push
- Peer discovery via mDNS

## Development Roadmap

### Phase 1: MVP Foundation (4-6 months)
**Goal**: Basic single-node functionality with CRUD operations

- [ ] Core traits and error types
- [ ] Multi-store KV architecture (RocksDB backend)
- [ ] Basic CRDT implementations (LWW, Counter)
- [ ] IPLD block storage with CID generation
- [ ] Schema definition and validation
- [ ] Document CRUD operations
- [ ] Simple query engine (filter, sort, limit)
- [ ] GraphQL parser
- [ ] HTTP API server (Axum)
- [ ] Block signing/verification

**Success Criteria**: Can create collections, insert documents, run simple queries

### Phase 2: P2P Collaboration (2-3 months)
**Goal**: Multi-node synchronization with conflict resolution

- [ ] Complete CRDT types (Composite)
- [ ] P2P pubsub synchronization
- [ ] DAG synchronization protocol
- [ ] Signature verification for blocks
- [ ] Merge conflict resolution
- [ ] Transaction semantics

**Success Criteria**: Two nodes can sync documents and resolve conflicts

### Phase 3: Advanced Features (2-3 months)
**Goal**: Production-ready features

- [ ] Access control (DAC/ReBAC)
- [ ] Field-level encryption
- [ ] Searchable encryption
- [ ] Secondary indexes
- [ ] Query optimization
- [ ] Replicator peering

**Success Criteria**: Can run DefraDB integration tests

### Phase 4: Optimization (1-2 months)
**Goal**: Performance and reliability

- [ ] Query planner optimizations
- [ ] Index performance tuning
- [ ] Memory optimization
- [ ] Comprehensive benchmarks
- [ ] Schema evolution support

**Success Criteria**: Performance comparable to Go implementation

## Technology Stack

### Core Dependencies

| Category | Crate | Purpose |
|----------|-------|---------|
| **Async Runtime** | `tokio` | Async I/O, task scheduling |
| **P2P Networking** | `libp2p` | Peer-to-peer communication |
| **Storage** | `rocksdb` | Embedded key-value store |
| **Serialization** | `serde`, `serde_cbor` | Data serialization |
| **Cryptography** | `ed25519-dalek`, `k256` | Signing and encryption |
| **IPLD** | `cid`, `multihash` | Content addressing |
| **GraphQL** | `graphql-parser` | Query parsing |
| **HTTP** | `axum`, `tower` | HTTP API server |
| **CLI** | `clap` | Command-line interface |

## Getting Started

### Prerequisites

- Rust 1.75 or higher
- Cargo

### Building

```bash
# Clone the repository
git clone https://github.com/sourcenetwork/defradb.rs
cd defradb.rs

# Build all crates
cargo build

# Run tests
cargo test

# Build in release mode
cargo build --release
```

### Running

```bash
# Start a DefraDB node
cargo run --bin cli -- start

# Run with debug logging
RUST_LOG=debug cargo run --bin cli -- start
```

## Testing Strategy

### Unit Tests
Each crate contains unit tests for individual components:
```bash
cargo test --lib
```

### Integration Tests
Cross-crate integration tests in `tests/`:
```bash
cargo test --test '*'
```

### Property-Based Tests
CRDT invariants tested with `proptest`:
```bash
cargo test --features proptest
```

### Go Integration Test Compatibility
Goal: Pass DefraDB's Go integration test suite by implementing a compatibility layer.

## Contributing

This is an early-stage exploration project. Contributions are welcome!

### Areas of Focus

1. **CRDT Implementation**: Help with delta-state merging logic
2. **Query Planner**: Port Go query planner operations to Rust
3. **Storage Layer**: Optimize RocksDB integration
4. **P2P Protocol**: Implement libp2p protocols
5. **Testing**: Write integration tests

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Implement the feature
5. Ensure tests pass: `cargo test`
6. Submit a pull request

## Comparison with Go Implementation

### Why Rust?

| Aspect | Benefit |
|--------|---------|
| **Memory Safety** | No GC pauses, zero-cost abstractions |
| **Performance** | Predictable latency, better CPU utilization |
| **Concurrency** | Strong type system prevents data races |
| **Ecosystem** | Mature libp2p, RocksDB, crypto crates |
| **Type System** | Stronger guarantees for CRDT correctness |

### Compatibility

The goal is to maintain wire-protocol compatibility with the Go implementation:
- Same IPLD block format
- Same P2P message structure
- Same GraphQL schema format
- Compatible CID generation

## Architecture Decisions

### ADR-001: RocksDB for Storage Backend
**Decision**: Use RocksDB instead of Badger (Go default)
**Rationale**: Mature Rust bindings, excellent performance, proven in production
**Trade-offs**: Different on-disk format than Go version

### ADR-002: Axum for HTTP Server
**Decision**: Use Axum instead of Chi (Go default)
**Rationale**: Type-safe routing, excellent tower middleware ecosystem
**Trade-offs**: Different HTTP implementation than Go

### ADR-003: Workspace Crate Organization
**Decision**: Organize as multi-crate workspace
**Rationale**: Clear module boundaries, parallel compilation, easier testing
**Trade-offs**: More complex dependency management

## Resources

### DefraDB Documentation
- [DefraDB GitHub](https://github.com/sourcenetwork/defradb)
- [DefraDB Docs](https://docs.source.network/defradb)

### Rust Libraries
- [rust-libp2p](https://github.com/libp2p/rust-libp2p)
- [IPLD in Rust](https://github.com/ipld/rust-cid)
- [RocksDB Rust](https://github.com/rust-rocksdb/rust-rocksdb)

### Academic Papers
- [Merkle-CRDTs](https://research.protocol.ai/blog/2019/a-new-lab-for-resilient-networks-research/)
- [IPLD Specification](https://ipld.io/specs/)

## License

Apache-2.0 OR MIT

## Contact

- GitHub Issues: [defradb.rs/issues](https://github.com/sourcenetwork/defradb.rs/issues)
- Source Network: [source.network](https://source.network)

---

**Note**: This is an exploratory project. For production use, refer to the official [DefraDB Go implementation](https://github.com/sourcenetwork/defradb).
