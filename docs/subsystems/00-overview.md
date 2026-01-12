# DefraDB Subsystem Documentation

This directory contains comprehensive implementation guides for porting DefraDB from Go to Rust.

## Subsystem Guides

Each guide provides:
- **Architecture overview** of the Go implementation
- **Key Go files** with purposes and line counts
- **Core algorithms** and data structures
- **Complete Rust implementation proposals** with working code examples
- **Test examples** demonstrating real usage patterns

## Subsystems

1. **[CRDT](01-crdt.md)** - Conflict-free replicated data types
   - LWW Register, Counter, Composite CRDTs
   - Delta generation and merge logic
   - Priority-based conflict resolution
   - ~1,600 lines of Go code analyzed

2. **[Storage](02-storage.md)** - Multi-store architecture
   - Blockstore, Datastore, Headstore, Systemstore
   - Transaction semantics with RocksDB
   - Namespace management
   - ~800+ lines of Go code analyzed

3. **[Blockstore](03-blockstore.md)** - IPLD content-addressed storage
   - CID generation with multihash
   - Block format and serialization
   - DAG traversal utilities
   - Encryption integration

4. **[Schema](04-schema.md)** - Schema definition and validation
   - GraphQL SDL parsing
   - Field types and CRDT compatibility
   - Validation framework
   - Schema evolution

5. **[Query](05-query.md)** - Query parsing and execution
   - 39+ planner operations
   - Filter operators and transformations
   - Index optimization
   - Fetcher/iterator pattern

6. **[P2P](06-p2p.md)** - Peer-to-peer networking
   - Replicator and PubSub synchronization
   - DAG sync protocol
   - Message formats
   - libp2p integration

7. **[Crypto](07-crypto.md)** - Cryptographic operations
   - Signing (Ed25519, secp256k1)
   - Encryption (AES-GCM, ECIES)
   - Key management
   - Searchable encryption

## Implementation Roadmap

Based on the analysis, here's the recommended implementation order:

### Phase 1: Foundation (Months 1-2)
1. Start with **CRDT** - it's the core innovation
2. Implement **Storage** - everything needs persistence
3. Build **Blockstore** - enables P2P sync

### Phase 2: Query & Schema (Months 3-4)
4. Implement **Schema** - needed for typed queries
5. Build **Query** planner - basic operations first

### Phase 3: Distribution (Months 5-6)
6. Implement **P2P** - start with pubsub
7. Add **Crypto** - signing and verification

### Phase 4: Advanced Features (Months 6+)
- Complete query optimization
- Field encryption
- Access control
- Performance tuning

## Using These Guides

Each subsystem guide is structured to be:
- **Reference**: Lookup Go implementation details
- **Blueprint**: Copy-paste Rust code to get started
- **Learning**: Understand DefraDB's architecture

## Total Scope

Based on the comprehensive analysis:
- **Go codebase**: ~50,000+ lines across subsystems
- **Rust MVP target**: ~25,000-35,000 lines
- **Time estimate**: 12-18 months for production-ready (small team)
- **MVP estimate**: 4-6 months for basic functionality

## Contributing

When implementing a subsystem:
1. Read the corresponding guide thoroughly
2. Start with the Rust types and traits
3. Implement core algorithms with tests
4. Add integration tests
5. Document deviations from Go implementation

## Notes

All Go file paths reference:
```
/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/
```

All Rust code targets:
```
/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/
```
