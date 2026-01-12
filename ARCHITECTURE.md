# Architecture Documentation

This document provides an in-depth view of DefraDB.rs architecture, design decisions, and implementation details.

## System Overview

DefraDB.rs is a distributed, content-addressed database system built on the following principles:

1. **Content Addressing**: All data is stored as content-addressed blocks (IPLD)
2. **Conflict-Free Replication**: Uses Merkle-CRDTs for automatic conflict resolution
3. **Multi-Node Collaboration**: P2P synchronization via libp2p
4. **Schema-Driven**: GraphQL schema defines the data model
5. **Eventually Consistent**: CAP theorem - choosing AP (Availability + Partition Tolerance)

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                        │
│  - CLI (defra-cli)                                          │
│  - GraphQL API (query, mutation, subscription)             │
└─────────────────────────────────────────────────────────────┘
                           ↕
┌─────────────────────────────────────────────────────────────┐
│                      Query Layer                             │
│  - GraphQL Parser (schema → AST)                            │
│  - Query Planner (optimization)                             │
│  - Query Executor (fetcher, iterators)                      │
└─────────────────────────────────────────────────────────────┘
                           ↕
┌─────────────────────────────────────────────────────────────┐
│                    Database Layer                            │
│  - Collection Management                                     │
│  - Document CRUD Operations                                  │
│  - Schema Validation                                         │
│  - Transaction Management                                    │
└─────────────────────────────────────────────────────────────┘
                           ↕
┌─────────────────────────────────────────────────────────────┐
│                      CRDT Layer                              │
│  - LWW Register (field-level last-write-wins)              │
│  - Counter CRDT (increment/decrement)                       │
│  - Composite CRDT (document-level merging)                  │
│  - Delta Generation & Merge Logic                           │
└─────────────────────────────────────────────────────────────┘
                           ↕
┌─────────────────────────────────────────────────────────────┐
│                     Storage Layer                            │
│  - Multi-Store Architecture:                                │
│    • Blockstore: IPLD content-addressed blocks             │
│    • Datastore: Materialized document state                │
│    • Headstore: Document heads (latest CIDs)               │
│    • Systemstore: Schema metadata                          │
└─────────────────────────────────────────────────────────────┘
                           ↕
┌─────────────────────────────────────────────────────────────┐
│                   Physical Storage                           │
│  - RocksDB (embedded KV store)                              │
│  - Transactional guarantees                                 │
│  - Key namespace management                                 │
└─────────────────────────────────────────────────────────────┘

         ┌──────────────────────────────────────┐
         │        Cross-Cutting Concerns         │
         │  - P2P Networking (libp2p)           │
         │  - Cryptography (signing, encryption)│
         │  - Event System (pub/sub)            │
         │  - Logging & Tracing                 │
         └──────────────────────────────────────┘
```

## Core Crate Responsibilities

### `defra-core`
**Purpose**: Foundation types and traits used across all crates

**Key Components**:
- `Error` / `Result`: Error handling throughout the system
- `DocId`, `CollectionId`: Type-safe identifiers
- `Document`: Document representation
- `Block`, `Cid`: IPLD block types
- Storage traits: `Store`, `Transaction`

**Dependencies**: None (only serde, thiserror)

### `crdt`
**Purpose**: Conflict-free replicated data types

**Key Components**:
- `LWWRegister`: Last-Write-Wins register for fields
- `Counter`: PN-Counter for increment/decrement
- `CompositeDAG`: Document-level CRDT composition
- `Delta`: Change representation
- `Merge`: Conflict resolution algorithms

**Design Decisions**:
- Priority-based resolution for LWW (using timestamp + node ID)
- Delta-state CRDT (send only changes, not full state)
- Merkle-CRDT: CRDTs embedded in a Merkle DAG

**Algorithms**:
```rust
// LWW Register merge
fn merge(current: Value, incoming: Value) -> Value {
    if incoming.priority > current.priority {
        incoming.value
    } else {
        current.value
    }
}

// Counter merge
fn merge(current: Counter, incoming: Counter) -> Counter {
    Counter {
        increments: current.increments + incoming.increments,
        decrements: current.decrements + incoming.decrements,
    }
}
```

### `storage`
**Purpose**: Multi-store abstraction and transaction management

**Key Components**:
- `MultiStore`: Coordinates blockstore, datastore, headstore, systemstore
- `RootStore`: Transactional base layer
- `Namespace`: Key prefixing for logical separation
- `Transaction`: Atomic multi-operation batches

**Store Layout**:
```
/blocks/<cid>              → IPLD block data
/data/<collectionId>/<docId>/<field> → Current field values
/head/<docId>              → Latest block CID for document
/system/collections/<id>   → Collection metadata
/system/schema/<id>        → Schema definitions
```

**Transaction Semantics**:
- ACID guarantees for single-node operations
- Read-committed isolation level
- Optimistic concurrency control
- Rollback on conflict

### `blockstore`
**Purpose**: Content-addressed IPLD block storage

**Key Components**:
- `Blockstore`: Store/retrieve blocks by CID
- CID generation: `hash(data) → CID`
- Block encoding: CBOR serialization
- DAG traversal utilities

**CID Structure**:
```
CID = multibase(multicodec + multihash(data))
      ↑           ↑          ↑
      base32      cbor       sha2-256
```

**Block Format** (CBOR):
```rust
{
    "delta": {
        "field_id": value,  // Field changes
        ...
    },
    "links": [cid1, cid2],  // Parent blocks
    "priority": 12345,      // Conflict resolution
}
```

### `schema`
**Purpose**: Schema definition, validation, and GraphQL type generation

**Key Components**:
- `SchemaDefinition`: Parsed schema representation
- `FieldDefinition`: Field types and constraints
- `Validator`: Schema validation rules
- GraphQL SDL parser

**Schema Example**:
```graphql
type User {
  name: String
  age: Int
  email: String @index
  friends: [User]
}
```

**Validation Rules**:
- Field types must be valid
- Primary key required (`_docID` implicit)
- Circular references allowed
- Index constraints validated

### `query`
**Purpose**: Query parsing, planning, and execution

**Key Components**:
- `Parser`: GraphQL → Request AST
- `Planner`: Request → Execution Plan
- `Fetcher`: Execute plan → Iterator
- `Filter`: Filter operations (eq, gt, like, etc.)

**Query Execution Flow**:
```
GraphQL String
    ↓ [Parse]
Request AST
    ↓ [Plan]
Execution Plan (tree of operations)
    ↓ [Execute]
Document Iterator
    ↓ [Format]
JSON Response
```

**Operations**:
- `Scan`: Full collection scan
- `Filter`: Predicate filtering
- `Select`: Field projection
- `Order`: Sort results
- `Limit`: Result limiting
- `Join`: Relationship traversal

**Optimization**:
- Index selection
- Filter pushdown
- Predicate reordering

### `p2p`
**Purpose**: Peer-to-peer networking and document synchronization

**Key Components**:
- `Node`: libp2p network node
- `PubSub`: Broadcast document updates
- `DAGSync`: Fetch missing blocks from peers
- `Replicator`: Targeted push synchronization

**P2P Protocols**:
```
/defra/identity/1.0.0    - Peer handshake
/defra/pushlog/1.0.0     - Push document updates
/defra/dagsync/1.0.0     - DAG block synchronization
```

**Sync Process**:
```
Node A updates document
    ↓
Generate delta block
    ↓
Broadcast via pubsub
    ↓
Node B receives update
    ↓
Check if all parent blocks present
    ↓ [missing blocks]
DAG sync to fetch missing
    ↓
Verify signatures
    ↓
Merge delta into document
    ↓
Update indices
```

### `crypto`
**Purpose**: Cryptographic operations

**Key Components**:
- `Signer`: Ed25519 signing
- `Verifier`: Signature verification
- `Encryptor`: AES-GCM encryption
- `KeyManager`: Key storage and retrieval

**Signing Flow**:
```rust
// Sign block before distribution
let signature = signer.sign(&block.data)?;
block.set_signature(signature);

// Verify on receive
verifier.verify(&block.data, &block.signature)?;
```

### `http`
**Purpose**: HTTP/GraphQL API server

**Key Components**:
- `Server`: Axum HTTP server
- `GraphQLHandler`: POST /graphql endpoint
- `CollectionHandler`: Collection management
- `P2PHandler`: P2P control endpoints

**Endpoints**:
```
POST   /api/v0/graphql             - GraphQL queries
GET    /api/v0/collections         - List collections
POST   /api/v0/schema               - Add schema
POST   /api/v0/p2p/replicator       - Setup replication
```

### `cli`
**Purpose**: Command-line interface

**Commands**:
```bash
defra start                    # Start node
defra client query <query>     # Run query
defra client collection list   # List collections
defra keyring generate         # Generate keys
```

## Data Flow Examples

### Document Creation

```
1. User sends GraphQL mutation:
   mutation { create_User(input: {name: "Alice", age: 30}) { _docID } }

2. Parser converts to Request AST

3. Planner creates execution plan:
   Create → Validate → Store

4. Executor:
   a. Generate new DocID
   b. Create initial CRDT state
   c. Serialize to CBOR block
   d. Compute CID
   e. Sign block
   f. Store in blockstore
   g. Update datastore (materialized view)
   h. Update headstore (doc → CID)

5. Broadcast via P2P pubsub

6. Return result to user
```

### Concurrent Updates (Conflict Resolution)

```
Scenario: Node A and Node B both update field "count"

Node A:
  count: 10 → 15 (priority: 100)
  Generate delta block with priority 100

Node B:
  count: 10 → 20 (priority: 101)
  Generate delta block with priority 101

Both broadcast updates

Node A receives B's update:
  - Merge algorithm detects conflict
  - Compare priorities: 101 > 100
  - B's value (20) wins
  - Store merged state
  - Update head to point to merged block

Node B receives A's update:
  - Compare priorities: 101 > 100
  - B's value (20) already wins
  - No change needed
  - Both nodes converge to count: 20
```

### Query with Index

```
Query: query { User(filter: {age: {_gt: 18}}, order: {age: ASC}, limit: 10) }

1. Parser extracts:
   - Filter: age > 18
   - Order: age ascending
   - Limit: 10

2. Planner:
   - Check for index on 'age' → found
   - Plan: IndexScan → Filter → Order → Limit

3. Executor:
   - IndexScan fetches documents with age > 18
   - Filter applies predicate
   - Order sorts by age
   - Limit takes first 10

4. Return results
```

## Design Decisions (ADRs)

### ADR-001: Async by Default
**Status**: Accepted

**Context**: Need high concurrency for P2P and storage operations

**Decision**: Use Tokio as async runtime, `async/await` throughout

**Consequences**:
- (+) High concurrency without thread overhead
- (+) Natural fit for libp2p (async-first)
- (-) Async trait complexity
- (-) Learning curve for contributors

### ADR-002: RocksDB for Storage
**Status**: Accepted

**Context**: Need embedded, transactional KV store

**Decision**: Use RocksDB instead of alternatives (sled, redb)

**Rationale**:
- Battle-tested in production
- Excellent Rust bindings
- ACID transactions
- Write-ahead logging

**Consequences**:
- (+) Proven reliability
- (+) Good performance
- (-) Different format than Go DefraDB (uses Badger)
- (-) Larger binary size

### ADR-003: Workspace Organization
**Status**: Accepted

**Context**: Large codebase with multiple subsystems

**Decision**: Organize as Cargo workspace with crates per subsystem

**Consequences**:
- (+) Clear boundaries
- (+) Parallel compilation
- (+) Independent testing
- (-) More complex dependency management

### ADR-004: Delta-State CRDTs
**Status**: Accepted

**Context**: Need efficient network synchronization

**Decision**: Use delta-state CRDTs (send only changes)

**Alternatives Considered**:
- State-based CRDTs (send full state)
- Operation-based CRDTs (send operations)

**Consequences**:
- (+) Efficient bandwidth usage
- (+) Scales with change size, not state size
- (-) Requires DAG traversal for full state reconstruction
- (-) More complex merge logic

## Performance Considerations

### Indexing Strategy
- Primary key (DocID) stored in headstore for O(1) lookup
- Secondary indexes stored in separate namespace
- Index updates in same transaction as document update

### Memory Management
- Streaming iterators for large result sets
- No full document load unless necessary
- Block cache for frequently accessed CIDs

### P2P Bandwidth
- Delta-state sync (only changes)
- Block deduplication via CIDs
- Configurable sync depth

## Testing Strategy

### Unit Tests
- Per-crate, testing individual components
- Property-based testing for CRDTs (proptest)
- Mocked dependencies

### Integration Tests
- Cross-crate workflows
- Full stack (storage → query → API)
- Multi-node scenarios

### Compatibility Tests
- Goal: Pass DefraDB Go integration tests
- Wire protocol compatibility
- IPLD format compatibility

## Future Work

### Phase 1 (Current)
- [ ] Complete CRDT implementations
- [ ] Basic query engine
- [ ] P2P pubsub sync

### Phase 2
- [ ] Advanced query optimization
- [ ] Schema evolution
- [ ] Access control (DAC/ReBAC)

### Phase 3
- [ ] Field encryption
- [ ] Searchable encryption
- [ ] Performance optimization

## References

- [IPLD Specification](https://ipld.io/)
- [libp2p Specification](https://docs.libp2p.io/)
- [Merkle-CRDTs Paper](https://research.protocol.ai/publications/merkle-crdts-merkle-dags-meet-crdts/)
- [DefraDB (Go) Documentation](https://docs.source.network/defradb)
