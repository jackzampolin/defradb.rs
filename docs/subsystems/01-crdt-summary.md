# CRDT Subsystem Implementation Guide

> **Full Analysis**: Agent `a2d10d8` completed a comprehensive 1,600+ line analysis.
> This is a condensed implementation guide.

## Overview

DefraDB's CRDT subsystem implements delta-state CRDTs with Merkle-DAG integration for distributed conflict resolution.

**Key Files** (`/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/internal/core/crdt/`):
- `lww.go` - Last-Write-Wins Register
- `counter.go` - PN-Counter implementation
- `composite.go` - Document-level CRDT
- `delta.go` - Delta interface
- `base.go` - Priority management

## Core Algorithm: LWW Merge

```
MERGE(current_value, incoming_delta):
  IF incoming_priority < current_priority:
    RETURN  // Ignore

  IF incoming_priority == current_priority:
    IF lexicographic_compare(current, incoming) >= 0:
      RETURN  // Current wins

  // Update
  IF incoming_delta.Data == NIL:
    DELETE value_key
  ELSE:
    SET value_key = incoming_delta.Data

  SET priority_key = incoming_priority
```

## Rust Implementation

### Core Traits

```rust
pub trait Delta: Send + Sync {
    fn get_priority(&self) -> u64;
    fn set_priority(&mut self, priority: u64);
}

pub trait ReplicatedData: Send + Sync {
    fn merge(&mut self, ctx: &Context, delta: &dyn Delta) -> Result<()>;
    fn headstore_prefix(&self) -> HeadstoreKey;
}
```

### LWW Register

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LwwDelta {
    pub doc_id: Vec<u8>,
    pub field_name: String,
    pub priority: u64,
    pub schema_version_id: String,
    pub data: Vec<u8>,
}

pub struct Lww {
    store: Arc<dyn KeyValueStore>,
    key: DataStoreKey,
    schema_version_id: String,
    field_name: String,
}

impl ReplicatedData for Lww {
    fn merge(&mut self, ctx: &Context, delta: &dyn Delta) -> Result<()> {
        let lww_delta = delta.as_any().downcast_ref::<LwwDelta>()?;
        self.set_value(ctx, &lww_delta.data, lww_delta.get_priority())
    }
}
```

### Counter CRDT

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterDelta {
    pub doc_id: Vec<u8>,
    pub field_name: String,
    pub priority: u64,
    pub nonce: i64,  // For uniqueness
    pub schema_version_id: String,
    pub data: Vec<u8>,
}

pub struct Counter {
    store: Arc<dyn KeyValueStore>,
    allow_decrement: bool,
    kind: NumericKind,
}

impl ReplicatedData for Counter {
    fn merge(&mut self, ctx: &Context, delta: &dyn Delta) -> Result<()> {
        let counter_delta = delta.as_any().downcast_ref::<CounterDelta>()?;
        let increment_value = decode(counter_delta.data)?;
        let current = self.get_current_value()?;
        let new_value = current + increment_value;
        self.store.set(&value_key, &encode(new_value)?)?;
        Ok(())
    }
}
```

## Key Implementation Notes

1. **Priority Management**: Use varint encoding for storage efficiency
2. **Determinism**: Lexicographic tie-breaking ensures convergence
3. **Nonce Strategy**: Counters use random nonces to ensure unique DAG blocks
4. **Deletion**: Use tombstone markers for composite CRDTs

## Testing Strategy

```rust
#[tokio::test]
async fn test_lww_conflict_resolution() {
    let store = Arc::new(MockStore::new());
    let mut lww = Lww::new(store, "v1".into(), key, "field".into());

    // Priority 10
    lww.merge(&ctx, &LwwDelta { priority: 10, data: b"Alice".to_vec(), .. }).await?;

    // Priority 5 - should be ignored
    lww.merge(&ctx, &LwwDelta { priority: 5, data: b"Bob".to_vec(), .. }).await?;

    assert_eq!(get_value(&store), b"Alice");
}
```

## Next Steps

1. Implement priority encoding/decoding utilities
2. Build LWW Register with full merge logic
3. Add Counter with nonce generation
4. Implement Composite CRDT for documents
5. Add comprehensive property-based tests
