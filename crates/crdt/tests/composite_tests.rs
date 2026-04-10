//! Unit tests for Composite CRDT
//!
//! Tests for document-level CRDT composition including:
//! - Multi-field operations
//! - Field type mismatch handling
//! - Unknown field handling
//! - Document/schema validation

use crdt::composite::{CompositeDAG, CompositeDelta, FieldDelta};
use crdt::counter::NumericKind;
use crdt::traits::{Context, ReplicatedData};
use defra_core::types::DocId;
use std::collections::HashMap;
use std::f64;
use storage::{MemoryStore, Store};

#[tokio::test]
async fn test_composite_multiple_fields() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());

    // Register fields
    composite.register_lww_field("name".to_string());
    composite.register_counter_field("count".to_string(), true, NumericKind::Int64);

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Create composite delta with multiple fields
    let mut field_deltas = HashMap::new();
    field_deltas.insert(
        "name".to_string(),
        FieldDelta::Lww {
            priority: 10,
            data: b"Alice".to_vec(),
        },
    );
    field_deltas.insert(
        "count".to_string(),
        FieldDelta::Counter {
            priority: 10,
            nonce: 12345,
            data: 5i64.to_be_bytes().to_vec(),
        },
    );

    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    for (name, fd) in field_deltas {
        delta.add_field_delta(name, fd).unwrap();
    }

    let mut txn = store.new_txn(false).await.unwrap();
    composite.merge(&mut *txn, &ctx, &delta).await.unwrap();
    txn.commit().await.unwrap();

    // Verify name field
    let txn = store.new_txn(true).await.unwrap();
    let name_key = b"/data/v1/doc1/name".to_vec();
    let name = txn.get(&name_key).await.unwrap().unwrap();
    assert_eq!(name, b"Alice");

    // Verify count field
    let count_key = b"/data/v1/doc1/count".to_vec();
    let count_bytes = txn.get(&count_key).await.unwrap().unwrap();
    let count = i64::from_be_bytes(count_bytes.try_into().unwrap());
    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_composite_field_type_mismatch_lww_to_counter() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());

    // Register field as LWW
    composite.register_lww_field("value".to_string());

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Try to apply Counter delta to LWW field
    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    delta
        .add_field_delta(
            "value".to_string(),
            FieldDelta::Counter {
                priority: 10,
                nonce: 12345,
                data: 5i64.to_be_bytes().to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("field type mismatch"));
}

#[tokio::test]
async fn test_composite_field_type_mismatch_counter_to_lww() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());

    // Register field as Counter
    composite.register_counter_field("count".to_string(), true, NumericKind::Int64);

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Try to apply LWW delta to Counter field
    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    delta
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"not_a_number".to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("field type mismatch"));
}

#[tokio::test]
async fn test_composite_unknown_field() {
    let store = MemoryStore::new();
    let composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());

    // Don't register any fields

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Try to apply delta to unknown field
    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    delta
        .add_field_delta(
            "unknown_field".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"value".to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("unknown field"));
}

#[tokio::test]
async fn test_composite_schema_evolution_type_change() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());

    // Register field as LWW in schema v1
    composite.register_lww_field("score".to_string());

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Apply LWW delta successfully
    let mut delta1 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    delta1
        .add_field_delta(
            "score".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"100".to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    composite.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    txn.commit().await.unwrap();

    // Now simulate schema evolution where "score" becomes a Counter
    // This should fail since the field is registered as LWW
    let mut delta2 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 20).unwrap();
    delta2
        .add_field_delta(
            "score".to_string(),
            FieldDelta::Counter {
                priority: 20,
                nonce: 12345,
                data: 50i64.to_be_bytes().to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = composite.merge(&mut *txn, &ctx, &delta2).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("field type mismatch"));
}

#[tokio::test]
async fn test_composite_doc_id_mismatch() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());

    composite.register_lww_field("name".to_string());

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Delta with wrong doc ID
    let mut delta = CompositeDelta::new(b"wrong_doc".to_vec(), "v1".to_string(), 10).unwrap();
    delta
        .add_field_delta(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"Alice".to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("document ID mismatch"));
}

#[tokio::test]
async fn test_composite_float64_counter_overflow_becomes_positive_infinity() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());
    composite.register_counter_field("count".to_string(), true, NumericKind::Float64);

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut delta1 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    delta1
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 10,
                nonce: 1,
                data: f64::MAX.to_be_bytes().to_vec(),
            },
        )
        .unwrap();

    let mut delta2 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 20).unwrap();
    delta2
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 20,
                nonce: 2,
                data: f64::MAX.to_be_bytes().to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    composite.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    composite.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let count_key = b"/data/v1/doc1/count".to_vec();
    let count_bytes = txn.get(&count_key).await.unwrap().unwrap();
    let count = f64::from_be_bytes(count_bytes.try_into().unwrap());
    assert!(count.is_infinite());
    assert!(count.is_sign_positive());
}

#[tokio::test]
async fn test_composite_float64_counter_nan_increment_propagates_nan() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());
    composite.register_counter_field("count".to_string(), true, NumericKind::Float64);

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    delta
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 10,
                nonce: 1,
                data: f64::NAN.to_be_bytes().to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    composite.merge(&mut *txn, &ctx, &delta).await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let count_key = b"/data/v1/doc1/count".to_vec();
    let count_bytes = txn.get(&count_key).await.unwrap().unwrap();
    let count = f64::from_be_bytes(count_bytes.try_into().unwrap());
    assert!(count.is_nan());
}

#[tokio::test]
async fn test_composite_float64_counter_negative_zero_normalizes_to_positive_zero() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());
    composite.register_counter_field("count".to_string(), true, NumericKind::Float64);

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    delta
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 10,
                nonce: 1,
                data: (-0.0f64).to_be_bytes().to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    composite.merge(&mut *txn, &ctx, &delta).await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let count_key = b"/data/v1/doc1/count".to_vec();
    let count_bytes = txn.get(&count_key).await.unwrap().unwrap();
    let count = f64::from_be_bytes(count_bytes.try_into().unwrap());
    assert_eq!(count.to_bits(), 0.0f64.to_bits());
}

#[tokio::test]
async fn test_composite_schema_version_mismatch() {
    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());

    composite.register_lww_field("name".to_string());

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Delta with wrong schema version
    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v2".to_string(), 10).unwrap();
    delta
        .add_field_delta(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"Alice".to_vec(),
            },
        )
        .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("schema version mismatch"));
}

#[test]
fn test_composite_delta_empty_doc_id_rejected() {
    let result = CompositeDelta::new(vec![], "v1".to_string(), 10);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("doc_id cannot be empty"));
}

#[test]
fn test_composite_delta_empty_schema_version_rejected() {
    let result = CompositeDelta::new(b"doc1".to_vec(), "".to_string(), 10);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("schema_version_id cannot be empty"));
}

#[test]
fn test_composite_delta_empty_field_name_rejected() {
    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
    let result = delta.add_field_delta(
        "".to_string(),
        FieldDelta::Lww {
            priority: 10,
            data: b"value".to_vec(),
        },
    );
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("field_name cannot be empty"));
}
