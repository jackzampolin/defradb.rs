//! Unit tests for Counter CRDT
//!
//! Tests for Counter behavior including:
//! - Increment/decrement operations
//! - Nonce-based idempotency
//! - Overflow/underflow wrapping
//! - Float64 support
//! - Validation and error cases

use crdt::traits::{Context, MergeResult, ReplicatedData, ValueReader};
use crdt::{Counter, CounterDelta, LwwDelta, NumericKind};
use defra_core::types::DocId;
use storage::{MemoryStore, Store};

#[tokio::test]
async fn test_counter_increment() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Increment by 5
    let delta1 = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();
    counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Increment by 3
    let delta2 = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        20,
        12346,
        "v1".to_string(),
        3,
    )
    .unwrap();
    counter.merge(&mut *txn, &ctx, &delta2).await.unwrap();

    // Should be 8
    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
    assert_eq!(value, 8);

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_counter_idempotency() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Apply same delta twice
    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();

    counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
    counter.merge(&mut *txn, &ctx, &delta).await.unwrap(); // Should be ignored

    // Should still be 5 (not 10)
    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
    assert_eq!(value, 5);

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_counter_decrement_not_allowed() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        false, // Decrement not allowed
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Try to decrement
    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        -5,
    )
    .unwrap();

    let result = counter.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_counter_overflow_wrapping() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Set counter to near max
    let delta1 = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        i64::MAX - 10,
    )
    .unwrap();
    counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Try to increment beyond max - should wrap to negative (matching Go behavior)
    let delta2 = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        20,
        2,
        "v1".to_string(),
        20,
    )
    .unwrap();
    counter.merge(&mut *txn, &ctx, &delta2).await.unwrap();

    // Should wrap: (i64::MAX - 10) + 20 = i64::MIN + 9
    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
    assert_eq!(value, (i64::MAX - 10).wrapping_add(20));
    assert_eq!(value, i64::MIN + 9); // Verify wrapping behavior

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_counter_field_name_mismatch() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Delta for wrong field
    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "wrong_field".to_string(),
        10,
        1,
        "v1".to_string(),
        5,
    )
    .unwrap();

    let result = counter.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("field name mismatch"));
}

#[tokio::test]
async fn test_counter_schema_version_mismatch() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Delta for wrong schema version
    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v2".to_string(),
        5,
    )
    .unwrap();

    let result = counter.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("schema version mismatch"));
}

#[test]
fn test_counter_float64_constructor_accepts_nan() {
    let result = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        f64::NAN,
    );
    assert!(result.is_ok());
}

#[test]
fn test_counter_float64_constructor_accepts_positive_infinity() {
    let result = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        f64::INFINITY,
    );
    assert!(result.is_ok());
}

#[test]
fn test_counter_float64_constructor_accepts_negative_infinity() {
    let result = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        f64::NEG_INFINITY,
    );
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_counter_float64_overflow_becomes_positive_infinity() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Float64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Set counter to near max
    let delta1 = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        f64::MAX,
    )
    .unwrap();
    counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Try to increment - should overflow to infinity and be rejected
    let delta2 = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        20,
        2,
        "v1".to_string(),
        f64::MAX,
    )
    .unwrap();

    counter.merge(&mut *txn, &ctx, &delta2).await.unwrap();

    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = f64::from_be_bytes(value_bytes.try_into().unwrap());
    assert!(value.is_infinite());
    assert!(value.is_sign_positive());
}

#[tokio::test]
async fn test_counter_float64_nan_increment_propagates_nan() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Float64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new("doc1").unwrap(),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    let delta = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        f64::NAN,
    )
    .unwrap();

    counter.merge(&mut *txn, &ctx, &delta).await.unwrap();

    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = f64::from_be_bytes(value_bytes.try_into().unwrap());
    assert!(value.is_nan());
}

#[tokio::test]
async fn test_counter_float64_negative_zero_normalizes_to_positive_zero() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Float64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new("doc1").unwrap(),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    let delta = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        -0.0,
    )
    .unwrap();

    counter.merge(&mut *txn, &ctx, &delta).await.unwrap();

    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = f64::from_be_bytes(value_bytes.try_into().unwrap());
    assert_eq!(value.to_bits(), 0.0f64.to_bits());
}

#[tokio::test]
async fn test_counter_float64_basic() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Float64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Increment by 5.5
    let delta1 = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        5.5,
    )
    .unwrap();
    counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Increment by 3.2
    let delta2 = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        20,
        2,
        "v1".to_string(),
        3.2,
    )
    .unwrap();
    counter.merge(&mut *txn, &ctx, &delta2).await.unwrap();

    // Should be 8.7
    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = f64::from_be_bytes(value_bytes.try_into().unwrap());
    assert!((value - 8.7).abs() < 0.0001);

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_counter_merge_result_applied() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();

    let result = counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
    assert!(matches!(result, MergeResult::Applied));
}

#[tokio::test]
async fn test_counter_merge_result_skipped_already_applied() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();

    // First merge should apply
    let result1 = counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
    assert!(matches!(result1, MergeResult::Applied));

    // Second merge with same nonce should be skipped
    let result2 = counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
    assert!(matches!(
        result2,
        MergeResult::SkippedAlreadyApplied { nonce: 12345 }
    ));
}

#[tokio::test]
async fn test_counter_wrong_delta_type() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Try to merge an LwwDelta into a Counter
    let wrong_delta = LwwDelta::new(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        "v1".to_string(),
        b"value".to_vec(),
    )
    .unwrap();

    let result = counter.merge(&mut *txn, &ctx, &wrong_delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid delta type for Counter"));
}

#[tokio::test]
async fn test_counter_numeric_kind_mismatch() {
    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64, // Int64 counter
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Try to apply a Float64 delta to an Int64 counter
    let delta = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5.0,
    )
    .unwrap();

    let result = counter.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("numeric kind mismatch"));
}

#[test]
fn test_counter_constructor_empty_schema_version() {
    let result = Counter::new(
        "".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        NumericKind::Int64,
    );
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err
        .to_string()
        .contains("schema_version_id cannot be empty"));
}

#[test]
fn test_counter_constructor_empty_doc_id() {
    let result = Counter::new(
        "v1".to_string(),
        b"",
        "count".to_string(),
        true,
        NumericKind::Int64,
    );
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("doc_id cannot be empty"));
}

#[test]
fn test_counter_constructor_empty_field_name() {
    let result = Counter::new(
        "v1".to_string(),
        b"doc1",
        "".to_string(),
        true,
        NumericKind::Int64,
    );
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("field_name cannot be empty"));
}

#[test]
fn test_counter_delta_validation_rejects_empty_values() {
    // Test that empty doc_id, field_name, and schema_version are rejected

    // Empty doc_id
    let result =
        CounterDelta::new_int64(Vec::new(), "count".to_string(), 10, 1, "v1".to_string(), 5);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("doc_id"));

    // Empty field_name
    let result =
        CounterDelta::new_int64(b"doc1".to_vec(), "".to_string(), 10, 1, "v1".to_string(), 5);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("field_name"));

    // Empty schema_version_id
    let result = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "".to_string(),
        5,
    );
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("schema_version_id"));
}
