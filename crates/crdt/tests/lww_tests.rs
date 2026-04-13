//! Unit tests for LWW Register CRDT
//!
//! Tests for Last-Write-Wins register behavior including:
//! - Priority-based conflict resolution
//! - Lexicographic tie-breaking
//! - Deletion (tombstone) handling
//! - Validation and error cases

use crdt::traits::{Context, MergeResult, ReplicatedData, ValueReader};
use crdt::{CounterDelta, Lww, LwwDelta};
use defra_core::types::DocId;
use storage::{MemoryStore, Store};

#[tokio::test]
async fn test_lww_higher_priority_wins() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // First write with priority 10
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Second write with higher priority 20
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_lww_lower_priority_ignored() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // First write with priority 20
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Second write with lower priority 10 - should be ignored
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_lww_same_priority_lexicographic() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // First write: "Alice" with priority 10
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Second write: "Bob" with same priority 10
    // "Bob" > "Alice" lexicographically, so Bob should win
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_lww_deletion() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Set value
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Delete (empty data)
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        Vec::new(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();

    // Value should be deleted
    assert!(lww.value(&*txn).await.is_err());

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_lww_empty_data_tie_breaking() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Write value at priority 10
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Delete (empty data) at same priority 10
    // Lexicographically, empty < "Alice", so "Alice" should win
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        Vec::new(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();

    // Value should still be "Alice" (empty data lost tie-break)
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Now delete at higher priority 20
    let delta3 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        Vec::new(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta3).await.unwrap();

    // Value should now be deleted
    assert!(lww.value(&*txn).await.is_err());

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_lww_deletion_resurrection_with_priority() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Write value at priority 20
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Try to delete at lower priority 10 (should be ignored)
    let delta2 =
        LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 10, "v1".to_string()).unwrap();
    lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();

    // Value should still exist (deletion was lower priority)
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Delete at same priority 20
    // Since priorities are equal, lexicographic tie-breaking applies
    // Empty data < "Alice", so "Alice" wins
    let delta3 =
        LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 20, "v1".to_string()).unwrap();
    lww.merge(&mut *txn, &ctx, &delta3).await.unwrap();

    // Value should still be "Alice" (tie-break)
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Delete at higher priority 30
    let delta4 =
        LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 30, "v1".to_string()).unwrap();
    lww.merge(&mut *txn, &ctx, &delta4).await.unwrap();

    // Value should now be deleted
    assert!(lww.value(&*txn).await.is_err());

    // Try to resurrect with lower priority 25 (should fail)
    let delta5 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        25,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta5).await.unwrap();

    // Value should still be deleted (resurrection priority too low)
    assert!(lww.value(&*txn).await.is_err());

    // Resurrect with higher priority 40
    let delta6 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        40,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta6).await.unwrap();

    // Value should now be resurrected
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");

    txn.commit().await.unwrap();
}

#[test]
fn test_lww_delta_validation_rejects_empty_values() {
    // Test that empty doc_id, field_name, and schema_version are rejected
    // for both new() and delete() constructors

    // Empty doc_id
    let result = LwwDelta::new(
        Vec::new(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"value".to_vec(),
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("doc_id"));

    // Empty field_name
    let result = LwwDelta::new(
        b"doc1".to_vec(),
        "".to_string(),
        10,
        "v1".to_string(),
        b"value".to_vec(),
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("field_name"));

    let result = LwwDelta::delete(b"doc1".to_vec(), "".to_string(), 10, "v1".to_string());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("field_name"));

    // Empty schema_version_id
    let result = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "".to_string(),
        b"value".to_vec(),
    );
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("schema_version_id"));

    let result = LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 10, "".to_string());
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("schema_version_id"));
}

#[tokio::test]
async fn test_lww_wrong_delta_type() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Try to merge a CounterDelta into an LWW register
    let wrong_delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = lww.merge(&mut *txn, &ctx, &wrong_delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid delta type for LWW"));
}

#[tokio::test]
async fn test_lww_merge_result_applied() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let delta = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
    assert!(matches!(result, MergeResult::Applied));
}

#[tokio::test]
async fn test_lww_merge_result_rejected_lower_priority() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // First write with priority 20
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    let result1 = lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    assert!(matches!(result1, MergeResult::Applied));

    // Second write with lower priority 10 - should be rejected
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    let result2 = lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert!(matches!(
        result2,
        MergeResult::RejectedLowerPriority {
            current: 20,
            incoming: 10
        }
    ));
}

#[tokio::test]
async fn test_lww_merge_result_rejected_tie_break() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // First write: "Bob" with priority 10
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    let result1 = lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    assert!(matches!(result1, MergeResult::Applied));

    // Second write: "Alice" with same priority 10
    // "Alice" < "Bob" lexicographically, so should be rejected
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    let result2 = lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert!(matches!(result2, MergeResult::RejectedTieBreak));
}

#[tokio::test]
async fn test_lww_priority_zero() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Write with priority 0 (lowest possible)
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        0,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    let result1 = lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    assert!(matches!(result1, MergeResult::Applied));
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Second write with priority 0 should use tie-breaking
    // "Bob" > "Alice" so Bob should win
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        0,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    let result2 = lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert!(matches!(result2, MergeResult::Applied));
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");
}

#[tokio::test]
async fn test_lww_priority_max() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // Write with priority u64::MAX (highest possible)
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        u64::MAX,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    let result1 = lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    assert!(matches!(result1, MergeResult::Applied));
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Any subsequent write with lower priority should be rejected
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        u64::MAX - 1,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    let result2 = lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert!(matches!(result2, MergeResult::RejectedLowerPriority { .. }));
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");
}

#[tokio::test]
async fn test_lww_field_name_mismatch() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Delta with wrong field name
    let delta = LwwDelta::new(
        b"doc1".to_vec(),
        "wrong_field".to_string(),
        10,
        "v1".to_string(),
        b"value".to_vec(),
    )
    .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = lww.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("field name mismatch"));
}

#[tokio::test]
async fn test_lww_schema_version_mismatch() {
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    // Delta with wrong schema version
    let delta = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v2".to_string(),
        b"value".to_vec(),
    )
    .unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    let result = lww.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("schema version mismatch"));
}

#[test]
fn test_lww_constructor_empty_schema_version() {
    let result = Lww::new("".to_string(), b"doc1", "name".to_string());
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err
        .to_string()
        .contains("schema_version_id cannot be empty"));
}

#[test]
fn test_lww_constructor_empty_doc_id() {
    let result = Lww::new("v1".to_string(), b"", "name".to_string());
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("doc_id cannot be empty"));
}

#[test]
fn test_lww_constructor_empty_field_name() {
    let result = Lww::new("v1".to_string(), b"doc1", "".to_string());
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("field_name cannot be empty"));
}

#[tokio::test]
async fn test_lww_large_payload() {
    // Test LWW with large payloads (1MB, 10MB)
    // Verifies no memory issues or data corruption with large values
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "content".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // 1MB payload
    let large_data_1mb: Vec<u8> = (0..1_048_576).map(|i| (i % 256) as u8).collect();
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "content".to_string(),
        100,
        "v1".to_string(),
        large_data_1mb.clone(),
    )
    .unwrap();

    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    let retrieved = lww.value(&*txn).await.unwrap();
    assert_eq!(
        retrieved.len(),
        1_048_576,
        "1MB payload should be stored correctly"
    );
    assert_eq!(retrieved, large_data_1mb, "1MB payload should match");

    // 10MB payload with higher priority should overwrite
    let large_data_10mb: Vec<u8> = (0..10_485_760).map(|i| ((i * 7) % 256) as u8).collect();
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "content".to_string(),
        200,
        "v1".to_string(),
        large_data_10mb.clone(),
    )
    .unwrap();

    lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    let retrieved = lww.value(&*txn).await.unwrap();
    assert_eq!(
        retrieved.len(),
        10_485_760,
        "10MB payload should be stored correctly"
    );
    assert_eq!(retrieved, large_data_10mb, "10MB payload should match");

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_lww_large_payload_priority_rejected() {
    // Test that large payloads with lower priority are correctly rejected
    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "content".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    };

    let mut txn = store.new_txn(false).await.unwrap();

    // First, set small value with high priority
    let small_data = b"small value";
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "content".to_string(),
        1000,
        "v1".to_string(),
        small_data.to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();

    // Try to overwrite with large payload but lower priority
    let large_data: Vec<u8> = vec![0u8; 1_000_000]; // 1MB of zeros
    let delta2 = LwwDelta::new(
        b"doc1".to_vec(),
        "content".to_string(),
        500, // Lower priority
        "v1".to_string(),
        large_data,
    )
    .unwrap();

    let result = lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();
    assert!(
        matches!(result, MergeResult::RejectedLowerPriority { .. }),
        "large payload with lower priority should be rejected"
    );

    // Value should still be the small one
    let retrieved = lww.value(&*txn).await.unwrap();
    assert_eq!(retrieved, small_data);

    txn.commit().await.unwrap();
}
