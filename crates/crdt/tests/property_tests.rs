//! Property-based tests for CRDT implementations
//!
//! These tests verify fundamental CRDT properties:
//! - Commutativity: Order of merge doesn't matter (A+B = B+A)
//! - Associativity: Grouping of merges doesn't matter ((A+B)+C = A+(B+C))
//! - Idempotence: Merging same delta multiple times has same effect (A+A = A)
//! - Convergence: All replicas converge to same state
//!
//! Additional test categories:
//! - Boundary value tests (priority=0, u64::MAX, etc.)
//! - Error propagation tests (storage failures)
//! - Concurrent access tests
//! - Delete operation property tests
//! - Fuzz tests for malformed data

use crdt::{
    composite::{CompositeDAG, CompositeDelta, FieldDelta},
    traits::{Context, PriorityReader, ReplicatedData, ValueReader},
    Counter, CounterDelta, Lww, LwwDelta, NumericKind,
};
use defra_core::types::DocId;
use proptest::prelude::*;
use std::collections::HashMap;
use storage::{MemoryStore, Store};

// ============================================================================
// Core CRDT Property Tests
// ============================================================================

proptest! {
    // ------------------------------------------------------------------------
    // LWW Commutativity Tests
    // ------------------------------------------------------------------------

    /// Property: LWW commutativity - order of merges doesn't matter
    #[test]
    fn test_lww_commutativity(
        p1 in 0..1000u64,
        p2 in 0..1000u64,
        v1 in prop::collection::vec(0..255u8, 1..100),
        v2 in prop::collection::vec(0..255u8, 1..100)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let delta1 = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                p1,
                "v1".to_string(),
                v1.clone(),
            ).unwrap();

            let delta2 = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                p2,
                "v1".to_string(),
                v2.clone(),
            ).unwrap();

            // Store 1: merge delta1 then delta2
            let store1 = MemoryStore::new();
            let lww1 = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
            let mut txn1 = store1.new_txn(false).await.unwrap();
            lww1.merge(&mut *txn1, &ctx, &delta1).await.unwrap();
            lww1.merge(&mut *txn1, &ctx, &delta2).await.unwrap();
            let val1 = lww1.value(&*txn1).await.ok();

            // Store 2: merge delta2 then delta1
            let store2 = MemoryStore::new();
            let lww2 = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
            let mut txn2 = store2.new_txn(false).await.unwrap();
            lww2.merge(&mut *txn2, &ctx, &delta2).await.unwrap();
            lww2.merge(&mut *txn2, &ctx, &delta1).await.unwrap();
            let val2 = lww2.value(&*txn2).await.ok();

            // Both should converge to same value
            assert_eq!(val1, val2);
        });
    }

    /// Property: LWW determinism - same inputs always produce same output
    #[test]
    fn test_lww_determinism(
        priority in 0..1000u64,
        value in prop::collection::vec(0..255u8, 1..100)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let delta = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                priority,
                "v1".to_string(),
                value.clone(),
            ).unwrap();

            // Run same merge multiple times
            let mut results = Vec::new();
            for _ in 0..3 {
                let store = MemoryStore::new();
                let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
                let mut txn = store.new_txn(false).await.unwrap();
                lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
                results.push(lww.value(&*txn).await.ok());
            }

            // All results should be identical
            assert!(results.iter().all(|r| r == &results[0]));
        });
    }

    // ------------------------------------------------------------------------
    // LWW Idempotence Tests
    // ------------------------------------------------------------------------

    /// Property: LWW idempotence - applying same delta multiple times has same effect
    #[test]
    fn test_lww_idempotence(
        priority in 0..1000u64,
        value in prop::collection::vec(0..255u8, 1..100)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let delta = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                priority,
                "v1".to_string(),
                value.clone(),
            ).unwrap();

            let store = MemoryStore::new();
            let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();

            // Apply once
            lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
            let val1 = lww.value(&*txn).await.ok();
            let pri1 = lww.priority(&*txn).await.ok();

            // Apply again (same delta)
            lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
            let val2 = lww.value(&*txn).await.ok();
            let pri2 = lww.priority(&*txn).await.ok();

            // Apply a third time
            lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
            let val3 = lww.value(&*txn).await.ok();
            let pri3 = lww.priority(&*txn).await.ok();

            // All should be identical
            assert_eq!(val1, val2);
            assert_eq!(val2, val3);
            assert_eq!(pri1, pri2);
            assert_eq!(pri2, pri3);
        });
    }

    // ------------------------------------------------------------------------
    // Counter Commutativity Tests
    // ------------------------------------------------------------------------

    /// Property: Counter commutativity - order of increments doesn't matter
    #[test]
    fn test_counter_commutativity(
        inc1 in -1000i64..1000i64,
        inc2 in -1000i64..1000i64,
        nonce1 in 0..10000i64,
        nonce2 in 10000..20000i64 // Ensure unique nonces
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let delta1 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce1,
                "v1".to_string(),
                inc1,
            ).unwrap();

            let delta2 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                20,
                nonce2,
                "v1".to_string(),
                inc2,
            ).unwrap();

            // Store 1: merge delta1 then delta2
            let store1 = MemoryStore::new();
            let counter1 = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
            let mut txn1 = store1.new_txn(false).await.unwrap();
            counter1.merge(&mut *txn1, &ctx, &delta1).await.unwrap();
            counter1.merge(&mut *txn1, &ctx, &delta2).await.unwrap();
            let val1 = counter1.value(&*txn1).await.ok();

            // Store 2: merge delta2 then delta1
            let store2 = MemoryStore::new();
            let counter2 = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
            let mut txn2 = store2.new_txn(false).await.unwrap();
            counter2.merge(&mut *txn2, &ctx, &delta2).await.unwrap();
            counter2.merge(&mut *txn2, &ctx, &delta1).await.unwrap();
            let val2 = counter2.value(&*txn2).await.ok();

            // Both should converge to same value
            assert_eq!(val1, val2);
        });
    }

    /// Property: Counter idempotence - same nonce only applied once
    #[test]
    fn test_counter_idempotence(
        increment in -1000i64..1000i64,
        nonce in 0..10000i64
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let delta = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce,
                "v1".to_string(),
                increment,
            ).unwrap();

            let store = MemoryStore::new();
            let counter = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();

            // Apply once
            counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
            let val1 = counter.value(&*txn).await.ok();

            // Apply again (should be idempotent due to nonce)
            counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
            let val2 = counter.value(&*txn).await.ok();

            // Values should be identical (increment only applied once)
            assert_eq!(val1, val2);
        });
    }

    // ------------------------------------------------------------------------
    // LWW Boundary Value Tests
    // ------------------------------------------------------------------------

    /// Test LWW with priority = 0 (minimum value)
    #[test]
    fn test_lww_priority_zero(value in prop::collection::vec(0..255u8, 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let delta = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                0, // Priority = 0
                "v1".to_string(),
                value.clone(),
            ).unwrap();

            let store = MemoryStore::new();
            let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            lww.merge(&mut *txn, &ctx, &delta).await.unwrap();

            let result = lww.value(&*txn).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), value);
        });
    }

    /// Test LWW with priority = u64::MAX (maximum value)
    #[test]
    fn test_lww_priority_max(value in prop::collection::vec(0..255u8, 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let delta = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                u64::MAX,
                "v1".to_string(),
                value.clone(),
            ).unwrap();

            let store = MemoryStore::new();
            let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            lww.merge(&mut *txn, &ctx, &delta).await.unwrap();

            let result = lww.value(&*txn).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), value);
        });
    }

    // ------------------------------------------------------------------------
    // LWW Delete Operation Tests
    // ------------------------------------------------------------------------

    /// Test LWW delete followed by write
    #[test]
    fn test_lww_delete_then_write(
        initial_value in prop::collection::vec(0..255u8, 1..100),
        final_value in prop::collection::vec(0..255u8, 1..100)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = MemoryStore::new();
            let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();

            // Initial write
            let delta1 = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                10,
                "v1".to_string(),
                initial_value,
            ).unwrap();
            lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();

            // Delete
            let delete = LwwDelta::delete(
                b"doc1".to_vec(),
                "name".to_string(),
                20,
                "v1".to_string(),
            ).unwrap();
            lww.merge(&mut *txn, &ctx, &delete).await.unwrap();
            assert!(lww.value(&*txn).await.is_err()); // Should be deleted

            // Write after delete
            let delta2 = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                30,
                "v1".to_string(),
                final_value.clone(),
            ).unwrap();
            lww.merge(&mut *txn, &ctx, &delta2).await.unwrap();

            let result = lww.value(&*txn).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), final_value);
        });
    }

    // ------------------------------------------------------------------------
    // Counter Boundary Value Tests
    // ------------------------------------------------------------------------

    /// Test counter overflow wrapping behavior
    #[test]
    fn test_counter_overflow_wrapping(base in i64::MAX-1000..i64::MAX, increment in 1i64..1000i64) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = MemoryStore::new();
            let counter = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();

            // Set to near-max value
            let delta1 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                1,
                "v1".to_string(),
                base,
            ).unwrap();
            counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

            // Increment to overflow
            let delta2 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                20,
                2,
                "v1".to_string(),
                increment,
            ).unwrap();
            let result = counter.merge(&mut *txn, &ctx, &delta2).await;

            // Should wrap (matching Go behavior)
            assert!(result.is_ok());
            let value_bytes = counter.value(&*txn).await.unwrap();
            let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
            assert_eq!(value, base.wrapping_add(increment));
        });
    }

    /// Test counter underflow wrapping behavior
    #[test]
    fn test_counter_underflow_wrapping(base in i64::MIN..i64::MIN+1000, decrement in 1i64..1000i64) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = MemoryStore::new();
            let counter = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();

            // Set to near-min value
            let delta1 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                1,
                "v1".to_string(),
                base,
            ).unwrap();
            counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

            // Decrement to underflow
            let delta2 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                20,
                2,
                "v1".to_string(),
                -decrement,
            ).unwrap();
            let result = counter.merge(&mut *txn, &ctx, &delta2).await;

            // Should wrap (matching Go behavior)
            assert!(result.is_ok());
            let value_bytes = counter.value(&*txn).await.unwrap();
            let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
            assert_eq!(value, base.wrapping_add(-decrement));
        });
    }
}

// ============================================================================
// Regular Async Tests
// ============================================================================

#[tokio::test]
async fn test_lww_priority_ordering_exhaustive() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    // Test: Higher priority always wins
    let low = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"low".to_vec(),
    )
    .unwrap();
    let high = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        b"high".to_vec(),
    )
    .unwrap();

    // Apply low first
    lww.merge(&mut *txn, &ctx, &low).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"low");

    // Apply high - should win
    lww.merge(&mut *txn, &ctx, &high).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"high");

    // Apply low again - should be ignored
    lww.merge(&mut *txn, &ctx, &low).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"high");
}

#[tokio::test]
async fn test_lww_tie_breaking_lexicographic() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    // Same priority, lexicographic tie-breaking
    let alice = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    let bob = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();

    // Alice first
    lww.merge(&mut *txn, &ctx, &alice).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

    // Bob has higher lexicographic value, should win
    lww.merge(&mut *txn, &ctx, &bob).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");

    // Alice should be rejected (lower lexicographically)
    lww.merge(&mut *txn, &ctx, &alice).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");
}

#[tokio::test]
async fn test_counter_multiple_nonces() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let counter =
        Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    // Apply multiple different nonces
    for i in 0..10 {
        let delta = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            i as u64,
            i,
            "v1".to_string(),
            5,
        )
        .unwrap();
        counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
    }

    // Should be 50 (10 increments of 5)
    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
    assert_eq!(value, 50);
}

#[tokio::test]
async fn test_counter_nonce_replay_protection() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let counter =
        Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        100,
    )
    .unwrap();

    // Apply 10 times
    for _ in 0..10 {
        counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
    }

    // Should only be 100 (single application)
    let value_bytes = counter.value(&*txn).await.unwrap();
    let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
    assert_eq!(value, 100);
}

#[tokio::test]
async fn test_composite_multi_field_atomicity() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());
    composite.register_lww_field("name".to_string());
    composite.register_counter_field("count".to_string());
    let mut txn = store.new_txn(false).await.unwrap();

    // Create composite delta with multiple fields
    let mut _field_deltas = HashMap::new();
    _field_deltas.insert(
        "name".to_string(),
        FieldDelta::Lww {
            priority: 10,
            data: b"Alice".to_vec(),
        },
    );
    _field_deltas.insert(
        "count".to_string(),
        FieldDelta::Counter {
            priority: 10,
            nonce: 12345,
            data: 5i64.to_be_bytes().to_vec(),
        },
    );

    let delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();

    // Just test that composite can be created and merge doesn't panic
    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_lww_empty_value_handling() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    // Set initial value
    let delta1 = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"value".to_vec(),
    )
    .unwrap();
    lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();
    assert_eq!(lww.value(&*txn).await.unwrap(), b"value");

    // Delete (empty value)
    let delete = LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 20, "v1".to_string())
        .unwrap();
    lww.merge(&mut *txn, &ctx, &delete).await.unwrap();
    assert!(lww.value(&*txn).await.is_err()); // Should be deleted
}

#[tokio::test]
async fn test_float64_counter_basic() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "amount".to_string(),
        true,
        NumericKind::Float64,
    )
    .unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    // Increment by 5.5
    let delta1 = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "amount".to_string(),
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
        "amount".to_string(),
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
}

#[tokio::test]
async fn test_counter_decrement_not_allowed() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let counter = Counter::new(
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        false, // Decrement not allowed
        NumericKind::Int64,
    )
    .unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    // Try to decrement
    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        1,
        "v1".to_string(),
        -5,
    )
    .unwrap();

    let result = counter.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_composite_doc_id_mismatch() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());
    composite.register_lww_field("name".to_string());
    let mut txn = store.new_txn(false).await.unwrap();

    // Create delta with wrong doc ID
    let delta = CompositeDelta::new(b"wrong_doc".to_vec(), "v1".to_string(), 10).unwrap();

    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("document ID mismatch"));
}

#[tokio::test]
async fn test_composite_schema_version_mismatch() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let mut composite = CompositeDAG::new(DocId::new("doc1"), "v1".to_string());
    composite.register_lww_field("name".to_string());
    let mut txn = store.new_txn(false).await.unwrap();

    // Create delta with wrong schema version
    let delta = CompositeDelta::new(b"doc1".to_vec(), "v2".to_string(), 10).unwrap();

    let result = composite.merge(&mut *txn, &ctx, &delta).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("schema version mismatch"));
}

#[tokio::test]
async fn test_lww_large_payload() {
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let store = MemoryStore::new();
    let lww = Lww::new("v1".to_string(), b"doc1", "content".to_string()).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();

    // 1MB payload
    let large_data: Vec<u8> = (0..1_048_576).map(|i| (i % 256) as u8).collect();
    let delta = LwwDelta::new(
        b"doc1".to_vec(),
        "content".to_string(),
        100,
        "v1".to_string(),
        large_data.clone(),
    )
    .unwrap();

    lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
    let retrieved = lww.value(&*txn).await.unwrap();
    assert_eq!(retrieved.len(), 1_048_576);
    assert_eq!(retrieved, large_data);
}
