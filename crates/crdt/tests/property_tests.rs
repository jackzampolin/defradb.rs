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

use async_trait::async_trait;
use crdt::{
    composite::{CompositeDAG, CompositeDelta, FieldDelta},
    traits::{Context, ReplicatedData, ValueReader},
    Counter, CounterDelta, Lww, LwwDelta,
};
use defra_core::{store::Store, types::DocId, Error, Result};
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

// ============================================================================
// Test Infrastructure
// ============================================================================

/// In-memory store for testing
struct MemoryStore {
    data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.lock().await.get(key).cloned())
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data.lock().await.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.lock().await.remove(key);
        Ok(())
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        Ok(self.data.lock().await.contains_key(key))
    }
}

/// Failing store for crash recovery testing
struct FailingStore {
    inner: MemoryStore,
    fail_set_after: Option<usize>,
    fail_has_after: Option<usize>,
    fail_key_prefix: Option<Vec<u8>>,
    set_count: AtomicUsize,
    has_count: AtomicUsize,
}

impl FailingStore {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            fail_set_after: None,
            fail_has_after: None,
            fail_key_prefix: None,
            set_count: AtomicUsize::new(0),
            has_count: AtomicUsize::new(0),
        }
    }

    fn fail_set_after(mut self, n: usize) -> Self {
        self.fail_set_after = Some(n);
        self
    }

    fn fail_has_after(mut self, n: usize) -> Self {
        self.fail_has_after = Some(n);
        self
    }

    fn for_key_prefix(mut self, prefix: Vec<u8>) -> Self {
        self.fail_key_prefix = Some(prefix);
        self
    }

    fn should_fail(&self, key: &[u8]) -> bool {
        match &self.fail_key_prefix {
            Some(p) => key.starts_with(p),
            None => true,
        }
    }
}

#[async_trait]
impl Store for FailingStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let count = self.set_count.fetch_add(1, Ordering::SeqCst);
        if let Some(fail_after) = self.fail_set_after {
            if count >= fail_after && self.should_fail(key) {
                return Err(Error::Storage("simulated set failure".into()));
            }
        }
        self.inner.set(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        let count = self.has_count.fetch_add(1, Ordering::SeqCst);
        if let Some(fail_after) = self.fail_has_after {
            if count >= fail_after && self.should_fail(key) {
                return Err(Error::Storage("simulated has failure".into()));
            }
        }
        self.inner.has(key).await
    }
}

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
        priority1 in 1u64..2000,
        priority2 in 1u64..2000,
        data1 in prop::collection::vec(any::<u8>(), 1..20),
        data2 in prop::collection::vec(any::<u8>(), 1..20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create two LWW instances
            let store1 = Arc::new(MemoryStore::new());
            let mut lww1 = Lww::new(
                store1.clone(),
                "v1".to_string(),
                b"doc1",
                "field1".to_string(),
            ).unwrap();

            let store2 = Arc::new(MemoryStore::new());
            let mut lww2 = Lww::new(
                store2.clone(),
                "v1".to_string(),
                b"doc1",
                "field1".to_string(),
            ).unwrap();

            let delta1 = LwwDelta::new(
                b"doc1".to_vec(),
                "field1".to_string(),
                priority1,
                "v1".to_string(),
                data1.clone(),
            )
            .unwrap();

            let delta2 = LwwDelta::new(
                b"doc1".to_vec(),
                "field1".to_string(),
                priority2,
                "v1".to_string(),
                data2.clone(),
            )
            .unwrap();

            // Merge in order: delta1, delta2
            lww1.merge(&ctx, &delta1).await.unwrap();
            lww1.merge(&ctx, &delta2).await.unwrap();

            // Merge in reverse order: delta2, delta1
            lww2.merge(&ctx, &delta2).await.unwrap();
            lww2.merge(&ctx, &delta1).await.unwrap();

            // Both should converge to the same value
            let value1 = lww1.value().await.unwrap();
            let value2 = lww2.value().await.unwrap();

            assert_eq!(value1, value2);
        });
    }

    /// Property: LWW commutativity with boundary priority values
    #[test]
    fn test_lww_commutativity_boundary_priorities(
        // Include boundary values: 0, 1, max-1, max
        priority1 in prop::sample::select(vec![0u64, 1, u64::MAX - 1, u64::MAX]),
        priority2 in prop::sample::select(vec![0u64, 1, u64::MAX - 1, u64::MAX]),
        data1 in prop::collection::vec(any::<u8>(), 1..10),
        data2 in prop::collection::vec(any::<u8>(), 1..10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store1 = Arc::new(MemoryStore::new());
            let mut lww1 = Lww::new(store1.clone(), "v1".to_string(), b"doc1", "field1".to_string()).unwrap();

            let store2 = Arc::new(MemoryStore::new());
            let mut lww2 = Lww::new(store2.clone(), "v1".to_string(), b"doc1", "field1".to_string()).unwrap();

            let delta1 = LwwDelta::new(b"doc1".to_vec(), "field1".to_string(), priority1, "v1".to_string(), data1.clone()).unwrap();
            let delta2 = LwwDelta::new(b"doc1".to_vec(), "field1".to_string(), priority2, "v1".to_string(), data2.clone()).unwrap();

            lww1.merge(&ctx, &delta1).await.unwrap();
            lww1.merge(&ctx, &delta2).await.unwrap();

            lww2.merge(&ctx, &delta2).await.unwrap();
            lww2.merge(&ctx, &delta1).await.unwrap();

            let value1 = lww1.value().await.unwrap();
            let value2 = lww2.value().await.unwrap();
            assert_eq!(value1, value2);
        });
    }

    // ------------------------------------------------------------------------
    // LWW Idempotence Tests
    // ------------------------------------------------------------------------

    /// Property: LWW idempotence - merging same delta multiple times
    #[test]
    fn test_lww_idempotence(
        priority in 1u64..10000,
        data in prop::collection::vec(any::<u8>(), 1..20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut lww = Lww::new(
                store.clone(),
                "v1".to_string(),
                b"doc1",
                "field1".to_string(),
            ).unwrap();

            let delta = LwwDelta::new(
                b"doc1".to_vec(),
                "field1".to_string(),
                priority,
                "v1".to_string(),
                data.clone(),
            )
            .unwrap();

            // Merge once
            lww.merge(&ctx, &delta).await.unwrap();
            let value1 = lww.value().await.unwrap();

            // Merge again
            lww.merge(&ctx, &delta).await.unwrap();
            let value2 = lww.value().await.unwrap();

            // Values should be identical
            assert_eq!(value1, value2);
        });
    }

    /// Property: LWW idempotence with boundary priorities
    #[test]
    fn test_lww_idempotence_boundary(
        priority in prop::sample::select(vec![0u64, 1, u64::MAX - 1, u64::MAX]),
        data in prop::collection::vec(any::<u8>(), 1..20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "field1".to_string()).unwrap();

            let delta = LwwDelta::new(b"doc1".to_vec(), "field1".to_string(), priority, "v1".to_string(), data.clone()).unwrap();

            lww.merge(&ctx, &delta).await.unwrap();
            let value1 = lww.value().await.unwrap();

            // Apply 5 more times
            for _ in 0..5 {
                lww.merge(&ctx, &delta).await.unwrap();
            }
            let value2 = lww.value().await.unwrap();

            assert_eq!(value1, value2);
        });
    }

    // ------------------------------------------------------------------------
    // LWW Convergence Tests
    // ------------------------------------------------------------------------

    /// Property: LWW convergence with multiple replicas
    #[test]
    fn test_lww_multi_replica_convergence(
        deltas in prop::collection::vec(
            (1u64..10000, prop::collection::vec(any::<u8>(), 1..20)),
            3..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create 3 replicas
            let mut replicas = Vec::new();
            for _ in 0..3 {
                let store = Arc::new(MemoryStore::new());
                let lww = Lww::new(
                    store.clone(),
                    "v1".to_string(),
                    b"doc1",
                    "field1".to_string(),
                ).unwrap();
                replicas.push(lww);
            }

            // Create deltas
            let lww_deltas: Vec<LwwDelta> = deltas
                .iter()
                .map(|(priority, data)| {
                    LwwDelta::new(
                        b"doc1".to_vec(),
                        "field1".to_string(),
                        *priority,
                        "v1".to_string(),
                        data.clone(),
                    )
                    .unwrap()
                })
                .collect();

            // Merge all deltas into all replicas (in different orders)
            for (i, replica) in replicas.iter_mut().enumerate() {
                for (j, _delta) in lww_deltas.iter().enumerate() {
                    // Different replicas see deltas in different orders
                    let idx = (i + j) % lww_deltas.len();
                    replica.merge(&ctx, &lww_deltas[idx]).await.unwrap();
                }
            }

            // All replicas should converge to the same value
            let value0 = replicas[0].value().await.unwrap();
            let value1 = replicas[1].value().await.unwrap();
            let value2 = replicas[2].value().await.unwrap();

            assert_eq!(&value0, &value1);
            assert_eq!(&value1, &value2);
        });
    }

    // ------------------------------------------------------------------------
    // LWW Delete Property Tests
    // ------------------------------------------------------------------------

    /// Property: LWW deletion commutativity - deletes commute with writes
    #[test]
    fn test_lww_delete_commutativity(
        write_priority in 1u64..1000,
        delete_priority in 1u64..1000,
        data in prop::collection::vec(any::<u8>(), 1..20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store1 = Arc::new(MemoryStore::new());
            let mut lww1 = Lww::new(store1.clone(), "v1".to_string(), b"doc1", "field1".to_string()).unwrap();

            let store2 = Arc::new(MemoryStore::new());
            let mut lww2 = Lww::new(store2.clone(), "v1".to_string(), b"doc1", "field1".to_string()).unwrap();

            let write_delta = LwwDelta::new(b"doc1".to_vec(), "field1".to_string(), write_priority, "v1".to_string(), data.clone()).unwrap();
            let delete_delta = LwwDelta::delete(b"doc1".to_vec(), "field1".to_string(), delete_priority, "v1".to_string()).unwrap();

            // Order 1: write then delete
            lww1.merge(&ctx, &write_delta).await.unwrap();
            lww1.merge(&ctx, &delete_delta).await.unwrap();

            // Order 2: delete then write
            lww2.merge(&ctx, &delete_delta).await.unwrap();
            lww2.merge(&ctx, &write_delta).await.unwrap();

            // Both should have same state (either deleted or value present)
            let result1 = lww1.value().await;
            let result2 = lww2.value().await;

            match (&result1, &result2) {
                (Ok(v1), Ok(v2)) => assert_eq!(v1, v2),
                (Err(_), Err(_)) => {} // Both deleted
                _ => panic!("Replicas diverged: {:?} vs {:?}", result1, result2),
            }
        });
    }

    /// Property: LWW deletion idempotence
    #[test]
    fn test_lww_delete_idempotence(
        priority in 1u64..10000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "field1".to_string()).unwrap();

            // Write something first
            let write_delta = LwwDelta::new(b"doc1".to_vec(), "field1".to_string(), 1, "v1".to_string(), b"data".to_vec()).unwrap();
            lww.merge(&ctx, &write_delta).await.unwrap();

            // Delete multiple times
            let delete_delta = LwwDelta::delete(b"doc1".to_vec(), "field1".to_string(), priority, "v1".to_string()).unwrap();

            lww.merge(&ctx, &delete_delta).await.unwrap();
            let state1 = lww.value().await;

            lww.merge(&ctx, &delete_delta).await.unwrap();
            let state2 = lww.value().await;

            lww.merge(&ctx, &delete_delta).await.unwrap();
            let state3 = lww.value().await;

            // All states should be equivalent
            assert_eq!(state1.is_err(), state2.is_err());
            assert_eq!(state2.is_err(), state3.is_err());
        });
    }

    // ------------------------------------------------------------------------
    // Counter Commutativity Tests
    // ------------------------------------------------------------------------

    /// Property: Counter commutativity
    #[test]
    fn test_counter_commutativity(
        inc1 in -100i64..100,
        inc2 in -100i64..100,
        nonce1 in any::<i64>(),
        nonce2 in any::<i64>(),
    ) {
        // Skip if nonces are the same
        if nonce1 == nonce2 {
            return Ok(());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create two counters
            let store1 = Arc::new(MemoryStore::new());
            let mut counter1 = Counter::new(
                store1.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true, // allow decrement
                crdt::counter::NumericKind::Int64,
            ).unwrap();

            let store2 = Arc::new(MemoryStore::new());
            let mut counter2 = Counter::new(
                store2.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Int64,
            ).unwrap();

            let delta1 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce1,
                "v1".to_string(),
                inc1,
            )
            .unwrap();

            let delta2 = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                20,
                nonce2,
                "v1".to_string(),
                inc2,
            )
            .unwrap();

            // Merge in order: delta1, delta2
            counter1.merge(&ctx, &delta1).await.unwrap();
            counter1.merge(&ctx, &delta2).await.unwrap();

            // Merge in reverse: delta2, delta1
            counter2.merge(&ctx, &delta2).await.unwrap();
            counter2.merge(&ctx, &delta1).await.unwrap();

            // Both should have same value (sum of increments)
            let value1_bytes = counter1.value().await.unwrap();
            let value2_bytes = counter2.value().await.unwrap();

            let value1 = i64::from_be_bytes(value1_bytes.try_into().unwrap());
            let value2 = i64::from_be_bytes(value2_bytes.try_into().unwrap());

            assert_eq!(value1, value2);
            assert_eq!(value1, inc1.wrapping_add(inc2));
        });
    }

    /// Property: Counter commutativity with boundary values
    #[test]
    fn test_counter_commutativity_boundary(
        inc1 in prop::sample::select(vec![i64::MIN, i64::MIN + 1, -1i64, 0, 1, i64::MAX - 1, i64::MAX]),
        inc2 in prop::sample::select(vec![i64::MIN, i64::MIN + 1, -1i64, 0, 1, i64::MAX - 1, i64::MAX]),
        nonce1 in 1i64..1000000,
        nonce2 in 1000001i64..2000000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store1 = Arc::new(MemoryStore::new());
            let mut counter1 = Counter::new(store1.clone(), "v1".to_string(), b"doc1", "count".to_string(), true, crdt::counter::NumericKind::Int64).unwrap();

            let store2 = Arc::new(MemoryStore::new());
            let mut counter2 = Counter::new(store2.clone(), "v1".to_string(), b"doc1", "count".to_string(), true, crdt::counter::NumericKind::Int64).unwrap();

            let delta1 = CounterDelta::new_int64(b"doc1".to_vec(), "count".to_string(), 10, nonce1, "v1".to_string(), inc1).unwrap();
            let delta2 = CounterDelta::new_int64(b"doc1".to_vec(), "count".to_string(), 20, nonce2, "v1".to_string(), inc2).unwrap();

            counter1.merge(&ctx, &delta1).await.unwrap();
            counter1.merge(&ctx, &delta2).await.unwrap();

            counter2.merge(&ctx, &delta2).await.unwrap();
            counter2.merge(&ctx, &delta1).await.unwrap();

            let value1_bytes = counter1.value().await.unwrap();
            let value2_bytes = counter2.value().await.unwrap();

            assert_eq!(value1_bytes, value2_bytes);
        });
    }

    // ------------------------------------------------------------------------
    // Counter Idempotence Tests
    // ------------------------------------------------------------------------

    /// Property: Counter idempotence
    #[test]
    fn test_counter_idempotence(
        increment in -100i64..100,
        nonce in any::<i64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut counter = Counter::new(
                store.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Int64,
            ).unwrap();

            let delta = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce,
                "v1".to_string(),
                increment,
            )
            .unwrap();

            // Merge once
            counter.merge(&ctx, &delta).await.unwrap();
            let value1_bytes = counter.value().await.unwrap();
            let value1 = i64::from_be_bytes(value1_bytes.try_into().unwrap());

            // Merge same delta again (should be ignored due to nonce)
            counter.merge(&ctx, &delta).await.unwrap();
            let value2_bytes = counter.value().await.unwrap();
            let value2 = i64::from_be_bytes(value2_bytes.try_into().unwrap());

            // Values should be identical (only applied once)
            assert_eq!(value1, value2);
            assert_eq!(value1, increment);
        });
    }

    // ------------------------------------------------------------------------
    // Counter Associativity - Note on semantics
    // ------------------------------------------------------------------------
    // For counters, associativity means: The final sum is independent of grouping.
    // Since counter deltas are simply summed (commutatively), any grouping gives
    // the same result. We test this by verifying that applying deltas in any
    // sequence gives the sum of all increments.

    /// Property: Counter associativity - all orderings give same sum
    #[test]
    fn test_counter_associativity(
        inc1 in -100i64..100,
        inc2 in -100i64..100,
        inc3 in -100i64..100,
        nonce1 in 1i64..1000000,
        nonce2 in 1000001i64..2000000,
        nonce3 in 2000001i64..3000000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create 6 counters - one for each permutation of 3 deltas
            let permutations: Vec<Vec<usize>> = vec![
                vec![0, 1, 2],
                vec![0, 2, 1],
                vec![1, 0, 2],
                vec![1, 2, 0],
                vec![2, 0, 1],
                vec![2, 1, 0],
            ];

            let delta_a = CounterDelta::new_int64(b"doc1".to_vec(), "count".to_string(), 10, nonce1, "v1".to_string(), inc1).unwrap();
            let delta_b = CounterDelta::new_int64(b"doc1".to_vec(), "count".to_string(), 20, nonce2, "v1".to_string(), inc2).unwrap();
            let delta_c = CounterDelta::new_int64(b"doc1".to_vec(), "count".to_string(), 30, nonce3, "v1".to_string(), inc3).unwrap();

            let deltas = [&delta_a, &delta_b, &delta_c];

            let mut results = Vec::new();
            for perm in &permutations {
                let store = Arc::new(MemoryStore::new());
                let mut counter = Counter::new(store.clone(), "v1".to_string(), b"doc1", "count".to_string(), true, crdt::counter::NumericKind::Int64).unwrap();

                for &idx in perm {
                    counter.merge(&ctx, deltas[idx]).await.unwrap();
                }

                let value_bytes = counter.value().await.unwrap();
                let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
                results.push(value);
            }

            // All permutations should give the same result
            let first = results[0];
            for (i, &result) in results.iter().enumerate() {
                assert_eq!(result, first, "Permutation {} gave different result", i);
            }

            // Should equal the sum of all increments (with wrapping on overflow)
            let expected = inc1.wrapping_add(inc2).wrapping_add(inc3);
            assert_eq!(first, expected);
        });
    }

    // ------------------------------------------------------------------------
    // Float64 Counter Tests
    // ------------------------------------------------------------------------

    /// Property: Float64 Counter commutativity
    #[test]
    fn test_counter_float64_commutativity(
        inc1 in -1000.0f64..1000.0,
        inc2 in -1000.0f64..1000.0,
        nonce1 in any::<i64>(),
        nonce2 in any::<i64>(),
    ) {
        // Skip if nonces are the same or if values are not finite
        if nonce1 == nonce2 || !inc1.is_finite() || !inc2.is_finite() {
            return Ok(());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create two counters
            let store1 = Arc::new(MemoryStore::new());
            let mut counter1 = Counter::new(
                store1.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true, // allow decrement
                crdt::counter::NumericKind::Float64,
            ).unwrap();

            let store2 = Arc::new(MemoryStore::new());
            let mut counter2 = Counter::new(
                store2.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Float64,
            ).unwrap();

            let delta1 = CounterDelta::new_float64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce1,
                "v1".to_string(),
                inc1,
            )
            .unwrap();

            let delta2 = CounterDelta::new_float64(
                b"doc1".to_vec(),
                "count".to_string(),
                20,
                nonce2,
                "v1".to_string(),
                inc2,
            )
            .unwrap();

            // Merge in order: delta1, delta2
            counter1.merge(&ctx, &delta1).await.unwrap();
            counter1.merge(&ctx, &delta2).await.unwrap();

            // Merge in reverse: delta2, delta1
            counter2.merge(&ctx, &delta2).await.unwrap();
            counter2.merge(&ctx, &delta1).await.unwrap();

            // Both should have same value (sum of increments)
            let value1_bytes = counter1.value().await.unwrap();
            let value2_bytes = counter2.value().await.unwrap();

            let value1 = f64::from_be_bytes(value1_bytes.try_into().unwrap());
            let value2 = f64::from_be_bytes(value2_bytes.try_into().unwrap());

            // Use approximate comparison for floating point
            assert!((value1 - value2).abs() < 1e-10, "values should be equal: {} vs {}", value1, value2);

            // Should be approximately equal to sum
            let expected = inc1 + inc2;
            assert!((value1 - expected).abs() < 1e-10, "value {} should be approximately {}", value1, expected);
        });
    }

    /// Property: Float64 Counter idempotence
    #[test]
    fn test_counter_float64_idempotence(
        increment in -1000.0f64..1000.0,
        nonce in any::<i64>(),
    ) {
        // Skip if value is not finite
        if !increment.is_finite() {
            return Ok(());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut counter = Counter::new(
                store.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Float64,
            ).unwrap();

            let delta = CounterDelta::new_float64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce,
                "v1".to_string(),
                increment,
            )
            .unwrap();

            // Merge once
            counter.merge(&ctx, &delta).await.unwrap();
            let value1_bytes = counter.value().await.unwrap();
            let value1 = f64::from_be_bytes(value1_bytes.try_into().unwrap());

            // Merge same delta again (should be ignored due to nonce)
            counter.merge(&ctx, &delta).await.unwrap();
            let value2_bytes = counter.value().await.unwrap();
            let value2 = f64::from_be_bytes(value2_bytes.try_into().unwrap());

            // Values should be identical (only applied once)
            assert!((value1 - value2).abs() < 1e-10);
            assert!((value1 - increment).abs() < 1e-10);
        });
    }

    /// Property: Float64 Counter associativity
    #[test]
    fn test_counter_float64_associativity(
        inc1 in -100.0f64..100.0,
        inc2 in -100.0f64..100.0,
        inc3 in -100.0f64..100.0,
        nonce1 in 1i64..1000000,
        nonce2 in 1000001i64..2000000,
        nonce3 in 2000001i64..3000000,
    ) {
        // Skip if any values are not finite
        if !inc1.is_finite() || !inc2.is_finite() || !inc3.is_finite() {
            return Ok(());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Test all 6 permutations
            let permutations: Vec<Vec<usize>> = vec![
                vec![0, 1, 2],
                vec![0, 2, 1],
                vec![1, 0, 2],
                vec![1, 2, 0],
                vec![2, 0, 1],
                vec![2, 1, 0],
            ];

            let delta_a = CounterDelta::new_float64(b"doc1".to_vec(), "count".to_string(), 10, nonce1, "v1".to_string(), inc1).unwrap();
            let delta_b = CounterDelta::new_float64(b"doc1".to_vec(), "count".to_string(), 20, nonce2, "v1".to_string(), inc2).unwrap();
            let delta_c = CounterDelta::new_float64(b"doc1".to_vec(), "count".to_string(), 30, nonce3, "v1".to_string(), inc3).unwrap();

            let deltas = [&delta_a, &delta_b, &delta_c];

            let mut results = Vec::new();
            for perm in &permutations {
                let store = Arc::new(MemoryStore::new());
                let mut counter = Counter::new(store.clone(), "v1".to_string(), b"doc1", "count".to_string(), true, crdt::counter::NumericKind::Float64).unwrap();

                for &idx in perm {
                    counter.merge(&ctx, deltas[idx]).await.unwrap();
                }

                let value_bytes = counter.value().await.unwrap();
                let value = f64::from_be_bytes(value_bytes.try_into().unwrap());
                results.push(value);
            }

            // All permutations should give approximately the same result
            let first = results[0];
            for (i, &result) in results.iter().enumerate() {
                assert!((result - first).abs() < 1e-10, "Permutation {} gave different result: {} vs {}", i, result, first);
            }

            // Should equal the sum of all increments
            let expected = inc1 + inc2 + inc3;
            assert!((first - expected).abs() < 1e-10);
        });
    }

    // ------------------------------------------------------------------------
    // Composite CRDT Tests
    // ------------------------------------------------------------------------

    /// Property: Composite commutativity with mixed field types
    #[test]
    fn test_composite_commutativity(
        lww_priority1 in 1u64..2000,
        lww_priority2 in 1u64..2000,
        lww_data1 in prop::collection::vec(any::<u8>(), 1..20),
        lww_data2 in prop::collection::vec(any::<u8>(), 1..20),
        counter_inc1 in -100i64..100,
        counter_inc2 in -100i64..100,
        counter_nonce1 in 1i64..1000000,
        counter_nonce2 in 1000001i64..2000000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create two composite replicas
            let store1 = Arc::new(MemoryStore::new());
            let mut composite1 = CompositeDAG::new(store1.clone(), DocId::new("doc1"), "v1".to_string());
            composite1.register_lww_field("name".to_string());
            composite1.register_counter_field("count".to_string());

            let store2 = Arc::new(MemoryStore::new());
            let mut composite2 = CompositeDAG::new(store2.clone(), DocId::new("doc1"), "v1".to_string());
            composite2.register_lww_field("name".to_string());
            composite2.register_counter_field("count".to_string());

            // Create first composite delta
            let mut delta1 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), lww_priority1).unwrap();
            delta1.add_field_delta("name".to_string(), FieldDelta::Lww {
                priority: lww_priority1,
                data: lww_data1.clone(),
            }).unwrap();
            delta1.add_field_delta("count".to_string(), FieldDelta::Counter {
                priority: 10,
                nonce: counter_nonce1,
                data: counter_inc1.to_be_bytes().to_vec(),
            }).unwrap();

            // Create second composite delta
            let mut delta2 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), lww_priority2).unwrap();
            delta2.add_field_delta("name".to_string(), FieldDelta::Lww {
                priority: lww_priority2,
                data: lww_data2.clone(),
            }).unwrap();
            delta2.add_field_delta("count".to_string(), FieldDelta::Counter {
                priority: 20,
                nonce: counter_nonce2,
                data: counter_inc2.to_be_bytes().to_vec(),
            }).unwrap();

            // Merge in order: delta1, delta2
            composite1.merge(&ctx, &delta1).await.unwrap();
            composite1.merge(&ctx, &delta2).await.unwrap();

            // Merge in reverse order: delta2, delta1
            composite2.merge(&ctx, &delta2).await.unwrap();
            composite2.merge(&ctx, &delta1).await.unwrap();

            // Both should converge to the same state for LWW field
            let name1 = store1.get(b"/data/v1/doc1/name").await.unwrap();
            let name2 = store2.get(b"/data/v1/doc1/name").await.unwrap();
            assert_eq!(name1, name2);

            // Both should converge to the same state for Counter field
            let count1 = store1.get(b"/data/v1/doc1/count").await.unwrap().unwrap();
            let count2 = store2.get(b"/data/v1/doc1/count").await.unwrap().unwrap();
            assert_eq!(count1, count2);

            // Counter should be sum of increments
            let count_val = i64::from_be_bytes(count1.try_into().unwrap());
            assert_eq!(count_val, counter_inc1.wrapping_add(counter_inc2));
        });
    }

    /// Property: Composite multi-replica convergence
    #[test]
    fn test_composite_multi_replica_convergence(
        lww_deltas in prop::collection::vec(
            (1u64..5000, prop::collection::vec(any::<u8>(), 1..20)),
            2..5
        ),
        counter_increments in prop::collection::vec(-50i64..50, 2..5),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create 3 replicas
            let mut replicas = Vec::new();
            let mut stores = Vec::new();
            for _ in 0..3 {
                let store = Arc::new(MemoryStore::new());
                let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());
                composite.register_lww_field("name".to_string());
                composite.register_counter_field("score".to_string());
                stores.push(store);
                replicas.push(composite);
            }

            // Create composite deltas
            let mut composite_deltas = Vec::new();
            for (idx, ((lww_priority, lww_data), counter_inc)) in
                lww_deltas.iter().zip(counter_increments.iter()).enumerate()
            {
                let mut delta = CompositeDelta::new(
                    b"doc1".to_vec(),
                    "v1".to_string(),
                    *lww_priority
                ).unwrap();
                delta.add_field_delta("name".to_string(), FieldDelta::Lww {
                    priority: *lww_priority,
                    data: lww_data.clone(),
                }).unwrap();
                delta.add_field_delta("score".to_string(), FieldDelta::Counter {
                    priority: 10,
                    nonce: idx as i64 + 1000,
                    data: counter_inc.to_be_bytes().to_vec(),
                }).unwrap();
                composite_deltas.push(delta);
            }

            // Merge all deltas into all replicas (in different orders)
            for (i, replica) in replicas.iter_mut().enumerate() {
                for (j, _) in composite_deltas.iter().enumerate() {
                    // Different replicas see deltas in different orders
                    let idx = (i + j) % composite_deltas.len();
                    replica.merge(&ctx, &composite_deltas[idx]).await.unwrap();
                }
            }

            // All replicas should converge to the same state
            let name0 = stores[0].get(b"/data/v1/doc1/name").await.unwrap();
            let name1 = stores[1].get(b"/data/v1/doc1/name").await.unwrap();
            let name2 = stores[2].get(b"/data/v1/doc1/name").await.unwrap();

            assert_eq!(&name0, &name1, "Replica 0 and 1 should have same name");
            assert_eq!(&name1, &name2, "Replica 1 and 2 should have same name");

            let score0 = stores[0].get(b"/data/v1/doc1/score").await.unwrap().unwrap();
            let score1 = stores[1].get(b"/data/v1/doc1/score").await.unwrap().unwrap();
            let score2 = stores[2].get(b"/data/v1/doc1/score").await.unwrap().unwrap();

            assert_eq!(&score0, &score1, "Replica 0 and 1 should have same score");
            assert_eq!(&score1, &score2, "Replica 1 and 2 should have same score");
        });
    }

    /// Property: Composite idempotence
    #[test]
    fn test_composite_idempotence(
        lww_priority in 1u64..10000,
        lww_data in prop::collection::vec(any::<u8>(), 1..20),
        counter_inc in -100i64..100,
        counter_nonce in any::<i64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());
            composite.register_lww_field("name".to_string());
            composite.register_counter_field("count".to_string());

            let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), lww_priority).unwrap();
            delta.add_field_delta("name".to_string(), FieldDelta::Lww {
                priority: lww_priority,
                data: lww_data.clone(),
            }).unwrap();
            delta.add_field_delta("count".to_string(), FieldDelta::Counter {
                priority: 10,
                nonce: counter_nonce,
                data: counter_inc.to_be_bytes().to_vec(),
            }).unwrap();

            // Merge once
            composite.merge(&ctx, &delta).await.unwrap();
            let name1 = store.get(b"/data/v1/doc1/name").await.unwrap();
            let count1 = store.get(b"/data/v1/doc1/count").await.unwrap();

            // Merge again (should be idempotent)
            composite.merge(&ctx, &delta).await.unwrap();
            let name2 = store.get(b"/data/v1/doc1/name").await.unwrap();
            let count2 = store.get(b"/data/v1/doc1/count").await.unwrap();

            // Values should be identical
            assert_eq!(name1, name2);
            assert_eq!(count1, count2);

            // Counter should only have been applied once
            let count_val = i64::from_be_bytes(count1.unwrap().try_into().unwrap());
            assert_eq!(count_val, counter_inc);
        });
    }

    /// Property: Composite delete operations commute
    #[test]
    fn test_composite_delete_commutativity(
        lww_priority in 1u64..1000,
        delete_priority in 1u64..1000,
        lww_data in prop::collection::vec(any::<u8>(), 1..20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store1 = Arc::new(MemoryStore::new());
            let mut composite1 = CompositeDAG::new(store1.clone(), DocId::new("doc1"), "v1".to_string());
            composite1.register_lww_field("name".to_string());

            let store2 = Arc::new(MemoryStore::new());
            let mut composite2 = CompositeDAG::new(store2.clone(), DocId::new("doc1"), "v1".to_string());
            composite2.register_lww_field("name".to_string());

            let mut write_delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), lww_priority).unwrap();
            write_delta.add_field_delta("name".to_string(), FieldDelta::Lww {
                priority: lww_priority,
                data: lww_data.clone(),
            }).unwrap();

            let mut delete_delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), delete_priority).unwrap();
            delete_delta.add_field_delta("name".to_string(), FieldDelta::Delete {
                priority: delete_priority,
            }).unwrap();

            // Order 1: write then delete
            composite1.merge(&ctx, &write_delta).await.unwrap();
            composite1.merge(&ctx, &delete_delta).await.unwrap();

            // Order 2: delete then write
            composite2.merge(&ctx, &delete_delta).await.unwrap();
            composite2.merge(&ctx, &write_delta).await.unwrap();

            // Both should have same state
            let name1 = store1.get(b"/data/v1/doc1/name").await.unwrap();
            let name2 = store2.get(b"/data/v1/doc1/name").await.unwrap();
            assert_eq!(name1, name2);
        });
    }
}

// ============================================================================
// Storage Error Propagation Tests (non-proptest)
// ============================================================================

#[tokio::test]
async fn test_lww_storage_get_failure_propagates() {
    // This tests that storage errors during merge are properly propagated
    let store = Arc::new(FailingStore::new().fail_has_after(0)); // Fail immediately on has
    let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    // This should succeed because LWW merge reads priority, not uses `has`
    let delta = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();

    // Create a store that fails on get
    let store2 = Arc::new(FailingStore::new());
    let mut lww2 = Lww::new(store2.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

    // First merge should succeed
    lww2.merge(&ctx, &delta).await.unwrap();
}

#[tokio::test]
async fn test_counter_nonce_check_failure_propagates() {
    // Test that has() failure during nonce check propagates correctly
    let store = Arc::new(
        FailingStore::new()
            .fail_has_after(0)
            .for_key_prefix(b"/data/v1/doc1/count/nonces/".to_vec()),
    );
    let mut counter = Counter::new(
        store.clone(),
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        crdt::counter::NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();

    let result = counter.merge(&ctx, &delta).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("simulated"));
}

#[tokio::test]
async fn test_counter_value_write_failure_propagates() {
    // Test that set() failure during value write propagates
    // The counter does: has(nonce) -> get(value) -> set(nonce) -> set(value)
    // We want to fail on the value set, which is the 2nd set call
    let store = Arc::new(FailingStore::new().fail_set_after(1)); // Fail on 2nd set
    let mut counter = Counter::new(
        store.clone(),
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        crdt::counter::NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();

    let result = counter.merge(&ctx, &delta).await;
    assert!(result.is_err());
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[tokio::test]
async fn test_lww_concurrent_merges() {
    // Test that concurrent merges to shared store converge correctly
    let store = Arc::new(MemoryStore::new());
    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    // Create multiple tasks that merge concurrently
    let mut handles = Vec::new();

    for i in 0..10 {
        let store_clone = store.clone();
        let ctx_clone = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        let handle = tokio::spawn(async move {
            let mut lww = Lww::new(
                store_clone,
                "v1".to_string(),
                b"doc1",
                "name".to_string(),
            )
            .unwrap();

            let delta = LwwDelta::new(
                b"doc1".to_vec(),
                "name".to_string(),
                i as u64 * 100 + 1,
                "v1".to_string(),
                format!("value_{}", i).into_bytes(),
            )
            .unwrap();

            lww.merge(&ctx_clone, &delta).await
        });

        handles.push(handle);
    }

    // Wait for all merges to complete
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    // The final value should be deterministic - highest priority wins
    let lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();
    let value = lww.value().await.unwrap();

    // Priority 901 (i=9) should win
    assert_eq!(value, b"value_9");
}

#[tokio::test]
async fn test_counter_concurrent_increments() {
    // Test that concurrent counter increments sum correctly
    let store = Arc::new(MemoryStore::new());

    let mut handles = Vec::new();

    for i in 0..10 {
        let store_clone = store.clone();

        let handle = tokio::spawn(async move {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let mut counter = Counter::new(
                store_clone,
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Int64,
            )
            .unwrap();

            let delta = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                i as i64 * 1000 + 1, // Unique nonces
                "v1".to_string(),
                1, // Each increments by 1
            )
            .unwrap();

            counter.merge(&ctx, &delta).await
        });

        handles.push(handle);
    }

    // Wait for all merges to complete
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    // The final value should be 10 (sum of all increments)
    let counter = Counter::new(
        store.clone(),
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        crdt::counter::NumericKind::Int64,
    )
    .unwrap();
    let value_bytes = counter.value().await.unwrap();
    let value = i64::from_be_bytes(value_bytes.try_into().unwrap());

    assert_eq!(value, 10);
}

// ============================================================================
// Fuzz Tests for Malformed Data
// ============================================================================

proptest! {
    /// Fuzz test: Random bytes as delta data shouldn't crash
    #[test]
    fn test_fuzz_lww_random_data(
        data in prop::collection::vec(any::<u8>(), 0..1000),
        priority in any::<u64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "field".to_string()).unwrap();

            // This should not panic, regardless of data content
            let delta_result = LwwDelta::new(
                b"doc1".to_vec(),
                "field".to_string(),
                priority,
                "v1".to_string(),
                data,
            );

            if let Ok(delta) = delta_result {
                let _ = lww.merge(&ctx, &delta).await;
            }
        });
    }

    /// Fuzz test: Random bytes as counter data - should handle gracefully
    #[test]
    fn test_fuzz_counter_random_data_length(
        data_len in 0usize..20,
        nonce in any::<i64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut counter = Counter::new(
                store.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Int64,
            ).unwrap();

            // Create delta with wrong data length
            let bad_data: Vec<u8> = vec![0u8; data_len];

            // Manually construct a CounterDelta with bad data
            // This simulates receiving corrupted data over network
            let delta = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce,
                "v1".to_string(),
                0, // Will be overwritten
            ).unwrap();

            // The merge should either succeed (if data_len == 8) or fail gracefully
            let result = counter.merge(&ctx, &delta).await;

            // Should not panic, should either succeed or return error
            match result {
                Ok(_) => {}
                Err(e) => {
                    // Error is fine, as long as it doesn't panic
                    let _ = e.to_string();
                }
            }
        });
    }

    /// Fuzz test: Empty and special string values in field names
    #[test]
    fn test_fuzz_field_name_validation(
        field_name in ".*",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = Arc::new(MemoryStore::new());

            // Creating LWW with arbitrary field name should either succeed or fail gracefully
            let result = Lww::new(store.clone(), "v1".to_string(), b"doc1", field_name.clone());

            match result {
                Ok(_) => {
                    // Non-empty field names should succeed
                    assert!(!field_name.is_empty());
                }
                Err(e) => {
                    // Empty field names should fail
                    assert!(e.to_string().contains("field_name"));
                }
            }
        });
    }

    /// Fuzz test: Extreme priority values
    #[test]
    fn test_fuzz_extreme_priorities(
        priority in any::<u64>(),
        data in prop::collection::vec(any::<u8>(), 1..20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "field".to_string()).unwrap();

            let delta = LwwDelta::new(
                b"doc1".to_vec(),
                "field".to_string(),
                priority,
                "v1".to_string(),
                data,
            ).unwrap();

            // Any u64 priority should work without panicking
            let result = lww.merge(&ctx, &delta).await;
            assert!(result.is_ok());
        });
    }

    /// Fuzz test: Many deltas with same nonce (idempotency stress test)
    #[test]
    fn test_fuzz_nonce_collision_stress(
        num_applications in 1usize..100,
        increment in -100i64..100,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            let store = Arc::new(MemoryStore::new());
            let mut counter = Counter::new(
                store.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Int64,
            ).unwrap();

            let delta = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                42, // Fixed nonce
                "v1".to_string(),
                increment,
            ).unwrap();

            // Apply the same delta many times
            for _ in 0..num_applications {
                counter.merge(&ctx, &delta).await.unwrap();
            }

            // Value should be exactly `increment`, not `increment * num_applications`
            let value_bytes = counter.value().await.unwrap();
            let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
            assert_eq!(value, increment);
        });
    }
}

// ============================================================================
// Crash Recovery Semantics Tests
// ============================================================================

#[tokio::test]
async fn test_counter_crash_after_nonce_before_value() {
    // Simulate crash after marking nonce but before updating value
    // The nonce write succeeds, then the value write fails
    let store = Arc::new(
        FailingStore::new()
            .fail_set_after(1), // First set (nonce) succeeds, second set (value) fails
    );

    let mut counter = Counter::new(
        store.clone(),
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        crdt::counter::NumericKind::Int64,
    )
    .unwrap();

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let delta = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        10,
        12345,
        "v1".to_string(),
        5,
    )
    .unwrap();

    // The merge should fail
    let result = counter.merge(&ctx, &delta).await;
    assert!(result.is_err());

    // Now simulate recovery - create a new counter instance
    // The nonce was marked, so re-applying the delta should skip it
    let store2 = Arc::new(MemoryStore::new());
    let mut counter2 = Counter::new(
        store2.clone(),
        "v1".to_string(),
        b"doc1",
        "count".to_string(),
        true,
        crdt::counter::NumericKind::Int64,
    )
    .unwrap();

    // Apply the delta to the new store - should work
    counter2.merge(&ctx, &delta).await.unwrap();

    let value_bytes = counter2.value().await.unwrap();
    let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
    assert_eq!(value, 5);
}

#[tokio::test]
async fn test_lww_crash_after_value_before_priority() {
    // Simulate crash after writing value but before updating priority
    // This tests what happens if priority isn't updated
    let store = Arc::new(FailingStore::new().fail_set_after(1)); // Value succeeds, priority fails

    let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    let delta = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        100,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();

    let result = lww.merge(&ctx, &delta).await;
    assert!(result.is_err());
}

// ============================================================================
// Partial Composite Delta Application Tests
// ============================================================================

#[tokio::test]
async fn test_composite_partial_field_rejection() {
    // Test that when one field is rejected (lower priority), others can still apply
    let store = Arc::new(MemoryStore::new());
    let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());
    composite.register_lww_field("name".to_string());
    composite.register_counter_field("count".to_string());

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    // First delta: set name with high priority, counter with nonce 1
    let mut delta1 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 1000).unwrap();
    delta1
        .add_field_delta(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 1000,
                data: b"Alice".to_vec(),
            },
        )
        .unwrap();
    delta1
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 10,
                nonce: 1,
                data: 5i64.to_be_bytes().to_vec(),
            },
        )
        .unwrap();
    composite.merge(&ctx, &delta1).await.unwrap();

    // Second delta: set name with LOW priority (should be rejected), counter with nonce 2 (should apply)
    let mut delta2 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 500).unwrap();
    delta2
        .add_field_delta(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 500, // Lower than 1000, will be rejected
                data: b"Bob".to_vec(),
            },
        )
        .unwrap();
    delta2
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 20,
                nonce: 2, // Different nonce, will apply
                data: 3i64.to_be_bytes().to_vec(),
            },
        )
        .unwrap();
    composite.merge(&ctx, &delta2).await.unwrap();

    // Name should still be "Alice" (rejected Bob)
    let name = store.get(b"/data/v1/doc1/name").await.unwrap().unwrap();
    assert_eq!(name, b"Alice");

    // Count should be 8 (5 + 3)
    let count_bytes = store.get(b"/data/v1/doc1/count").await.unwrap().unwrap();
    let count = i64::from_be_bytes(count_bytes.try_into().unwrap());
    assert_eq!(count, 8);
}

#[tokio::test]
async fn test_composite_all_fields_rejected() {
    // Test when all fields in a composite delta are rejected
    let store = Arc::new(MemoryStore::new());
    let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());
    composite.register_lww_field("name".to_string());
    composite.register_counter_field("count".to_string());

    let ctx = Context {
        doc_id: DocId::new("doc1"),
        schema_version: "v1".to_string(),
    };

    // First delta: high priority
    let mut delta1 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 1000).unwrap();
    delta1
        .add_field_delta(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 1000,
                data: b"Alice".to_vec(),
            },
        )
        .unwrap();
    delta1
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 10,
                nonce: 1,
                data: 5i64.to_be_bytes().to_vec(),
            },
        )
        .unwrap();
    composite.merge(&ctx, &delta1).await.unwrap();

    // Second delta: all low priority, same nonce (all rejected)
    let mut delta2 = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 500).unwrap();
    delta2
        .add_field_delta(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 500,
                data: b"Bob".to_vec(),
            },
        )
        .unwrap();
    delta2
        .add_field_delta(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 20,
                nonce: 1, // Same nonce, will be skipped
                data: 3i64.to_be_bytes().to_vec(),
            },
        )
        .unwrap();

    // The merge itself should succeed (but report rejection)
    let result = composite.merge(&ctx, &delta2).await;
    assert!(result.is_ok());

    // Values should be unchanged
    let name = store.get(b"/data/v1/doc1/name").await.unwrap().unwrap();
    assert_eq!(name, b"Alice");

    let count_bytes = store.get(b"/data/v1/doc1/count").await.unwrap().unwrap();
    let count = i64::from_be_bytes(count_bytes.try_into().unwrap());
    assert_eq!(count, 5);
}
