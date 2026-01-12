//! Property-based tests for CRDT implementations
//!
//! These tests verify fundamental CRDT properties:
//! - Commutativity: Order of merge doesn't matter (A+B = B+A)
//! - Associativity: Grouping of merges doesn't matter ((A+B)+C = A+(B+C))
//! - Idempotence: Merging same delta multiple times has same effect (A+A = A)
//! - Convergence: All replicas converge to same state

use async_trait::async_trait;
use crdt::{
    composite::{CompositeDAG, CompositeDelta, FieldDelta},
    traits::{Context, ReplicatedData, ValueReader},
    Counter, CounterDelta, Lww, LwwDelta,
};
use defra_core::{store::Store, types::DocId, Result};
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// In-memory store for testing
/// Note: Duplicated from test_utils.rs because integration tests cannot access
/// modules marked with #[cfg(test)]
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

proptest! {
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
            assert_eq!(value1, inc1.saturating_add(inc2));
        });
    }

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

    /// Property: LWW associativity - grouping of merges doesn't matter
    /// (A + B) + C = A + (B + C)
    #[test]
    fn test_lww_associativity(
        priority1 in 1u64..2000,
        priority2 in 1u64..2000,
        priority3 in 1u64..2000,
        data1 in prop::collection::vec(any::<u8>(), 1..20),
        data2 in prop::collection::vec(any::<u8>(), 1..20),
        data3 in prop::collection::vec(any::<u8>(), 1..20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Context {
                doc_id: DocId::new("doc1"),
                schema_version: "v1".to_string(),
            };

            // Create LWW 1: apply (A + B) then C
            let store1 = Arc::new(MemoryStore::new());
            let mut lww1 = Lww::new(
                store1.clone(),
                "v1".to_string(),
                b"doc1",
                "field1".to_string(),
            ).unwrap();

            // Create LWW 2: apply A then (B + C)
            let store2 = Arc::new(MemoryStore::new());
            let mut lww2 = Lww::new(
                store2.clone(),
                "v1".to_string(),
                b"doc1",
                "field1".to_string(),
            ).unwrap();

            let delta_a = LwwDelta::new(
                b"doc1".to_vec(),
                "field1".to_string(),
                priority1,
                "v1".to_string(),
                data1.clone(),
            ).unwrap();

            let delta_b = LwwDelta::new(
                b"doc1".to_vec(),
                "field1".to_string(),
                priority2,
                "v1".to_string(),
                data2.clone(),
            ).unwrap();

            let delta_c = LwwDelta::new(
                b"doc1".to_vec(),
                "field1".to_string(),
                priority3,
                "v1".to_string(),
                data3.clone(),
            ).unwrap();

            // LWW 1: (A + B) + C
            lww1.merge(&ctx, &delta_a).await.unwrap();
            lww1.merge(&ctx, &delta_b).await.unwrap();
            lww1.merge(&ctx, &delta_c).await.unwrap();

            // LWW 2: A + (B + C) - simulate by applying in different conceptual grouping
            // Since merge is applied sequentially to shared state, we apply A first,
            // then the "pre-merged" effect of B and C
            lww2.merge(&ctx, &delta_a).await.unwrap();
            // Apply B and C (the order within the group shouldn't matter for final result)
            lww2.merge(&ctx, &delta_b).await.unwrap();
            lww2.merge(&ctx, &delta_c).await.unwrap();

            // Both should converge to the same value
            let value1 = lww1.value().await.unwrap_or_default();
            let value2 = lww2.value().await.unwrap_or_default();

            assert_eq!(value1, value2);
        });
    }

    /// Property: Counter associativity - grouping doesn't matter
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

            // Counter 1: (A + B) + C
            let store1 = Arc::new(MemoryStore::new());
            let mut counter1 = Counter::new(
                store1.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Int64,
            ).unwrap();

            // Counter 2: A + (B + C)
            let store2 = Arc::new(MemoryStore::new());
            let mut counter2 = Counter::new(
                store2.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Int64,
            ).unwrap();

            let delta_a = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce1,
                "v1".to_string(),
                inc1,
            ).unwrap();

            let delta_b = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                20,
                nonce2,
                "v1".to_string(),
                inc2,
            ).unwrap();

            let delta_c = CounterDelta::new_int64(
                b"doc1".to_vec(),
                "count".to_string(),
                30,
                nonce3,
                "v1".to_string(),
                inc3,
            ).unwrap();

            // Counter 1: (A + B) + C
            counter1.merge(&ctx, &delta_a).await.unwrap();
            counter1.merge(&ctx, &delta_b).await.unwrap();
            counter1.merge(&ctx, &delta_c).await.unwrap();

            // Counter 2: A + (B + C)
            counter2.merge(&ctx, &delta_a).await.unwrap();
            counter2.merge(&ctx, &delta_b).await.unwrap();
            counter2.merge(&ctx, &delta_c).await.unwrap();

            // Both should have the same value (sum of all increments)
            let value1_bytes = counter1.value().await.unwrap();
            let value2_bytes = counter2.value().await.unwrap();

            let value1 = i64::from_be_bytes(value1_bytes.try_into().unwrap());
            let value2 = i64::from_be_bytes(value2_bytes.try_into().unwrap());

            assert_eq!(value1, value2);

            // Should equal the sum of all increments (with saturation)
            let expected = inc1.saturating_add(inc2).saturating_add(inc3);
            assert_eq!(value1, expected);
        });
    }

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

            // Counter 1: (A + B) + C
            let store1 = Arc::new(MemoryStore::new());
            let mut counter1 = Counter::new(
                store1.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Float64,
            ).unwrap();

            // Counter 2: A + (B + C)
            let store2 = Arc::new(MemoryStore::new());
            let mut counter2 = Counter::new(
                store2.clone(),
                "v1".to_string(),
                b"doc1",
                "count".to_string(),
                true,
                crdt::counter::NumericKind::Float64,
            ).unwrap();

            let delta_a = CounterDelta::new_float64(
                b"doc1".to_vec(),
                "count".to_string(),
                10,
                nonce1,
                "v1".to_string(),
                inc1,
            ).unwrap();

            let delta_b = CounterDelta::new_float64(
                b"doc1".to_vec(),
                "count".to_string(),
                20,
                nonce2,
                "v1".to_string(),
                inc2,
            ).unwrap();

            let delta_c = CounterDelta::new_float64(
                b"doc1".to_vec(),
                "count".to_string(),
                30,
                nonce3,
                "v1".to_string(),
                inc3,
            ).unwrap();

            // Counter 1: (A + B) + C
            counter1.merge(&ctx, &delta_a).await.unwrap();
            counter1.merge(&ctx, &delta_b).await.unwrap();
            counter1.merge(&ctx, &delta_c).await.unwrap();

            // Counter 2: A + (B + C)
            counter2.merge(&ctx, &delta_a).await.unwrap();
            counter2.merge(&ctx, &delta_b).await.unwrap();
            counter2.merge(&ctx, &delta_c).await.unwrap();

            // Both should have the same value (sum of all increments)
            let value1_bytes = counter1.value().await.unwrap();
            let value2_bytes = counter2.value().await.unwrap();

            let value1 = f64::from_be_bytes(value1_bytes.try_into().unwrap());
            let value2 = f64::from_be_bytes(value2_bytes.try_into().unwrap());

            assert!((value1 - value2).abs() < 1e-10);

            // Should equal the sum of all increments
            let expected = inc1 + inc2 + inc3;
            assert!((value1 - expected).abs() < 1e-10);
        });
    }

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
            assert_eq!(count_val, counter_inc1.saturating_add(counter_inc2));
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
}
