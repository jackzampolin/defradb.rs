//! Property-based tests for CRDT implementations
//!
//! These tests verify fundamental CRDT properties:
//! - Commutativity: Order of merge doesn't matter (A+B = B+A)
//! - Associativity: Grouping of merges doesn't matter ((A+B)+C = A+(B+C))
//! - Idempotence: Merging same delta multiple times has same effect (A+A = A)
//! - Convergence: All replicas converge to same state

mod lww_properties {
    use crdt::{
        traits::{Context, PriorityReader, ReplicatedData, ValueReader},
        Lww, LwwDelta,
    };
    use defra_core::types::DocId;
    use proptest::prelude::*;
    use storage::{MemoryStore, Store};

    proptest! {
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
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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
                let pri1 = lww1.priority(&*txn1).await.ok();

                // Store 2: merge delta2 then delta1
                let store2 = MemoryStore::new();
                let lww2 = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
                let mut txn2 = store2.new_txn(false).await.unwrap();
                lww2.merge(&mut *txn2, &ctx, &delta2).await.unwrap();
                lww2.merge(&mut *txn2, &ctx, &delta1).await.unwrap();
                let val2 = lww2.value(&*txn2).await.ok();
                let pri2 = lww2.priority(&*txn2).await.ok();

                // Both should converge
                assert_eq!(val1, val2);
                assert_eq!(pri1, pri2);
            });
        }

        #[test]
        fn test_lww_determinism(
            priority in 0..1000u64,
            value in prop::collection::vec(0..255u8, 1..100)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
                };

                let delta = LwwDelta::new(
                    b"doc1".to_vec(),
                    "name".to_string(),
                    priority,
                    "v1".to_string(),
                    value.clone(),
                ).unwrap();

                let mut results = Vec::new();
                for _ in 0..3 {
                    let store = MemoryStore::new();
                    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
                    let mut txn = store.new_txn(false).await.unwrap();
                    lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
                    results.push(lww.value(&*txn).await.ok());
                }

                assert!(results.iter().all(|r| r == &results[0]));
            });
        }

        #[test]
        fn test_lww_idempotence(
            priority in 0..1000u64,
            value in prop::collection::vec(0..255u8, 1..100)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

                lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
                let val1 = lww.value(&*txn).await.ok();
                let pri1 = lww.priority(&*txn).await.ok();

                lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
                let val2 = lww.value(&*txn).await.ok();
                let pri2 = lww.priority(&*txn).await.ok();

                lww.merge(&mut *txn, &ctx, &delta).await.unwrap();
                let val3 = lww.value(&*txn).await.ok();
                let pri3 = lww.priority(&*txn).await.ok();

                assert_eq!(val1, val2);
                assert_eq!(val2, val3);
                assert_eq!(pri1, pri2);
                assert_eq!(pri2, pri3);
            });
        }

        #[test]
        fn test_lww_priority_zero(value in prop::collection::vec(0..255u8, 1..100)) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
                };

                let delta = LwwDelta::new(
                    b"doc1".to_vec(),
                    "name".to_string(),
                    0,
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

        #[test]
        fn test_lww_priority_max(value in prop::collection::vec(0..255u8, 1..100)) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

        #[test]
        fn test_lww_delete_then_write(
            initial_value in prop::collection::vec(0..255u8, 1..100),
            final_value in prop::collection::vec(0..255u8, 1..100)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
                };

                let store = MemoryStore::new();
                let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
                let mut txn = store.new_txn(false).await.unwrap();

                let delta1 = LwwDelta::new(
                    b"doc1".to_vec(),
                    "name".to_string(),
                    10,
                    "v1".to_string(),
                    initial_value,
                ).unwrap();
                lww.merge(&mut *txn, &ctx, &delta1).await.unwrap();

                let delete = LwwDelta::delete(
                    b"doc1".to_vec(),
                    "name".to_string(),
                    20,
                    "v1".to_string(),
                ).unwrap();
                lww.merge(&mut *txn, &ctx, &delete).await.unwrap();
                assert!(lww.value(&*txn).await.is_err());

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

        #[test]
        fn test_lww_full_convergence(
            p1 in 0..1000u64,
            p2 in 0..1000u64,
            p3 in 0..1000u64,
            v1 in prop::collection::vec(0..255u8, 1..30),
            v2 in prop::collection::vec(0..255u8, 1..30),
            v3 in prop::collection::vec(0..255u8, 1..30)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

                let delta3 = LwwDelta::new(
                    b"doc1".to_vec(),
                    "name".to_string(),
                    p3,
                    "v1".to_string(),
                    v3.clone(),
                ).unwrap();

                let permutations: Vec<Vec<&LwwDelta>> = vec![
                    vec![&delta1, &delta2, &delta3],
                    vec![&delta1, &delta3, &delta2],
                    vec![&delta2, &delta1, &delta3],
                    vec![&delta2, &delta3, &delta1],
                    vec![&delta3, &delta1, &delta2],
                    vec![&delta3, &delta2, &delta1],
                ];

                let mut results = Vec::new();
                for perm in permutations {
                    let store = MemoryStore::new();
                    let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
                    let mut txn = store.new_txn(false).await.unwrap();
                    for delta in perm {
                        lww.merge(&mut *txn, &ctx, delta).await.unwrap();
                    }
                    results.push(lww.value(&*txn).await.ok());
                }

                for i in 1..results.len() {
                    assert_eq!(results[0], results[i], "Permutation {} diverged", i);
                }
            });
        }
    }

    #[tokio::test]
    async fn test_lww_priority_ordering_exhaustive() {
        let ctx = Context {
            doc_id: DocId::new_unchecked("doc1"),
            schema_version: "v1".to_string(),
            is_create: false,
        };

        let store = MemoryStore::new();
        let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();

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

        lww.merge(&mut *txn, &ctx, &low).await.unwrap();
        assert_eq!(lww.value(&*txn).await.unwrap(), b"low");

        lww.merge(&mut *txn, &ctx, &high).await.unwrap();
        assert_eq!(lww.value(&*txn).await.unwrap(), b"high");

        lww.merge(&mut *txn, &ctx, &low).await.unwrap();
        assert_eq!(lww.value(&*txn).await.unwrap(), b"high");
    }

    #[tokio::test]
    async fn test_lww_tie_breaking_lexicographic() {
        let ctx = Context {
            doc_id: DocId::new_unchecked("doc1"),
            schema_version: "v1".to_string(),
            is_create: false,
        };

        let store = MemoryStore::new();
        let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();

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

        lww.merge(&mut *txn, &ctx, &alice).await.unwrap();
        assert_eq!(lww.value(&*txn).await.unwrap(), b"Alice");

        lww.merge(&mut *txn, &ctx, &bob).await.unwrap();
        assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");

        lww.merge(&mut *txn, &ctx, &alice).await.unwrap();
        assert_eq!(lww.value(&*txn).await.unwrap(), b"Bob");
    }

    #[tokio::test]
    async fn test_lww_empty_value_handling() {
        let ctx = Context {
            doc_id: DocId::new_unchecked("doc1"),
            schema_version: "v1".to_string(),
            is_create: false,
        };

        let store = MemoryStore::new();
        let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();

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

        let delete =
            LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 20, "v1".to_string()).unwrap();
        lww.merge(&mut *txn, &ctx, &delete).await.unwrap();
        assert!(lww.value(&*txn).await.is_err());
    }
}

mod counter_properties {
    use crdt::{
        traits::{Context, ReplicatedData, ValueReader},
        Counter, CounterDelta, NumericKind,
    };
    use defra_core::types::DocId;
    use proptest::prelude::*;
    use storage::{MemoryStore, Store};

    proptest! {
        #[test]
        fn test_counter_commutativity(
            inc1 in -1000i64..1000i64,
            inc2 in -1000i64..1000i64,
            nonce1 in 0..10000i64,
            nonce2 in 10000..20000i64
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

                let store1 = MemoryStore::new();
                let counter1 = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
                let mut txn1 = store1.new_txn(false).await.unwrap();
                counter1.merge(&mut *txn1, &ctx, &delta1).await.unwrap();
                counter1.merge(&mut *txn1, &ctx, &delta2).await.unwrap();
                let val1 = counter1.value(&*txn1).await.ok();

                let store2 = MemoryStore::new();
                let counter2 = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
                let mut txn2 = store2.new_txn(false).await.unwrap();
                counter2.merge(&mut *txn2, &ctx, &delta2).await.unwrap();
                counter2.merge(&mut *txn2, &ctx, &delta1).await.unwrap();
                let val2 = counter2.value(&*txn2).await.ok();

                assert_eq!(val1, val2);
            });
        }

        // Counter merge is unconditional — idempotency is the blockstore's
        // job via is_merged(cid), not the CRDT's (#847). Property: applying
        // a delta twice through counter.merge() must apply twice. This
        // matches Go's counter.Merge behaviour.
        #[test]
        fn test_counter_double_apply_accumulates(
            increment in -1000i64..1000i64,
            nonce in 0..10000i64
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

                counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
                let val1 = counter.value(&*txn).await.ok().map(|b| i64::from_be_bytes(b.try_into().unwrap()));

                counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
                let val2 = counter.value(&*txn).await.ok().map(|b| i64::from_be_bytes(b.try_into().unwrap()));

                assert_eq!(val1, Some(increment.wrapping_mul(1)));
                assert_eq!(val2, Some(increment.wrapping_mul(2)));
            });
        }

        #[test]
        fn test_counter_overflow_wrapping(base in i64::MAX-1000..i64::MAX, increment in 1i64..1000i64) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
                };

                let store = MemoryStore::new();
                let counter = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
                let mut txn = store.new_txn(false).await.unwrap();

                let delta1 = CounterDelta::new_int64(
                    b"doc1".to_vec(),
                    "count".to_string(),
                    10,
                    1,
                    "v1".to_string(),
                    base,
                ).unwrap();
                counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

                let delta2 = CounterDelta::new_int64(
                    b"doc1".to_vec(),
                    "count".to_string(),
                    20,
                    2,
                    "v1".to_string(),
                    increment,
                ).unwrap();
                let result = counter.merge(&mut *txn, &ctx, &delta2).await;

                assert!(result.is_ok());
                let value_bytes = counter.value(&*txn).await.unwrap();
                let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
                assert_eq!(value, base.wrapping_add(increment));
            });
        }

        #[test]
        fn test_counter_underflow_wrapping(base in i64::MIN..i64::MIN+1000, decrement in 1i64..1000i64) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
                };

                let store = MemoryStore::new();
                let counter = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
                let mut txn = store.new_txn(false).await.unwrap();

                let delta1 = CounterDelta::new_int64(
                    b"doc1".to_vec(),
                    "count".to_string(),
                    10,
                    1,
                    "v1".to_string(),
                    base,
                ).unwrap();
                counter.merge(&mut *txn, &ctx, &delta1).await.unwrap();

                let delta2 = CounterDelta::new_int64(
                    b"doc1".to_vec(),
                    "count".to_string(),
                    20,
                    2,
                    "v1".to_string(),
                    -decrement,
                ).unwrap();
                let result = counter.merge(&mut *txn, &ctx, &delta2).await;

                assert!(result.is_ok());
                let value_bytes = counter.value(&*txn).await.unwrap();
                let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
                assert_eq!(value, base.wrapping_add(-decrement));
            });
        }

        #[test]
        fn test_counter_full_convergence(
            inc1 in -500i64..500i64,
            inc2 in -500i64..500i64,
            inc3 in -500i64..500i64,
            nonce1 in 0..10000i64,
            nonce2 in 10000..20000i64,
            nonce3 in 20000..30000i64
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

                let delta3 = CounterDelta::new_int64(
                    b"doc1".to_vec(),
                    "count".to_string(),
                    30,
                    nonce3,
                    "v1".to_string(),
                    inc3,
                ).unwrap();

                let permutations: Vec<Vec<&CounterDelta>> = vec![
                    vec![&delta1, &delta2, &delta3],
                    vec![&delta1, &delta3, &delta2],
                    vec![&delta2, &delta1, &delta3],
                    vec![&delta2, &delta3, &delta1],
                    vec![&delta3, &delta1, &delta2],
                    vec![&delta3, &delta2, &delta1],
                ];

                let mut results = Vec::new();
                for perm in permutations {
                    let store = MemoryStore::new();
                    let counter = Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, NumericKind::Int64).unwrap();
                    let mut txn = store.new_txn(false).await.unwrap();
                    for delta in perm {
                        counter.merge(&mut *txn, &ctx, delta).await.unwrap();
                    }
                    results.push(counter.value(&*txn).await.ok());
                }

                for i in 1..results.len() {
                    assert_eq!(results[0], results[i], "Permutation {} diverged", i);
                }
            });
        }

        #[test]
        fn test_pcounter_commutativity(
            inc1 in 1i64..1000i64,
            inc2 in 1i64..1000i64,
            nonce1 in 0..10000i64,
            nonce2 in 10000..20000i64
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

                let store1 = MemoryStore::new();
                let counter1 = Counter::new("v1".to_string(), b"doc1", "count".to_string(), false, NumericKind::Int64).unwrap();
                let mut txn1 = store1.new_txn(false).await.unwrap();
                counter1.merge(&mut *txn1, &ctx, &delta1).await.unwrap();
                counter1.merge(&mut *txn1, &ctx, &delta2).await.unwrap();
                let val1 = counter1.value(&*txn1).await.ok();

                let store2 = MemoryStore::new();
                let counter2 = Counter::new("v1".to_string(), b"doc1", "count".to_string(), false, NumericKind::Int64).unwrap();
                let mut txn2 = store2.new_txn(false).await.unwrap();
                counter2.merge(&mut *txn2, &ctx, &delta2).await.unwrap();
                counter2.merge(&mut *txn2, &ctx, &delta1).await.unwrap();
                let val2 = counter2.value(&*txn2).await.ok();

                assert_eq!(val1, val2);
            });
        }

        #[test]
        fn test_pcounter_full_convergence(
            inc1 in 1i64..500i64,
            inc2 in 1i64..500i64,
            inc3 in 1i64..500i64,
            nonce1 in 0..10000i64,
            nonce2 in 10000..20000i64,
            nonce3 in 20000..30000i64
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
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

                let delta3 = CounterDelta::new_int64(
                    b"doc1".to_vec(),
                    "count".to_string(),
                    30,
                    nonce3,
                    "v1".to_string(),
                    inc3,
                ).unwrap();

                let permutations: Vec<Vec<&CounterDelta>> = vec![
                    vec![&delta1, &delta2, &delta3],
                    vec![&delta1, &delta3, &delta2],
                    vec![&delta2, &delta1, &delta3],
                    vec![&delta2, &delta3, &delta1],
                    vec![&delta3, &delta1, &delta2],
                    vec![&delta3, &delta2, &delta1],
                ];

                let mut results = Vec::new();
                for perm in permutations {
                    let store = MemoryStore::new();
                    let counter = Counter::new("v1".to_string(), b"doc1", "count".to_string(), false, NumericKind::Int64).unwrap();
                    let mut txn = store.new_txn(false).await.unwrap();
                    for delta in perm {
                        counter.merge(&mut *txn, &ctx, delta).await.unwrap();
                    }
                    results.push(counter.value(&*txn).await.ok());
                }

                for i in 1..results.len() {
                    assert_eq!(results[0], results[i], "PCounter permutation {} diverged", i);
                }
            });
        }
    }

    #[tokio::test]
    async fn test_counter_multiple_nonces() {
        let ctx = Context {
            doc_id: DocId::new_unchecked("doc1"),
            schema_version: "v1".to_string(),
            is_create: false,
        };

        let store = MemoryStore::new();
        let counter = Counter::new(
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        )
        .unwrap();
        let mut txn = store.new_txn(false).await.unwrap();

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

        let value_bytes = counter.value(&*txn).await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, 50);
    }
}

mod float64_counter_properties {
    use crdt::{
        traits::{Context, ReplicatedData, ValueReader},
        Counter, CounterDelta, NumericKind,
    };
    use defra_core::types::DocId;
    use proptest::prelude::*;
    use storage::{MemoryStore, Store};

    proptest! {
        #[test]
        fn test_float64_counter_commutativity(
            inc1 in -1000.0f64..1000.0f64,
            inc2 in -1000.0f64..1000.0f64,
            nonce1 in 0..10000i64,
            nonce2 in 10000..20000i64
        ) {
            prop_assume!(inc1.is_finite() && inc2.is_finite());

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
                };

                let delta1 = CounterDelta::new_float64(
                    b"doc1".to_vec(),
                    "amount".to_string(),
                    10,
                    nonce1,
                    "v1".to_string(),
                    inc1,
                ).unwrap();

                let delta2 = CounterDelta::new_float64(
                    b"doc1".to_vec(),
                    "amount".to_string(),
                    20,
                    nonce2,
                    "v1".to_string(),
                    inc2,
                ).unwrap();

                let store1 = MemoryStore::new();
                let counter1 = Counter::new("v1".to_string(), b"doc1", "amount".to_string(), true, NumericKind::Float64).unwrap();
                let mut txn1 = store1.new_txn(false).await.unwrap();
                counter1.merge(&mut *txn1, &ctx, &delta1).await.unwrap();
                counter1.merge(&mut *txn1, &ctx, &delta2).await.unwrap();
                let val1 = counter1.value(&*txn1).await.ok();

                let store2 = MemoryStore::new();
                let counter2 = Counter::new("v1".to_string(), b"doc1", "amount".to_string(), true, NumericKind::Float64).unwrap();
                let mut txn2 = store2.new_txn(false).await.unwrap();
                counter2.merge(&mut *txn2, &ctx, &delta2).await.unwrap();
                counter2.merge(&mut *txn2, &ctx, &delta1).await.unwrap();
                let val2 = counter2.value(&*txn2).await.ok();

                assert_eq!(val1, val2);
            });
        }

        // Float64 variant of test_counter_double_apply_accumulates (#847).
        // Counter merge is unconditional; the blockstore dedups by CID.
        #[test]
        fn test_float64_counter_double_apply_accumulates(
            increment in -1000.0f64..1000.0f64,
            nonce in 0..10000i64
        ) {
            prop_assume!(increment.is_finite());

            let rt = tokio::runtime::Runtime::new().unwrap();
            let (val1, val2) = rt.block_on(async {
                let ctx = Context {
                    doc_id: DocId::new_unchecked("doc1"),
                    schema_version: "v1".to_string(),
                    is_create: false,
                };

                let delta = CounterDelta::new_float64(
                    b"doc1".to_vec(),
                    "amount".to_string(),
                    10,
                    nonce,
                    "v1".to_string(),
                    increment,
                ).unwrap();

                let store = MemoryStore::new();
                let counter = Counter::new("v1".to_string(), b"doc1", "amount".to_string(), true, NumericKind::Float64).unwrap();
                let mut txn = store.new_txn(false).await.unwrap();

                counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
                let val1 = counter.value(&*txn).await.ok().map(|b| f64::from_be_bytes(b.try_into().unwrap()));

                counter.merge(&mut *txn, &ctx, &delta).await.unwrap();
                let val2 = counter.value(&*txn).await.ok().map(|b| f64::from_be_bytes(b.try_into().unwrap()));

                (val1, val2)
            });

            prop_assert_eq!(val1, Some(increment));
            prop_assert_eq!(val2, Some(increment + increment));
        }
    }
}
