use std::sync::OnceLock;

use crdt::traits::{Context, ReplicatedData, ValueReader};
use crdt::{decode_priority, encode_priority, Lww, LwwDelta};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use defra_core::types::DocId;
use storage::{MemoryStore, Store};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

fn make_context() -> Context {
    Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    }
}

fn make_merge_setup(
    initial: LwwDelta,
    incoming: LwwDelta,
) -> (Box<dyn storage::Txn>, Lww, Context, LwwDelta) {
    runtime().block_on(async move {
        let store = MemoryStore::new();
        let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
        let ctx = make_context();
        let mut txn = store.new_txn(false).await.unwrap();
        lww.merge(&mut *txn, &ctx, &initial).await.unwrap();
        (txn, lww, ctx, incoming)
    })
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt");

    let clear_winner_initial = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"low".to_vec(),
    )
    .unwrap();
    let clear_winner_incoming = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        20,
        "v1".to_string(),
        b"high".to_vec(),
    )
    .unwrap();
    group.bench_function(BenchmarkId::from_parameter("lww_merge_clear_winner"), |b| {
        b.iter_batched(
            || make_merge_setup(clear_winner_initial.clone(), clear_winner_incoming.clone()),
            |(txn, lww, ctx, incoming)| {
                runtime().block_on(async {
                    let mut txn = txn;
                    black_box(lww.merge(&mut *txn, &ctx, &incoming).await.unwrap());
                    black_box(lww.value(&*txn).await.unwrap());
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    let tiebreak_initial = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Alice".to_vec(),
    )
    .unwrap();
    let tiebreak_incoming = LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        10,
        "v1".to_string(),
        b"Bob".to_vec(),
    )
    .unwrap();
    group.bench_function(BenchmarkId::from_parameter("lww_merge_tiebreak"), |b| {
        b.iter_batched(
            || make_merge_setup(tiebreak_initial.clone(), tiebreak_incoming.clone()),
            |(txn, lww, ctx, incoming)| {
                runtime().block_on(async {
                    let mut txn = txn;
                    black_box(lww.merge(&mut *txn, &ctx, &incoming).await.unwrap());
                    black_box(lww.value(&*txn).await.unwrap());
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("priority_encode_small"), |b| {
        b.iter(|| black_box(encode_priority(black_box(127))));
    });

    group.bench_function(BenchmarkId::from_parameter("priority_encode_large"), |b| {
        b.iter(|| black_box(encode_priority(black_box(u64::MAX - 1))));
    });

    group.bench_function(BenchmarkId::from_parameter("priority_roundtrip"), |b| {
        b.iter(|| {
            let encoded = encode_priority(black_box(1_700_000_000_000_000_000u64));
            black_box(decode_priority(black_box(&encoded)).unwrap());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
