use std::future::Future;
use std::pin::pin;
use std::sync::OnceLock;
use std::task::{Context as TaskContext, Poll, Waker};

use crdt::composite::FieldDelta;
use crdt::traits::{Context, ReplicatedData, ValueReader};
use crdt::{
    decode_priority, encode_priority, CompositeDAG, CompositeDelta, Counter, CounterDelta, Lww,
    LwwDelta, NumericKind,
};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use defra_core::types::DocId;
use std::hint::black_box;
use storage::{MemoryStore, Store, Txn};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

/// Single-poll executor for futures that never yield.
///
/// Every `MemoryStore` operation resolves without suspending, so one `poll`
/// always completes. Using this instead of `tokio::runtime::Runtime::block_on`
/// keeps the runtime's park/unpark and task bookkeeping out of the timed body,
/// so the "clean" benches below measure CRDT merge work rather than executor work.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future yielded: this executor only drives non-yielding futures"),
    }
}

fn make_context() -> Context {
    Context {
        doc_id: DocId::new_unchecked("doc1"),
        schema_version: "v1".to_string(),
        is_create: false,
    }
}

fn new_txn() -> Box<dyn Txn> {
    block_on(async { MemoryStore::new().new_txn(false).await.unwrap() })
}

fn lww_delta(priority: u64, data: &[u8]) -> LwwDelta {
    LwwDelta::new(
        b"doc1".to_vec(),
        "name".to_string(),
        priority,
        "v1".to_string(),
        data.to_vec(),
    )
    .unwrap()
}

fn make_merge_setup(
    initial: LwwDelta,
    incoming: LwwDelta,
) -> (Box<dyn Txn>, Lww, Context, LwwDelta) {
    runtime().block_on(async move {
        let store = MemoryStore::new();
        let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
        let ctx = make_context();
        let mut txn = store.new_txn(false).await.unwrap();
        lww.merge(&mut *txn, &ctx, &initial).await.unwrap();
        (txn, lww, ctx, incoming)
    })
}

/// Same fixture as [`make_merge_setup`], but built on the single-poll executor so
/// the setup itself does not depend on a tokio runtime.
fn make_clean_lww_setup(initial: LwwDelta) -> (Box<dyn Txn>, Lww, Context) {
    block_on(async move {
        let lww = Lww::new("v1".to_string(), b"doc1", "name".to_string()).unwrap();
        let ctx = make_context();
        let mut txn = MemoryStore::new().new_txn(false).await.unwrap();
        lww.merge(&mut *txn, &ctx, &initial).await.unwrap();
        (txn, lww, ctx)
    })
}

fn make_counter_setup(
    kind: NumericKind,
    initial: &CounterDelta,
) -> (Box<dyn Txn>, Counter, Context) {
    block_on(async move {
        let counter =
            Counter::new("v1".to_string(), b"doc1", "count".to_string(), true, kind).unwrap();
        let ctx = make_context();
        let mut txn = MemoryStore::new().new_txn(false).await.unwrap();
        counter.merge(&mut *txn, &ctx, initial).await.unwrap();
        (txn, counter, ctx)
    })
}

fn composite_dag(field_count: usize) -> CompositeDAG {
    let mut dag = CompositeDAG::new(DocId::new_unchecked("doc1"), "v1");
    for index in 0..field_count {
        dag.register_lww_field(format!("field_{index}"));
    }
    dag
}

fn composite_delta(field_count: usize, priority: u64, value: &[u8]) -> CompositeDelta {
    let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), priority).unwrap();
    for index in 0..field_count {
        delta
            .add_field_delta(
                format!("field_{index}"),
                FieldDelta::Lww {
                    priority,
                    data: value.to_vec(),
                },
            )
            .unwrap();
    }
    delta
}

fn make_composite_setup(field_count: usize, initial: &CompositeDelta) -> (Box<dyn Txn>, Context) {
    block_on(async move {
        let dag = composite_dag(field_count);
        let ctx = make_context();
        let mut txn = MemoryStore::new().new_txn(false).await.unwrap();
        dag.merge(&mut *txn, &ctx, initial).await.unwrap();
        (txn, ctx)
    })
}

const FIELD_COUNTS: [usize; 3] = [1, 4, 16];

/// The original merge benches. Their timed body includes
/// `Runtime::block_on`, a `MemoryStore` transaction get/set, a value read and
/// `txn.discard()`, so they measure the whole stack rather than the merge
/// algorithm. Retained as-is: the gap against the `*_clean` variants, which
/// merge the same deltas from the same fixtures, is what quantifies that
/// contamination.
fn bench_merge_contaminated(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt");

    let clear_winner_initial = lww_delta(10, b"low");
    let clear_winner_incoming = lww_delta(20, b"high");
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

    let tiebreak_initial = lww_delta(10, b"Alice");
    let tiebreak_incoming = lww_delta(10, b"Bob");
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

/// Merge benches whose timed body is only the `merge` call. Fixture construction
/// and teardown live in `iter_batched_ref` setup / drop - the routine borrows the
/// fixture rather than taking it by value, so nothing is dropped inside the timed
/// region - and the executor is the single-poll [`block_on`] above.
fn bench_merge_clean(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt_clean");

    let lww_low = lww_delta(10, b"low");
    let lww_high = lww_delta(20, b"high");
    group.bench_function(BenchmarkId::from_parameter("lww_merge_clean"), |b| {
        b.iter_batched_ref(
            || make_clean_lww_setup(lww_low.clone()),
            |(txn, lww, ctx)| {
                black_box(block_on(async {
                    lww.merge(&mut **txn, ctx, &lww_high).await.unwrap()
                }))
            },
            BatchSize::SmallInput,
        );
    });

    let tie_low = lww_delta(10, b"Alice");
    let tie_high = lww_delta(10, b"Bob");
    group.bench_function(
        BenchmarkId::from_parameter("lww_merge_clean_tiebreak"),
        |b| {
            b.iter_batched_ref(
                || make_clean_lww_setup(tie_low.clone()),
                |(txn, lww, ctx)| {
                    black_box(block_on(async {
                        lww.merge(&mut **txn, ctx, &tie_high).await.unwrap()
                    }))
                },
                BatchSize::SmallInput,
            );
        },
    );

    let reject = lww_delta(5, b"stale");
    group.bench_function(
        BenchmarkId::from_parameter("lww_merge_clean_rejected"),
        |b| {
            b.iter_batched_ref(
                || make_clean_lww_setup(lww_low.clone()),
                |(txn, lww, ctx)| {
                    black_box(block_on(async {
                        lww.merge(&mut **txn, ctx, &reject).await.unwrap()
                    }))
                },
                BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

fn bench_counter_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt_clean");

    let int_initial = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        1,
        1,
        "v1".to_string(),
        5,
    )
    .unwrap();
    let int_incoming = CounterDelta::new_int64(
        b"doc1".to_vec(),
        "count".to_string(),
        2,
        2,
        "v1".to_string(),
        7,
    )
    .unwrap();
    group.bench_function(BenchmarkId::from_parameter("counter_merge_int64"), |b| {
        b.iter_batched_ref(
            || make_counter_setup(NumericKind::Int64, &int_initial),
            |(txn, counter, ctx)| {
                black_box(block_on(async {
                    counter.merge(&mut **txn, ctx, &int_incoming).await.unwrap()
                }))
            },
            BatchSize::SmallInput,
        );
    });

    let f32_initial = CounterDelta::new_float32(
        b"doc1".to_vec(),
        "count".to_string(),
        1,
        1,
        "v1".to_string(),
        1.5,
    )
    .unwrap();
    let f32_incoming = CounterDelta::new_float32(
        b"doc1".to_vec(),
        "count".to_string(),
        2,
        2,
        "v1".to_string(),
        2.25,
    )
    .unwrap();
    group.bench_function(BenchmarkId::from_parameter("counter_merge_float32"), |b| {
        b.iter_batched_ref(
            || make_counter_setup(NumericKind::Float32, &f32_initial),
            |(txn, counter, ctx)| {
                black_box(block_on(async {
                    counter.merge(&mut **txn, ctx, &f32_incoming).await.unwrap()
                }))
            },
            BatchSize::SmallInput,
        );
    });

    let f64_initial = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        1,
        1,
        "v1".to_string(),
        1.5,
    )
    .unwrap();
    let f64_incoming = CounterDelta::new_float64(
        b"doc1".to_vec(),
        "count".to_string(),
        2,
        2,
        "v1".to_string(),
        2.25,
    )
    .unwrap();
    group.bench_function(BenchmarkId::from_parameter("counter_merge_float64"), |b| {
        b.iter_batched_ref(
            || make_counter_setup(NumericKind::Float64, &f64_initial),
            |(txn, counter, ctx)| {
                black_box(block_on(async {
                    counter.merge(&mut **txn, ctx, &f64_incoming).await.unwrap()
                }))
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_composite_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt_clean");

    for field_count in FIELD_COUNTS {
        let initial = composite_delta(field_count, 10, b"aaa");
        let clear_winner = composite_delta(field_count, 20, b"zzz");
        let tiebreak = composite_delta(field_count, 10, b"zzz");

        group.bench_function(
            BenchmarkId::new("composite_merge_clear_winner", field_count),
            |b| {
                b.iter_batched_ref(
                    || {
                        let (txn, ctx) = make_composite_setup(field_count, &initial);
                        (txn, composite_dag(field_count), ctx)
                    },
                    |(txn, dag, ctx)| {
                        black_box(block_on(async {
                            dag.merge(&mut **txn, ctx, &clear_winner).await.unwrap()
                        }))
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_function(
            BenchmarkId::new("composite_merge_tiebreak", field_count),
            |b| {
                b.iter_batched_ref(
                    || {
                        let (txn, ctx) = make_composite_setup(field_count, &initial);
                        (txn, composite_dag(field_count), ctx)
                    },
                    |(txn, dag, ctx)| {
                        black_box(block_on(async {
                            dag.merge(&mut **txn, ctx, &tiebreak).await.unwrap()
                        }))
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Standalone costs of the machinery that surrounds a merge: the two executors,
/// the transaction lifecycle and a transaction get/set.
///
/// These are independent measurements, not terms to subtract from
/// `crdt/lww_merge_*`. `txn_new_and_discard` covers `MemoryStore::new()` and
/// transaction creation, which `bench_merge_contaminated` performs in its
/// untimed setup, and the contaminated body also times a `lww.value()` read
/// that has no baseline here. They size each component individually; they do
/// not sum to the gap between `crdt/*` and `crdt_clean/*`.
fn bench_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt_overhead");

    group.bench_function(BenchmarkId::from_parameter("tokio_block_on_ready"), |b| {
        b.iter(|| runtime().block_on(async { black_box(1u64) }));
    });

    group.bench_function(BenchmarkId::from_parameter("noop_block_on_ready"), |b| {
        b.iter(|| block_on(async { black_box(1u64) }));
    });

    group.bench_function(BenchmarkId::from_parameter("txn_new_and_discard"), |b| {
        b.iter(|| {
            let txn = new_txn();
            txn.discard();
        });
    });

    group.bench_function(BenchmarkId::from_parameter("txn_get_set"), |b| {
        b.iter_batched(
            new_txn,
            |mut txn| {
                block_on(async {
                    black_box(txn.get(b"bench-key").await.unwrap());
                    txn.set(b"bench-key", b"bench-value").await.unwrap();
                });
                txn
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_merge_contaminated,
    bench_merge_clean,
    bench_counter_merge,
    bench_composite_merge,
    bench_overhead
);
criterion_main!(benches);
