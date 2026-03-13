use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use storage::backends::RedbStore;
use storage::corekv::{IterOptions, Reader, Store, Writer};
use tempfile::TempDir;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

struct BenchStore {
    _temp_dir: TempDir,
    store: Arc<RedbStore>,
    tree_key: Vec<u8>,
    pending_key: Vec<u8>,
    pending_value: Vec<u8>,
    scan_prefix: Vec<u8>,
    scan_large_prefix: Vec<u8>,
    keys_only_prefix: Vec<u8>,
    random_keys: Vec<Vec<u8>>,
    set_counter: AtomicUsize,
}

impl BenchStore {
    fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let store = runtime().block_on(async {
            Arc::new(RedbStore::open(temp_dir.path().join("bench.redb")).unwrap())
        });

        let tree_key = b"tree:hot".to_vec();
        let pending_key = b"pending:hot".to_vec();
        let pending_value = b"pending-value".to_vec();
        let scan_prefix = b"scan:".to_vec();
        let scan_large_prefix = b"scan_large:".to_vec();
        let keys_only_prefix = b"scan_keys:".to_vec();
        let random_keys: Vec<Vec<u8>> = (0..1000)
            .map(|index| format!("rand:{:04}", (index * 37) % 1000).into_bytes())
            .collect();

        runtime().block_on(async {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(&tree_key, b"tree-value").await.unwrap();

            for index in 0..100 {
                txn.set(
                    format!("scan:{index:03}").as_bytes(),
                    format!("value-{index:03}").as_bytes(),
                )
                .await
                .unwrap();
            }

            for index in 0..1000 {
                txn.set(
                    format!("scan_large:{index:04}").as_bytes(),
                    format!("value-large-{index:04}").as_bytes(),
                )
                .await
                .unwrap();
            }

            for index in 0..1000 {
                let value = vec![b'x'; 1024];
                txn.set(format!("scan_keys:{index:04}").as_bytes(), &value)
                    .await
                    .unwrap();
            }

            for key in &random_keys {
                txn.set(key, b"random-value").await.unwrap();
            }

            for index in 0..256 {
                txn.set(
                    format!("set:{index:03}").as_bytes(),
                    format!("seed-{index:03}").as_bytes(),
                )
                .await
                .unwrap();
            }

            txn.commit().await.unwrap();
        });

        Self {
            _temp_dir: temp_dir,
            store,
            tree_key,
            pending_key,
            pending_value,
            scan_prefix,
            scan_large_prefix,
            keys_only_prefix,
            random_keys,
            set_counter: AtomicUsize::new(0),
        }
    }

    fn next_set_payload(&self) -> (Vec<u8>, Vec<u8>) {
        let slot = self.set_counter.fetch_add(1, Ordering::Relaxed) % 256;
        (
            format!("set:{slot:03}").into_bytes(),
            format!("value-{slot:03}").into_bytes(),
        )
    }
}

fn fixture() -> &'static BenchStore {
    static FIXTURE: OnceLock<BenchStore> = OnceLock::new();
    FIXTURE.get_or_init(BenchStore::new)
}

fn bench_storage(c: &mut Criterion) {
    let fixture = fixture();
    let mut group = c.benchmark_group("storage");

    group.bench_function(BenchmarkId::from_parameter("get_from_tree"), |b| {
        b.iter_batched(
            || {
                runtime().block_on(async {
                    (
                        fixture.store.new_txn(true).await.unwrap(),
                        fixture.tree_key.clone(),
                    )
                })
            },
            |(txn, key)| {
                runtime().block_on(async {
                    let value = txn.get(&key).await.unwrap();
                    black_box(value);
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("get_from_pending"), |b| {
        b.iter_batched(
            || {
                runtime().block_on(async {
                    let mut txn = fixture.store.new_txn(false).await.unwrap();
                    txn.set(&fixture.pending_key, &fixture.pending_value)
                        .await
                        .unwrap();
                    (txn, fixture.pending_key.clone())
                })
            },
            |(txn, key)| {
                runtime().block_on(async {
                    let value = txn.get(&key).await.unwrap();
                    black_box(value);
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("set_single"), |b| {
        b.iter_batched(
            || fixture.next_set_payload(),
            |(key, value)| {
                runtime().block_on(async {
                    let mut txn = fixture.store.new_txn(false).await.unwrap();
                    txn.set(&key, &value).await.unwrap();
                    txn.commit().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("sequential_scan_100"), |b| {
        b.iter_batched(
            || runtime().block_on(async { fixture.store.new_txn(true).await.unwrap() }),
            |txn| {
                runtime().block_on(async {
                    let mut iter = txn
                        .iterator(IterOptions::new().with_prefix(fixture.scan_prefix.clone()))
                        .await
                        .unwrap();
                    let mut scanned = 0usize;
                    while let Some(entry) = iter.next().await.unwrap() {
                        black_box(entry);
                        scanned += 1;
                        if scanned == 100 {
                            break;
                        }
                    }
                    black_box(scanned);
                    iter.close().await.unwrap();
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("sequential_scan_1000"), |b| {
        b.iter_batched(
            || runtime().block_on(async { fixture.store.new_txn(true).await.unwrap() }),
            |txn| {
                runtime().block_on(async {
                    let mut iter = txn
                        .iterator(IterOptions::new().with_prefix(fixture.scan_large_prefix.clone()))
                        .await
                        .unwrap();
                    let mut scanned = 0usize;
                    while let Some(entry) = iter.next().await.unwrap() {
                        black_box(entry);
                        scanned += 1;
                    }
                    black_box(scanned);
                    iter.close().await.unwrap();
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("keys_only_scan_1000"), |b| {
        b.iter_batched(
            || runtime().block_on(async { fixture.store.new_txn(true).await.unwrap() }),
            |txn| {
                runtime().block_on(async {
                    let mut iter = txn
                        .iterator(
                            IterOptions::new()
                                .with_prefix(fixture.keys_only_prefix.clone())
                                .with_keys_only(true),
                        )
                        .await
                        .unwrap();
                    let mut scanned = 0usize;
                    while let Some(entry) = iter.next().await.unwrap() {
                        black_box(entry);
                        scanned += 1;
                    }
                    black_box(scanned);
                    iter.close().await.unwrap();
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("random_get_1000"), |b| {
        b.iter_batched(
            || runtime().block_on(async { fixture.store.new_txn(true).await.unwrap() }),
            |txn| {
                runtime().block_on(async {
                    let mut hits = 0usize;
                    for key in &fixture.random_keys {
                        if txn.get(key).await.unwrap().is_some() {
                            hits += 1;
                        }
                    }
                    black_box(hits);
                    txn.discard();
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_storage);
criterion_main!(benches);
