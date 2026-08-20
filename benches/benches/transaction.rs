use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use storage::corekv::{IterOptions, Reader, Store, Writer};
use tempfile::TempDir;

mod common;

/// Fixture holding a seeded store plus the keys the benchmarks read against.
///
/// Generic over the backend so the same workload runs identically against
/// every enabled store (`redb`, `lark`, `rocksdb`), giving a side-by-side
/// comparison of the DefraDB transaction wrapper on each. Lark and RocksDB
/// are constructed through their `*_`-prefixed environment options, so a
/// configured backend is measured rather than just raw defaults; redb has no
/// environment-configuration path and is built with its defaults (#1009).
struct BenchStore<S: Store> {
    store: Arc<S>,
    tree_key: Vec<u8>,
    pending_key: Vec<u8>,
    pending_value: Vec<u8>,
    scan_prefix: Vec<u8>,
    scan_large_prefix: Vec<u8>,
    keys_only_prefix: Vec<u8>,
    random_keys: Vec<Vec<u8>>,
    set_counter: AtomicUsize,
}

impl<S: Store> BenchStore<S> {
    fn new(store: Arc<S>) -> Self {
        let tree_key = b"tree:hot".to_vec();
        let pending_key = b"pending:hot".to_vec();
        let pending_value = b"pending-value".to_vec();
        let scan_prefix = b"scan:".to_vec();
        let scan_large_prefix = b"scan_large:".to_vec();
        let keys_only_prefix = b"scan_keys:".to_vec();
        let random_keys: Vec<Vec<u8>> = (0..1000)
            .map(|index| format!("rand:{:04}", (index * 37) % 1000).into_bytes())
            .collect();

        common::shared_runtime().block_on(async {
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

/// Run the full workload against one backend, grouped as `storage/<backend>`.
fn bench_backend<S: Store>(c: &mut Criterion, backend: &str, fixture: &BenchStore<S>) {
    let mut group = c.benchmark_group(format!("storage/{backend}"));

    group.bench_function(BenchmarkId::from_parameter("get_from_tree"), |b| {
        b.iter_batched(
            || {
                common::shared_runtime().block_on(async {
                    (
                        fixture.store.new_txn(true).await.unwrap(),
                        fixture.tree_key.clone(),
                    )
                })
            },
            |(txn, key)| {
                common::shared_runtime().block_on(async {
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
                common::shared_runtime().block_on(async {
                    let mut txn = fixture.store.new_txn(false).await.unwrap();
                    txn.set(&fixture.pending_key, &fixture.pending_value)
                        .await
                        .unwrap();
                    (txn, fixture.pending_key.clone())
                })
            },
            |(txn, key)| {
                common::shared_runtime().block_on(async {
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
                common::shared_runtime().block_on(async {
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
            || {
                common::shared_runtime()
                    .block_on(async { fixture.store.new_txn(true).await.unwrap() })
            },
            |txn| {
                common::shared_runtime().block_on(async {
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
            || {
                common::shared_runtime()
                    .block_on(async { fixture.store.new_txn(true).await.unwrap() })
            },
            |txn| {
                common::shared_runtime().block_on(async {
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
            || {
                common::shared_runtime()
                    .block_on(async { fixture.store.new_txn(true).await.unwrap() })
            },
            |txn| {
                common::shared_runtime().block_on(async {
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
            || {
                common::shared_runtime()
                    .block_on(async { fixture.store.new_txn(true).await.unwrap() })
            },
            |txn| {
                common::shared_runtime().block_on(async {
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

fn bench_storage(c: &mut Criterion) {
    // Each backend is gated on its feature; run
    // `cargo bench -p storage --features "redb,lark,rocksdb"` for a full
    // side-by-side. The temp dir for each store outlives its `bench_backend`
    // call because that call measures synchronously before the block ends.
    #[cfg(feature = "redb")]
    {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(storage::RedbStore::open(dir.path().join("bench.redb")).unwrap());
        bench_backend(c, "redb", &BenchStore::new(store));
    }

    #[cfg(feature = "lark")]
    {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            storage::LarkStore::open_with_options(
                dir.path(),
                storage::LarkStoreOptions::from_env(),
            )
            .unwrap(),
        );
        bench_backend(c, "lark", &BenchStore::new(store));
    }

    #[cfg(feature = "rocksdb")]
    {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            storage::RocksDbStore::open_with_options(
                dir.path(),
                storage::RocksDbStoreOptions::from_env(),
            )
            .unwrap(),
        );
        bench_backend(c, "rocksdb", &BenchStore::new(store));
    }
}

criterion_group!(benches, bench_storage);
criterion_main!(benches);
