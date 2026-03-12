use std::sync::{Arc, OnceLock};

use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use multihash::MultihashGeneric;
use sha2::{Digest, Sha256};
use storage::backends::MemoryStore;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

fn cid_from_data(data: &[u8]) -> Cid {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let hash = MultihashGeneric::<64>::wrap(0x12, &digest).unwrap();
    Cid::new_v1(0x55, hash)
}

fn make_payload(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
}

fn make_blockstore() -> DefraBlockstore<MemoryStore> {
    DefraBlockstore::new(Arc::new(MemoryStore::new()), false)
}

fn bench_blockstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("blockstore");

    for (name, payload) in [
        ("put_256b", make_payload(256, 7)),
        ("put_4kb", make_payload(4096, 11)),
    ] {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter_batched(
                || {
                    let blockstore = make_blockstore();
                    let cid = cid_from_data(&payload);
                    (blockstore, cid, payload.clone())
                },
                |(blockstore, cid, payload)| {
                    runtime().block_on(async {
                        blockstore.put(&cid, &payload).await.unwrap();
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.bench_function(BenchmarkId::from_parameter("get_cache_hit"), |b| {
        let payload = make_payload(1024, 19);
        b.iter_batched(
            || {
                let blockstore = make_blockstore();
                let cid = cid_from_data(&payload);
                runtime().block_on(async {
                    blockstore.put(&cid, &payload).await.unwrap();
                    black_box(blockstore.get(&cid).await.unwrap());
                });
                (blockstore, cid)
            },
            |(blockstore, cid)| {
                runtime().block_on(async {
                    black_box(blockstore.get(&cid).await.unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("get_cache_miss"), |b| {
        let payload = make_payload(1024, 23);
        b.iter_batched(
            || {
                let store = Arc::new(MemoryStore::new());
                let writer = DefraBlockstore::new(store.clone(), false);
                let cid = cid_from_data(&payload);
                runtime().block_on(async {
                    writer.put(&cid, &payload).await.unwrap();
                });
                let reader = DefraBlockstore::new(store, false);
                (reader, cid)
            },
            |(blockstore, cid)| {
                runtime().block_on(async {
                    black_box(blockstore.get(&cid).await.unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("has_check"), |b| {
        let payload = make_payload(512, 29);
        b.iter_batched(
            || {
                let store = Arc::new(MemoryStore::new());
                let writer = DefraBlockstore::new(store.clone(), false);
                let cid = cid_from_data(&payload);
                runtime().block_on(async {
                    writer.put(&cid, &payload).await.unwrap();
                });
                let checker = DefraBlockstore::new(store, false);
                (checker, cid)
            },
            |(blockstore, cid)| {
                runtime().block_on(async {
                    black_box(blockstore.has(&cid).await.unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::from_parameter("put_get_cycle_100"), |b| {
        let blocks: Vec<(Cid, Vec<u8>)> = (0..100)
            .map(|index| {
                let payload = make_payload(256, index as u8);
                (cid_from_data(&payload), payload)
            })
            .collect();

        b.iter_batched(
            || (make_blockstore(), blocks.clone()),
            |(blockstore, blocks)| {
                runtime().block_on(async {
                    for (cid, payload) in &blocks {
                        blockstore.put(cid, payload).await.unwrap();
                        black_box(blockstore.get(cid).await.unwrap());
                    }
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_blockstore);
criterion_main!(benches);
