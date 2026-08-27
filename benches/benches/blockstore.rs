use std::num::NonZeroUsize;
use std::sync::Arc;

use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use lru::LruCache;
use multihash::Multihash;
use sha2::{Digest, Sha256};
use std::hint::black_box;
use storage::backends::MemoryStore;

mod common;

fn cid_from_data(data: &[u8]) -> Cid {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let hash = Multihash::<64>::wrap(0x12, &digest).unwrap();
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

fn bench_raw_lru(c: &mut Criterion) {
    let mut cache: LruCache<Cid, Vec<u8>> = LruCache::new(NonZeroUsize::new(1_000).unwrap());
    let payload = make_payload(256, 37);
    let cid = cid_from_data(&payload);
    cache.put(cid, payload);

    c.bench_function("raw_lru_get_clone", |b| {
        b.iter(|| {
            let data = cache.get(&cid).unwrap();
            black_box(data.clone())
        })
    });
}

fn bench_raw_lru_arc(c: &mut Criterion) {
    let mut cache: LruCache<Cid, Arc<Vec<u8>>> = LruCache::new(NonZeroUsize::new(1_000).unwrap());
    let payload = make_payload(256, 41);
    let cid = cid_from_data(&payload);
    cache.put(cid, Arc::new(payload));

    c.bench_function("raw_lru_get_arc", |b| {
        b.iter(|| {
            let data = cache.get(&cid).unwrap();
            black_box(Arc::clone(data))
        })
    });
}

fn bench_txn_creation(c: &mut Criterion) {
    let blockstore = make_blockstore();
    let payload = make_payload(1024, 43);
    let cid = cid_from_data(&payload);

    common::shared_runtime().block_on(async {
        blockstore.put(&cid, &payload).await.unwrap();
    });

    c.bench_function("txn_creation_only", |b| {
        b.to_async(common::shared_runtime()).iter(|| async {
            let txn = blockstore.new_store_txn(true).await.unwrap();
            black_box(txn);
        })
    });
}

fn bench_blockstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("blockstore");

    for (name, payload) in [
        ("put_256b", make_payload(256, 7)),
        ("put_4kb", make_payload(4096, 11)),
    ] {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.to_async(common::shared_runtime()).iter_batched(
                || {
                    let blockstore = make_blockstore();
                    let cid = cid_from_data(&payload);
                    (blockstore, cid, payload.clone())
                },
                |(blockstore, cid, payload)| async move {
                    blockstore.put(&cid, &payload).await.unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }

    let hit_payload = make_payload(1024, 19);
    let hit_blockstore = make_blockstore();
    let hit_cid = cid_from_data(&hit_payload);
    common::shared_runtime().block_on(async {
        hit_blockstore.put(&hit_cid, &hit_payload).await.unwrap();
        black_box(hit_blockstore.get(&hit_cid).await.unwrap());
    });

    group.bench_function(BenchmarkId::from_parameter("get_cache_hit"), |b| {
        b.to_async(common::shared_runtime()).iter(|| async {
            black_box(hit_blockstore.get(&hit_cid).await.unwrap());
        });
    });

    let miss_payload = make_payload(1024, 23);
    let miss_store = Arc::new(MemoryStore::new());
    let miss_writer = DefraBlockstore::new(miss_store.clone(), false);
    let miss_cid = cid_from_data(&miss_payload);
    common::shared_runtime().block_on(async {
        miss_writer.put(&miss_cid, &miss_payload).await.unwrap();
    });

    group.bench_function(BenchmarkId::from_parameter("get_cache_miss"), |b| {
        let miss_store = miss_store.clone();
        b.to_async(common::shared_runtime()).iter_batched(
            || DefraBlockstore::new(miss_store.clone(), false),
            |blockstore| async move {
                black_box(blockstore.get(&miss_cid).await.unwrap());
            },
            BatchSize::SmallInput,
        );
    });

    let has_payload = make_payload(512, 29);
    let has_store = Arc::new(MemoryStore::new());
    let has_writer = DefraBlockstore::new(has_store.clone(), false);
    let has_cid = cid_from_data(&has_payload);
    common::shared_runtime().block_on(async {
        has_writer.put(&has_cid, &has_payload).await.unwrap();
    });

    group.bench_function(BenchmarkId::from_parameter("has_check"), |b| {
        let has_store = has_store.clone();
        b.to_async(common::shared_runtime()).iter_batched(
            || DefraBlockstore::new(has_store.clone(), false),
            |blockstore| async move {
                black_box(blockstore.has(&has_cid).await.unwrap());
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

        b.to_async(common::shared_runtime()).iter_batched(
            || (make_blockstore(), blocks.clone()),
            |(blockstore, blocks)| async move {
                for (cid, payload) in &blocks {
                    blockstore.put(cid, payload).await.unwrap();
                    black_box(blockstore.get(cid).await.unwrap());
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_raw_lru,
    bench_raw_lru_arc,
    bench_txn_creation,
    bench_blockstore
);
criterion_main!(benches);
