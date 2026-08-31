//! Sender marker registration cost at the durable peerstore boundary.
//!
//! Stage 3 deliberately makes a committed head durable before queue admission.
//! This benchmark keeps that integrity boundary visible while measuring its
//! current O(documents x replicators) transaction cost.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use storage::stores::Peerstore;
use storage::RegolithStore;

const DOCUMENT_COUNTS: [usize; 3] = [1, 10, 100];
const REPLICATOR_COUNTS: [usize; 2] = [1, 4];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn fixture(replicators: usize) -> Peerstore<RegolithStore> {
    let peerstore = Peerstore::new(Arc::new(RegolithStore::in_memory().unwrap()));
    for peer in 0..replicators {
        peerstore
            .create_replicator(&format!("peer-{peer}"), b"replicator")
            .await
            .unwrap();
    }
    peerstore
}

fn bench_retry_markers(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("sender_marker_registration");

    for documents in DOCUMENT_COUNTS {
        for replicators in REPLICATOR_COUNTS {
            let operations = documents * replicators;
            group.throughput(Throughput::Elements(operations as u64));
            group.bench_function(
                BenchmarkId::new(format!("docs{documents}"), format!("peers{replicators}")),
                |b| {
                    b.iter_batched_ref(
                        || rt.block_on(fixture(replicators)),
                        |peerstore| {
                            rt.block_on(async {
                                for document in 0..documents {
                                    for peer in 0..replicators {
                                        peerstore
                                            .observe_push_head(
                                                &format!("peer-{peer}"),
                                                &format!("doc-{document}"),
                                                "collection",
                                            )
                                            .await
                                            .unwrap();
                                    }
                                }
                                black_box(operations);
                            })
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_retry_markers);
criterion_main!(benches);
