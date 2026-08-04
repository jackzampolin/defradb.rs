//! Block write pipeline benchmarks.
//!
//! Times [`write_document_blocks`], the Rust counterpart of Go DefraDB's
//! `ProcessBlock -> updateHeads` flow (see the doc comment on that function).
//! Parameterized by field count, field value size, and create-vs-update so the
//! per-field cost curve is comparable to Go's.
//!
//! No `tracing` subscriber is installed, so the debug-only decode round-trip in
//! `write.rs` (guarded by `tracing::enabled!(DEBUG)`) stays disabled and does not
//! pollute the measurement.

use std::collections::HashSet;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datastore::{Namespace, NamespaceView, SharedTxn};
use db_blocks::{write_document_blocks, DocStorageIdentity};
use document::{DocID, Document, NormalValue};
use storage::{backends::MemoryStore, Store};

const SCHEMA_VERSION_ID: &str = "bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq";
const FIELD_COUNTS: [usize; 3] = [2, 8, 32];
const VALUE_SIZES: [usize; 2] = [16, 1024];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

fn make_document(field_count: usize, value_size: usize) -> Document {
    let value = "x".repeat(value_size);
    let mut doc = Document::new();
    for index in 0..field_count {
        doc.set(format!("field_{index}"), NormalValue::String(value.clone()));
    }
    doc
}

fn modified_fields(field_count: usize) -> HashSet<String> {
    (0..field_count).map(|i| format!("field_{i}")).collect()
}

/// A fresh transaction with the blockstore and headstore namespace views the
/// write path needs.
async fn new_stores() -> (NamespaceView, NamespaceView) {
    let txn = MemoryStore::new().new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    (
        NamespaceView::new(shared.clone(), Namespace::Blockstore),
        NamespaceView::new(shared.clone(), Namespace::Headstore),
    )
}

fn bench_write_create(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("block_write_create");

    for field_count in FIELD_COUNTS {
        for value_size in VALUE_SIZES {
            let doc = make_document(field_count, value_size);
            let label = format!("fields{field_count}_value{value_size}");
            group.throughput(Throughput::Elements(field_count as u64));

            group.bench_function(BenchmarkId::from_parameter(&label), |b| {
                b.iter_batched_ref(
                    || rt.block_on(new_stores()),
                    |(blockstore, headstore)| {
                        rt.block_on(async {
                            black_box(
                                write_document_blocks(
                                    blockstore,
                                    headstore,
                                    &doc,
                                    SCHEMA_VERSION_ID,
                                    DocStorageIdentity::new(1, 1),
                                    None,
                                    None,
                                    None,
                                    None,
                                )
                                .await
                                .unwrap(),
                            )
                        })
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

fn bench_write_update(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("block_write_update");

    for field_count in FIELD_COUNTS {
        for value_size in VALUE_SIZES {
            let doc = make_document(field_count, value_size);
            let fields = modified_fields(field_count);
            let label = format!("fields{field_count}_value{value_size}");
            group.throughput(Throughput::Elements(field_count as u64));

            group.bench_function(BenchmarkId::from_parameter(&label), |b| {
                b.iter_batched_ref(
                    || {
                        rt.block_on(async {
                            let (blockstore, headstore) = new_stores().await;
                            let created = write_document_blocks(
                                &blockstore,
                                &headstore,
                                &doc,
                                SCHEMA_VERSION_ID,
                                DocStorageIdentity::new(1, 1),
                                None,
                                None,
                                None,
                                None,
                            )
                            .await
                            .unwrap();

                            // The update path reads the DocID off the document,
                            // which only exists once the genesis composite CID has
                            // been derived by the create above.
                            let mut updated = make_document(field_count, value_size);
                            updated.set_id(DocID::from_string(&created.doc_id).unwrap());
                            (blockstore, headstore, updated)
                        })
                    },
                    |(blockstore, headstore, updated)| {
                        rt.block_on(async {
                            black_box(
                                write_document_blocks(
                                    blockstore,
                                    headstore,
                                    updated,
                                    SCHEMA_VERSION_ID,
                                    DocStorageIdentity::new(1, 1),
                                    Some(&fields),
                                    None,
                                    None,
                                    None,
                                )
                                .await
                                .unwrap(),
                            )
                        })
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_write_create, bench_write_update);
criterion_main!(benches);
