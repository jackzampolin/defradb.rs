//! Document reads through the real fetcher.
//!
//! ```text
//! cargo bench -p benches --bench document_read
//! ```
//!
//! [`document_write`](../document_write.rs) covers the other direction. This
//! covers the four shapes every read in the system resolves to: fetch by id,
//! fetch the whole collection, stream it rather than materialize it, and
//! resolve a foreign key by field value. None of them was measured, and the
//! difference between the second and the third is the difference between
//! holding one document and holding all of them.
//!
//! Swept by corpus size, because the point of the streaming row is that it
//! should not grow the way the materializing one does.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use db::DB;
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use query::DocFetcher;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::RegolithStore;

mod common;

const COLLECTION: &str = "User";
const CORPUS: [usize; 3] = [100, 1_000, 5_000];

fn collection_version() -> CollectionVersion {
    CollectionVersion::new(
        COLLECTION,
        "bafkdocread",
        "docread",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "city", FieldKind::string()),
        ],
    )
}

type Fetcher = db::AutoCommitFetcher<RegolithStore>;

/// A database holding `count` documents, the fetcher over it, and the ids it
/// wrote, so the by-id rows read documents that are actually there.
async fn fixture(count: usize) -> (Fetcher, Vec<String>) {
    let store = Arc::new(RegolithStore::in_memory().expect("an in-memory store"));
    let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
    db.create_collection(collection_version())
        .await
        .expect("the collection to register");

    let mutator = db::write::autocommit::AutoCommitMutator::new(db.clone());
    let mut ids = Vec::with_capacity(count);
    for seq in 0..count {
        let mut doc = Document::new();
        doc.set("name", NormalValue::String(format!("person-{seq}")));
        doc.set("city", NormalValue::String(format!("city-{}", seq % 10)));
        let created = mutator
            .create(COLLECTION, doc)
            .await
            .expect("the seed create to succeed");
        ids.push(created.doc_id.to_string());
    }
    (db::AutoCommitFetcher::new(db), ids)
}

fn by_id(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("document_read_by_id");
    for count in CORPUS {
        let (fetcher, ids) = rt.block_on(fixture(count));
        // One id from the middle: the first and the last can both be
        // accidentally cheap depending on how the store lays keys out.
        let one = vec![ids[ids.len() / 2].clone()];
        // Set for every benchmark, not just the batched one: criterion keeps a
        // group's throughput until it is replaced, so leaving it unset here
        // would report the single read at the previous iteration's batch size.
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("single", count), &one, |b, ids| {
            b.iter(|| {
                black_box(
                    rt.block_on(fetcher.get_by_ids(COLLECTION, black_box(ids)))
                        .expect("the read to succeed"),
                )
            })
        });
        // A batched read is what a relation resolves to, so its per-document
        // cost against the single-id row is the number that decides whether
        // batching a join is worth it.
        let many: Vec<String> = ids.iter().take(50).cloned().collect();
        group.throughput(Throughput::Elements(many.len() as u64));
        group.bench_with_input(BenchmarkId::new("batch_50", count), &many, |b, ids| {
            b.iter(|| {
                black_box(
                    rt.block_on(fetcher.get_by_ids(COLLECTION, black_box(ids)))
                        .expect("the read to succeed"),
                )
            })
        });
    }
    group.finish();
}

/// Materializing the collection against streaming it. Both walk the same
/// bytes; what differs is how much a caller has to hold to do it.
fn collection_scan(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("document_read_scan");
    group.sample_size(20);
    for count in CORPUS {
        let (fetcher, _) = rt.block_on(fixture(count));
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("get_all", count), &count, |b, _| {
            b.iter(|| {
                let docs = rt
                    .block_on(fetcher.get_all(COLLECTION))
                    .expect("the scan to succeed");
                assert_eq!(docs.len(), count, "the scan must see every document");
                black_box(docs)
            })
        });
        group.bench_with_input(BenchmarkId::new("stream", count), &count, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let mut stream = fetcher
                        .stream_all_with_deleted(COLLECTION, false)
                        .await
                        .expect("the stream to open");
                    let mut seen = 0usize;
                    while let Some(doc) = stream.next().await.expect("the stream to advance") {
                        black_box(&doc);
                        seen += 1;
                    }
                    assert_eq!(seen, count, "the stream must see every document");
                })
            })
        });
    }
    group.finish();
}

/// The foreign-key path: every relation traversal resolves through this.
fn by_field_value(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("document_read_by_field");
    group.sample_size(20);
    for count in CORPUS {
        let (fetcher, _) = rt.block_on(fixture(count));
        // A tenth of the corpus matches, so the row measures selecting rather
        // than either scanning everything or finding nothing.
        group.throughput(Throughput::Elements(count as u64 / 10));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                black_box(
                    rt.block_on(fetcher.get_by_field_value(COLLECTION, "city", "city-3"))
                        .expect("the lookup to succeed"),
                )
            })
        });
    }
    group.finish();
}

criterion_group!(benches, by_id, collection_scan, by_field_value);
criterion_main!(benches);
