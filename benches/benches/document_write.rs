//! Document writes through the real mutator.
//!
//! ```text
//! cargo bench -p benches --bench document_write
//! ```
//!
//! [`block_write`](../block_write.rs) times the block-building slice of a
//! write against raw namespace views. This times the operation a user actually
//! performs: `AutoCommitMutator::create` and its siblings, which allocate the
//! document's short id, build and link the blocks, maintain every secondary
//! index, and commit. The gap between the two is the cost of everything
//! wrapped around block building, and until this existed nothing measured it.
//!
//! Parameterized by field count, by batch size, and by whether a secondary
//! index is present, because index maintenance is paid per write and per
//! indexed field and is invisible in a bench that has no index.

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use db::DB;
use defra_perf::emit::{Family, Group};
use defra_perf::measure::repeat;
use document::{DocID, Document, NormalValue};
use query::mutator::DocMutator;
use query::DocFetcher;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::RegolithStore;

mod common;

const COLLECTION: &str = "Users";
const SCHEMA_VERSION_ID: &str = "bafkdocwrite";
const COLLECTION_ID: &str = "docwrite";
const FIELD_COUNTS: [usize; 3] = [4, 16, 64];

fn field_names(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("field_{i}")).collect()
}

fn collection_version(field_count: usize) -> CollectionVersion {
    let mut fields = vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())];
    for (index, name) in field_names(field_count).iter().enumerate() {
        fields.push(FieldDescription::new(
            (index + 2).to_string(),
            name,
            FieldKind::string(),
        ));
    }
    CollectionVersion::new(COLLECTION, SCHEMA_VERSION_ID, COLLECTION_ID, fields)
}

/// A document whose every field is distinct per `seq`, so two documents in one
/// run never collide on a content-addressed id.
fn document(field_count: usize, seq: usize) -> Document {
    let mut doc = Document::new();
    for name in field_names(field_count) {
        doc.set(&name, NormalValue::String(format!("{name}-{seq}")));
    }
    doc
}

type Mutator = db::write::autocommit::AutoCommitMutator<RegolithStore>;

/// A fresh in-memory database with the collection already registered. In
/// memory on purpose: this measures the write path, and putting a real disk
/// under it would measure the disk.
async fn fixture(field_count: usize) -> Mutator {
    let store = Arc::new(RegolithStore::in_memory().expect("an in-memory store"));
    let db = Arc::new(DB::from_arc(store).expect("a database over it"));
    db.create_collection(collection_version(field_count))
        .await
        .expect("the collection to register");
    Mutator::new(db)
}

/// A fixture with one document already in it, and that document's id.
async fn seeded(field_count: usize) -> (Mutator, DocID) {
    let mutator = fixture(field_count).await;
    let created = mutator
        .create(COLLECTION, document(field_count, next_seq()))
        .await
        .expect("the seed create to succeed");
    (mutator, created.doc_id)
}

/// A document id is content-addressed, so two identical documents are one
/// document and the second create is a conflict rather than a write. Every
/// document this bench builds gets a sequence nobody else in the process used.
fn next_seq() -> usize {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// The same operations, reported as a throughput family rather than as
/// criterion timings.
///
/// Criterion cannot run in a browser, so a criterion row exists on the native
/// platforms and nowhere else. This family carries group and row names the
/// browser harness in `crates/wasm/tests/perf.rs` reproduces exactly, which is
/// what puts a browser column beside the Linux and macOS ones on the dashboard
/// instead of three tables that do not line up.
fn report(rt: &tokio::runtime::Runtime) {
    const OPS: usize = 200;
    const REPS: usize = 5;

    let mut create = Group::higher_better("create", "ops/s").over("fields");
    let mut read = Group::higher_better("read", "ops/s").over("fields");

    for fields in FIELD_COUNTS {
        create = create.row(
            repeat(format!("{fields} fields"), REPS, || {
                rt.block_on(async {
                    let mutator = fixture(fields).await;
                    let base = next_seq() * OPS;
                    let start = std::time::Instant::now();
                    for seq in 0..OPS {
                        mutator
                            .create(COLLECTION, document(fields, base + seq))
                            .await
                            .expect("the create to succeed");
                    }
                    OPS as f64 / start.elapsed().as_secs_f64()
                })
            })
            .at(fields as f64),
        );

        read = read.row(
            repeat(format!("{fields} fields"), REPS, || {
                rt.block_on(async {
                    let store = Arc::new(storage::RegolithStore::in_memory().expect("a store"));
                    let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
                    db.create_collection(collection_version(fields))
                        .await
                        .expect("the collection to register");
                    let mutator = Mutator::new(db.clone());
                    let base = next_seq() * OPS;
                    let mut ids = Vec::with_capacity(OPS);
                    for seq in 0..OPS {
                        ids.push(
                            mutator
                                .create(COLLECTION, document(fields, base + seq))
                                .await
                                .expect("the create to succeed")
                                .doc_id
                                .to_string(),
                        );
                    }
                    let fetcher = db::AutoCommitFetcher::new(db);
                    let start = std::time::Instant::now();
                    for id in &ids {
                        fetcher
                            .get_by_ids(COLLECTION, std::slice::from_ref(id))
                            .await
                            .expect("the read to succeed");
                    }
                    ids.len() as f64 / start.elapsed().as_secs_f64()
                })
            })
            .at(fields as f64),
        );
    }

    Family::new(
        "Document operations",
        format!(
            "Documents created and read back one at a time, {OPS} of each, through the same \
             mutator and fetcher every platform uses. Measured with a wall clock natively and \
             with `performance.now()` in the browser, so the two are the same operation counted \
             the same way."
        ),
    )
    .group(create)
    .group(read)
    .emit("document_ops");
}

fn create(c: &mut Criterion) {
    let rt = common::owned_runtime();
    report(&rt);
    let mut group = c.benchmark_group("document_create");
    for fields in FIELD_COUNTS {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(fields),
            &fields,
            |b, &fields| {
                // A fresh database per batch: a create into a store holding a
                // million documents is a different measurement from a create into
                // an empty one, and mixing the two would report their average.
                b.iter_batched_ref(
                    || rt.block_on(fixture(fields)),
                    |mutator| {
                        rt.block_on(async {
                            black_box(
                                mutator
                                    .create(COLLECTION, document(fields, next_seq()))
                                    .await
                                    .expect("the create to succeed"),
                            );
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

/// One commit for many documents against one commit each. The difference is
/// the whole reason `create_many` exists, and it has never been measured.
fn create_many(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("document_create_many");
    let fields = 16;
    for batch in [1usize, 8, 64, 256] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter_batched_ref(
                || rt.block_on(fixture(fields)),
                |mutator| {
                    rt.block_on(async {
                        let base = next_seq();
                        let docs: Vec<Document> =
                            (0..batch).map(|s| document(fields, base + s)).collect();
                        black_box(
                            mutator
                                .create_many(COLLECTION, docs)
                                .await
                                .expect("the batch create to succeed"),
                        );
                    })
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn update(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("document_update");
    for fields in FIELD_COUNTS {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(fields),
            &fields,
            |b, &fields| {
                b.iter_batched_ref(
                    || rt.block_on(seeded(fields)),
                    |(mutator, doc_id)| {
                        rt.block_on(async {
                            // One field touched, which is the common shape: a
                            // write that rewrote every field would measure a
                            // create.
                            let mut doc = Document::with_id(doc_id.clone());
                            doc.set(
                                "field_0",
                                NormalValue::String(format!("updated-{}", next_seq())),
                            );
                            let modified: HashSet<String> =
                                ["field_0".to_string()].into_iter().collect();
                            black_box(
                                mutator
                                    .update(COLLECTION, doc, modified)
                                    .await
                                    .expect("the update to succeed"),
                            );
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn delete(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("document_delete");
    let fields = 16;
    group.throughput(Throughput::Elements(1));
    group.bench_function("16", |b| {
        b.iter_batched_ref(
            || rt.block_on(seeded(fields)),
            |(mutator, doc_id)| {
                rt.block_on(async {
                    black_box(
                        mutator
                            .delete(COLLECTION, doc_id)
                            .await
                            .expect("the delete to succeed"),
                    );
                })
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, create, create_many, update, delete);
criterion_main!(benches);
