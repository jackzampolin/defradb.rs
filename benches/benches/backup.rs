//! Database export and import.
//!
//! ```text
//! cargo bench -p benches --bench backup
//! ```
//!
//! Backup is the one operation whose cost scales with the whole database
//! rather than with a request, so it is the operation most likely to be
//! discovered to be slow at exactly the wrong moment. Export renders every
//! document to JSON through the query engine; import writes every one of them
//! back through the mutator. Neither had a number.
//!
//! Swept by document count, because the question is whether either side grows
//! worse than linearly.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use db::DB;
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use query::{QueryExecutor, QueryRunner};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::RegolithStore;

mod common;

const COLLECTION: &str = "User";
const CORPUS: [usize; 3] = [100, 1_000, 5_000];

fn collection_version() -> CollectionVersion {
    CollectionVersion::new(
        COLLECTION,
        "bafkbackup",
        "backup",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "city", FieldKind::string()),
            FieldDescription::new("4", "age", FieldKind::int()),
        ],
    )
}

struct Fixture {
    db: Arc<DB<RegolithStore>>,
    runner: Arc<dyn QueryExecutor>,
}

async fn fixture(count: usize) -> Fixture {
    let store = Arc::new(RegolithStore::in_memory().expect("an in-memory store"));
    let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
    db.create_collection(collection_version())
        .await
        .expect("the collection to register");

    let mutator = Arc::new(db::write::autocommit::AutoCommitMutator::new(db.clone()));
    for seq in 0..count {
        let mut doc = Document::new();
        doc.set("name", NormalValue::String(format!("person-{seq}")));
        doc.set("city", NormalValue::String(format!("city-{}", seq % 10)));
        doc.set("age", NormalValue::Int((seq % 80) as i64 + 18));
        mutator
            .create(COLLECTION, doc)
            .await
            .expect("the seed create to succeed");
    }

    let fetcher = db::AutoCommitFetcher::new(db.clone());
    let runner: Arc<dyn QueryExecutor> = Arc::new(
        QueryRunner::new(fetcher, vec![collection_version()])
            .with_mutator(mutator as Arc<dyn DocMutator>),
    );
    Fixture { db, runner }
}

fn export(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("backup_export");
    group.sample_size(10);
    for count in CORPUS {
        let fixture = rt.block_on(fixture(count));
        let names = vec![COLLECTION.to_string()];
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                black_box(
                    rt.block_on(db::backup::export_database(
                        &fixture.db,
                        &fixture.runner,
                        &names,
                        false,
                    ))
                    .expect("the export to succeed"),
                )
            })
        });
    }
    group.finish();
}

/// Restore, into an empty database.
///
/// Not into a populated one: `import_database` refuses a document whose id is
/// already present ("a document with the given ID already exists"), so restore
/// is a fresh-database operation and timing it against a populated one would
/// measure the rejection path. A fresh database per batch, built outside the
/// measured region, is what keeps the row honest.
fn import(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("backup_import");
    group.sample_size(10);
    let names = vec![COLLECTION.to_string()];
    for count in CORPUS {
        let source = rt.block_on(fixture(count));
        let dump = rt
            .block_on(db::backup::export_database(
                &source.db,
                &source.runner,
                &names,
                false,
            ))
            .expect("the export to succeed");
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &dump, |b, dump| {
            b.iter_batched_ref(
                || rt.block_on(fixture(0)),
                |target| {
                    black_box(
                        rt.block_on(db::backup::import_database(
                            &target.db,
                            &target.runner,
                            black_box(dump),
                        ))
                        .expect("the import to succeed"),
                    )
                },
                criterion::BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, export, import);
criterion_main!(benches);
