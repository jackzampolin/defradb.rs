//! GraphQL execution end to end, over a real database.
//!
//! ```text
//! cargo bench -p benches --bench query_e2e
//! ```
//!
//! [`parsing`](../parsing.rs) times the parser and [`planner`](../planner.rs)
//! times plan construction. [`transport`](../transport.rs) times the HTTP layer
//! against a stub executor that does no database work at all. Between them, the
//! thing a user actually waits for, a query running against documents on disk,
//! was never measured.
//!
//! This drives `QueryRunner` over the real fetcher and the real store, so a row
//! here is the whole cost: parse, plan, index selection, scan, filter, sort,
//! aggregate and render. Parameterized by result-set size, because a query
//! shape that is fine over ten documents is not automatically fine over ten
//! thousand.
//!
//! Top-level aggregates are absent, and deliberately so rather than by
//! oversight: `_count(User: {})` does not parse. `AggregateType::parse` in
//! `crates/query/src/mapper/types.rs` matches `COUNT` and `AVG`, so the
//! dispatch in `crates/query/src/query_parse/parser.rs` that would route a
//! top-level aggregate never fires for `_count`, and the field falls through to
//! the collection path where the collection name is rejected as an argument.
//! Benchmarking a shape the engine rejects would measure its error path.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use db::DB;
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use query::{QueryExecutor, QueryRequest, QueryRunner};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::RegolithStore;

mod common;

const COLLECTION: &str = "User";
const CORPUS: [usize; 3] = [100, 1_000, 5_000];

fn collection_version() -> CollectionVersion {
    CollectionVersion::new(
        COLLECTION,
        "bafkquerye2e",
        "querye2e",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "city", FieldKind::string()),
            FieldDescription::new("4", "age", FieldKind::int()),
        ],
    )
}

/// A database holding `count` documents, and a runner over it.
async fn fixture(count: usize) -> impl QueryExecutor {
    let store = Arc::new(RegolithStore::in_memory().expect("an in-memory store"));
    let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
    db.create_collection(collection_version())
        .await
        .expect("the collection to register");

    let mutator = Arc::new(db::write::autocommit::AutoCommitMutator::new(db.clone()));
    // Ten cities and a spread of ages, so a filter selects a tenth of the
    // corpus rather than all of it or none: a predicate nothing matches
    // measures the scan and never the rest of the pipeline.
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
    QueryRunner::new(fetcher, vec![collection_version()])
        .with_mutator(mutator as Arc<dyn DocMutator>)
}

/// Every shape a read-heavy application actually issues.
const SHAPES: [(&str, &str); 6] = [
    ("scan_all", "query { User { name city age } }"),
    (
        "filter_eq",
        r#"query { User(filter: {city: {_eq: "city-3"}}) { name age } }"#,
    ),
    (
        "filter_range",
        "query { User(filter: {age: {_gt: 40, _lt: 60}}) { name age } }",
    ),
    (
        "order_limit",
        "query { User(order: {age: DESC}, limit: 20) { name age } }",
    ),
    (
        "filter_order_limit",
        r#"query { User(filter: {city: {_eq: "city-3"}}, order: {age: ASC}, limit: 10) { name age } }"#,
    ),
    (
        "offset_page",
        "query { User(limit: 20, offset: 200) { name age } }",
    ),
];

fn shapes(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("query_e2e");
    group.sample_size(20);

    for count in CORPUS {
        let runner = rt.block_on(fixture(count));
        for (name, query) in SHAPES {
            group.throughput(Throughput::Elements(count as u64));
            group.bench_with_input(BenchmarkId::new(name, count), &query, |b, query| {
                b.iter(|| {
                    let response =
                        rt.block_on(runner.execute(QueryRequest::new(black_box(*query))));
                    // A query that errored did no work, and timing it would
                    // report the error path as if it were the query.
                    assert!(
                        response.errors.is_empty(),
                        "{name} failed: {:?}",
                        response.errors
                    );
                    black_box(response)
                })
            });
        }
    }
    group.finish();
}

/// A mutation through the same surface, so the write path is comparable with
/// the read one at the level a user sees rather than at the mutator's.
fn mutation(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let runner = rt.block_on(fixture(100));
    let mut group = c.benchmark_group("query_e2e_mutation");
    group.sample_size(20);
    let mut seq = 0usize;
    group.bench_function("create", |b| {
        b.iter(|| {
            seq += 1;
            let query = format!(
                r#"mutation {{ create_User(input: {{name: "new-{seq}", city: "city-9", age: 33}}) {{ name }} }}"#
            );
            let response = rt.block_on(runner.execute(QueryRequest::new(query)));
            assert!(
                response.errors.is_empty(),
                "create failed: {:?}",
                response.errors
            );
            black_box(response)
        })
    });
    group.finish();
}

criterion_group!(benches, shapes, mutation);
criterion_main!(benches);
