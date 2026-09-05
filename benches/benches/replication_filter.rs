//! Validating a replicator's filter predicate.
//!
//! ```text
//! cargo bench -p benches --bench replication_filter
//! ```
//!
//! A filtered replicator checks its predicate against the collection's fields
//! before it is accepted, and the check walks every condition against every
//! field. Both grow with the schema, so the cost is swept over predicate width
//! rather than quoted for one shape.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use p2p::replicator::ReplicationFilter;
use replication_filter::validate_replication_filter;
use schema::{FieldDescription, FieldKind};

const WIDTHS: [usize; 4] = [1, 4, 16, 64];
const COLLECTION_ID: &str = "replfilter";

/// Immutable scalar LWW fields, which are the only kind a replication filter
/// may reference.
fn fields(count: usize) -> Vec<FieldDescription> {
    let mut fields = vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())];
    for i in 0..count {
        let mut field = FieldDescription::new(
            (i + 2).to_string(),
            format!("field_{i}"),
            FieldKind::string(),
        );
        field.immutable = true;
        fields.push(field);
    }
    fields
}

fn predicate(width: usize) -> ReplicationFilter {
    let mut map = serde_json::Map::new();
    for i in 0..width {
        map.insert(
            format!("field_{i}"),
            serde_json::json!({ "_eq": format!("value-{i}") }),
        );
    }
    ReplicationFilter::Predicate(map)
}

fn validate(c: &mut Criterion) {
    let mut group = c.benchmark_group("replication_filter");
    for width in WIDTHS {
        // The schema is always wider than the predicate, which is the shape a
        // real collection has: the check has to find each referenced field.
        let schema = fields(width.max(16));
        let filter = predicate(width);
        group.bench_with_input(BenchmarkId::new("accept", width), &filter, |b, filter| {
            b.iter(|| {
                validate_replication_filter(&schema, COLLECTION_ID, black_box(filter))
                    .expect("the predicate to validate")
            })
        });
    }

    // Rejection is the untrusted-input path: a filter arrives from a peer's
    // replicator registration and has to be refused, so its cost is real.
    let schema = fields(16);
    let unknown = ReplicationFilter::Predicate(
        [("no_such_field".to_string(), serde_json::json!({"_eq": "x"}))]
            .into_iter()
            .collect(),
    );
    group.bench_function("reject_unknown_field", |b| {
        b.iter(|| {
            let rejected =
                validate_replication_filter(&schema, COLLECTION_ID, black_box(&unknown)).is_err();
            assert!(rejected, "a predicate on an unknown field must be refused");
            black_box(rejected)
        })
    });
    group.finish();
}

criterion_group!(benches, validate);
criterion_main!(benches);
