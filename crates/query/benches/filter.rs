use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use query::{DocumentMapping, Filter};
use serde_json::{json, Value as JsonValue};

fn make_mapping() -> DocumentMapping {
    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "name");
    mapping.add(2, "age");
    mapping.add(3, "active");
    mapping
}

fn make_doc(doc_id: &str, name: &str, age: u64, active: bool) -> Vec<Option<JsonValue>> {
    vec![
        Some(json!(doc_id)),
        Some(json!(name)),
        Some(json!(age)),
        Some(json!(active)),
    ]
}

fn bench_filter_cases(c: &mut Criterion) {
    let mapping = make_mapping();
    let fields = make_doc("doc-001", "Alice", 30, true);
    let mut group = c.benchmark_group("filter");

    let cases = [
        (
            "eq_single",
            Filter::from_conditions(HashMap::from([(
                "name".to_string(),
                json!({"_eq": "Alice"}),
            )])),
        ),
        (
            "numeric_gte",
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gte": 25}))])),
        ),
        (
            "like_pattern",
            Filter::from_conditions(HashMap::from([(
                "name".to_string(),
                json!({"_like": "Ali%"}),
            )])),
        ),
        (
            "and_3_conditions",
            Filter::from_conditions(HashMap::from([(
                "_and".to_string(),
                json!([
                    {"name": {"_eq": "Alice"}},
                    {"age": {"_gte": 25}},
                    {"active": {"_eq": true}}
                ]),
            )])),
        ),
        (
            "or_5_conditions",
            Filter::from_conditions(HashMap::from([(
                "_or".to_string(),
                json!([
                    {"name": {"_eq": "Bob"}},
                    {"age": {"_eq": 21}},
                    {"age": {"_eq": 22}},
                    {"age": {"_eq": 23}},
                    {"active": {"_eq": true}}
                ]),
            )])),
        ),
        (
            "nested_and_or",
            Filter::from_conditions(HashMap::from([(
                "_and".to_string(),
                json!([
                    {"_or": [
                        {"name": {"_like": "A%"}},
                        {"name": {"_eq": "Bob"}}
                    ]},
                    {"_or": [
                        {"age": {"_gte": 25}},
                        {"active": {"_eq": false}}
                    ]}
                ]),
            )])),
        ),
        (
            "in_10_values",
            Filter::from_conditions(HashMap::from([(
                "name".to_string(),
                json!({"_in": [
                    "Ada", "Anya", "Ari", "April", "Alice",
                    "Ben", "Cara", "Drew", "Elle", "Finn"
                ]}),
            )])),
        ),
    ];

    for (name, filter) in cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &filter, |b, filter| {
            b.iter(|| {
                filter
                    .matches(black_box(&fields), black_box(&mapping))
                    .unwrap()
            });
        });
    }

    let batch_filter = Filter::from_conditions(HashMap::from([(
        "_and".to_string(),
        json!([
            {"age": {"_gte": 25}},
            {"name": {"_like": "A%"}}
        ]),
    )]));
    let batch_docs: Vec<_> = (0..100)
        .map(|index| {
            let prefix = if index % 3 == 0 { "Alice" } else { "Bob" };
            make_doc(
                &format!("doc-{index:03}"),
                &format!("{prefix}-{index:03}"),
                18 + (index % 50) as u64,
                index % 2 == 0,
            )
        })
        .collect();

    group.bench_function(BenchmarkId::from_parameter("batch_100_docs"), |b| {
        b.iter(|| {
            batch_docs
                .iter()
                .filter(|doc| {
                    batch_filter
                        .matches(black_box(doc), black_box(&mapping))
                        .unwrap()
                })
                .count()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_filter_cases);
criterion_main!(benches);
