use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use query::query_parse::parse_request_with_variables;

fn bench_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse");
    let cases = [
        ("simple_select", "{ Users { id name } }"),
        (
            "filtered",
            "{ Users(filter: {age: {_gte: 25}}) { id name } }",
        ),
        (
            "complex_filter",
            "{ Users(filter: {_and: [{age: {_gte: 25}}, {name: {_like: \"A%\"}}]}) { id } }",
        ),
        (
            "nested_join",
            "{ Users { id posts { id title comments { id body } } } }",
        ),
        (
            "mutation",
            "mutation { add_Users(input: {name: \"Alice\", age: 30}) { id } }",
        ),
        (
            "commits",
            "{ _commits(docID: [\"bae-abc123\"]) { cid height delta } }",
        ),
    ];

    for (name, query) in cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &query, |b, query| {
            b.iter(|| parse_request_with_variables(black_box(query), None, None).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parsing);
criterion_main!(benches);
