//! Schema and collection validation.
//!
//! ```text
//! cargo bench -p benches --bench schema_validate
//! ```
//!
//! Validation runs when a schema is added or patched, and again on every
//! collection load at startup, so it sits on the path the cold-open curve in
//! [`startup`](../startup.rs) measures. Both the whole-schema check and the
//! per-collection one walk every field, so the cost is swept over width and
//! over how many collections the schema holds.
//!
//! `field_by_name` is measured separately because it is a linear scan called
//! once per field per document on the read path, where the width of the
//! collection is the multiplier nobody had a number for.

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use schema::{CollectionVersion, FieldDescription, FieldKind};

const WIDTHS: [usize; 4] = [4, 16, 64, 256];
const COUNTS: [usize; 4] = [1, 10, 100, 500];

fn collection(index: usize, width: usize) -> CollectionVersion {
    let mut fields = vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())];
    for i in 0..width {
        fields.push(FieldDescription::new(
            (i + 2).to_string(),
            format!("field_{i}"),
            if i % 3 == 0 {
                FieldKind::int()
            } else {
                FieldKind::string()
            },
        ));
    }
    CollectionVersion::new(
        format!("Collection{index}"),
        format!("bafkschema{index:04}"),
        format!("schema{index:04}"),
        fields,
    )
}

fn one_collection(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_collection_validate");
    for width in WIDTHS {
        let version = collection(0, width);
        group.throughput(Throughput::Elements(width as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(width),
            &version,
            |b, version| {
                b.iter(|| {
                    black_box(version)
                        .validate()
                        .expect("the collection to validate")
                })
            },
        );
    }
    group.finish();
}

/// The whole-schema check, which is what a startup with many collections pays.
fn whole_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_validate_all");
    for count in COUNTS {
        let collections: HashMap<String, CollectionVersion> = (0..count)
            .map(|i| {
                let version = collection(i, 16);
                (version.name.clone(), version)
            })
            .collect();
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &collections,
            |b, collections| {
                b.iter(|| {
                    schema::validate_schema(black_box(collections)).expect("the schema to validate")
                })
            },
        );
    }
    group.finish();
}

/// A linear scan over the field list, run once per field per document. The
/// worst case is the field that is not there, because it walks the whole list
/// before saying so.
fn field_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_field_by_name");
    for width in WIDTHS {
        let version = collection(0, width);
        let last = format!("field_{}", width - 1);
        group.bench_with_input(BenchmarkId::new("last", width), &version, |b, version| {
            b.iter(|| black_box(black_box(version).field_by_name(black_box(&last))))
        });
        group.bench_with_input(
            BenchmarkId::new("missing", width),
            &version,
            |b, version| {
                b.iter(|| black_box(black_box(version).field_by_name(black_box("no_such_field"))))
            },
        );
    }
    group.finish();
}

criterion_group!(benches, one_collection, whole_schema, field_lookup);
criterion_main!(benches);
