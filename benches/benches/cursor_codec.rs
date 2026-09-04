//! Pagination token encode and decode.
//!
//! ```text
//! cargo bench -p benches --bench cursor_codec
//! ```
//!
//! Two documents' worth of work on every paged query: one token decoded on the
//! way in and one encoded on the way out. Small, but on the path of every
//! paginated read, and swept by key count because a composite index cursor
//! carries one key per indexed field.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use cursor::Cursor;

const KEY_COUNTS: [usize; 4] = [0, 1, 4, 16];

fn sample(keys: usize) -> Cursor {
    let mut cursor = Cursor::from_doc_id("bae-8a1f9c2d-4e5b-4c3a-9d7e-1f2a3b4c5d6e");
    for i in 0..keys {
        cursor.keys.insert(
            format!("field_{i}"),
            serde_json::json!(format!("value-{i}")),
        );
    }
    cursor.direction = "ASC".into();
    cursor
}

fn round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("cursor_codec");
    for keys in KEY_COUNTS {
        let cursor = sample(keys);
        let token = cursor.encode();
        group.bench_with_input(BenchmarkId::new("encode", keys), &cursor, |b, cursor| {
            b.iter(|| black_box(black_box(cursor).encode()))
        });
        group.bench_with_input(BenchmarkId::new("decode", keys), &token, |b, token| {
            b.iter(|| black_box(Cursor::decode(black_box(token)).expect("a valid token")))
        });
    }
    group.finish();
}

/// A malformed token is the untrusted-input path: it arrives from a client and
/// has to be rejected rather than trusted, so its cost is a real one.
fn rejection(c: &mut Criterion) {
    let mut group = c.benchmark_group("cursor_reject");
    for (name, token) in [
        ("not_base64", "!!!not-base64!!!"),
        ("not_json", "aGVsbG8gd29ybGQ"),
        ("empty_doc_id", &Cursor::from_doc_id("").encode() as &str),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(name), &token, |b, token| {
            b.iter(|| black_box(Cursor::decode(black_box(token)).is_err()))
        });
    }
    group.finish();
}

criterion_group!(benches, round_trip, rejection);
criterion_main!(benches);
