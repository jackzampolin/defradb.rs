//! Document field-value conversion, content addressing and timestamps.
//!
//! ```text
//! cargo bench -p benches --bench document_codec
//! ```
//!
//! [`block`](../block.rs) covers the block envelope, and the CBOR value codec
//! underneath it along with it. This covers the rest of what every document
//! read and write pays for and nothing measured: JSON in and out, deriving a
//! content-addressed id, and parsing a timestamp.
//!
//! Parameterized by document width and by value kind, because the cost is per
//! value and the kinds do not cost the same.

use std::hint::black_box;

use cid::Cid;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use document::{DocID, Document, NormalValue};
use multihash::Multihash;
use sha2::{Digest, Sha256};

const WIDTHS: [usize; 3] = [4, 16, 64];
const SHA2_256: u64 = 0x12;
const DAG_CBOR: u64 = 0x71;

fn value_kinds() -> Vec<(&'static str, NormalValue)> {
    vec![
        (
            "string",
            NormalValue::String("a moderately sized value".into()),
        ),
        ("int", NormalValue::Int(-4_611_686_018_427_387_904)),
        ("float", NormalValue::Float64(1.234_567_890_123_456_7)),
        ("bool", NormalValue::Bool(true)),
        ("bytes", NormalValue::Bytes(vec![0xa5; 256])),
    ]
}

fn wide_document(width: usize) -> Document {
    let mut doc = Document::new();
    let kinds = value_kinds();
    for index in 0..width {
        let (_, value) = &kinds[index % kinds.len()];
        doc.set(format!("field_{index}"), value.clone());
    }
    doc
}

fn json_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_json");
    for width in WIDTHS {
        let doc = wide_document(width);
        let json = serde_json::to_vec(&doc.to_map().expect("the document to serialize"))
            .expect("a JSON encoding of it");
        group.throughput(Throughput::Elements(width as u64));
        group.bench_with_input(BenchmarkId::new("to_map", width), &doc, |b, doc| {
            b.iter(|| black_box(doc.to_map().expect("the document to serialize")))
        });
        group.bench_with_input(BenchmarkId::new("from_json", width), &json, |b, json| {
            b.iter(|| {
                black_box(Document::from_json(black_box(json)).expect("the document to parse"))
            })
        });
    }
    group.finish();
}

/// A document id is derived from a CID, and every create pays for it.
fn doc_id(c: &mut Criterion) {
    let digest = Sha256::digest(b"a document's canonical bytes");
    let cid = Cid::new_v1(
        DAG_CBOR,
        Multihash::wrap(SHA2_256, &digest).expect("a well-formed multihash"),
    );
    let mut group = c.benchmark_group("codec_doc_id");
    group.bench_function("new_v0", |b| {
        b.iter(|| black_box(DocID::new_v0(black_box(cid))))
    });
    group.bench_function("to_string", |b| {
        let id = DocID::new_v0(cid);
        b.iter(|| black_box(black_box(&id).to_string()))
    });
    group.finish();
}

/// Every datetime field on every document goes through this.
fn timestamps(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_rfc3339");
    for (name, text) in [
        ("utc", "2026-09-04T12:34:56Z"),
        ("offset", "2026-09-04T12:34:56+02:00"),
        ("fractional", "2026-09-04T12:34:56.123456789Z"),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(name), &text, |b, text| {
            b.iter(|| black_box(document::parse_rfc3339(black_box(text))))
        });
    }
    group.finish();
}

criterion_group!(benches, json_round_trip, doc_id, timestamps);
criterion_main!(benches);
