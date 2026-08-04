use std::str::FromStr;

use cid::Cid;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use defra_core::block::generate_cid_from_bytes;
use defra_core::{
    Block, CollectionDeltaPayload, CompositeDeltaPayload, CounterDeltaPayload, CrdtDelta, DAGLink,
    LwwDeltaPayload,
};
use std::hint::black_box;

// ============================================================================
// Go golden vectors
//
// Byte-exact DAG-CBOR emitted by Go DefraDB, with Go-computed CIDs. Duplicated
// from `crates/defra-core/tests/block_tests.rs` (GO_* constants) so the bench
// measures decode/encode/CID against real cross-language wire bytes rather than
// Rust-generated input. Keep in sync with that file.
// ============================================================================

/// `block_tests.rs` GO_LWW_SIMPLE_BYTES
const GO_LWW_SIMPLE_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA4, 0x64, 0x64, 0x61,
    0x74, 0x61, 0x44, 0x4A, 0x6F, 0x68, 0x6E, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79,
    0x01, 0x69, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x64, 0x6E, 0x61, 0x6D, 0x65,
    0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69,
    0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_LWW_SIMPLE_CID: &str = "bafyreihgg6a5auqhikq4nvw6fj3kbreovdbazlisbs5kerkahoqwwiz75i";

/// `block_tests.rs` GO_LWW_HIGH_PRIORITY_BYTES
const GO_LWW_HIGH_PRIORITY_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA4, 0x64, 0x64, 0x61,
    0x74, 0x61, 0x42, 0x18, 0x1E, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x18, 0x64,
    0x69, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x63, 0x61, 0x67, 0x65, 0x73, 0x63,
    0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E,
    0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_LWW_HIGH_PRIORITY_CID: &str =
    "bafyreidwus7muqrpwwf22gvpqpow6xg37woh4ikztgl27deo37ehs5ehaa";

/// `block_tests.rs` GO_COUNTER_BYTES
const GO_COUNTER_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x67, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x65, 0x72,
    0xA5, 0x64, 0x64, 0x61, 0x74, 0x61, 0x41, 0x0A, 0x65, 0x6E, 0x6F, 0x6E, 0x63, 0x65, 0x19, 0x30,
    0x39, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x01, 0x69, 0x66, 0x69, 0x65, 0x6C,
    0x64, 0x4E, 0x61, 0x6D, 0x65, 0x65, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x73, 0x63, 0x6F, 0x6C, 0x6C,
    0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67,
    0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_COUNTER_CID: &str = "bafyreiazwbpd5i2zhwomwgldn47nq2tluij5dgj2uqz5pctx2cy3nbxgyu";

/// `block_tests.rs` GO_COMPOSITE_ACTIVE_BYTES
const GO_COMPOSITE_ACTIVE_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x69, 0x63, 0x6F, 0x6D, 0x70, 0x6F, 0x73, 0x69,
    0x74, 0x65, 0xA3, 0x66, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x01, 0x68, 0x70, 0x72, 0x69, 0x6F,
    0x72, 0x69, 0x74, 0x79, 0x01, 0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E,
    0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61,
    0x31,
];
const GO_COMPOSITE_ACTIVE_CID: &str = "bafyreie7rtdexuf47f633477mfieshkeh5rwnjeommkgqrzl22n6g4bfmm";

/// `block_tests.rs` GO_COMPOSITE_DELETED_BYTES
const GO_COMPOSITE_DELETED_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x69, 0x63, 0x6F, 0x6D, 0x70, 0x6F, 0x73, 0x69,
    0x74, 0x65, 0xA3, 0x66, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x02, 0x68, 0x70, 0x72, 0x69, 0x6F,
    0x72, 0x69, 0x74, 0x79, 0x02, 0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E,
    0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61,
    0x31,
];
const GO_COMPOSITE_DELETED_CID: &str =
    "bafyreib35xrgvyzzf6uwwqbavrowwzzg5gytspimnwgiyoi6e5nyb3uyp4";

/// `block_tests.rs` GO_COLLECTION_BYTES
const GO_COLLECTION_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x6A, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74,
    0x69, 0x6F, 0x6E, 0xA2, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x01, 0x73, 0x63,
    0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E,
    0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_COLLECTION_CID: &str = "bafyreiggkftgtbppmz66sctbbswtgoy7jxrqbrx3edsq5pzqqpctstekdm";

/// `block_tests.rs` GO_LWW_DELETION_BYTES (empty data = tombstone)
const GO_LWW_DELETION_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA4, 0x64, 0x64, 0x61,
    0x74, 0x61, 0x40, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x02, 0x69, 0x66, 0x69,
    0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x64, 0x6E, 0x61, 0x6D, 0x65, 0x73, 0x63, 0x6F, 0x6C,
    0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44,
    0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_LWW_DELETION_CID: &str = "bafyreia4ihbzcpuuuesmdswmfbbj435aew44rfebj7wt3ldagffukbptf4";

/// Every golden vector as `(name, go_bytes, go_cid)`.
const GO_VECTORS: &[(&str, &[u8], &str)] = &[
    ("lww_simple", GO_LWW_SIMPLE_BYTES, GO_LWW_SIMPLE_CID),
    (
        "lww_high_priority",
        GO_LWW_HIGH_PRIORITY_BYTES,
        GO_LWW_HIGH_PRIORITY_CID,
    ),
    ("lww_deletion", GO_LWW_DELETION_BYTES, GO_LWW_DELETION_CID),
    ("counter", GO_COUNTER_BYTES, GO_COUNTER_CID),
    (
        "composite_active",
        GO_COMPOSITE_ACTIVE_BYTES,
        GO_COMPOSITE_ACTIVE_CID,
    ),
    (
        "composite_deleted",
        GO_COMPOSITE_DELETED_BYTES,
        GO_COMPOSITE_DELETED_CID,
    ),
    ("collection", GO_COLLECTION_BYTES, GO_COLLECTION_CID),
];

/// Correctness gate: every golden vector must decode, re-encode byte-identically,
/// and hash to the CID Go computed. Runs before any timing so a parity break is a
/// hard failure rather than a silently fast number.
fn assert_go_parity() {
    for (name, go_bytes, go_cid) in GO_VECTORS {
        let block = Block::from_dag_cbor(go_bytes)
            .unwrap_or_else(|e| panic!("GO PARITY BREAK [{name}]: decode failed: {e}"));

        let rust_bytes = block
            .to_dag_cbor()
            .unwrap_or_else(|e| panic!("GO PARITY BREAK [{name}]: encode failed: {e}"));
        assert_eq!(
            rust_bytes.as_slice(),
            *go_bytes,
            "GO PARITY BREAK [{name}]: re-encoded bytes differ from Go bytes"
        );

        let expected = Cid::from_str(go_cid).unwrap();
        let from_bytes = generate_cid_from_bytes(go_bytes).unwrap();
        assert_eq!(
            from_bytes, expected,
            "GO PARITY BREAK [{name}]: generate_cid_from_bytes CID differs from Go CID"
        );
        assert_eq!(
            block.generate_cid().unwrap(),
            expected,
            "GO PARITY BREAK [{name}]: Block::generate_cid CID differs from Go CID"
        );
    }
}

// ============================================================================
// Synthetic blocks (parameterized by link count and payload size)
// ============================================================================

fn links(count: usize) -> Vec<DAGLink> {
    (0..count)
        .map(|index| {
            DAGLink::new(
                format!("field_{index}"),
                generate_cid_from_bytes(format!("link-{index}").as_bytes()).unwrap(),
            )
        })
        .collect()
}

fn lww_block(link_count: usize, payload_len: usize) -> Block {
    Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "name".to_string(),
            priority: 1,
            schema_version_id: "schema1".to_string(),
            data: vec![0xABu8; payload_len],
        }),
        vec![],
        links(link_count),
    )
}

fn composite_block_with_links() -> Block {
    Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema1".to_string(),
            priority: 7,
            status: 1,
        }),
        vec![],
        links(5),
    )
}

const LINK_COUNTS: [usize; 3] = [0, 5, 20];
const PAYLOAD_SIZES: [usize; 2] = [16, 1024];

// ============================================================================
// Benches
// ============================================================================

fn bench_block(c: &mut Criterion) {
    let mut group = c.benchmark_group("cbor");

    let lww = lww_block(0, 4);
    group.bench_function(BenchmarkId::from_parameter("encode_lww_small"), |b| {
        b.iter(|| black_box(lww.to_dag_cbor().unwrap()));
    });

    let composite = composite_block_with_links();
    group.bench_function(
        BenchmarkId::from_parameter("encode_composite_5_links"),
        |b| {
            b.iter(|| black_box(composite.to_dag_cbor().unwrap()));
        },
    );

    let lww_bytes = lww.to_dag_cbor().unwrap();
    group.bench_function(BenchmarkId::from_parameter("decode_lww"), |b| {
        b.iter(|| black_box(Block::from_dag_cbor(black_box(&lww_bytes)).unwrap()));
    });

    group.bench_function(BenchmarkId::from_parameter("generate_cid"), |b| {
        b.iter(|| black_box(lww.generate_cid().unwrap()));
    });

    group.bench_function(BenchmarkId::from_parameter("encode_and_cid"), |b| {
        b.iter(|| {
            let bytes = lww.to_dag_cbor().unwrap();
            black_box(generate_cid_from_bytes(black_box(&bytes)).unwrap());
        });
    });

    group.finish();
}

/// Benches driven by the byte-exact Go vectors above.
fn bench_go_vectors(c: &mut Criterion) {
    assert_go_parity();

    let mut group = c.benchmark_group("cbor_go_vectors");

    for (name, go_bytes, go_cid) in GO_VECTORS {
        let expected_cid = Cid::from_str(go_cid).unwrap();
        group.throughput(Throughput::Bytes(go_bytes.len() as u64));

        group.bench_function(BenchmarkId::new("decode_go", name), |b| {
            b.iter(|| black_box(Block::from_dag_cbor(black_box(go_bytes)).unwrap()));
        });

        group.bench_function(BenchmarkId::new("reencode_go", name), |b| {
            // `_ref`: criterion drops the routine's return value outside the measurement
            // but not its argument, so a by-value `Block` would have its teardown timed
            // alongside the encode.
            b.iter_batched_ref(
                || Block::from_dag_cbor(go_bytes).unwrap(),
                |block| black_box(block.to_dag_cbor().unwrap()),
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("decode_reencode_go", name), |b| {
            b.iter(|| {
                let block = Block::from_dag_cbor(black_box(go_bytes)).unwrap();
                black_box(block.to_dag_cbor().unwrap())
            });
        });

        group.bench_function(BenchmarkId::new("cid_from_go_bytes", name), |b| {
            b.iter(|| {
                let cid = generate_cid_from_bytes(black_box(go_bytes)).unwrap();
                debug_assert_eq!(cid, expected_cid);
                black_box(cid)
            });
        });
    }

    group.finish();
}

/// Encode/decode/CID curves over link count and payload size, matching the
/// shape of the equivalent Go block benchmarks.
fn bench_block_shape(c: &mut Criterion) {
    let mut group = c.benchmark_group("cbor_shape");

    for link_count in LINK_COUNTS {
        for payload in PAYLOAD_SIZES {
            let block = lww_block(link_count, payload);
            let bytes = block.to_dag_cbor().unwrap();
            let label = format!("links{link_count}_payload{payload}");
            group.throughput(Throughput::Bytes(bytes.len() as u64));

            group.bench_function(BenchmarkId::new("encode", &label), |b| {
                b.iter(|| black_box(block.to_dag_cbor().unwrap()));
            });

            group.bench_function(BenchmarkId::new("decode", &label), |b| {
                b.iter(|| black_box(Block::from_dag_cbor(black_box(&bytes)).unwrap()));
            });

            group.bench_function(BenchmarkId::new("cid", &label), |b| {
                b.iter(|| black_box(generate_cid_from_bytes(black_box(&bytes)).unwrap()));
            });

            group.bench_function(BenchmarkId::new("encode_and_cid", &label), |b| {
                b.iter(|| {
                    let encoded = block.to_dag_cbor().unwrap();
                    black_box(generate_cid_from_bytes(black_box(&encoded)).unwrap())
                });
            });
        }
    }

    group.finish();
}

/// Per-delta-variant encode cost at a fixed shape, so variant overhead is
/// separable from payload/link overhead.
fn bench_delta_variants(c: &mut Criterion) {
    let mut group = c.benchmark_group("cbor_variants");

    let variants: Vec<(&str, Block)> = vec![
        (
            "lww",
            Block::new(
                CrdtDelta::Lww(LwwDeltaPayload {
                    field_name: "name".to_string(),
                    priority: 1,
                    schema_version_id: "schema1".to_string(),
                    data: b"John".to_vec(),
                }),
                vec![],
                vec![],
            ),
        ),
        (
            "counter",
            Block::new(
                CrdtDelta::Counter(CounterDeltaPayload {
                    field_name: "count".to_string(),
                    priority: 1,
                    nonce: 12345,
                    schema_version_id: "schema1".to_string(),
                    data: vec![0x0A],
                }),
                vec![],
                vec![],
            ),
        ),
        (
            "composite",
            Block::new(
                CrdtDelta::Composite(CompositeDeltaPayload {
                    schema_version_id: "schema1".to_string(),
                    priority: 1,
                    status: 1,
                }),
                vec![],
                vec![],
            ),
        ),
        (
            "collection",
            Block::new(
                CrdtDelta::Collection(CollectionDeltaPayload {
                    schema_version_id: "schema1".to_string(),
                    priority: 1,
                }),
                vec![],
                vec![],
            ),
        ),
    ];

    for (name, block) in &variants {
        let bytes = block.to_dag_cbor().unwrap();
        group.throughput(Throughput::Bytes(bytes.len() as u64));

        group.bench_function(BenchmarkId::new("encode", name), |b| {
            b.iter(|| black_box(block.to_dag_cbor().unwrap()));
        });

        group.bench_function(BenchmarkId::new("decode", name), |b| {
            b.iter(|| black_box(Block::from_dag_cbor(black_box(&bytes)).unwrap()));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_block,
    bench_go_vectors,
    bench_block_shape,
    bench_delta_variants
);
criterion_main!(benches);
