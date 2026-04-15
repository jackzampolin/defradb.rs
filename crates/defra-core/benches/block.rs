use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use defra_core::block::generate_cid_from_bytes;
use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};
use std::hint::black_box;

fn lww_block() -> Block {
    Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 1,
            schema_version_id: "schema1".to_string(),
            data: b"John".to_vec(),
        }),
        vec![],
        vec![],
    )
}

fn composite_block_with_links() -> Block {
    let links = (0..5)
        .map(|index| {
            DAGLink::new(
                format!("field_{index}"),
                generate_cid_from_bytes(format!("link-{index}").as_bytes()).unwrap(),
            )
        })
        .collect();

    Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            doc_id: b"doc-composite".to_vec(),
            schema_version_id: "schema1".to_string(),
            priority: 7,
            status: 1,
        }),
        vec![],
        links,
    )
}

fn bench_block(c: &mut Criterion) {
    let mut group = c.benchmark_group("cbor");

    let lww = lww_block();
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

criterion_group!(benches, bench_block);
criterion_main!(benches);
