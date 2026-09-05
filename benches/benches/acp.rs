//! Access-control checks, and how they multiply across a result set.
//!
//! ```text
//! cargo bench -p benches --bench acp
//! ```
//!
//! `check_doc_access` runs once per document, not once per query, so a read
//! returning a thousand documents pays for a thousand checks. That multiplier
//! is the number nobody had: a per-check figure in microseconds is fine on its
//! own and is the whole query budget at scale.
//!
//! Both answers are measured. A denial walks the relationship set without
//! finding a match, so it is not automatically the same cost as an approval,
//! and a query that filters most of a collection away pays the denial cost far
//! more often than the approval one.

use std::hint::black_box;
use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity, LocalDocumentACP, MemoryAcpStore};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use identity::Did;

mod common;

const POLICY: &str = "policy1";
const RESOURCE: &str = "file";
const REGISTERED: [usize; 4] = [1, 100, 1_000, 10_000];
const FANOUT: [usize; 4] = [1, 10, 100, 1_000];

fn owner() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
        .expect("a well-formed owner did")
}

fn stranger() -> Did {
    Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH")
        .expect("a well-formed stranger did")
}

fn doc_id(index: usize) -> String {
    format!("bae-doc-{index:08}")
}

/// An ACP holding `count` registered documents, all owned by [`owner`].
async fn fixture(count: usize) -> LocalDocumentACP {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let owner = owner();
    for index in 0..count {
        acp.register_doc_object(&owner, POLICY, RESOURCE, &doc_id(index))
            .await
            .expect("the document to register");
    }
    acp
}

/// One check, against how many documents the store already holds. A per-check
/// cost that grows with the store is the difference between a node that scales
/// and one that does not.
fn single_check(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let allowed = Identity::authenticated(owner());
    let denied = Identity::authenticated(stranger());

    let mut group = c.benchmark_group("acp_check");
    for count in REGISTERED {
        let acp = rt.block_on(fixture(count));
        // A document from the middle of the set, so neither the first nor the
        // last insertion order is what gets measured.
        let target = doc_id(count / 2);
        group.bench_with_input(BenchmarkId::new("allow", count), &target, |b, target| {
            b.iter(|| {
                let ok = rt
                    .block_on(acp.check_doc_access(
                        &allowed,
                        DocumentPermission::Read,
                        POLICY,
                        RESOURCE,
                        black_box(target),
                    ))
                    .expect("the check to answer");
                assert!(ok, "the owner must be allowed to read their own document");
                black_box(ok)
            })
        });
        group.bench_with_input(BenchmarkId::new("deny", count), &target, |b, target| {
            b.iter(|| {
                let ok = rt
                    .block_on(acp.check_doc_access(
                        &denied,
                        DocumentPermission::Read,
                        POLICY,
                        RESOURCE,
                        black_box(target),
                    ))
                    .expect("the check to answer");
                assert!(!ok, "a stranger must not be allowed to read the document");
                black_box(ok)
            })
        });
    }
    group.finish();
}

/// What a result set costs. This is the row that matters: it is the same check
/// as above, multiplied by how many documents a query returned.
fn result_set(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let allowed = Identity::authenticated(owner());
    let acp = rt.block_on(fixture(*FANOUT.last().expect("a fan-out size")));

    let mut group = c.benchmark_group("acp_result_set");
    group.sample_size(20);
    for count in FANOUT {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                rt.block_on(async {
                    for index in 0..count {
                        black_box(
                            acp.check_doc_access(
                                &allowed,
                                DocumentPermission::Read,
                                POLICY,
                                RESOURCE,
                                &doc_id(index),
                            )
                            .await
                            .expect("the check to answer"),
                        );
                    }
                })
            })
        });
    }
    group.finish();
}

/// Registration, which every document create pays once when ACP is on.
fn registration(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let owner = owner();
    let mut group = c.benchmark_group("acp_register");
    let mut seq = 0usize;
    group.bench_function("register_doc_object", |b| {
        b.iter_batched_ref(
            || rt.block_on(fixture(0)),
            |acp| {
                seq += 1;
                rt.block_on(acp.register_doc_object(&owner, POLICY, RESOURCE, &doc_id(seq)))
                    .expect("the document to register")
            },
            criterion::BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, single_check, result_set, registration);
criterion_main!(benches);
