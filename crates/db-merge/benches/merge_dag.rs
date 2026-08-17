//! Merge-side DAG walk benchmarks.
//!
//! Times [`DbMergeHandler::handle_block`] on a composite tip whose ancestry is
//! present in the blockstore but unmerged, which drives
//! `process_composite_delta` -> `prepare_composite_merge` -> head advance in
//! `composite_heads.rs`. Parameterized by DAG depth and per-composite field
//! link count.
//!
//! The ancestry is produced by the real write path (`write_document_blocks` on a
//! separate source store) and copied into the target blockstore, so the blocks
//! are wire-identical to replicated ones.
//!
//! The merge-depth policy caps the walk; the depths benched here stay under the
//! default so the measurement is of the walk itself rather than the guard.

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datastore::{Namespace, NamespaceView, SharedTxn};
use db::DB;
use db_merge::DbMergeHandler;
use defra_core::merge::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome};
use document::{DocID, Document, NormalValue};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;
use storage::Store;

const SCHEMA_VERSION_ID: &str = "v1";
const COLLECTION_ID: &str = "col-users";
const CREATOR: &str = "did:key:z6MkrMergeDagBench";
const DEPTHS: [usize; 3] = [1, 8, 64];
const FIELD_COUNTS: [usize; 2] = [1, 4];
const BATCH_ROOT_COUNTS: [usize; 3] = [1, 8, 32];

type BenchHandler<S> = DbMergeHandler<S, DefraBlockstore<S>>;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

fn field_names(field_count: usize) -> Vec<String> {
    (0..field_count).map(|i| format!("field_{i}")).collect()
}

fn collection_version(field_count: usize) -> CollectionVersion {
    let mut fields = vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())];
    for (index, name) in field_names(field_count).iter().enumerate() {
        fields.push(FieldDescription::new(
            (index + 2).to_string(),
            name,
            FieldKind::string(),
        ));
    }
    CollectionVersion::new("Users", SCHEMA_VERSION_ID, COLLECTION_ID, fields)
}

async fn make_handler_with_store<S: Store + 'static>(
    field_count: usize,
    store: Arc<S>,
) -> (BenchHandler<S>, Arc<DefraBlockstore<S>>) {
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());
    db.create_collection(collection_version(field_count))
        .await
        .unwrap();
    let blockstore = Arc::new(DefraBlockstore::new(store, false));
    let handler = DbMergeHandler::new(db, blockstore.clone());
    (handler, blockstore)
}

async fn make_handler(
    field_count: usize,
) -> (BenchHandler<MemoryStore>, Arc<DefraBlockstore<MemoryStore>>) {
    make_handler_with_store(field_count, Arc::new(MemoryStore::new())).await
}

fn make_document(field_count: usize, revision: usize, document_index: usize) -> Document {
    let mut doc = Document::new();
    for name in field_names(field_count) {
        doc.set(
            &name,
            NormalValue::String(format!("{name}-doc{document_index}-rev{revision}")),
        );
    }
    doc
}

/// A composite tip plus every ancestor block needed to merge it.
struct SyntheticDag {
    tip_cid: Cid,
    tip_bytes: Vec<u8>,
    doc_id: String,
    blocks: Vec<(Cid, Vec<u8>)>,
}

/// Build a linear composite chain of `depth` revisions on a throwaway store,
/// then collect every block so it can be replayed into a fresh handler.
async fn build_dag(depth: usize, field_count: usize) -> SyntheticDag {
    build_dag_for_document(depth, field_count, 0).await
}

async fn build_dag_for_document(
    depth: usize,
    field_count: usize,
    document_index: usize,
) -> SyntheticDag {
    let txn = MemoryStore::new().new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
    let headstore = NamespaceView::new(shared.clone(), Namespace::Headstore);
    let identity = db_blocks::DocStorageIdentity::new(1, 1);
    let modified: HashSet<String> = field_names(field_count).into_iter().collect();

    let mut doc = make_document(field_count, 0, document_index);
    let mut result = db_blocks::write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        SCHEMA_VERSION_ID,
        identity,
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let doc_id = result.doc_id.clone();
    let mut cids: Vec<Cid> = vec![result.cid];
    cids.extend(result.field_cids.iter().copied());

    for revision in 1..depth {
        doc = make_document(field_count, revision, document_index);
        doc.set_id(DocID::from_string(&doc_id).unwrap());
        result = db_blocks::write_document_blocks(
            &blockstore,
            &headstore,
            &doc,
            SCHEMA_VERSION_ID,
            identity,
            Some(&modified),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        cids.push(result.cid);
        cids.extend(result.field_cids.iter().copied());
    }

    let mut blocks = Vec::with_capacity(cids.len());
    for cid in &cids {
        let bytes = blockstore
            .get(&cid.to_bytes())
            .await
            .unwrap()
            .expect("block written by the write path must be readable");
        blocks.push((*cid, bytes));
    }

    SyntheticDag {
        tip_cid: result.cid,
        tip_bytes: result.block,
        doc_id,
        blocks,
    }
}

/// A fresh handler whose blockstore holds `dag`'s blocks but which has merged none
/// of them, so `handle_block` on the tip performs the full walk.
async fn seed_handler(dag: &SyntheticDag, field_count: usize) -> BenchHandler<MemoryStore> {
    seed_handler_many(std::slice::from_ref(dag), field_count).await
}

async fn seed_handler_many(dags: &[SyntheticDag], field_count: usize) -> BenchHandler<MemoryStore> {
    let (handler, blockstore) = make_handler(field_count).await;
    for dag in dags {
        for (cid, bytes) in &dag.blocks {
            blockstore.put(cid, bytes).await.unwrap();
        }
    }
    handler
}

#[cfg(feature = "bench-redb")]
struct RedbBenchFixture {
    handler: BenchHandler<storage::RedbStore>,
    _directory: tempfile::TempDir,
}

#[cfg(feature = "bench-redb")]
async fn seed_redb_handler_many(dags: &[SyntheticDag], field_count: usize) -> RedbBenchFixture {
    let directory = tempfile::tempdir().unwrap();
    let store =
        Arc::new(storage::RedbStore::open(directory.path().join("merge-bench.redb")).unwrap());
    let (handler, blockstore) = make_handler_with_store(field_count, store).await;
    for dag in dags {
        for (cid, bytes) in &dag.blocks {
            blockstore.put(cid, bytes).await.unwrap();
        }
    }
    RedbBenchFixture {
        handler,
        _directory: directory,
    }
}

fn merge_block(dag: &SyntheticDag) -> MergeBlock {
    MergeBlock {
        cid: dag.tip_cid,
        block_data: dag.tip_bytes.clone().into(),
        doc_id: dag.doc_id.clone(),
        collection_id: COLLECTION_ID.to_string(),
        creator: CREATOR.to_string(),
        sender_peer: None,
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    }
}

fn bench_merge_dag(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("merge_dag");
    group.sample_size(10);

    for field_count in FIELD_COUNTS {
        for depth in DEPTHS {
            let dag = rt.block_on(build_dag(depth, field_count));
            let label = format!("depth{depth}_fields{field_count}");
            group.throughput(Throughput::Elements(depth as u64));

            group.bench_function(BenchmarkId::from_parameter(&label), |b| {
                // `_ref`: the handler owns the DB and blockstore holding every block in
                // the DAG, so moving it into the routine would put its teardown inside
                // the measurement.
                b.iter_batched_ref(
                    || rt.block_on(seed_handler(&dag, field_count)),
                    |handler| {
                        rt.block_on(async {
                            let metadata = BlockMetadata::normal(
                                &dag.doc_id,
                                COLLECTION_ID,
                                CREATOR,
                                None,
                                false,
                            );
                            let outcome = handler
                                .handle_block(&dag.tip_cid, &dag.tip_bytes, metadata)
                                .await
                                .unwrap();
                            // A skipped or rejected block does no walk, so timing it
                            // would report a merge cost that was never paid.
                            assert!(
                                matches!(outcome, MergeOutcome::Merged),
                                "tip was not merged: {outcome:?}"
                            );
                            black_box(outcome)
                        })
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

/// Compare the two production merge entry points under an ordered receiver
/// workload. Both preserve the per-document single-writer boundary; the batch
/// path amortizes transaction creation and commit across independent roots.
fn bench_merge_transactions(c: &mut Criterion) {
    const DEPTH: usize = 8;
    const FIELD_COUNT: usize = 4;

    let rt = runtime();
    let mut group = c.benchmark_group("merge_transactions");
    group.sample_size(10);

    for root_count in BATCH_ROOT_COUNTS {
        let dags: Vec<_> = rt.block_on(async {
            let mut dags = Vec::with_capacity(root_count);
            for document_index in 0..root_count {
                dags.push(build_dag_for_document(DEPTH, FIELD_COUNT, document_index).await);
            }
            dags
        });
        let blocks: Vec<_> = dags.iter().map(merge_block).collect();
        let label = format!("roots{root_count}_depth{DEPTH}_fields{FIELD_COUNT}");
        group.throughput(Throughput::Elements(root_count as u64));

        group.bench_with_input(
            BenchmarkId::new("sequential_transactions", &label),
            &root_count,
            |b, _| {
                b.iter_batched_ref(
                    || rt.block_on(seed_handler_many(&dags, FIELD_COUNT)),
                    |handler| {
                        rt.block_on(async {
                            for block in &blocks {
                                let metadata = BlockMetadata::normal(
                                    &block.doc_id,
                                    &block.collection_id,
                                    &block.creator,
                                    None,
                                    false,
                                );
                                let outcome = handler
                                    .handle_block(&block.cid, &block.block_data, metadata)
                                    .await
                                    .unwrap();
                                assert!(matches!(outcome, MergeOutcome::Merged));
                            }
                        })
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ordered_batch_transaction", &label),
            &root_count,
            |b, _| {
                b.iter_batched_ref(
                    || rt.block_on(seed_handler_many(&dags, FIELD_COUNT)),
                    |handler| {
                        rt.block_on(async {
                            let outcomes = handler.handle_block_batch(&blocks).await;
                            assert_eq!(outcomes.len(), blocks.len());
                            assert!(outcomes
                                .into_iter()
                                .all(|outcome| { matches!(outcome, Ok(MergeOutcome::Merged)) }));
                        })
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

#[cfg(feature = "bench-redb")]
fn bench_merge_transactions_redb(c: &mut Criterion) {
    const DEPTH: usize = 8;
    const FIELD_COUNT: usize = 4;

    let rt = runtime();
    let mut group = c.benchmark_group("merge_transactions_redb");
    group.sample_size(10);

    for root_count in BATCH_ROOT_COUNTS {
        let dags: Vec<_> = rt.block_on(async {
            let mut dags = Vec::with_capacity(root_count);
            for document_index in 0..root_count {
                dags.push(build_dag_for_document(DEPTH, FIELD_COUNT, document_index).await);
            }
            dags
        });
        let blocks: Vec<_> = dags.iter().map(merge_block).collect();
        let label = format!("roots{root_count}_depth{DEPTH}_fields{FIELD_COUNT}");
        group.throughput(Throughput::Elements(root_count as u64));

        group.bench_with_input(
            BenchmarkId::new("sequential_transactions", &label),
            &root_count,
            |b, _| {
                b.iter_batched_ref(
                    || rt.block_on(seed_redb_handler_many(&dags, FIELD_COUNT)),
                    |fixture| {
                        rt.block_on(async {
                            for block in &blocks {
                                let metadata = BlockMetadata::normal(
                                    &block.doc_id,
                                    &block.collection_id,
                                    &block.creator,
                                    None,
                                    false,
                                );
                                let outcome = fixture
                                    .handler
                                    .handle_block(&block.cid, &block.block_data, metadata)
                                    .await
                                    .unwrap();
                                assert!(matches!(outcome, MergeOutcome::Merged));
                            }
                        })
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ordered_batch_transaction", &label),
            &root_count,
            |b, _| {
                b.iter_batched_ref(
                    || rt.block_on(seed_redb_handler_many(&dags, FIELD_COUNT)),
                    |fixture| {
                        rt.block_on(async {
                            let outcomes = fixture.handler.handle_block_batch(&blocks).await;
                            assert_eq!(outcomes.len(), blocks.len());
                            assert!(outcomes
                                .into_iter()
                                .all(|outcome| { matches!(outcome, Ok(MergeOutcome::Merged)) }));
                        })
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

#[cfg(not(feature = "bench-redb"))]
fn bench_merge_transactions_redb(_c: &mut Criterion) {}

criterion_group!(
    benches,
    bench_merge_dag,
    bench_merge_transactions,
    bench_merge_transactions_redb
);
criterion_main!(benches);
