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
use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};
use document::{DocID, Document, NormalValue};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;
use storage::Store;

const SCHEMA_VERSION_ID: &str = "v1";
const COLLECTION_ID: &str = "col-users";
const CREATOR: &str = "did:key:z6MkrMergeDagBench";
const DEPTHS: [usize; 3] = [1, 8, 64];
const FIELD_COUNTS: [usize; 2] = [1, 4];

type BenchHandler = DbMergeHandler<MemoryStore, DefraBlockstore<MemoryStore>>;

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

async fn make_handler(field_count: usize) -> (BenchHandler, Arc<DefraBlockstore<MemoryStore>>) {
    let store = Arc::new(MemoryStore::new());
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());
    db.create_collection(collection_version(field_count))
        .await
        .unwrap();
    let blockstore = Arc::new(DefraBlockstore::new(store, false));
    let handler = DbMergeHandler::new(db, blockstore.clone());
    (handler, blockstore)
}

fn make_document(field_count: usize, revision: usize) -> Document {
    let mut doc = Document::new();
    for name in field_names(field_count) {
        doc.set(&name, NormalValue::String(format!("{name}-rev{revision}")));
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
    let txn = MemoryStore::new().new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
    let headstore = NamespaceView::new(shared.clone(), Namespace::Headstore);
    let identity = db_blocks::DocStorageIdentity::new(1, 1);
    let modified: HashSet<String> = field_names(field_count).into_iter().collect();

    let mut doc = make_document(field_count, 0);
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
        doc = make_document(field_count, revision);
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
async fn seed_handler(dag: &SyntheticDag, field_count: usize) -> BenchHandler {
    let (handler, blockstore) = make_handler(field_count).await;
    for (cid, bytes) in &dag.blocks {
        blockstore.put(cid, bytes).await.unwrap();
    }
    handler
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

criterion_group!(benches, bench_merge_dag);
criterion_main!(benches);
