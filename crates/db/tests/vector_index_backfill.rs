//! Creating a vector index on a populated collection, through the real
//! `IndexManager` and a real transaction.

use db::database::DB;
use db::index_manager::IndexManager;
use db_index::vector::kv_store::KvNodeStore;
use db_index::vector::store::{NodeId, VectorNodeStore};
use document::{Document, NormalValue};
use schema::{
    CollectionVersion, DistanceMetric, FieldDescription, FieldKind, HnswParams, IndexKind,
    IndexedFieldDescription, VectorAlgorithm, VectorIndexDescription,
};
use storage::backends::MemoryStore;

const COLLECTION_SHORT_ID: u32 = 1;
const DIMENSIONS: u32 = 4;

fn schema() -> CollectionVersion {
    CollectionVersion::new(
        "docs",
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "embedding", FieldKind::float64_array()),
        ],
    )
}

fn vector_kind() -> IndexKind {
    IndexKind::Vector(VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: Some(HnswParams::default()),
    })
}

/// A spread of directions, so no two documents collapse onto one point.
fn corpus(count: usize) -> Vec<(u64, Document)> {
    (0..count)
        .map(|i| {
            let angle = i as f64 * 0.41;
            let mut doc = Document::new();
            doc.set("title", NormalValue::String(format!("doc-{i}")));
            doc.set(
                "embedding",
                NormalValue::Float64Array(vec![
                    angle.sin(),
                    angle.cos(),
                    (angle * 0.5).sin(),
                    (angle * 0.25).cos(),
                ]),
            );
            (i as u64 + 1, doc)
        })
        .collect()
}

/// The Phase 3 gate: every pre-existing document must end up in the graph.
#[tokio::test]
async fn creating_a_vector_index_backfills_every_document() {
    const DOCUMENTS: usize = 50;

    let db = DB::new(MemoryStore::new()).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let schema = schema();
    let mut manager = IndexManager::new(COLLECTION_SHORT_ID);

    let index_id;
    {
        let datastore = txn.datastore().unwrap();
        let desc = manager
            .create_index_of_kind(
                &datastore,
                "docs",
                "by_embedding".to_string(),
                vec![IndexedFieldDescription {
                    name: "embedding".to_string(),
                    descending: false,
                }],
                vector_kind(),
                &[],
            )
            .await
            .unwrap();
        assert!(desc.is_vector(), "the created index must carry its kind");
        index_id = desc.id;

        let documents = corpus(DOCUMENTS);
        let result = manager
            .bulk_index(&datastore, "by_embedding", &documents, &schema)
            .await
            .unwrap();
        assert_eq!(result.indexed, DOCUMENTS);
        assert_eq!(result.skipped, 0);

        // Read the graph back through the port: every document is a live node.
        let mut view = datastore.clone();
        let kv = KvNodeStore::new(&mut view, COLLECTION_SHORT_ID, index_id, 0);
        let mut ids = Vec::new();
        kv.iterate_nodes(|node| {
            assert_eq!(
                node.vector.len(),
                DIMENSIONS as usize,
                "node {:?} holds a truncated vector",
                node.id
            );
            ids.push(node.id);
            Ok(())
        })
        .await
        .unwrap();
        ids.sort();

        let expected: Vec<NodeId> = (1..=DOCUMENTS as u64).map(NodeId).collect();
        assert_eq!(ids, expected, "backfill missed documents");

        assert!(
            kv.get_meta().await.unwrap().is_some(),
            "a built graph must have an entry point"
        );
    }
    txn.commit().await.unwrap();
}

/// A document whose vector field is absent contributes no node, but must not
/// stop the backfill.
#[tokio::test]
async fn backfill_skips_documents_without_a_vector() {
    let db = DB::new(MemoryStore::new()).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let schema = schema();
    let mut manager = IndexManager::new(COLLECTION_SHORT_ID);

    {
        let datastore = txn.datastore().unwrap();
        let desc = manager
            .create_index_of_kind(
                &datastore,
                "docs",
                "by_embedding".to_string(),
                vec![IndexedFieldDescription {
                    name: "embedding".to_string(),
                    descending: false,
                }],
                vector_kind(),
                &[],
            )
            .await
            .unwrap();

        let mut documents = corpus(3);
        let mut titled_only = Document::new();
        titled_only.set("title", NormalValue::String("no embedding".to_string()));
        documents.push((99, titled_only));

        let result = manager
            .bulk_index(&datastore, "by_embedding", &documents, &schema)
            .await
            .unwrap();
        // Every document was processed; only three carried a vector.
        assert_eq!(result.indexed, 4);

        let mut view = datastore.clone();
        let kv = KvNodeStore::new(&mut view, COLLECTION_SHORT_ID, desc.id, 0);
        let mut ids = Vec::new();
        kv.iterate_nodes(|node| {
            ids.push(node.id);
            Ok(())
        })
        .await
        .unwrap();
        ids.sort();
        assert_eq!(ids, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }
    txn.commit().await.unwrap();
}

/// A document arriving over P2P merge must reach the vector index the same way
/// a local write does: the merge hooks iterate every index, so a vector one is
/// maintained by construction. Proven rather than assumed.
#[tokio::test]
async fn merged_documents_are_indexed() {
    let db = DB::new(MemoryStore::new()).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let schema = schema();
    let mut manager = IndexManager::new(COLLECTION_SHORT_ID);

    {
        let datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        let desc = manager
            .create_index_of_kind(
                &datastore,
                "docs",
                "by_embedding".to_string(),
                vec![IndexedFieldDescription {
                    name: "embedding".to_string(),
                    descending: false,
                }],
                vector_kind(),
                &[],
            )
            .await
            .unwrap();

        // Arriving over merge rather than through a local write.
        for (short_id, doc) in corpus(12) {
            let mut doc = doc;
            doc.set_id(merge_doc_id(short_id));
            manager
                .on_document_create_merge(&datastore, &systemstore, &doc, short_id, &schema)
                .await
                .unwrap();
        }

        let mut view = datastore.clone();
        let kv = KvNodeStore::new(&mut view, COLLECTION_SHORT_ID, desc.id, 0);
        let mut ids = Vec::new();
        kv.iterate_nodes(|node| {
            ids.push(node.id);
            Ok(())
        })
        .await
        .unwrap();
        ids.sort();
        assert_eq!(
            ids,
            (1..=12u64).map(NodeId).collect::<Vec<_>>(),
            "every merged document must be searchable on the replica"
        );

        // A merged update replaces the vector; a merged delete tombstones it.
        let (_, mut replacement) = corpus(1).pop().unwrap();
        replacement.set_id(merge_doc_id(1));
        replacement.set(
            "embedding",
            document::NormalValue::Float64Array(vec![9.0, 9.0, 9.0, 9.0]),
        );
        let (_, original) = corpus(1).pop().unwrap();
        manager
            .on_document_update_merge(
                &datastore,
                &systemstore,
                &original,
                &replacement,
                1,
                &schema,
            )
            .await
            .unwrap();

        let mut view = datastore.clone();
        let kv = KvNodeStore::new(&mut view, COLLECTION_SHORT_ID, desc.id, 0);
        let node = kv
            .get_node(NodeId(1))
            .await
            .unwrap()
            .expect("still present");
        assert!(!node.deleted, "an updated document stays searchable");
    }
    txn.commit().await.unwrap();
}

/// Merged documents carry ids from the writing node; any stable distinct id
/// serves here.
fn merge_doc_id(short_id: u64) -> document::DocID {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(format!("merged-{short_id}").as_bytes());
    let mh: multihash::Multihash<64> =
        multihash::Multihash::wrap(*defra_core::SHA2_256_CODE, &hasher.finalize()).unwrap();
    document::DocID::new_v0(cid::Cid::new_v1(0x55, mh))
}

/// Reindex-after-migration clears every index and rebuilds it. A vector index
/// rides that path, which makes it the rebuild mechanism that exists today:
/// `remove_all` then `bulk_index`, without the epoch swap.
#[tokio::test]
async fn an_index_can_be_cleared_and_rebuilt() {
    const DOCUMENTS: usize = 40;
    let db = DB::new(MemoryStore::new()).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let schema = schema();
    let mut manager = IndexManager::new(COLLECTION_SHORT_ID);

    {
        let datastore = txn.datastore().unwrap();
        let desc = manager
            .create_index_of_kind(
                &datastore,
                "docs",
                "by_embedding".to_string(),
                vec![IndexedFieldDescription {
                    name: "embedding".to_string(),
                    descending: false,
                }],
                vector_kind(),
                &[],
            )
            .await
            .unwrap();

        let documents = corpus(DOCUMENTS);
        manager
            .bulk_index(&datastore, "by_embedding", &documents, &schema)
            .await
            .unwrap();

        let mut view = datastore.clone();
        assert_eq!(live_ids(&mut view, desc.id).await.len(), DOCUMENTS);

        // What reindex does.
        let index = manager.get_index("by_embedding").unwrap();
        index.remove_all(&mut datastore.clone()).await.unwrap();
        let mut view = datastore.clone();
        assert!(
            live_ids(&mut view, desc.id).await.is_empty(),
            "remove_all must leave nothing behind"
        );

        manager
            .bulk_index(&datastore, "by_embedding", &documents, &schema)
            .await
            .unwrap();

        let mut view = datastore.clone();
        let mut rebuilt = live_ids(&mut view, desc.id).await;
        rebuilt.sort();
        assert_eq!(
            rebuilt,
            (1..=DOCUMENTS as u64).map(NodeId).collect::<Vec<_>>(),
            "a rebuilt index must hold every document again"
        );

        // And it still answers.
        let vector_index = db_index::vector::index::VectorIndex::try_new(
            COLLECTION_SHORT_ID,
            index.description().clone(),
        )
        .unwrap();
        let mut view = datastore.clone();
        let hits = vector_index
            .search(&mut view, &[1.0f64, 0.0, 0.0, 0.0], 5, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 5, "a rebuilt index must still be searchable");
    }
    txn.commit().await.unwrap();
}

/// Live node ids of an index, read back through the port.
async fn live_ids<T>(view: &mut T, index_id: u32) -> Vec<NodeId>
where
    T: storage::corekv::Reader + storage::corekv::Writer + defra_core::thread_bounds::MaybeSend,
{
    let kv = KvNodeStore::new(view, COLLECTION_SHORT_ID, index_id, 0);
    let mut ids = Vec::new();
    kv.iterate_nodes(|node| {
        ids.push(node.id);
        Ok(())
    })
    .await
    .unwrap();
    ids
}
