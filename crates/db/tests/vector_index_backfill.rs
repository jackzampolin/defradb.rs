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
