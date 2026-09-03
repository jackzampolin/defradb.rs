//! #1463: a schema selecting a trainable algorithm must actually train once
//! the corpus warrants it, through the production write paths, not only when
//! a test calls `build()` directly.

use db::database::DB;
use db::index::manager::IndexManager;
use db::index::manager::SliceSource;
use db::index::vector::engine::ivfpq::IvfPq;
use db::index::vector::engine::ivfpq::IvfPqParams;
use db::index::vector::engine::ivfpq::TRAIN_PER_LIST;
use db::index::vector::index::VectorIndex;
use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::store::NodeId;
use defra_core::thread_bounds::MaybeSend;
use defra_core::vector::Metric;
use document::Document;
use document::NormalValue;
use schema::CollectionVersion;
use schema::DistanceMetric;
use schema::FieldDescription;
use schema::FieldKind;
use schema::IndexDescription;
use schema::IndexKind;
use schema::IndexedFieldDescription;
use schema::VectorAlgorithm;
use schema::VectorIndexDescription;
use storage::corekv::Reader;
use storage::corekv::Store;
use storage::corekv::Txn;
use storage::corekv::Writer;
use storage::index::CollectionIndex;
use storage::RegolithStore;

const COLLECTION: u32 = 71;
const INDEX_ID: u32 = 5;
const DIMENSIONS: u32 = 16;
const NLIST: u32 = 8;
const SEED: u64 = 0x1463_1463;

fn ivfpq_vector_description() -> VectorIndexDescription {
    VectorIndexDescription {
        algorithm: VectorAlgorithm::IvfPq,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: None,
        ivfpq: Some(schema::IvfPqParams {
            nlist: NLIST,
            nprobe: NLIST,
            m: 4,
            ..schema::IvfPqParams::default()
        }),
        ivfflat: None,
        ssg: None,
    }
}

fn hnsw_vector_description() -> VectorIndexDescription {
    VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: Some(schema::HnswParams::default()),
        ivfpq: None,
        ivfflat: None,
        ssg: None,
    }
}

fn description(id: u32, vector: VectorIndexDescription) -> IndexDescription {
    IndexDescription {
        name: "by_embedding".to_string(),
        id,
        fields: vec![IndexedFieldDescription {
            name: "embedding".to_string(),
            descending: false,
        }],
        unique: false,
        kind: None,
        auto_generated: false,
    }
    .as_vector(vector)
}

fn schema() -> CollectionVersion {
    CollectionVersion::new(
        "docs",
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "embedding", FieldKind::float64_array()),
        ],
    )
}

async fn txn(store: &RegolithStore) -> Box<dyn Txn> {
    store.new_txn(false).await.unwrap()
}

fn wide(vector: &[f32]) -> Vec<NormalValue> {
    vec![NormalValue::Float64Array(
        vector.iter().map(|x| *x as f64).collect(),
    )]
}

fn document(vector: &[f32]) -> Document {
    let mut doc = Document::new();
    doc.set(
        "embedding",
        NormalValue::Float64Array(vector.iter().map(|x| *x as f64).collect()),
    );
    doc
}

/// Reads trained state directly off whatever transaction wrote it: a raw
/// store transaction for the direct-save path, a `NamespaceView` for the
/// manager path. `is_trained` needs none of `IvfPq`'s own configuration, only
/// the store location, so any valid params construct a throwaway reader.
async fn is_trained<T: Reader + Writer + MaybeSend>(txn: &mut T, index_id: u32) -> bool {
    let kv = KvNodeStore::new(txn, COLLECTION, index_id, 0);
    let engine = IvfPq::try_new(kv, Metric::Cosine, IvfPqParams::default(), 0).unwrap();
    engine.is_trained().await.unwrap()
}

/// How many of `sample` find themselves in their own top 5, through a fresh
/// read transaction.
async fn self_match_count(
    store: &RegolithStore,
    index: &VectorIndex,
    vectors: &[Vec<f32>],
    sample: &[usize],
) -> usize {
    let mut read = txn(store).await;
    let mut found = 0usize;
    for &i in sample {
        let hits = index
            .search(&mut read, vectors[i].as_slice(), 5, None)
            .await
            .unwrap();
        if hits.iter().any(|h| h.id == NodeId(i as u64 + 1)) {
            found += 1;
        }
    }
    found
}

/// The regression #1463 closes: an IVF-PQ description must end up trained
/// through ordinary document writes, with nothing in this test calling
/// `build()`. Searches stay correct on both sides of the threshold, so
/// crossing it is not observable as a wrong answer.
#[tokio::test]
async fn training_triggers_through_vector_index_save() {
    let threshold = u64::from(NLIST) * u64::from(TRAIN_PER_LIST);
    let below = threshold as usize - 12;
    let corpus_size = threshold as usize + 88;

    let store = RegolithStore::in_memory().unwrap();
    let index = VectorIndex::try_new(
        COLLECTION,
        description(INDEX_ID, ivfpq_vector_description()),
    )
    .unwrap();

    let mut corpus = crate::support::Corpus::new(SEED ^ 0x01);
    let vectors = corpus.clustered(corpus_size, DIMENSIONS as usize, 8, 0.1);

    let mut write = txn(&store).await;
    for (i, vector) in vectors[..below].iter().enumerate() {
        index
            .save(&mut write, i as u64 + 1, &wide(vector))
            .await
            .unwrap();
    }
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    assert!(
        !is_trained(&mut read, INDEX_ID).await,
        "must not train before the threshold"
    );
    let hits = index
        .search(&mut read, vectors[3].as_slice(), 1, None)
        .await
        .unwrap();
    assert_eq!(
        hits[0].id,
        NodeId(4),
        "an untrained index is an exact scan, so a vector must find itself first"
    );

    let mut write = txn(&store).await;
    for (i, vector) in vectors[below..].iter().enumerate() {
        index
            .save(&mut write, (below + i) as u64 + 1, &wide(vector))
            .await
            .unwrap();
    }
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    assert!(
        is_trained(&mut read, INDEX_ID).await,
        "#1463: no test call to build() here, so the write path must trigger it"
    );

    let sample = [0usize, 100, 250, 350, corpus_size - 1];
    let found = self_match_count(&store, &index, &vectors, &sample).await;
    assert!(
        found >= 4,
        "a trained index must still find most vectors' own documents: {found}/{}",
        sample.len()
    );
}

/// The same regression, through the index manager's bulk backfill path.
#[tokio::test]
async fn training_triggers_through_bulk_index_from() {
    let threshold = u64::from(NLIST) * u64::from(TRAIN_PER_LIST);
    let corpus_size = threshold as usize + 88;

    let mut corpus = crate::support::Corpus::new(SEED ^ 0x02);
    let vectors = corpus.clustered(corpus_size, DIMENSIONS as usize, 8, 0.1);
    let documents: Vec<(u64, Document)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64 + 1, document(v)))
        .collect();

    let store = RegolithStore::in_memory().unwrap();
    let db = DB::new(store.clone()).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(COLLECTION);
    let desc = manager
        .create_index_of_kind(
            &datastore,
            "docs",
            "by_embedding".to_string(),
            vec![IndexedFieldDescription {
                name: "embedding".to_string(),
                descending: false,
            }],
            IndexKind::Vector(ivfpq_vector_description()),
            &schema().fields,
        )
        .await
        .unwrap();

    let mut source = SliceSource::new(&documents);
    let result = manager
        .bulk_index_from(&datastore, "by_embedding", &mut source, &schema())
        .await
        .unwrap();
    assert_eq!(result.indexed, documents.len());

    // A commit refuses while any `NamespaceView` still references the shared
    // transaction, so this must go before it.
    drop(datastore);
    txn.commit().await.unwrap();

    let read_txn = db.new_txn(false).await.unwrap();
    let mut read_datastore = read_txn.datastore().unwrap();
    assert!(
        is_trained(&mut read_datastore, desc.id).await,
        "#1463: no test call to build() here, so bulk_index_from must trigger it"
    );
}

/// HNSW has nothing to train, so it must never observably "build", and must
/// keep answering correctly across the same corpus size that trains IVF-PQ.
#[tokio::test]
async fn hnsw_never_builds_and_keeps_answering() {
    let threshold = u64::from(NLIST) * u64::from(TRAIN_PER_LIST);
    let below = threshold as usize - 12;
    let corpus_size = threshold as usize + 88;

    let store = RegolithStore::in_memory().unwrap();
    let index =
        VectorIndex::try_new(COLLECTION, description(INDEX_ID, hnsw_vector_description())).unwrap();

    let mut corpus = crate::support::Corpus::new(SEED ^ 0x03);
    let vectors = corpus.clustered(corpus_size, DIMENSIONS as usize, 8, 0.1);

    let mut write = txn(&store).await;
    for (i, vector) in vectors[..below].iter().enumerate() {
        index
            .save(&mut write, i as u64 + 1, &wide(vector))
            .await
            .unwrap();
    }
    write.commit().await.unwrap();

    let early_sample = [0usize, 50, below - 1];
    let found = self_match_count(&store, &index, &vectors, &early_sample).await;
    assert_eq!(
        found,
        early_sample.len(),
        "HNSW recall dropped below the IVF-PQ threshold size: {found}/{}",
        early_sample.len()
    );

    let mut write = txn(&store).await;
    for (i, vector) in vectors[below..].iter().enumerate() {
        index
            .save(&mut write, (below + i) as u64 + 1, &wide(vector))
            .await
            .unwrap();
    }
    write.commit().await.unwrap();

    let late_sample = [0usize, 100, 250, 350, corpus_size - 1];
    let found = self_match_count(&store, &index, &vectors, &late_sample).await;
    assert!(
        found >= 4,
        "HNSW recall dropped past the IVF-PQ threshold size: {found}/{}",
        late_sample.len()
    );
}
