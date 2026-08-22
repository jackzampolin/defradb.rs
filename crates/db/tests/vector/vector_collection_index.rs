//! The vector index seen through `CollectionIndex`: what a document write does
//! to the graph.

use db::index::vector::index::VectorIndex;
use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::store::NodeId;
use db::index::vector::store::VectorNodeStore;
use document::NormalValue;
use schema::DistanceMetric;
use schema::HnswParams;
use schema::IndexDescription;
use schema::IndexedFieldDescription;
use schema::VectorAlgorithm;
use schema::VectorIndexDescription;
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::corekv::Txn;
use storage::index::CollectionIndex;

const COLLECTION: u32 = 7;
const DIMENSIONS: u32 = 4;

fn vector_config(dimensions: u32) -> VectorIndexDescription {
    VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ssg: None,
    }
}

fn description(dimensions: u32) -> IndexDescription {
    IndexDescription {
        name: "by_embedding".to_string(),
        id: 3,
        fields: vec![IndexedFieldDescription {
            name: "embedding".to_string(),
            descending: false,
        }],
        unique: false,
        kind: None,
        auto_generated: false,
    }
    .as_vector(vector_config(dimensions))
}

fn index() -> VectorIndex {
    VectorIndex::try_new(COLLECTION, description(DIMENSIONS)).expect("a valid vector description")
}

async fn txn(store: &MemoryStore) -> Box<dyn Txn> {
    store.new_txn(false).await.unwrap()
}

fn wide(values: &[f64]) -> Vec<NormalValue> {
    vec![NormalValue::Float64Array(values.to_vec())]
}

fn narrow(values: &[f32]) -> Vec<NormalValue> {
    vec![NormalValue::Float32Array(values.to_vec())]
}

/// How many live nodes the index holds, read back through a fresh transaction.
async fn live_ids(store: &MemoryStore, index: &VectorIndex) -> Vec<NodeId> {
    let mut read = txn(store).await;
    let kv = KvNodeStore::new(&mut read, COLLECTION, index.description().id, 0);
    let mut ids = Vec::new();
    kv.iterate_nodes(|node| {
        ids.push(node.id);
        Ok(())
    })
    .await
    .unwrap();
    ids.sort();
    ids
}

#[tokio::test]
async fn a_saved_document_becomes_a_searchable_node() {
    let store = MemoryStore::new();
    let index = index();

    let mut write = txn(&store).await;
    for (id, values) in [
        (1u64, wide(&[1.0, 0.0, 0.0, 0.0])),
        (2, wide(&[0.0, 1.0, 0.0, 0.0])),
        (3, wide(&[0.9, 0.1, 0.0, 0.0])),
    ] {
        index.save(&mut write, id, &values).await.unwrap();
    }
    write.commit().await.unwrap();

    assert_eq!(
        live_ids(&store, &index).await,
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );
}

/// Every width a document field can carry must be accepted, and all of them
/// must store the same direction. `f32`/`f64` carry the value exactly;
/// `IntArray` carries the same whole numbers.
#[tokio::test]
async fn every_element_width_indexes_identically() {
    let components = [3.0f32, -2.0, 6.0, 1.0];
    let as_wide: Vec<f64> = components.iter().map(|x| *x as f64).collect();
    let as_int: Vec<i64> = components.iter().map(|x| *x as i64).collect();

    let mut stored = Vec::new();
    for values in [
        narrow(&components),
        wide(&as_wide),
        vec![NormalValue::IntArray(as_int)],
    ] {
        let store = MemoryStore::new();
        let index = index();
        let mut write = txn(&store).await;
        index.save(&mut write, 1, &values).await.unwrap();
        write.commit().await.unwrap();

        let mut read = txn(&store).await;
        let kv = KvNodeStore::new(&mut read, COLLECTION, 3, 0);
        stored.push(kv.get_node(NodeId(1)).await.unwrap().unwrap().vector);
    }
    assert!(
        stored.windows(2).all(|pair| pair[0] == pair[1]),
        "the widths stored different data: {stored:?}"
    );
}

/// Go's `Similarity` accepts a vector "of type Int, Float32 or Float64", so a
/// peer can send an integer vector and this must index it, not refuse it.
#[tokio::test]
async fn an_integer_vector_is_accepted() {
    let store = MemoryStore::new();
    let index = index();
    let mut write = txn(&store).await;
    index
        .save(&mut write, 1, &[NormalValue::IntArray(vec![3, 0, 4, 0])])
        .await
        .unwrap();
    index
        .save(
            &mut write,
            2,
            &[NormalValue::NillableIntArray(Some(vec![0, 1, 0, 0]))],
        )
        .await
        .unwrap();
    write.commit().await.unwrap();

    assert_eq!(live_ids(&store, &index).await, vec![NodeId(1), NodeId(2)]);

    // Stored normalized, and 3-0-4-0 has norm 5, so the direction is preserved.
    let mut read = txn(&store).await;
    let kv = KvNodeStore::new(&mut read, COLLECTION, 3, 0);
    let node = kv.get_node(NodeId(1)).await.unwrap().unwrap();
    assert!((node.vector[0] - 0.6).abs() < 1e-6);
    assert!((node.vector[2] - 0.8).abs() < 1e-6);
}

/// A vector is one value. Indexing it component-by-component would be both
/// wrong and enormous, so the whole array must reach the index intact.
#[tokio::test]
async fn a_vector_is_indexed_whole_not_per_component() {
    let store = MemoryStore::new();
    let index = index();
    let mut write = txn(&store).await;
    index
        .save(&mut write, 1, &wide(&[1.0, 2.0, 3.0, 4.0]))
        .await
        .unwrap();
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    let kv = KvNodeStore::new(&mut read, COLLECTION, 3, 0);
    let node = kv.get_node(NodeId(1)).await.unwrap().unwrap();
    assert_eq!(
        node.vector.len(),
        DIMENSIONS as usize,
        "the stored node must hold the whole vector"
    );
    assert_eq!(live_ids(&store, &index).await, vec![NodeId(1)]);
}

/// A document with no vector is simply not in the index, which is the same
/// answer a null field gives.
#[tokio::test]
async fn documents_without_a_usable_vector_are_not_indexed() {
    let store = MemoryStore::new();
    let index = index();
    let mut write = txn(&store).await;

    for (id, values) in [
        (1u64, vec![NormalValue::Null]),
        (2, vec![NormalValue::NillableFloat64Array(None)]),
        // No direction: cosine cannot rank it against anything.
        (3, wide(&[0.0, 0.0, 0.0, 0.0])),
        (4, Vec::new()),
    ] {
        index.save(&mut write, id, &values).await.unwrap();
    }
    index
        .save(&mut write, 5, &wide(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();
    write.commit().await.unwrap();

    assert_eq!(live_ids(&store, &index).await, vec![NodeId(5)]);
}

/// A dimension mismatch is a user error worth naming, unlike a missing vector.
#[tokio::test]
async fn a_dimension_mismatch_is_rejected() {
    let store = MemoryStore::new();
    let index = index();
    let mut write = txn(&store).await;

    let err = index
        .save(&mut write, 1, &wide(&[1.0, 0.0]))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("expects 4 dimensions"),
        "the error must name what was expected, got: {err}"
    );

    // Declaring zero dimensions means an embedding model fixes the length, so
    // the description has nothing to check against and the first vector
    // indexed fixes it instead. Everything after must still agree: a mixed
    // index would rank on the shared leading elements.
    let free = VectorIndex::try_new(COLLECTION, description(0)).unwrap();
    free.save(&mut write, 2, &wide(&[1.0, 0.0])).await.unwrap();
    let err = free
        .save(&mut write, 3, &wide(&[1.0, 0.0, 0.0]))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("2-dimension"),
        "the error must name the width the index holds, got: {err}"
    );
}

#[tokio::test]
async fn deleting_a_document_removes_it_from_results() {
    let store = MemoryStore::new();
    let index = index();

    let mut write = txn(&store).await;
    index
        .save(&mut write, 1, &wide(&[1.0, 0.0, 0.0, 0.0]))
        .await
        .unwrap();
    index
        .save(&mut write, 2, &wide(&[0.0, 1.0, 0.0, 0.0]))
        .await
        .unwrap();
    write.commit().await.unwrap();
    assert_eq!(live_ids(&store, &index).await.len(), 2);

    let mut write = txn(&store).await;
    index.delete(&mut write, 1, &[]).await.unwrap();
    write.commit().await.unwrap();
    assert_eq!(live_ids(&store, &index).await, vec![NodeId(2)]);
}

/// A field that became null must not leave the old vector ranking.
#[tokio::test]
async fn clearing_a_vector_on_update_removes_the_node() {
    let store = MemoryStore::new();
    let index = index();

    let mut write = txn(&store).await;
    let original = wide(&[1.0, 0.0, 0.0, 0.0]);
    index.save(&mut write, 1, &original).await.unwrap();
    index
        .save(&mut write, 2, &wide(&[0.0, 1.0, 0.0, 0.0]))
        .await
        .unwrap();
    write.commit().await.unwrap();

    let mut write = txn(&store).await;
    index
        .update(&mut write, 1, &original, &[NormalValue::Null])
        .await
        .unwrap();
    write.commit().await.unwrap();

    assert_eq!(live_ids(&store, &index).await, vec![NodeId(2)]);
}

#[tokio::test]
async fn dropping_the_index_removes_every_key() {
    let store = MemoryStore::new();
    let index = index();

    let mut write = txn(&store).await;
    for id in 1..=20u64 {
        let angle = id as f64 * 0.3;
        index
            .save(
                &mut write,
                id,
                &wide(&[angle.sin(), angle.cos(), 0.5, 0.25]),
            )
            .await
            .unwrap();
    }
    write.commit().await.unwrap();
    assert_eq!(live_ids(&store, &index).await.len(), 20);

    let mut write = txn(&store).await;
    index.remove_all(&mut write).await.unwrap();
    write.commit().await.unwrap();

    assert!(live_ids(&store, &index).await.is_empty());
    let mut read = txn(&store).await;
    let kv = KvNodeStore::new(&mut read, COLLECTION, 3, 0);
    assert_eq!(
        kv.get_meta().await.unwrap(),
        None,
        "the meta key must go too"
    );
}

/// Out-of-range parameters are refused where the index is built, not silently
/// clamped and not discovered at query time.
#[test]
fn out_of_range_parameters_are_refused_at_construction() {
    let mut desc = description(DIMENSIONS);
    desc = desc.as_vector(VectorIndexDescription {
        hnsw: Some(HnswParams {
            m: 16,
            ef_construction: 1_000_000,
            ef_search: 64,
        }),
        ..vector_config(DIMENSIONS)
    });
    let err = VectorIndex::try_new(COLLECTION, desc).unwrap_err();
    assert!(
        err.to_string().contains("efConstruction"),
        "the error must name the parameter, got: {err}"
    );
}

#[test]
fn an_ordered_description_is_not_a_vector_index() {
    let desc = IndexDescription::new("by_name").with_field("name", false);
    assert!(VectorIndex::try_new(COLLECTION, desc).is_err());
}
