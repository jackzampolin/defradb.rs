//! The batch-built kinds over a real transaction.
//!
//! Every other test of these kinds runs on `MemoryNodeStore`. This one builds
//! through `KvNodeStore`, commits, and reopens, so what is asserted is that the
//! trained structures actually persist rather than that the algorithm works.

use db::index::vector::core::Metric;
use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::ivfpq::{IvfPq, IvfPqParams};
use db::index::vector::engine::ssg::{Ssg, SsgParams};
use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::params::{Params, DEFAULT_M};
use db::index::vector::store::NodeId;
use storage::backends::MemoryStore;
use storage::corekv::{Store, Txn};

const COLLECTION: u32 = 51;
const INDEX: u32 = 3;
const DIMENSIONS: usize = 16;
const SEED: u64 = 0x0D15_C0DE;

async fn txn(store: &MemoryStore) -> Box<dyn Txn> {
    store.new_txn(false).await.unwrap()
}

fn corpus(count: usize) -> Vec<Vec<f32>> {
    crate::support::Corpus::new(SEED).clustered(count, DIMENSIONS, 8, 0.2)
}

/// Trained state, centroids, codebooks and list codes must survive a commit,
/// or a reopened index silently answers from its staging path forever.
#[tokio::test]
async fn ivfpq_training_survives_a_commit() {
    let backing = MemoryStore::new();
    let vectors = corpus(400);
    let params = IvfPqParams {
        nlist: 8,
        nprobe: 8,
        m: 4,
        ..IvfPqParams::default()
    };

    let mut write = txn(&backing).await;
    {
        let store = KvNodeStore::new(&mut write, COLLECTION, INDEX, 0);
        let mut index = IvfPq::try_new(store, Metric::Cosine, params, SEED).unwrap();
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
        }
        assert!(!index.is_trained().await.unwrap());
        let report = index.build().await.unwrap();
        assert_eq!(report.indexed, 400);
    }
    write.commit().await.unwrap();

    let mut read = txn(&backing).await;
    let store = KvNodeStore::new(&mut read, COLLECTION, INDEX, 0);
    let reopened = IvfPq::try_new(store, Metric::Cosine, params, SEED).unwrap();

    let state = reopened
        .trained()
        .await
        .unwrap()
        .expect("the trained state must have been persisted");
    assert_eq!(state.nlist, 8);
    assert_eq!(state.m, 4);
    assert_eq!(state.dimensions, DIMENSIONS as u32);

    let hits = reopened
        .search(vectors[3].as_slice(), 5, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 5);
    assert_eq!(hits[0].id, NodeId(4), "a vector is nearest itself");
}

#[tokio::test]
async fn ssg_graph_survives_a_commit() {
    let backing = MemoryStore::new();
    let vectors = corpus(300);

    let mut write = txn(&backing).await;
    {
        let store = KvNodeStore::new(&mut write, COLLECTION, INDEX + 1, 0);
        let mut index = Ssg::try_new(
            store,
            Metric::Cosine,
            Params::new(DEFAULT_M),
            SsgParams::default(),
            SEED,
        )
        .unwrap();
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
        }
        assert!(!index.is_built().await.unwrap());
        let report = index.build().await.unwrap();
        assert_eq!(report.nodes, 300);
        assert!(report.edges > 0);
    }
    write.commit().await.unwrap();

    let mut read = txn(&backing).await;
    let store = KvNodeStore::new(&mut read, COLLECTION, INDEX + 1, 0);
    let reopened = Ssg::try_new(
        store,
        Metric::Cosine,
        Params::new(DEFAULT_M),
        SsgParams::default(),
        SEED,
    )
    .unwrap();

    assert!(
        reopened.is_built().await.unwrap(),
        "the built state must have been persisted"
    );
    assert!(
        !reopened.neighbours(NodeId(1)).await.unwrap().is_empty(),
        "the pruned adjacency must have been persisted"
    );

    let hits = reopened
        .search(vectors[2].as_slice(), 5, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 5);
    assert_eq!(hits[0].id, NodeId(3), "a vector is nearest itself");
}

/// A build writes into one epoch, so a different epoch must see none of it.
/// This is what makes a rebuild-and-swap possible.
#[tokio::test]
async fn a_build_stays_inside_its_epoch() {
    let backing = MemoryStore::new();
    let vectors = corpus(400);
    let params = IvfPqParams {
        nlist: 8,
        nprobe: 8,
        m: 4,
        ..IvfPqParams::default()
    };

    let mut write = txn(&backing).await;
    {
        let store = KvNodeStore::new(&mut write, COLLECTION, INDEX + 2, 0);
        let mut index = IvfPq::try_new(store, Metric::Cosine, params, SEED).unwrap();
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
        }
        index.build().await.unwrap();
    }
    write.commit().await.unwrap();

    let mut read = txn(&backing).await;
    let next = KvNodeStore::new(&mut read, COLLECTION, INDEX + 2, 1);
    let fresh = IvfPq::try_new(next, Metric::Cosine, params, SEED).unwrap();
    assert!(
        !fresh.is_trained().await.unwrap(),
        "epoch 1 must not see epoch 0's training"
    );
}
