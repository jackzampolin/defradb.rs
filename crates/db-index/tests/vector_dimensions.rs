//! The width an index holds, when nothing declared it.
//!
//! An index may be created with `dimensions: 0`, meaning an embedding model
//! fixes the length. Nothing in the schema can resolve that: Go's
//! `VectorEmbeddingDescription` carries a model name and a provider, and the
//! model name does not determine the width (OpenAI's v3 models take a
//! `dimensions` parameter). So the first vector inserted fixes it, and the
//! graph enforces it from there.

mod common;

use db_index::vector::engine::ann::VectorIndexEngine;

const SEED: u64 = 0x0D1E_2F30;

/// Mixing widths is not an approximation. Cosine over a shared prefix ranks on
/// the leading elements and ignores the rest, so a 3-dimension vector in a
/// 4-dimension index would be scored confidently and wrongly.
#[tokio::test]
async fn a_second_vector_of_a_different_width_is_refused() {
    let mut graph = common::graph(SEED);
    graph
        .insert(db_index::vector::store::NodeId(1), &[1.0f32, 0.0, 0.0, 0.0])
        .await
        .expect("the first vector fixes the width");

    let err = graph
        .insert(db_index::vector::store::NodeId(2), &[0.0f32, 1.0, 0.0])
        .await
        .expect_err("a narrower vector must be refused");

    let message = err.to_string();
    assert!(
        message.contains('4') && message.contains('3'),
        "the error must name both widths, got: {message}"
    );
}

/// A wider one is the same mistake in the other direction.
#[tokio::test]
async fn a_wider_vector_is_refused_too() {
    let mut graph = common::graph(SEED);
    graph
        .insert(db_index::vector::store::NodeId(1), &[1.0f32, 0.0])
        .await
        .unwrap();
    assert!(graph
        .insert(db_index::vector::store::NodeId(2), &[0.0f32, 1.0, 0.5])
        .await
        .is_err());
}

/// The check is against what the index holds, not against the first id seen, so
/// it survives the entry point moving to a later node.
#[tokio::test]
async fn the_width_holds_across_many_inserts() {
    let mut graph = common::graph(SEED);
    let mut corpus = common::Corpus::new(SEED);
    for (id, vector) in corpus.vectors(64, 8).into_iter().enumerate() {
        graph
            .insert(db_index::vector::store::NodeId(id as u64 + 1), &vector)
            .await
            .expect("every vector is 8 wide");
    }

    assert!(graph
        .insert(db_index::vector::store::NodeId(65), &[0.5f32; 7])
        .await
        .is_err());
    assert!(graph
        .insert(db_index::vector::store::NodeId(66), &[0.5f32; 9])
        .await
        .is_err());
}

/// A query of the wrong width must fail loudly rather than return confident,
/// wrong neighbours. This is the only guard when the index declares no
/// dimensions, since the planner's check needs a declared width to compare to.
#[tokio::test]
async fn a_query_of_the_wrong_width_is_refused() {
    let mut graph = common::graph(SEED);
    let mut corpus = common::Corpus::new(SEED);
    for (id, vector) in corpus.vectors(16, 4).into_iter().enumerate() {
        graph
            .insert(db_index::vector::store::NodeId(id as u64 + 1), &vector)
            .await
            .unwrap();
    }

    assert!(
        graph.search(&[1.0f32, 0.0], 5, None).await.is_err(),
        "a short query must not be answered from its shared prefix"
    );
    assert!(graph
        .search(&[1.0f32, 0.0, 0.0, 0.0], 5, None)
        .await
        .is_ok());
}

/// An empty graph has no width to disagree with, so a search returns nothing
/// rather than an error: there is no wrong answer to give.
#[tokio::test]
async fn an_empty_graph_answers_any_width_with_nothing() {
    let graph = common::graph(SEED);
    assert!(graph
        .search(&[1.0f32, 0.0, 0.0], 5, None)
        .await
        .unwrap()
        .is_empty());
}

/// Every element width reaches the same check, so an integer query against a
/// float index is caught on its length and not on its type.
#[tokio::test]
async fn the_check_applies_to_every_element_width() {
    let mut graph = common::graph(SEED);
    graph
        .insert(db_index::vector::store::NodeId(1), &[3.0f64, 4.0, 0.0, 0.0])
        .await
        .unwrap();

    assert!(graph.search(&[3i32, 4, 0], 1, None).await.is_err());
    assert!(graph.search(&[3i64, 4, 0, 0], 1, None).await.is_ok());
    assert!(graph
        .insert(db_index::vector::store::NodeId(2), &[1i32, 2])
        .await
        .is_err());
}
