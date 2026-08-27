//! An index created with `dimensions: 0` takes its width from the first
//! vector inserted; nothing in the schema can resolve it.

use db::index::vector::engine::ann::VectorIndexEngine;

const SEED: u64 = 0x0D1E_2F30;

#[tokio::test]
async fn a_second_vector_of_a_different_width_is_refused() {
    let mut graph = crate::support::graph(SEED);
    graph
        .insert(
            db::index::vector::store::NodeId(1),
            &[1.0f32, 0.0, 0.0, 0.0],
        )
        .await
        .expect("the first vector fixes the width");

    let err = graph
        .insert(db::index::vector::store::NodeId(2), &[0.0f32, 1.0, 0.0])
        .await
        .expect_err("a narrower vector must be refused");

    let message = err.to_string();
    assert!(
        message.contains('4') && message.contains('3'),
        "the error must name both widths, got: {message}"
    );
}

#[tokio::test]
async fn a_wider_vector_is_refused_too() {
    let mut graph = crate::support::graph(SEED);
    graph
        .insert(db::index::vector::store::NodeId(1), &[1.0f32, 0.0])
        .await
        .unwrap();
    assert!(graph
        .insert(db::index::vector::store::NodeId(2), &[0.0f32, 1.0, 0.5])
        .await
        .is_err());
}

#[tokio::test]
async fn the_width_holds_across_many_inserts() {
    let mut graph = crate::support::graph(SEED);
    let mut corpus = crate::support::Corpus::new(SEED);
    for (id, vector) in corpus.vectors(64, 8).into_iter().enumerate() {
        graph
            .insert(db::index::vector::store::NodeId(id as u64 + 1), &vector)
            .await
            .expect("every vector is 8 wide");
    }

    assert!(graph
        .insert(db::index::vector::store::NodeId(65), &[0.5f32; 7])
        .await
        .is_err());
    assert!(graph
        .insert(db::index::vector::store::NodeId(66), &[0.5f32; 9])
        .await
        .is_err());
}

#[tokio::test]
async fn a_query_of_the_wrong_width_is_refused() {
    let mut graph = crate::support::graph(SEED);
    let mut corpus = crate::support::Corpus::new(SEED);
    for (id, vector) in corpus.vectors(16, 4).into_iter().enumerate() {
        graph
            .insert(db::index::vector::store::NodeId(id as u64 + 1), &vector)
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

#[tokio::test]
async fn an_empty_graph_answers_any_width_with_nothing() {
    let graph = crate::support::graph(SEED);
    assert!(graph
        .search(&[1.0f32, 0.0, 0.0], 5, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn the_check_applies_to_every_element_width() {
    let mut graph = crate::support::graph(SEED);
    graph
        .insert(
            db::index::vector::store::NodeId(1),
            &[3.0f64, 4.0, 0.0, 0.0],
        )
        .await
        .unwrap();

    assert!(graph.search(&[3i32, 4, 0], 1, None).await.is_err());
    assert!(graph.search(&[3i64, 4, 0, 0], 1, None).await.is_ok());
    assert!(graph
        .insert(db::index::vector::store::NodeId(2), &[1i32, 2])
        .await
        .is_err());
}
