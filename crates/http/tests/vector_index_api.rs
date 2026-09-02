//! The create-index request must be able to express a vector index, or a Go
//! client's `Vector` config is silently dropped and an ordinary index is built
//! over the vector field instead.

use defra_http::handlers::index::{GoCreateIndexRequest, GoIndexDescription};
use schema::{DistanceMetric, HnswParams, IndexKind, VectorAlgorithm, VectorIndexDescription};

#[test]
fn a_vector_request_round_trips_gos_field_names() {
    let json = r#"{
        "Name": "by_embedding",
        "Fields": [{"Name": "embedding"}],
        "Vector": {
            "Algorithm": "HNSW",
            "Metric": "COSINE",
            "Dimensions": 768,
            "HNSW": {"M": 32, "EfConstruction": 200, "EfSearch": 100}
        }
    }"#;
    let request: GoCreateIndexRequest = serde_json::from_str(json).unwrap();

    let vector = request.vector.expect("the vector config must survive");
    assert_eq!(vector.algorithm, VectorAlgorithm::Hnsw);
    assert_eq!(vector.metric, DistanceMetric::Cosine);
    assert_eq!(vector.dimensions, 768);
    let hnsw = vector.hnsw.expect("HNSW params");
    assert_eq!(
        (hnsw.m, hnsw.ef_construction, hnsw.ef_search),
        (32, 200, 100)
    );
    assert!(!request.unique, "a vector index is never unique");
}

/// A request without `Vector` is an ordinary index, unchanged.
#[test]
fn an_ordinary_request_carries_no_vector() {
    let json = r#"{"Fields": [{"Name": "name"}], "Unique": true}"#;
    let request: GoCreateIndexRequest = serde_json::from_str(json).unwrap();
    assert!(request.vector.is_none());
    assert!(request.unique);
}

/// The response reports the kind, so a client can tell what was built rather
/// than assuming its request was honoured.
#[test]
fn the_response_reports_the_kind() {
    let described = GoIndexDescription {
        name: "by_embedding".to_string(),
        id: 3,
        fields: Vec::new(),
        unique: false,
        kind: IndexKind::Vector(VectorIndexDescription {
            algorithm: VectorAlgorithm::Hnsw,
            metric: DistanceMetric::Cosine,
            dimensions: 4,
            hnsw: Some(HnswParams::default()),
            ivfpq: None,
            ivfflat: None,
            ssg: None,
        }),
    };
    let json = serde_json::to_value(&described).unwrap();
    assert_eq!(json["Kind"], 1);
    assert_eq!(json["KindDescription"]["Algorithm"], "HNSW");
    assert_eq!(json["KindDescription"]["Dimensions"], 4);

    let ordinary = GoIndexDescription {
        kind: IndexKind::Ordered(schema::OrderedIndexDescription { unique: false }),
        ..described
    };
    let json = serde_json::to_value(&ordinary).unwrap();
    assert_eq!(json["Kind"], 0);
    assert_eq!(json["KindDescription"]["Unique"], false);
}
