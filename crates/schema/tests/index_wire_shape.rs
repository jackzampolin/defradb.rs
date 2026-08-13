//! The exact JSON a Go node exchanges for an index description.
//!
//! Transcribed from `sourcenetwork/defradb` PR 5096 @ `e5dd907e`,
//! `client/index.go`. None of those structs carry json tags, so
//! `encoding/json` uses the Go field names verbatim, and `IndexDescription`
//! marshals through the `indexDescription` mirror: `Name`, `ID`, `Fields`,
//! `Kind`, `Unique`.
//!
//! **This is read from source, not from a running Go node.** No Go toolchain is
//! available here, and PR 5096 is not in `GO_COMPAT_COMMIT`, so nothing in CI
//! exchanges a vector index with Go yet. What this pins is that our shape
//! matches the shape that source produces; the executed check has to wait for
//! the baseline bump.
//!
//! The key-set assertions are the point: a field Go emits and we do not, or one
//! we add, fails here rather than at a peer.

use schema::{
    DistanceMetric, HnswParams, IndexDescription, IndexKind, OrderedIndexDescription,
    VectorAlgorithm, VectorIndexDescription,
};
use serde_json::{json, Value};

fn keys(value: &Value) -> Vec<String> {
    let mut names: Vec<String> = value
        .as_object()
        .expect("a JSON object")
        .keys()
        .cloned()
        .collect();
    names.sort();
    names
}

fn vector() -> VectorIndexDescription {
    VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: 768,
        hnsw: Some(HnswParams {
            m: 16,
            ef_construction: 128,
            ef_search: 64,
        }),
    }
}

#[test]
fn a_vector_index_matches_gos_marshalled_shape() {
    let desc = IndexDescription::new("by_embedding")
        .with_field("embedding", false)
        .as_vector(vector())
        .normalized();

    assert_eq!(
        serde_json::to_value(&desc).unwrap(),
        json!({
            "Name": "by_embedding",
            "ID": 0,
            "Fields": [{"Name": "embedding", "Descending": false}],
            "Kind": {
                "Algorithm": "HNSW",
                "Metric": "COSINE",
                "Dimensions": 768,
                "HNSW": {"M": 16, "EfConstruction": 128, "EfSearch": 64}
            },
            "Unique": false
        })
    );
}

#[test]
fn an_ordered_index_matches_gos_marshalled_shape() {
    let desc = IndexDescription::new("by_email")
        .with_field("email", false)
        .as_unique()
        .normalized();

    assert_eq!(
        serde_json::to_value(&desc).unwrap(),
        json!({
            "Name": "by_email",
            "ID": 0,
            "Fields": [{"Name": "email", "Descending": false}],
            "Kind": {"Unique": true},
            "Unique": true
        })
    );
}

#[test]
fn no_level_carries_a_field_go_does_not() {
    let desc = IndexDescription::new("by_embedding")
        .with_field("embedding", false)
        .as_vector(vector())
        .normalized();
    let json = serde_json::to_value(&desc).unwrap();

    assert_eq!(keys(&json), ["Fields", "ID", "Kind", "Name", "Unique"]);
    assert_eq!(
        keys(&json["Kind"]),
        ["Algorithm", "Dimensions", "HNSW", "Metric"]
    );
    assert_eq!(
        keys(&json["Kind"]["HNSW"]),
        ["EfConstruction", "EfSearch", "M"]
    );
    assert_eq!(keys(&json["Fields"][0]), ["Descending", "Name"]);

    let ordered =
        serde_json::to_value(IndexDescription::new("i").as_unique().normalized()).unwrap();
    assert_eq!(keys(&ordered["Kind"]), ["Unique"]);
}

/// Go's `parseIndexKind` sniffs `Algorithm != nil || Dimensions != nil`, so a
/// partial Kind a Go node could emit must resolve the same way here.
#[test]
fn a_partial_kind_resolves_as_go_resolves_it() {
    let by_algorithm: IndexDescription =
        serde_json::from_value(json!({"Name": "i", "ID": 1, "Kind": {"Algorithm": "HNSW"}}))
            .unwrap();
    assert!(by_algorithm.is_vector());

    let by_dimensions: IndexDescription =
        serde_json::from_value(json!({"Name": "i", "ID": 1, "Kind": {"Dimensions": 4}})).unwrap();
    assert!(by_dimensions.is_vector());

    let ordered: IndexDescription =
        serde_json::from_value(json!({"Name": "i", "ID": 1, "Kind": {"Unique": true}})).unwrap();
    assert!(!ordered.is_vector());
    assert!(ordered.resolved_unique());
}

/// `HNSW` is a pointer in Go, so it is `null` when absent rather than missing.
#[test]
fn an_absent_hnsw_block_is_null_and_parses_back() {
    let desc = IndexDescription::new("i")
        .as_vector(VectorIndexDescription {
            hnsw: None,
            ..vector()
        })
        .normalized();
    let json = serde_json::to_value(&desc).unwrap();
    assert_eq!(json["Kind"]["HNSW"], Value::Null);

    let back: IndexDescription = serde_json::from_value(json).unwrap();
    assert_eq!(back.vector().map(|v| v.hnsw), Some(None));
}

/// `DOT` is ours alone: Go's `DistanceMetric` defines only `COSINE`, so a
/// definition carrying it is not parseable by a Go node. The divergence has to
/// be visible to anything deciding whether a definition can leave this node.
#[test]
fn dot_is_the_one_metric_go_cannot_parse() {
    assert!(DistanceMetric::Cosine.is_go_compatible());
    assert!(!DistanceMetric::Dot.is_go_compatible());

    assert_eq!(
        serde_json::to_value(DistanceMetric::Cosine).unwrap(),
        json!("COSINE")
    );
    assert_eq!(
        serde_json::to_value(DistanceMetric::Dot).unwrap(),
        json!("DOT")
    );

    let desc = IndexDescription::new("i")
        .as_vector(VectorIndexDescription {
            metric: DistanceMetric::Dot,
            ..vector()
        })
        .normalized();
    assert_eq!(
        serde_json::to_value(&desc).unwrap()["Kind"]["Metric"],
        json!("DOT")
    );
}

/// Go writes the compat top-level `Unique` from the resolved kind, so a reader
/// predating `Kind` still sees uniqueness.
#[test]
fn the_compat_unique_field_tracks_the_kind() {
    let mut desc = IndexDescription::new("i");
    desc.kind = Some(IndexKind::Ordered(OrderedIndexDescription { unique: true }));
    let json = serde_json::to_value(desc.normalized()).unwrap();
    assert_eq!(json["Unique"], true);
    assert_eq!(json["Kind"]["Unique"], true);
}
