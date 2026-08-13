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
        ivfpq: None,
        ssg: None,
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

/// `HNSW` is a nil pointer in Go, which `encoding/json` emits as `null`; serde
/// omits the key instead. Go's decoder reads a missing field as nil, so the two
/// round-trip, but the emitted bytes differ and that is worth knowing.
#[test]
fn an_absent_hnsw_block_is_omitted_and_parses_back() {
    let desc = IndexDescription::new("i")
        .as_vector(VectorIndexDescription {
            hnsw: None,
            ..vector()
        })
        .normalized();
    let json = serde_json::to_value(&desc).unwrap();
    assert!(
        !json["Kind"].as_object().unwrap().contains_key("HNSW"),
        "serde omits the key rather than emitting null"
    );

    let back: IndexDescription = serde_json::from_value(json).unwrap();
    assert_eq!(back.vector().map(|v| v.hnsw), Some(None));

    // Go's `null` must still parse, since that is what a Go node sends.
    let from_go: IndexDescription = serde_json::from_value(json!({
        "Name": "i", "ID": 1,
        "Kind": {"Algorithm": "HNSW", "Metric": "COSINE", "Dimensions": 4, "HNSW": null}
    }))
    .unwrap();
    assert_eq!(from_go.vector().map(|v| v.hnsw), Some(None));
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

/// `FLAT` is the second divergence: Go's `VectorAlgorithm` defines only `HNSW`.
/// Its `HNSW` block is null, since it has no build parameters.
#[test]
fn flat_is_an_algorithm_go_cannot_parse() {
    assert!(VectorAlgorithm::Hnsw.is_go_compatible());
    assert!(!VectorAlgorithm::Flat.is_go_compatible());

    assert_eq!(
        serde_json::to_value(VectorAlgorithm::Hnsw).unwrap(),
        json!("HNSW")
    );
    assert_eq!(
        serde_json::to_value(VectorAlgorithm::Flat).unwrap(),
        json!("FLAT")
    );

    let desc = IndexDescription::new("i")
        .as_vector(VectorIndexDescription {
            algorithm: VectorAlgorithm::Flat,
            hnsw: None,
            ..vector()
        })
        .normalized();
    let json = serde_json::to_value(&desc).unwrap();
    assert_eq!(json["Kind"]["Algorithm"], json!("FLAT"));
    assert!(!json["Kind"].as_object().unwrap().contains_key("HNSW"));

    let back: IndexDescription = serde_json::from_value(json).unwrap();
    assert_eq!(
        back.vector().map(|v| v.algorithm),
        Some(VectorAlgorithm::Flat)
    );
}

/// A definition is exchangeable only when every divergent value is absent.
#[test]
fn go_compatibility_is_checkable_across_both_axes() {
    let ok = vector();
    assert!(ok.algorithm.is_go_compatible() && ok.metric.is_go_compatible());

    for diverged in [
        VectorIndexDescription {
            algorithm: VectorAlgorithm::Flat,
            hnsw: None,
            ..vector()
        },
        VectorIndexDescription {
            metric: DistanceMetric::Dot,
            ..vector()
        },
    ] {
        assert!(
            !(diverged.algorithm.is_go_compatible() && diverged.metric.is_go_compatible()),
            "{diverged:?} must be flagged"
        );
    }
}

/// `IVF_PQ` is the third divergence, and it brings a parameter block Go has no
/// field for at all.
#[test]
fn ivfpq_is_an_algorithm_go_cannot_parse() {
    use schema::IvfPqParams;

    assert!(!VectorAlgorithm::IvfPq.is_go_compatible());
    assert_eq!(
        serde_json::to_value(VectorAlgorithm::IvfPq).unwrap(),
        json!("IVF_PQ")
    );

    let desc = IndexDescription::new("i")
        .as_vector(VectorIndexDescription {
            algorithm: VectorAlgorithm::IvfPq,
            hnsw: None,
            ivfpq: Some(IvfPqParams {
                nlist: 256,
                nprobe: 16,
                m: 32,
                sample_bytes: 1 << 20,
            }),
            ssg: None,
            ..vector()
        })
        .normalized();

    let json = serde_json::to_value(&desc).unwrap();
    assert_eq!(json["Kind"]["Algorithm"], json!("IVF_PQ"));
    assert_eq!(
        json["Kind"]["IVFPQ"],
        json!({"NList": 256, "NProbe": 16, "M": 32, "SampleBytes": 1_048_576})
    );
    assert!(!json["Kind"].as_object().unwrap().contains_key("HNSW"));

    let back: IndexDescription = serde_json::from_value(json).unwrap();
    assert_eq!(
        back.vector().map(|v| v.algorithm),
        Some(VectorAlgorithm::IvfPq)
    );
    assert_eq!(
        back.vector().and_then(|v| v.ivfpq).map(|p| p.nlist),
        Some(256)
    );
}

/// An HNSW description must not grow an IVFPQ key, or every Go-compatible
/// definition would start carrying a field Go cannot parse.
#[test]
fn an_hnsw_description_carries_no_ivfpq_key() {
    let json =
        serde_json::to_value(IndexDescription::new("i").as_vector(vector()).normalized()).unwrap();
    assert_eq!(
        keys(&json["Kind"]),
        ["Algorithm", "Dimensions", "HNSW", "Metric"]
    );
}

/// `SSG` is the fourth divergence.
#[test]
fn ssg_is_an_algorithm_go_cannot_parse() {
    use schema::SsgParams;

    assert!(!VectorAlgorithm::Ssg.is_go_compatible());
    assert_eq!(
        serde_json::to_value(VectorAlgorithm::Ssg).unwrap(),
        json!("SSG")
    );

    let desc = IndexDescription::new("i")
        .as_vector(VectorIndexDescription {
            algorithm: VectorAlgorithm::Ssg,
            hnsw: None,
            ssg: Some(SsgParams {
                r: 32,
                angle: 45,
                pool: 200,
            }),
            ..vector()
        })
        .normalized();

    let json = serde_json::to_value(&desc).unwrap();
    assert_eq!(json["Kind"]["Algorithm"], json!("SSG"));
    assert_eq!(
        json["Kind"]["SSG"],
        json!({"R": 32, "Angle": 45, "Pool": 200})
    );

    let back: IndexDescription = serde_json::from_value(json).unwrap();
    assert_eq!(back.vector().and_then(|v| v.ssg).map(|p| p.r), Some(32));
}
