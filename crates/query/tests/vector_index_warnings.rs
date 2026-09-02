//! The `VECTOR_INDEX_UNUSED` warning: which `NotRouted` reasons produce it, and
//! how it serializes into a `QueryResponse`'s `extensions`.

use query::executor::QueryResponse;
use query::planner::vector_routing::{vector_index_unused_warning, NotRouted};
use schema::DistanceMetric;

fn all_not_routed_variants() -> [NotRouted; 8] {
    [
        NotRouted::NoLimit,
        NotRouted::NotOneSimilarity,
        NotRouted::NotOrderedBySimilarity,
        NotRouted::NoVectorIndex,
        NotRouted::ShowDeleted,
        NotRouted::DimensionMismatch {
            expected: 4,
            actual: 2,
        },
        NotRouted::AmbiguousMetric,
        NotRouted::MetricMismatch {
            requested: DistanceMetric::Dot,
        },
    ]
}

/// An exhaustive match: a new `NotRouted` variant fails to compile here before
/// it can ship without a reason string.
#[test]
fn every_not_routed_variant_maps_to_its_reason_string() {
    for variant in all_not_routed_variants() {
        let expected = match variant {
            NotRouted::NoLimit => "noLimit",
            NotRouted::NotOneSimilarity => "multipleSimilarityFields",
            NotRouted::NotOrderedBySimilarity => "notOrderedBySimilarityDesc",
            NotRouted::NoVectorIndex => "noVectorIndex",
            NotRouted::ShowDeleted => "showDeleted",
            NotRouted::DimensionMismatch { .. } => "dimensionMismatch",
            NotRouted::AmbiguousMetric => "ambiguousMetric",
            NotRouted::MetricMismatch { .. } => "metricMismatch",
        };
        assert_eq!(variant.reason(), expected);
    }
}

#[test]
fn vector_index_unused_warning_is_none_only_for_no_vector_index() {
    for variant in all_not_routed_variants() {
        let warning = vector_index_unused_warning(&variant, "embedding");
        assert_eq!(
            warning.is_none(),
            variant == NotRouted::NoVectorIndex,
            "{variant:?}"
        );
    }
}

#[test]
fn vector_index_unused_warning_has_the_exact_shape() {
    let warning =
        vector_index_unused_warning(&NotRouted::NoLimit, "embedding").expect("NoLimit must warn");
    assert_eq!(
        serde_json::to_value(&warning).unwrap(),
        serde_json::json!({
            "code": "VECTOR_INDEX_UNUSED",
            "message": "similarity query on field 'embedding' did not use the vector index and read the whole collection",
            "detail": {
                "field": "embedding",
                "reason": "noLimit",
            },
        })
    );
}

#[test]
fn empty_extensions_are_omitted_from_the_response() {
    let response =
        QueryResponse::success(serde_json::json!({"Note": []})).with_warnings(Vec::new());
    let value = serde_json::to_value(&response).unwrap();
    assert!(
        value.as_object().unwrap().get("extensions").is_none(),
        "{value:#}"
    );
}

#[test]
fn a_response_with_one_warning_serializes_with_extensions() {
    let warning = vector_index_unused_warning(&NotRouted::NoLimit, "embedding").unwrap();
    let response =
        QueryResponse::success(serde_json::json!({"Note": []})).with_warnings(vec![warning]);

    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        serde_json::json!({
            "data": {"Note": []},
            "extensions": {
                "warnings": [{
                    "code": "VECTOR_INDEX_UNUSED",
                    "message": "similarity query on field 'embedding' did not use the vector index and read the whole collection",
                    "detail": {
                        "field": "embedding",
                        "reason": "noLimit",
                    },
                }],
            },
        })
    );
}
