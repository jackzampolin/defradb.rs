//! When a query can be answered by a vector index, and when it cannot.

use query::planner::vector_routing::{
    route, NotRouted, OrderKey, SimilarityField, SimilarityQuery,
};
use schema::{
    DistanceMetric, HnswParams, IndexDescription, IndexedFieldDescription, VectorAlgorithm,
    VectorIndexDescription,
};

const DIMENSIONS: u32 = 4;

/// `SIMILARITY` ranks by dot product, so a routable index must be built with
/// the matching metric. A cosine index is covered separately.
fn vector_index(id: u32, field: &str, dimensions: u32) -> IndexDescription {
    IndexDescription {
        name: format!("by_{field}"),
        id,
        fields: vec![IndexedFieldDescription {
            name: field.to_string(),
            descending: false,
        }],
        unique: false,
        kind: None,
        auto_generated: false,
    }
    .as_vector(VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Dot,
        dimensions,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ivfflat: None,
        ssg: None,
    })
}

fn ordered_index(id: u32, field: &str) -> IndexDescription {
    let mut index = IndexDescription::new(format!("by_{field}")).with_field(field, false);
    index.id = id;
    index
}

fn similarity(field: &str, output_name: &str) -> SimilarityField {
    SimilarityField {
        target_field: field.to_string(),
        vector: vec![1.0, 0.0, 0.0, 0.0],
        output_name: output_name.to_string(),
        metric: None,
    }
}

/// A vector index built with a chosen metric, so a field can carry several
/// that disagree.
fn vector_index_with_metric(id: u32, field: &str, metric: DistanceMetric) -> IndexDescription {
    IndexDescription {
        name: format!("by_{field}_{}", metric.as_str()),
        id,
        fields: vec![IndexedFieldDescription {
            name: field.to_string(),
            descending: false,
        }],
        unique: false,
        kind: None,
        auto_generated: false,
    }
    .as_vector(VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric,
        dimensions: DIMENSIONS,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ivfflat: None,
        ssg: None,
    })
}

fn order(field: &str, descending: bool) -> Option<OrderKey> {
    Some(OrderKey {
        field: field.to_string(),
        descending,
    })
}

/// The shape that routes: one similarity, ordered by it descending, limited.
fn routable() -> SimilarityQuery {
    SimilarityQuery {
        limit: Some(10),
        offset: 0,
        similarities: vec![similarity("embedding", "SIMILARITY")],
        sole_order: order("SIMILARITY", true),
        show_deleted: false,
    }
}

/// The index holds live documents only, so narrowing a `showDeleted` query to
/// it would drop exactly the rows that were asked for.
#[test]
fn a_show_deleted_query_does_not_route() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];
    let query = SimilarityQuery {
        show_deleted: true,
        ..routable()
    };
    assert_eq!(route(&query, &indexes), Err(NotRouted::ShowDeleted));
}

#[test]
fn the_canonical_shape_routes() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];
    let route = route(&routable(), &indexes).expect("should route");
    assert_eq!(route.index_id, 7);
    assert_eq!(route.k, 10);
    assert_eq!(route.query_vector, vec![1.0, 0.0, 0.0, 0.0]);
}

/// The offset skips results the graph still has to produce, so it is part of
/// `k`. Asking for only the limit would leave the query short by the offset.
#[test]
fn the_offset_is_added_to_k() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];
    let query = SimilarityQuery {
        offset: 25,
        ..routable()
    };
    assert_eq!(route(&query, &indexes).unwrap().k, 35);
}

#[test]
fn a_query_without_a_usable_limit_does_not_route() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];
    for limit in [None, Some(0)] {
        let query = SimilarityQuery {
            limit,
            ..routable()
        };
        assert_eq!(route(&query, &indexes), Err(NotRouted::NoLimit));
    }
}

/// With two similarity fields, which one drives the search is ambiguous.
#[test]
fn zero_or_several_similarities_do_not_route() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];

    let none = SimilarityQuery {
        similarities: Vec::new(),
        ..routable()
    };
    assert_eq!(route(&none, &indexes), Err(NotRouted::NotOneSimilarity));

    let two = SimilarityQuery {
        similarities: vec![
            similarity("embedding", "SIMILARITY"),
            similarity("other", "other_sim"),
        ],
        ..routable()
    };
    assert_eq!(route(&two, &indexes), Err(NotRouted::NotOneSimilarity));
}

/// Ascending would ask for the *farthest*; ordering by something else, or by
/// several keys, needs documents beyond the k the graph returns.
#[test]
fn the_ordering_must_be_that_similarity_alone_descending() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];

    for key in [None, order("SIMILARITY", false), order("name", true)] {
        let query = SimilarityQuery {
            sole_order: key.clone(),
            ..routable()
        };
        assert_eq!(
            route(&query, &indexes),
            Err(NotRouted::NotOrderedBySimilarity),
            "{key:?} should not route"
        );
    }
}

/// An alias resolves to the index of the field it names, so an alias-ordered
/// query is the same shape here as a directly-ordered one. This is the query
/// the hybrid retrieval path emits, so it is the one that most needs to route.
#[test]
fn an_alias_ordered_query_routes() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];
    let query = SimilarityQuery {
        similarities: vec![similarity("embedding", "dense_score")],
        sole_order: order("dense_score", true),
        ..routable()
    };
    assert_eq!(route(&query, &indexes).unwrap().index_id, 7);
}

#[test]
fn a_field_without_a_vector_index_does_not_route() {
    for indexes in [
        Vec::new(),
        vec![ordered_index(1, "embedding")],
        vec![vector_index(7, "other_field", DIMENSIONS)],
    ] {
        assert_eq!(
            route(&routable(), &indexes),
            Err(NotRouted::NoVectorIndex),
            "{indexes:?} should not route"
        );
    }
}

/// A wrong-length query would be scored on its shared leading elements only:
/// silently wrong rather than merely approximate.
#[test]
fn a_dimension_mismatch_does_not_route() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];
    let query = SimilarityQuery {
        similarities: vec![SimilarityField {
            vector: vec![1.0, 0.0],
            ..similarity("embedding", "SIMILARITY")
        }],
        ..routable()
    };
    assert_eq!(
        route(&query, &indexes),
        Err(NotRouted::DimensionMismatch {
            expected: 4,
            actual: 2
        })
    );
}

/// Zero dimensions means an embedding model fixes the length, so there is
/// nothing to check the query against yet.
#[test]
fn an_unfixed_dimension_accepts_any_length() {
    let indexes = [vector_index(7, "embedding", 0)];
    for length in [2usize, 4, 768] {
        let query = SimilarityQuery {
            similarities: vec![SimilarityField {
                vector: vec![0.5; length],
                ..similarity("embedding", "SIMILARITY")
            }],
            ..routable()
        };
        assert!(route(&query, &indexes).is_ok(), "length {length}");
    }
}

/// A filter does not appear in the decision at all. Go refuses a filtered
/// query because its graph cannot filter during the walk; this one can, and
/// the hybrid retrieval path folds `exclude_doc_ids` into a filter on
/// essentially every call.
#[test]
fn a_filter_is_not_a_reason_to_refuse() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];
    // There is no filter field on the query shape: routing is decided without
    // reference to one, which is the divergence from Go stated in the module.
    assert!(route(&routable(), &indexes).is_ok());
}

/// The right index must be picked when a collection carries several, which is
/// what multi-model embedding support needs.
#[test]
fn the_index_on_the_target_field_is_chosen() {
    let indexes = [
        ordered_index(1, "title"),
        vector_index(4, "summary_v", DIMENSIONS),
        vector_index(9, "embedding", DIMENSIONS),
    ];
    assert_eq!(route(&routable(), &indexes).unwrap().index_id, 9);
}

/// The scan scores by whatever metric the field's index carries, so every
/// combination routes: there is no pair for which the index ranks one way and
/// the query scores another. Before this, only a `DOT` index routed, because
/// the scan always computed a raw dot product regardless of the index.
#[test]
fn every_algorithm_and_metric_routes() {
    for algorithm in VectorAlgorithm::ALL {
        for metric in DistanceMetric::ALL {
            // The schema refuses a pair the algorithm cannot rank by, so such an
            // index never reaches routing.
            if !algorithm.supports_metric(*metric) {
                continue;
            }
            let index = IndexDescription {
                name: "by_embedding".to_string(),
                id: 1,
                fields: vec![IndexedFieldDescription {
                    name: "embedding".to_string(),
                    descending: false,
                }],
                unique: false,
                kind: None,
                auto_generated: false,
            }
            .as_vector(VectorIndexDescription::with_defaults(
                *algorithm, *metric, DIMENSIONS,
            ));

            assert!(
                route(&routable(), &[index]).is_ok(),
                "{} with {} must route",
                algorithm.as_str(),
                metric.as_str()
            );
        }
    }
}

/// The metric the scan scores by is the one the field's index declares, and
/// cosine when it declares none. A disagreement here is a ranking that changes
/// with whether the index happened to be used.
#[test]
fn the_scoring_metric_follows_the_field_index() {
    use query::planner::vector_routing::scoring_metric;

    let indexes = [
        ordered_index(1, "title"),
        vector_index(9, "embedding", DIMENSIONS),
    ];

    assert_eq!(
        scoring_metric(&indexes, "embedding", None),
        DistanceMetric::Dot
    );
    assert_eq!(
        scoring_metric(&indexes, "title", None),
        DistanceMetric::Cosine,
        "an ordered index is not a vector index"
    );
    assert_eq!(
        scoring_metric(&indexes, "unindexed", None),
        DistanceMetric::Cosine,
        "an unindexed field scores as cosine"
    );
    assert_eq!(
        scoring_metric(&[], "embedding", None),
        DistanceMetric::Cosine,
        "a collection with no indexes at all scores as cosine"
    );
    assert_eq!(
        scoring_metric(&[], "embedding", Some(DistanceMetric::Dot)),
        DistanceMetric::Dot,
        "a named metric scores by what the query asked for, even on an \
         unindexed field"
    );
}

/// With one index on the field, that index is the only candidate whatever
/// metric it was built with, so no metric needs naming.
#[test]
fn a_single_index_routes_without_a_named_metric() {
    for metric in DistanceMetric::ALL {
        let indexes = [vector_index_with_metric(7, "embedding", *metric)];
        let route = route(&routable(), &indexes)
            .unwrap_or_else(|e| panic!("{metric:?} with a single index must route: {e:?}"));
        assert_eq!(route.index_id, 7);
    }
}

/// Two indexes and no named metric: which one would answer is ambiguous, so
/// routing declines rather than silently picking one.
#[test]
fn several_indexes_without_a_named_metric_do_not_route() {
    let indexes = [
        vector_index_with_metric(1, "embedding", DistanceMetric::Cosine),
        vector_index_with_metric(2, "embedding", DistanceMetric::Dot),
    ];
    assert_eq!(
        route(&routable(), &indexes),
        Err(NotRouted::AmbiguousMetric)
    );
}

/// With a metric named, the index built with that metric answers, whichever
/// index carries it.
#[test]
fn a_named_metric_selects_its_own_index_among_several() {
    let indexes = [
        vector_index_with_metric(1, "embedding", DistanceMetric::Cosine),
        vector_index_with_metric(2, "embedding", DistanceMetric::Dot),
    ];

    for (metric, expected_id) in [(DistanceMetric::Cosine, 1), (DistanceMetric::Dot, 2)] {
        let query = SimilarityQuery {
            similarities: vec![SimilarityField {
                metric: Some(metric),
                ..similarity("embedding", "SIMILARITY")
            }],
            ..routable()
        };
        assert_eq!(
            route(&query, &indexes).unwrap().index_id,
            expected_id,
            "{metric:?} should select index {expected_id}"
        );
    }
}

/// Naming a metric no index on the field carries is a mismatch: it does not
/// fall back to whatever the field does have.
#[test]
fn a_metric_no_index_carries_does_not_route() {
    let indexes = [vector_index_with_metric(
        7,
        "embedding",
        DistanceMetric::Cosine,
    )];
    let query = SimilarityQuery {
        similarities: vec![SimilarityField {
            metric: Some(DistanceMetric::Dot),
            ..similarity("embedding", "SIMILARITY")
        }],
        ..routable()
    };
    assert_eq!(
        route(&query, &indexes),
        Err(NotRouted::MetricMismatch {
            requested: DistanceMetric::Dot
        })
    );
}
