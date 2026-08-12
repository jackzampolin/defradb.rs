//! When a query can be answered by a vector index, and when it cannot.

use query_plan::planner::vector_routing::{
    route, NotRouted, OrderKey, SimilarityField, SimilarityQuery,
};
use schema::{
    DistanceMetric, HnswParams, IndexDescription, IndexedFieldDescription, VectorAlgorithm,
    VectorIndexDescription,
};

const DIMENSIONS: u32 = 4;

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
        metric: DistanceMetric::Cosine,
        dimensions,
        hnsw: Some(HnswParams::default()),
    })
}

fn ordered_index(id: u32, field: &str) -> IndexDescription {
    let mut index = IndexDescription::new(format!("by_{field}")).with_field(field, false);
    index.id = id;
    index
}

fn similarity(field: &str, index: usize) -> SimilarityField {
    SimilarityField {
        target_field: field.to_string(),
        vector: vec![1.0, 0.0, 0.0, 0.0],
        index,
    }
}

/// The shape that routes: one similarity, ordered by it descending, limited.
fn routable() -> SimilarityQuery {
    SimilarityQuery {
        limit: Some(10),
        offset: 0,
        similarities: vec![similarity("embedding", 3)],
        sole_order: Some(OrderKey {
            field_index: 3,
            descending: true,
        }),
    }
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
        similarities: vec![similarity("embedding", 3), similarity("other", 4)],
        ..routable()
    };
    assert_eq!(route(&two, &indexes), Err(NotRouted::NotOneSimilarity));
}

/// Ascending would ask for the *farthest*; ordering by something else, or by
/// several keys, needs documents beyond the k the graph returns.
#[test]
fn the_ordering_must_be_that_similarity_alone_descending() {
    let indexes = [vector_index(7, "embedding", DIMENSIONS)];

    for order in [
        None,
        Some(OrderKey {
            field_index: 3,
            descending: false,
        }),
        Some(OrderKey {
            field_index: 1,
            descending: true,
        }),
    ] {
        let query = SimilarityQuery {
            sole_order: order,
            ..routable()
        };
        assert_eq!(
            route(&query, &indexes),
            Err(NotRouted::NotOrderedBySimilarity),
            "{order:?} should not route"
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
        similarities: vec![similarity("embedding", 5)],
        sole_order: Some(OrderKey {
            field_index: 5,
            descending: true,
        }),
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
            ..similarity("embedding", 3)
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
                ..similarity("embedding", 3)
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
