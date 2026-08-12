//! Deciding whether a query can be answered by a vector index.
//!
//! Ported from Go's `tryRouteSimilarityToVectorIndex`, with one deliberate
//! difference: Go refuses a query that carries a filter, because its graph
//! would have to over-fetch and backfill (its issue 5071). This engine filters
//! during the walk, so a filtered query routes like any other. That matters
//! because the hybrid retrieval path in `db-search` folds `exclude_doc_ids`
//! into a filter on essentially every call, and under Go's rule it could never
//! be routed at all.

use schema::{IndexDescription, VectorIndexDescription};

/// What a routable query resolved to.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorRoute {
    /// The index to search.
    pub index_id: u32,
    /// The vector to search for.
    pub query_vector: Vec<f64>,
    /// How many documents the graph must return.
    ///
    /// `limit + offset`, because the offset skips results the graph still has
    /// to produce; the limit node applies the offset afterwards.
    pub k: usize,
}

/// Why a query was not routed. Diagnostic only, but it is what an explain
/// output needs in order to say something more useful than "no".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotRouted {
    /// No limit, so there is no `k` to ask for.
    NoLimit,
    /// No `_similarity` field, or more than one: with two, which drives the
    /// search is ambiguous.
    NotOneSimilarity,
    /// Not ordered by that similarity alone, descending. Descending because a
    /// larger cosine means nearer; alone because a second sort key would need
    /// documents beyond the `k` returned.
    NotOrderedBySimilarity,
    /// No vector index on the target field.
    NoVectorIndex,
    /// The query vector's length does not match the index's declared
    /// dimensions. Scoring it would silently use the shared prefix only.
    DimensionMismatch { expected: u32, actual: usize },
}

/// The query shape the decision is made from.
///
/// A plain description rather than a planner type, so the rule is testable
/// without building a plan, and so the caller decides how to extract it.
#[derive(Debug, Clone, Default)]
pub struct SimilarityQuery {
    /// `limit`, absent when the query has none.
    pub limit: Option<usize>,
    /// `offset`, zero when absent.
    pub offset: usize,
    /// Every `_similarity` field in the selection, by target field name and
    /// query vector.
    pub similarities: Vec<SimilarityField>,
    /// The sole order-by key, when the query has exactly one. Ordering by
    /// several keys is not routable, so anything else is represented as
    /// `None`.
    pub sole_order: Option<OrderKey>,
}

/// One `_similarity` field in the selection.
#[derive(Debug, Clone, PartialEq)]
pub struct SimilarityField {
    /// The document field the vector is compared against.
    pub target_field: String,
    /// The vector to compare it to.
    pub vector: Vec<f64>,
    /// The name an order-by refers to: the alias when set, `SIMILARITY`
    /// otherwise. An alias-ordered query is therefore the same shape here as a
    /// directly-ordered one.
    pub output_name: String,
}

/// The single order-by key of a routable query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    /// The name being ordered by.
    pub field: String,
    /// Descending is required.
    pub descending: bool,
}

/// Decides whether `query` can be answered by one of `indexes`.
pub fn route(
    query: &SimilarityQuery,
    indexes: &[IndexDescription],
) -> Result<VectorRoute, NotRouted> {
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .ok_or(NotRouted::NoLimit)?;

    let [similarity] = query.similarities.as_slice() else {
        return Err(NotRouted::NotOneSimilarity);
    };

    let order = query
        .sole_order
        .as_ref()
        .ok_or(NotRouted::NotOrderedBySimilarity)?;
    if !order.descending || order.field != similarity.output_name {
        return Err(NotRouted::NotOrderedBySimilarity);
    }

    let (index_id, vector) =
        vector_index_on(indexes, &similarity.target_field).ok_or(NotRouted::NoVectorIndex)?;

    // A wrong-length query would be scored on its shared leading elements
    // only, which is silently wrong rather than merely approximate. Zero
    // dimensions means an embedding model fixes the length, so nothing to
    // check against yet.
    if vector.dimensions > 0 && similarity.vector.len() != vector.dimensions as usize {
        return Err(NotRouted::DimensionMismatch {
            expected: vector.dimensions,
            actual: similarity.vector.len(),
        });
    }

    Ok(VectorRoute {
        index_id,
        query_vector: similarity.vector.clone(),
        k: limit.saturating_add(query.offset),
    })
}

/// The vector index over `field_name`, if a collection has one.
fn vector_index_on(
    indexes: &[IndexDescription],
    field_name: &str,
) -> Option<(u32, VectorIndexDescription)> {
    indexes.iter().find_map(|index| {
        let vector = index.vector()?;
        let indexes_field = index
            .fields
            .first()
            .is_some_and(|field| field.name == field_name);
        indexes_field.then_some((index.id, *vector))
    })
}
