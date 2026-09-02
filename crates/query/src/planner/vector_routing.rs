//! Deciding whether a query can be answered by a vector index.
//!
//! Ported from Go's `tryRouteSimilarityToVectorIndex`, with one deliberate
//! difference: Go refuses a query that carries a filter, because its graph
//! would have to over-fetch and backfill (its issue 5071). This engine does the
//! over-fetch instead of refusing: the filter is applied to documents, and
//! `ScanNode` asks the index for a wider `k` whenever the filter leaves the page
//! short, so a filtered query routes like any other and still returns a full
//! page. That matters because the hybrid retrieval path in `db-search` folds
//! `exclude_doc_ids` into a filter on essentially every call, and under Go's
//! rule it could never be routed at all.

use crate::executor::{GqlWarning, WARNING_CODE_VECTOR_INDEX_UNUSED};
use crate::mapper::{OrderDirection, Requestable, Select};
use schema::{DistanceMetric, IndexDescription, VectorIndexDescription};
use serde_json::Value as JsonValue;

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
    /// The query wants deleted documents; the index holds only live ones.
    ShowDeleted,
    /// The query vector's length does not match the index's declared
    /// dimensions. Scoring it would silently use the shared prefix only.
    DimensionMismatch { expected: u32, actual: usize },
}

impl NotRouted {
    /// The `reason` detail of a `VECTOR_INDEX_UNUSED` warning. Clients match on
    /// these, so they do not change once released. The first four are the
    /// reference's spellings; the last two are ours, for shapes it does not check.
    pub fn reason(&self) -> &'static str {
        match self {
            NotRouted::NoLimit => "noLimit",
            NotRouted::NotOneSimilarity => "multipleSimilarityFields",
            NotRouted::NotOrderedBySimilarity => "notOrderedBySimilarityDesc",
            NotRouted::NoVectorIndex => "noVectorIndex",
            NotRouted::ShowDeleted => "showDeleted",
            NotRouted::DimensionMismatch { .. } => "dimensionMismatch",
        }
    }
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
    /// Whether the query asks for deleted documents.
    pub show_deleted: bool,
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

/// The routable shape of a parsed query.
pub fn similarity_query(select: &Select) -> SimilarityQuery {
    SimilarityQuery {
        limit: select
            .limit
            .as_ref()
            .and_then(|limit| limit.limit)
            .map(|limit| limit as usize),
        offset: select
            .limit
            .as_ref()
            .map_or(0, |limit| limit.offset as usize),
        similarities: select
            .fields
            .iter()
            .filter_map(|field| match field {
                Requestable::Similarity(similarity) => Some(SimilarityField {
                    target_field: similarity.target_field.clone(),
                    vector: similarity.vector.clone(),
                    output_name: similarity.output_name().to_string(),
                }),
                _ => None,
            })
            .collect(),
        sole_order: select
            .order_by
            .as_ref()
            .and_then(|order| match order.conditions.as_slice() {
                [condition] => condition.fields.first().map(|field| OrderKey {
                    field: field.clone(),
                    descending: condition.direction == OrderDirection::Desc,
                }),
                _ => None,
            }),
        show_deleted: select.show_deleted,
    }
}

/// Decides whether `query` can be answered by one of `indexes`.
pub fn route(
    query: &SimilarityQuery,
    indexes: &[IndexDescription],
) -> Result<VectorRoute, NotRouted> {
    if query.show_deleted {
        return Err(NotRouted::ShowDeleted);
    }

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

    // No metric check: `SimilarityNode` scores by whatever metric this index
    // was built with (see `scoring_metric`), so the index's nearest neighbours
    // are the query's highest scorers whichever metric that is. A collection
    // cannot carry two vector indexes on one field with different metrics, so
    // the metric the scan would use and the metric the index ranks by are the
    // same one by construction.

    Ok(VectorRoute {
        index_id,
        query_vector: similarity.vector.clone(),
        k: limit.saturating_add(query.offset),
    })
}

/// The warning a declined routing raises, or `None` when there is nothing worth
/// saying.
///
/// `NoVectorIndex` produces nothing: a query scoring an unindexed field has no
/// index to miss, so warning about it would be noise on every ordinary
/// similarity query. That precision condition is the whole design; steady-state
/// noise has to be zero or the channel gets ignored.
pub fn vector_index_unused_warning(reason: &NotRouted, field: &str) -> Option<GqlWarning> {
    if matches!(reason, NotRouted::NoVectorIndex) {
        return None;
    }
    Some(build_warning(reason.reason(), field))
}

/// The `VECTOR_INDEX_UNUSED` warning for a query whose filter the vector index
/// path cannot serve, which keeps it from ever reaching `route()`. Go's own
/// planner calls this shape `filter`.
pub fn vector_index_unused_warning_for_filter(field: &str) -> GqlWarning {
    build_warning("filter", field)
}

/// The `VECTOR_INDEX_UNUSED` warning for a fetcher that does not support
/// vector search, which keeps the query from ever reaching `route()`. Ours:
/// the reference has no fetcher abstraction to decline through.
pub fn vector_index_unused_warning_for_unsupported_fetcher(field: &str) -> GqlWarning {
    build_warning("unsupportedFetcher", field)
}

fn build_warning(reason: &str, field: &str) -> GqlWarning {
    let mut detail = serde_json::Map::new();
    detail.insert("field".to_string(), JsonValue::String(field.to_string()));
    detail.insert("reason".to_string(), JsonValue::String(reason.to_string()));
    GqlWarning {
        code: WARNING_CODE_VECTOR_INDEX_UNUSED.to_string(),
        message: format!(
            "similarity query on field '{field}' did not use the vector index and read the whole collection"
        ),
        detail: Some(detail),
    }
}

/// The first entry in `similarities` whose field has a vector index, or `None`
/// when none do (including when `similarities` is empty).
///
/// This is the field a `VECTOR_INDEX_UNUSED` warning names: a field without an
/// index has no index to have missed, so nothing about it is worth reporting,
/// however the rest of the query is shaped.
pub fn first_indexed_similarity<'a>(
    similarities: &'a [SimilarityField],
    indexes: &[IndexDescription],
) -> Option<&'a str> {
    similarities
        .iter()
        .find(|field| vector_index_on(indexes, &field.target_field).is_some())
        .map(|field| field.target_field.as_str())
}

/// The metric a `SIMILARITY` on `field_name` is scored by.
///
/// The field's vector index metric, or cosine when it has no vector index,
/// matching the reference. Routing and the unrouted scan both resolve through
/// here, so the ranking a query gets does not depend on whether the index was
/// used.
pub fn scoring_metric(indexes: &[IndexDescription], field_name: &str) -> DistanceMetric {
    vector_index_on(indexes, field_name)
        .map(|(_, vector)| vector.metric)
        .unwrap_or_default()
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
