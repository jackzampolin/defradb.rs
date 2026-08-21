//! The one vector-index lookup every `DocFetcher` shares.

use crate::index::vector::index::VectorIndex;
use datastore::NamespaceView;

use crate::collection::Collection;

/// Resolve `index_id` against the collection and search it.
///
/// Both halves of this must agree with the rest of the engine or the search
/// reads an empty namespace and returns nothing: the short id is
/// `resolved_root_id()`, which is what every write site indexes under, and the
/// descriptor comes from the queryable set, which is what the planner was
/// allowed to route to. A copy of this that resolves either differently is a
/// silent no-results bug.
pub(crate) async fn search_vector_index(
    collection: &Collection,
    datastore: &NamespaceView,
    index_id: u32,
    query_vector: &[f64],
    k: usize,
    effort: Option<usize>,
) -> query::error::Result<Vec<u64>> {
    let execution = query::error::QueryError::execution;

    let desc = collection
        .queryable_indexes()
        .iter()
        .find(|index| index.id == index_id && index.is_vector())
        .cloned()
        .ok_or_else(|| execution(format!("no queryable vector index with id {index_id}")))?;

    let index = VectorIndex::try_new(collection.resolved_root_id(), desc)
        .map_err(|e| execution(format!("vector index: {e}")))?;

    let mut view = datastore.clone();
    index
        .search(&mut view, query_vector, k, effort)
        .await
        .map(|hits| hits.into_iter().map(|hit| hit.id.0).collect())
        .map_err(|e| execution(format!("vector search: {e}")))
}
