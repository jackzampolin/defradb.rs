//! The trait an index kind implements.

use defra_core::thread_bounds::MaybeSendSync;

use super::{IndexKind, Neighbor};
use crate::error::Result;
use crate::vector::store::NodeId;

/// One vector index kind.
///
/// Every method speaks only of a node id and a vector, so a kind never sees a
/// document, a transaction or a collection. That is what keeps a kind testable
/// with no database, and what makes the next one cheap.
///
/// Deliberately not a port of pulsejetdb's `ANNIndex`, which is
/// build-then-query, holds its nodes in memory, and panics on a dimension
/// mismatch. Same idea, our shape: incremental, transactional, and
/// storage-free through [`VectorNodeStore`](crate::vector::store::VectorNodeStore).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait VectorIndexEngine: MaybeSendSync {
    /// Which algorithm this is.
    fn kind(&self) -> IndexKind;

    /// Adds `vector` under `id`, replacing any vector already there.
    async fn insert(&mut self, id: NodeId, vector: &[f32]) -> Result<()>;

    /// Removes `id`, returning whether this call was the one that did it.
    async fn delete(&mut self, id: NodeId) -> Result<bool>;

    /// Up to `k` nearest live nodes to `query`, nearest first.
    ///
    /// `effort` is how hard to look, in whatever unit the kind uses: HNSW reads
    /// it as `ef_search`, an IVF kind would read it as probes, an exact kind
    /// ignores it. `None` takes the kind's configured default. One knob rather
    /// than a per-kind options type because every kind's knob answers the same
    /// question, and a planner has to turn it without knowing which kind it is
    /// talking to.
    async fn search(&self, query: &[f32], k: usize, effort: Option<usize>)
        -> Result<Vec<Neighbor>>;
}
