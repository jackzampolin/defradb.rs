//! The trait an index kind implements.

use defra_core::thread_bounds::MaybeSendSync;

use super::{IndexKind, Neighbor};
use crate::error::Result;
use crate::vector::store::NodeId;

/// One vector index kind.
///
/// Every method speaks only of a node id and a vector, so a kind never sees a
/// document, a transaction or a collection, and stays testable with no
/// database. Same idea as pulsejetdb's `ANNIndex` but not its shape, which is
/// build-then-query, in-memory, and panics on a dimension mismatch.
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
    /// `effort` is how hard to look, in the kind's own unit: `ef_search` for
    /// HNSW, probes for an IVF kind, ignored by an exact one. `None` takes the
    /// kind's default. One knob rather than a per-kind options type, because a
    /// planner has to turn it without knowing which kind it holds.
    async fn search(&self, query: &[f32], k: usize, effort: Option<usize>)
        -> Result<Vec<Neighbor>>;
}
