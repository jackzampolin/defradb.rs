//! The trait an index kind implements.

use defra_core::thread_bounds::MaybeSendSync;

use super::{EngineKind, Neighbor};
use crate::error::Result;
use crate::vector::core::Element;
use crate::vector::store::NodeId;

/// Decides which nodes a search may return.
///
/// Separate from the traversal on purpose: a node that fails the predicate is
/// still walked through, exactly as a tombstone is, because the graph's
/// connectivity does not care what the query is looking for. Excluding such a
/// node from the walk instead of from the results would strand whole regions
/// behind it.
pub trait Admit: MaybeSendSync {
    fn admits(&self, id: NodeId) -> bool;
}

/// Every node qualifies.
#[derive(Debug, Clone, Copy)]
pub struct AdmitAll;

impl Admit for AdmitAll {
    fn admits(&self, _id: NodeId) -> bool {
        true
    }
}

impl<F: Fn(NodeId) -> bool + MaybeSendSync> Admit for F {
    fn admits(&self, id: NodeId) -> bool {
        self(id)
    }
}

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
    fn kind(&self) -> EngineKind;

    /// Adds `vector` under `id`, replacing any vector already there.
    ///
    /// Generic over the element width because a vector reaches an index as
    /// `f64` at least as often as `f32`: JSON and GraphQL have no other number
    /// type. Narrowing at the call site would make every caller decide how to
    /// do it; here it happens once, where the stored width is known.
    async fn insert<E: Element>(&mut self, id: NodeId, vector: &[E]) -> Result<()>;

    /// Removes `id`, returning whether this call was the one that did it.
    async fn delete(&mut self, id: NodeId) -> Result<bool>;

    /// Up to `k` nearest live nodes to `query`, nearest first.
    ///
    /// Takes any element width, for the same reason [`insert`](Self::insert)
    /// does.
    ///
    /// Defaulted, so a kind implements
    /// [`search_where`](Self::search_where) alone and the two can never
    /// disagree about anything but the filter.
    ///
    /// `effort` is how hard to look, in the kind's own unit: `ef_search` for
    /// HNSW, probes for an IVF kind, ignored by an exact one. `None` takes the
    /// kind's default. One knob rather than a per-kind options type, because a
    /// planner has to turn it without knowing which kind it holds.
    async fn search<E: Element>(
        &self,
        query: &[E],
        k: usize,
        effort: Option<usize>,
    ) -> Result<Vec<Neighbor>> {
        self.search_where(query, k, effort, &AdmitAll).await
    }

    /// Up to `k` nearest live nodes that `admit` accepts, nearest first.
    ///
    /// **Every kind must provide this**, not only the graph ones: filtering is
    /// part of what a vector index is for, so it belongs in the contract rather
    /// than in whichever implementation happened to need it first.
    ///
    /// This is filtered nearest-neighbour search. The predicate decides what
    /// may be *returned*, never what may be *traversed*: a rejected node is
    /// still a route to its neighbors, so the graph stays connected however
    /// selective the filter is.
    ///
    /// The cost of that selectivity is real and worth stating. The walk stops
    /// early once it holds `ef` results none of which can be improved on; a
    /// filter that rejects nearly everything never fills those `ef` slots, so
    /// the walk keeps going and degrades toward a full traversal. That is the
    /// correct amount of work rather than a pathology: it is bounded by the
    /// corpus, which is exactly what the unrouted query would have read.
    async fn search_where<E: Element, A: Admit>(
        &self,
        query: &[E],
        k: usize,
        effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>>;
}
