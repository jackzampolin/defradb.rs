//! The persistence port, mirroring Go's `hnsw.NodeStore`.
//!
//! The engine never imports a concrete store, so it is testable with no
//! database at all. Any medium can back this: the in-memory store here, or a
//! transactional KV adapter.

pub mod memory;

pub use memory::MemoryNodeStore;

use defra_core::thread_bounds::{MaybeSend, MaybeSendSync};

use crate::error::Result;

/// Identifies one vector in the graph.
///
/// The adapter maps its own identifiers (a document short id) onto this; the
/// engine attaches no meaning to the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

/// The persisted form of a graph node.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub id: NodeId,
    /// Normalized under the cosine metric, which is the only one the query
    /// surface exposes.
    pub vector: Vec<f32>,
    /// `layers[l]` holds the neighbor ids at layer `l`, so `layers.len()` is
    /// the node's top layer plus one.
    pub layers: Vec<Vec<NodeId>>,
    /// Tombstone. The node stays linked so traversal through it is preserved,
    /// but it is never returned from a search.
    pub deleted: bool,
}

impl Node {
    /// A node of the given height with no links yet.
    pub fn new(id: NodeId, vector: Vec<f32>, top_layer: usize) -> Self {
        Self {
            id,
            vector,
            layers: vec![Vec::new(); top_layer + 1],
            deleted: false,
        }
    }

    /// Neighbors at `layer`, empty when the node does not reach that high.
    pub fn neighbors(&self, layer: usize) -> &[NodeId] {
        self.layers.get(layer).map_or(&[], Vec::as_slice)
    }
}

/// The graph's global state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Meta {
    /// Where every search starts.
    pub entry_point: NodeId,
    /// Highest layer any node currently occupies.
    pub top_layer: usize,
}

/// Where a graph lives.
///
/// Generic rather than `dyn`, matching `CollectionIndex`: the KV adapter is
/// parameterised over the transaction type, which a trait object cannot carry.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait VectorNodeStore: MaybeSendSync {
    /// `None` when no such node has been stored.
    async fn get_node(&self, id: NodeId) -> Result<Option<Node>>;

    /// Stores `node`, replacing any node with the same id.
    async fn put_node(&mut self, node: Node) -> Result<()>;

    /// `None` when no graph has been built yet, so there is no entry point to
    /// start from.
    async fn get_meta(&self) -> Result<Option<Meta>>;

    /// Stores `meta`, replacing any previous value.
    async fn put_meta(&mut self, meta: Meta) -> Result<()>;

    /// Visits every **non-deleted** node, in unspecified order, stopping early
    /// if `visit` returns an error. Used for brute-force baselines and, later,
    /// for rebuilding an epoch.
    async fn iterate_nodes<F>(&self, visit: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()> + MaybeSend;

    /// A namespaced blob space private to this index and epoch, for whatever a
    /// kind needs beyond nodes: coarse centroids, codebooks, inverted lists.
    ///
    /// `kind` separates concepts and `key` is the kind's own encoding, so a
    /// kind adds a concept without a port change. Graph-only kinds never call
    /// these.
    async fn get_aux(&self, kind: u8, key: &[u8]) -> Result<Option<Vec<u8>>>;

    async fn put_aux(&mut self, kind: u8, key: &[u8], value: &[u8]) -> Result<()>;

    /// Visits every entry of `kind` whose key starts with `key_prefix`, in key
    /// order, one at a time.
    async fn iterate_aux<F>(&self, kind: u8, key_prefix: &[u8], visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()> + MaybeSend;
}
