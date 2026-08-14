//! An in-memory [`VectorNodeStore`].
//!
//! Not a test fixture: it is the store an ephemeral or embedded index uses, and
//! it is what lets the engine be exercised with no database. A `BTreeMap` keeps
//! iteration order stable, so a brute-force baseline built from it is
//! reproducible.

use std::collections::BTreeMap;

use defra_core::thread_bounds::MaybeSend;

use super::{Meta, Node, NodeId, VectorNodeStore};
use crate::error::Result;

/// A graph held entirely in memory.
#[derive(Debug, Default, Clone)]
pub struct MemoryNodeStore {
    nodes: BTreeMap<NodeId, Node>,
    meta: Option<Meta>,
}

impl MemoryNodeStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Nodes held, tombstones included.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl VectorNodeStore for MemoryNodeStore {
    async fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        Ok(self.nodes.get(&id).cloned())
    }

    async fn put_node(&mut self, node: Node) -> Result<()> {
        self.nodes.insert(node.id, node);
        Ok(())
    }

    async fn get_meta(&self) -> Result<Option<Meta>> {
        Ok(self.meta)
    }

    async fn put_meta(&mut self, meta: Meta) -> Result<()> {
        self.meta = Some(meta);
        Ok(())
    }

    async fn iterate_nodes<F>(&self, mut visit: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()> + MaybeSend,
    {
        for node in self.nodes.values() {
            if node.deleted {
                continue;
            }
            visit(node.clone())?;
        }
        Ok(())
    }
}
