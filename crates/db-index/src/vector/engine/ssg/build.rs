//! Building the pruned graph from HNSW's layer 0.

use std::collections::HashSet;

use super::codec::{self, BuiltState};
use super::Ssg;
use crate::error::{Error, Result};
use crate::vector::engine::ann::{Candidate, EdgeSelector};
use crate::vector::engine::select::Angular;
use crate::vector::store::{NodeId, VectorNodeStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsgBuildReport {
    pub nodes: u64,
    pub edges: u64,
    /// Nodes the connectivity pass had to attach, which the pruning had left
    /// unreachable from the entry point.
    pub reattached: u64,
    pub state: BuiltState,
}

impl<S: VectorNodeStore> Ssg<S> {
    /// Prunes every node's layer-0 neighbours by angle, then repairs
    /// connectivity so no node is stranded.
    ///
    /// One node's neighbour list is resident at a time; the visited set is `N`
    /// bits, which is the only term that grows with the corpus.
    pub async fn build(&mut self) -> Result<SsgBuildReport> {
        let Some(meta) = self.store().get_meta().await? else {
            return Err(Error::Other(
                "vector index: nothing to build an SSG graph from".into(),
            ));
        };

        let selector = Angular::new(self.params().angle as f32);
        let max = self.params().r as usize;
        let metric = self.metric();

        let mut ids = Vec::new();
        self.store()
            .iterate_nodes(|node| {
                ids.push(node.id);
                Ok(())
            })
            .await?;
        if ids.is_empty() {
            return Err(Error::Other(
                "vector index: nothing to build an SSG graph from".into(),
            ));
        }

        let mut edges = 0u64;
        for id in &ids {
            let Some(node) = self.store().get_node(*id).await? else {
                continue;
            };
            let mut candidates = Vec::new();
            for neighbour in node.neighbors(0) {
                if let Some(other) = self.store().get_node(*neighbour).await? {
                    if other.deleted {
                        continue;
                    }
                    candidates.push(Candidate {
                        id: other.id,
                        distance: metric.distance_stored(&node.vector, &other.vector),
                        vector: other.vector.into(),
                    });
                }
            }

            let kept: Vec<NodeId> = selector
                .select(metric, &node.vector, &candidates, max)
                .into_iter()
                .map(|c| c.id)
                .collect();
            edges += kept.len() as u64;
            self.store_mut()
                .put_aux(
                    codec::ADJACENCY,
                    &codec::node_key(*id),
                    &codec::encode_neighbours(&kept),
                )
                .await?;
        }

        let reattached = self
            .repair_connectivity(meta.entry_point, &ids, max)
            .await?;

        let state = BuiltState {
            entry_point: meta.entry_point,
            nodes: ids.len() as u64,
        };
        self.store_mut()
            .put_aux(codec::STATE, b"", &codec::encode_state(&state))
            .await?;

        Ok(SsgBuildReport {
            nodes: ids.len() as u64,
            edges,
            reattached,
            state,
        })
    }

    /// Attaches a node written after the build.
    ///
    /// Without this the node reaches the HNSW graph but never the pruned one a
    /// search walks, so it is invisible until the next rebuild.
    pub(super) async fn attach(&mut self, id: NodeId) -> Result<()> {
        let Some(node) = self.store().get_node(id).await? else {
            return Ok(());
        };
        let metric = self.metric();
        let max = self.params().r as usize;
        let selector = Angular::new(self.params().angle as f32);

        let mut candidates = Vec::new();
        for neighbour in node.neighbors(0) {
            if let Some(other) = self.store().get_node(*neighbour).await? {
                if other.deleted {
                    continue;
                }
                candidates.push(Candidate {
                    id: other.id,
                    distance: metric.distance_stored(&node.vector, &other.vector),
                    vector: other.vector.into(),
                });
            }
        }

        let kept: Vec<NodeId> = selector
            .select(metric, &node.vector, &candidates, max)
            .into_iter()
            .map(|c| c.id)
            .collect();
        self.store_mut()
            .put_aux(
                codec::ADJACENCY,
                &codec::node_key(id),
                &codec::encode_neighbours(&kept),
            )
            .await?;

        // Edges out are not enough: a walk arrives from somewhere, so the
        // neighbours need edges back or nothing ever reaches the new node.
        for neighbour in kept {
            let mut theirs = self.neighbours(neighbour).await?;
            if theirs.contains(&id) {
                continue;
            }
            if theirs.len() >= max {
                theirs.pop();
            }
            theirs.push(id);
            self.store_mut()
                .put_aux(
                    codec::ADJACENCY,
                    &codec::node_key(neighbour),
                    &codec::encode_neighbours(&theirs),
                )
                .await?;
        }
        Ok(())
    }

    /// Angular pruning can strand a node: nothing reachable from the entry
    /// point points at it, so no search can ever return it. Each stranded node
    /// is attached to the nearest node the walk *can* reach.
    async fn repair_connectivity(
        &mut self,
        entry: NodeId,
        ids: &[NodeId],
        max: usize,
    ) -> Result<u64> {
        let mut visited: HashSet<NodeId> = HashSet::with_capacity(ids.len());
        let mut stack = vec![entry];
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            for neighbour in self.neighbours(id).await? {
                if !visited.contains(&neighbour) {
                    stack.push(neighbour);
                }
            }
        }

        let mut reattached = 0u64;
        for id in ids {
            if visited.contains(id) {
                continue;
            }
            let Some(node) = self.store().get_node(*id).await? else {
                continue;
            };

            // The nearest reachable node, found by walking the graph as a
            // search would, so the repair matches how it will be traversed.
            let host = self.nearest_reachable(&node.vector, entry).await?;
            let mut hosts = self.neighbours(host).await?;
            if !hosts.contains(id) {
                if hosts.len() >= max {
                    hosts.pop();
                }
                hosts.push(*id);
                self.store_mut()
                    .put_aux(
                        codec::ADJACENCY,
                        &codec::node_key(host),
                        &codec::encode_neighbours(&hosts),
                    )
                    .await?;
            }

            reattached += 1;
            visited.insert(*id);
            for neighbour in self.neighbours(*id).await? {
                visited.insert(neighbour);
            }
        }
        Ok(reattached)
    }

    async fn nearest_reachable(&self, target: &[f32], entry: NodeId) -> Result<NodeId> {
        let metric = self.metric();
        let mut current = entry;
        let mut best = match self.store().get_node(entry).await? {
            Some(node) => metric.distance_stored(target, &node.vector),
            None => return Ok(entry),
        };

        loop {
            let mut moved = false;
            for neighbour in self.neighbours(current).await? {
                let Some(node) = self.store().get_node(neighbour).await? else {
                    continue;
                };
                let distance = metric.distance_stored(target, &node.vector);
                if distance < best {
                    best = distance;
                    current = neighbour;
                    moved = true;
                }
            }
            if !moved {
                return Ok(current);
            }
        }
    }
}
