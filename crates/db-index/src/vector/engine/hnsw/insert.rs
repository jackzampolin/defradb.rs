//! Graph construction: INSERT (paper Algorithm 1) and link maintenance.

use super::Hnsw;
use crate::error::{Error, Result};
use crate::vector::store::{Meta, Node, NodeId, VectorNodeStore};

impl<S: VectorNodeStore> Hnsw<S> {
    /// Adds `vector` under `id`, replacing any node already stored there.
    pub async fn insert(&mut self, id: NodeId, vector: &[f32]) -> Result<()> {
        let vector = self.prepared(vector);
        let top_level = self.sampler.level(self.params.ml);

        let Some(mut meta) = self.store.get_meta().await? else {
            self.store
                .put_node(Node::new(id, vector, top_level))
                .await?;
            return self
                .store
                .put_meta(Meta {
                    entry_point: id,
                    top_layer: top_level,
                })
                .await;
        };

        // Corruption, not an empty graph: starting from nothing here would
        // silently build a second disconnected component.
        let Some(entry) = self.store.get_node(meta.entry_point).await? else {
            return Err(Error::VectorEntryPointNotFound {
                entry_point: meta.entry_point.0,
            });
        };

        // Descend from the top, keeping only the closest node found so far,
        // down to the first layer this node will occupy.
        let mut current = self.candidate(&vector, entry);
        for layer in (top_level + 1..=meta.top_layer).rev() {
            if let Some(best) = self
                .search_greedy(&vector, current.id, layer)
                .await?
                .into_iter()
                .next()
            {
                current = best;
            }
        }

        // Stored before linking, unlike the reference. `add_link` prunes a
        // saturated neighbor by re-running the heuristic over its links, which
        // reads each from the store; a node that is not there yet reads as
        // absent and loses the back-link it was just given. The layers are
        // filled in by the second write below.
        self.store
            .put_node(Node::new(id, vector.clone(), top_level))
            .await?;

        let mut layers: Vec<Vec<NodeId>> = vec![Vec::new(); top_level + 1];
        let mut entry_points = vec![current];
        for layer in (0..=meta.top_layer.min(top_level)).rev() {
            let found = self
                .search_layer(&vector, entry_points, self.params.ef_construction, layer)
                .await?;

            let selected = self.select_neighbors(&found, self.params.m);
            layers[layer] = selected.iter().map(|c| c.id).collect();

            let max_links = self.params.max_links(layer);
            for neighbor in &selected {
                self.add_link(neighbor.id, id, layer, max_links).await?;
            }

            // Every neighbor found here seeds the next layer down, not just the
            // closest: narrowing to one point too early costs recall.
            entry_points = found;
        }

        let isolated = layers[0].is_empty();
        self.store
            .put_node(Node {
                id,
                vector,
                layers,
                deleted: false,
            })
            .await?;

        if top_level > meta.top_layer {
            meta.entry_point = id;
            meta.top_layer = top_level;
        }
        // Nothing to link to means everything reachable from the entry point is
        // tombstoned, so this node would be unreachable. Promoting it keeps it
        // findable and gives later inserts something live to attach to.
        if isolated {
            meta.entry_point = id;
        }
        self.store.put_meta(meta).await
    }

    /// Adds a back-link from `from` to `to`, pruning `from` back to
    /// `max_links` if that pushed it over. A link can outlive the node it
    /// points at; that is skipped, since one fewer link costs only recall.
    async fn add_link(
        &mut self,
        from: NodeId,
        to: NodeId,
        layer: usize,
        max_links: usize,
    ) -> Result<()> {
        let Some(mut node) = self.store.get_node(from).await? else {
            return Ok(());
        };

        // `from`'s own height was drawn at random and may be below `layer`.
        while node.layers.len() <= layer {
            node.layers.push(Vec::new());
        }
        node.layers[layer].push(to);

        if node.layers[layer].len() > max_links {
            let links = node.layers[layer].clone();
            let candidates = self.candidates_from_ids(&node.vector, &links).await?;
            let selected = self.select_neighbors(&candidates, max_links);
            node.layers[layer] = selected.iter().map(|c| c.id).collect();
        }

        self.store.put_node(node).await
    }
}
