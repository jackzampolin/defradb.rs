//! [`VectorNodeStore`] over a transaction.
//!
//! Borrows the transaction rather than owning it, so every write lands in the
//! caller's transaction. That is also what gives the engine its single-writer
//! discipline without a lock.

use bytes::Bytes;
use defra_core::thread_bounds::MaybeSend;
use storage::corekv::{IterOptions, Key, Reader, Writer};
use storage::keys::datastore::{vector_epoch_prefix, VectorAuxKey, VectorIndexKey};

use super::codec::{decode_meta, decode_node, encode_meta, encode_node};
use super::store::{Meta, Node, NodeId, VectorNodeStore};
use crate::index::error::Result;

/// One epoch of one vector index, over a transaction.
#[derive(Debug)]
pub struct KvNodeStore<'txn, T> {
    txn: &'txn mut T,
    collection_short_id: u32,
    index_id: u32,
    epoch: u32,
}

impl<'txn, T> KvNodeStore<'txn, T> {
    pub fn new(txn: &'txn mut T, collection_short_id: u32, index_id: u32, epoch: u32) -> Self {
        Self {
            txn,
            collection_short_id,
            index_id,
            epoch,
        }
    }

    /// Which build of the index this reads and writes.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    fn aux_key(&self, kind: u8, key: &[u8]) -> Vec<u8> {
        VectorAuxKey::new(
            self.collection_short_id,
            self.index_id,
            self.epoch,
            kind,
            key,
        )
        .bytes()
    }

    fn node_key(&self, id: NodeId) -> Vec<u8> {
        VectorIndexKey::node(self.collection_short_id, self.index_id, self.epoch, id.0).bytes()
    }

    fn meta_key(&self) -> Vec<u8> {
        VectorIndexKey::meta(self.collection_short_id, self.index_id, self.epoch).bytes()
    }

    fn node_prefix(&self) -> Vec<u8> {
        VectorIndexKey::node_prefix(self.collection_short_id, self.index_id, self.epoch).bytes()
    }

    /// Every key of this build: nodes, meta, and every aux kind beside them.
    fn epoch_prefix(&self) -> Vec<u8> {
        vector_epoch_prefix(self.collection_short_id, self.index_id, self.epoch)
    }
}

impl<T: Reader + Writer + MaybeSend> KvNodeStore<'_, T> {
    /// Removes every key of this epoch, in batches, so the memory held is
    /// bounded by the batch size rather than by the number of nodes.
    ///
    /// The scan is over the whole epoch prefix rather than the node prefix,
    /// which is what makes "every key" true: the aux space holds a partitioned
    /// kind's centroids, codebooks and inverted lists, and a graph kind's
    /// adjacency, and none of it is under the node prefix. Leaving it behind
    /// corrupts a reindex, because a surviving trained-state marker makes the
    /// engine append to lists built from centroids that are gone, and leaks the
    /// whole aux keyspace on a drop, because nothing else ever removes it.
    ///
    /// Prefix rather than a per-kind sweep so a kind added later is covered
    /// without anyone remembering to add it here.
    pub async fn clear(&mut self) -> Result<()> {
        loop {
            let mut batch = Vec::new();
            {
                let mut iter = self
                    .txn
                    .iterator(IterOptions::default().with_prefix(self.epoch_prefix()))
                    .await?;
                while let Some(pair) = iter.next().await? {
                    batch.push(pair.key);
                    if batch.len() >= CLEAR_BATCH {
                        break;
                    }
                }
            }
            if batch.is_empty() {
                break;
            }
            for key in batch {
                self.txn.delete(&key).await?;
            }
        }
        Ok(())
    }
}

/// The iterator is reopened per batch because deleting through a live one is
/// not defined for every backend.
const CLEAR_BATCH: usize = 1024;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<T: Reader + Writer + MaybeSend> VectorNodeStore for KvNodeStore<'_, T> {
    async fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        match self.txn.get(&self.node_key(id)).await? {
            Some(bytes) => Ok(Some(decode_node(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn put_node(&mut self, node: Node) -> Result<()> {
        let key = self.node_key(node.id);
        self.txn.set(&key, &encode_node(&node)).await?;
        Ok(())
    }

    async fn get_meta(&self) -> Result<Option<Meta>> {
        match self.txn.get(&self.meta_key()).await? {
            Some(bytes) => Ok(Some(decode_meta(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn put_meta(&mut self, meta: Meta) -> Result<()> {
        let key = self.meta_key();
        self.txn.set(&key, &encode_meta(&meta)).await?;
        Ok(())
    }

    /// Streams the epoch's nodes, holding one at a time rather than the graph.
    /// The prefix stops at this epoch's node discriminator.
    async fn iterate_nodes<F>(&self, mut visit: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()> + MaybeSend,
    {
        let mut iter = self
            .txn
            .iterator(IterOptions::default().with_prefix(self.node_prefix()))
            .await?;
        while let Some(pair) = iter.next().await? {
            let node = decode_node(&pair.value)?;
            if node.deleted {
                continue;
            }
            visit(node)?;
        }
        Ok(())
    }

    async fn get_aux(&self, kind: u8, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.txn.get(&self.aux_key(kind, key)).await?)
    }

    async fn put_aux(&mut self, kind: u8, key: &[u8], value: &[u8]) -> Result<()> {
        let full = self.aux_key(kind, key);
        self.txn.set(&full, value).await?;
        Ok(())
    }

    async fn delete_aux(&mut self, kind: u8, key: &[u8]) -> Result<()> {
        let full = self.aux_key(kind, key);
        self.txn.delete(&full).await?;
        Ok(())
    }

    /// Streams one kind's entries, holding one at a time. The prefix stops at
    /// this epoch's discriminator, so a scan cannot reach the graph or another
    /// kind.
    async fn iterate_aux<F>(&self, kind: u8, key_prefix: &[u8], mut visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()> + MaybeSend,
    {
        let prefix = self.aux_key(kind, key_prefix);
        // Every stored entry has a separator after the discriminator, whether
        // or not the scan narrowed by a prefix.
        let strip = self.aux_key(kind, &[]).len() + 1;
        let mut iter = self
            .txn
            .iterator(IterOptions::default().with_prefix(prefix))
            .await?;
        while let Some(pair) = iter.next().await? {
            let key = pair.key.get(strip..).unwrap_or_default();
            visit(key, &pair.value)?;
        }
        Ok(())
    }
}
