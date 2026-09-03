//! Keys addressing a vector index's graph.
//!
//! Layout, matching Go's `keys.VectorIndexKey`:
//!
//! ```text
//! /<collShortID>/<indexID>/<epoch>/m            the meta singleton
//! /<collShortID>/<indexID>/<epoch>/n            the node prefix
//! /<collShortID>/<indexID>/<epoch>/n/<nodeID>   one node
//! ```
//!
//! The epoch namespaces one build of the index: a rebuild fills a fresh one,
//! the meta pointer moves, and the old epoch is dropped. It is in the layout
//! from the first commit because retrofitting it would migrate every entry.
//!
//! Integers use `encoding::encode_uvarint_ascending`, **not** the `keys::utils`
//! function of the same name that the other key types use. The two disagree for
//! every value and only this one matches Go.

use super::super::utils::SEPARATOR;
use crate::corekv::Key;
use crate::encoding::encode_uvarint_ascending;

/// Marks the meta singleton.
const META_DISCRIMINATOR: u8 = b'm';

/// Marks a node entry, and the prefix that scans them all.
const NODE_DISCRIMINATOR: u8 = b'n';

/// A key into one vector index's graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorIndexKey {
    pub collection_short_id: u32,
    pub index_id: u32,
    /// Namespaces a single build of the index.
    pub epoch: u32,
    /// True for the meta singleton, false for a node entry.
    pub is_meta: bool,
    /// Only meaningful when `is_meta` is false. Node ids start at 1, so zero
    /// marks the node-prefix key `.../n` rather than a node, matching how the
    /// other key types in this crate use a zero component.
    pub node_id: u64,
}

impl VectorIndexKey {
    /// The meta singleton for one epoch.
    pub fn meta(collection_short_id: u32, index_id: u32, epoch: u32) -> Self {
        Self {
            collection_short_id,
            index_id,
            epoch,
            is_meta: true,
            node_id: 0,
        }
    }

    /// One node.
    pub fn node(collection_short_id: u32, index_id: u32, epoch: u32, node_id: u64) -> Self {
        Self {
            collection_short_id,
            index_id,
            epoch,
            is_meta: false,
            node_id,
        }
    }

    /// Every node of one epoch and nothing else: the discriminator sits above
    /// the node id, so a scan cannot reach the meta key or another epoch.
    pub fn node_prefix(collection_short_id: u32, index_id: u32, epoch: u32) -> Self {
        Self {
            collection_short_id,
            index_id,
            epoch,
            is_meta: false,
            node_id: 0,
        }
    }
}

/// `/<collShortID>/<indexID>/<epoch>/`, shared by every key of one build of one
/// index: its nodes, its meta key, and every aux kind beside them.
///
/// Public because it is the only prefix that covers all of them, and dropping a
/// build means dropping all of them. Enumerating the kinds instead would leak
/// whichever kind an engine added last.
pub fn vector_epoch_prefix(collection_short_id: u32, index_id: u32, epoch: u32) -> Vec<u8> {
    epoch_prefix(collection_short_id, index_id, epoch)
}

/// `/<collShortID>/<indexID>/<epoch>/`, shared by every key in this space.
fn epoch_prefix(collection_short_id: u32, index_id: u32, epoch: u32) -> Vec<u8> {
    let mut buf = vec![SEPARATOR];
    buf = encode_uvarint_ascending(buf, collection_short_id as u64);
    buf.push(SEPARATOR);
    buf = encode_uvarint_ascending(buf, index_id as u64);
    buf.push(SEPARATOR);
    buf = encode_uvarint_ascending(buf, epoch as u64);
    buf.push(SEPARATOR);
    buf
}

impl Key for VectorIndexKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = epoch_prefix(self.collection_short_id, self.index_id, self.epoch);
        if self.is_meta {
            buf.push(META_DISCRIMINATOR);
        } else {
            buf.push(NODE_DISCRIMINATOR);
            if self.node_id != 0 {
                buf.push(SEPARATOR);
                buf = encode_uvarint_ascending(buf, self.node_id);
            }
        }
        buf
    }

    fn to_string(&self) -> String {
        let tail = if self.is_meta {
            "m".to_string()
        } else if self.node_id == 0 {
            "n".to_string()
        } else {
            format!("n/{}", self.node_id)
        };
        format!(
            "/{}/{}/{}/{}",
            self.collection_short_id, self.index_id, self.epoch, tail
        )
    }
}

/// A key in one index kind's private space, beside its graph.
///
/// `kind` separates concepts (coarse centroids from codebooks from inverted
/// lists) and `key` is the kind's own encoding, so a kind adds a concept
/// without a new key type. `m` and `n` are taken by the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorAuxKey<'k> {
    pub collection_short_id: u32,
    pub index_id: u32,
    pub epoch: u32,
    pub kind: u8,
    /// Empty scans every entry of `kind`.
    pub key: &'k [u8],
}

impl<'k> VectorAuxKey<'k> {
    pub fn new(
        collection_short_id: u32,
        index_id: u32,
        epoch: u32,
        kind: u8,
        key: &'k [u8],
    ) -> Self {
        Self {
            collection_short_id,
            index_id,
            epoch,
            kind,
            key,
        }
    }
}

impl Key for VectorAuxKey<'_> {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = epoch_prefix(self.collection_short_id, self.index_id, self.epoch);
        buf.push(self.kind);
        if !self.key.is_empty() {
            buf.push(SEPARATOR);
            buf.extend_from_slice(self.key);
        }
        buf
    }

    fn to_string(&self) -> String {
        format!(
            "/{}/{}/{}/{}/{}",
            self.collection_short_id,
            self.index_id,
            self.epoch,
            self.kind as char,
            hex::encode(self.key)
        )
    }
}
