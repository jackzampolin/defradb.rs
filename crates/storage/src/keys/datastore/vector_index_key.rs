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
//! The epoch namespaces one build of the index. A rebuild fills a fresh epoch
//! that is disjoint from the live one, then the meta pointer moves and the old
//! epoch is dropped. It is in the layout from the first commit even though
//! nothing triggers a rebuild yet, because retrofitting it later would mean
//! migrating every entry of every vector index.
//!
//! Integer components use `encoding::encode_uvarint_ascending`, the
//! CockroachDB-derived encoder, because that is what Go's `internal/encoding`
//! uses and these keys are checked against bytes Go produced. Note that this is
//! *not* the `keys::utils` encoder of the same name that the other key types
//! reach for: the two disagree for every value, and only this one matches Go.

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
    /// Only meaningful when `is_meta` is false.
    ///
    /// Node ids come from a sequence starting at 1, so zero never addresses a
    /// node. It marks the node-prefix key `.../n`, which scans every node of
    /// the epoch, rather than a full node key `.../n/<id>`. The other key types
    /// in this crate use a zero component as their prefix the same way.
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

    /// Every node of one epoch, and nothing else. A scan from here never
    /// crosses into another epoch, another index, or the meta key, because the
    /// discriminator sits above the node id in the key.
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

impl Key for VectorIndexKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, self.collection_short_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, self.index_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, self.epoch as u64);
        buf.push(SEPARATOR);
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
