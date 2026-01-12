//! IPLD block types and operations

use crate::{types::DocId, Result};
use serde::{Deserialize, Serialize};

/// Content Identifier (CID) for content-addressed blocks
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Cid(String);

impl Cid {
    /// Create a new CID from a string
    pub fn new(cid: impl Into<String>) -> Self {
        Self(cid.into())
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse a CID from a string
    pub fn parse(s: &str) -> Result<Self> {
        // TODO: Validate CID format
        Ok(Self(s.to_string()))
    }
}

impl std::fmt::Display for Cid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An IPLD block - content-addressed data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    /// Content identifier
    pub cid: Cid,

    /// Block data (CBOR-encoded)
    pub data: Vec<u8>,

    /// Links to other blocks
    pub links: Vec<Cid>,
}

impl Block {
    /// Create a new block
    pub fn new(cid: Cid, data: Vec<u8>) -> Self {
        Self {
            cid,
            data,
            links: Vec::new(),
        }
    }

    /// Add a link to another block
    pub fn add_link(&mut self, cid: Cid) {
        self.links.push(cid);
    }

    /// Get the block size in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// Block delta - represents a change to a document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockDelta {
    /// Document this delta applies to
    pub doc_id: DocId,

    /// The block CID
    pub cid: Cid,

    /// Priority for conflict resolution
    pub priority: u64,

    /// Links to parent blocks (previous document states)
    pub links: Vec<Cid>,

    /// The actual delta data
    pub data: Vec<u8>,
}

impl BlockDelta {
    /// Create a new block delta
    pub fn new(doc_id: DocId, cid: Cid, priority: u64) -> Self {
        Self {
            doc_id,
            cid,
            priority,
            links: Vec::new(),
            data: Vec::new(),
        }
    }
}

/// Block header - metadata about a block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    /// Block CID
    pub cid: Cid,

    /// Block height in the DAG
    pub height: u64,

    /// Links to previous blocks
    pub links: Vec<Cid>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cid_creation() {
        let cid = Cid::new("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
        assert!(!cid.as_str().is_empty());
    }

    #[test]
    fn test_block_creation() {
        let cid = Cid::new("test-cid");
        let data = vec![1, 2, 3, 4];
        let block = Block::new(cid, data);

        assert_eq!(block.size(), 4);
        assert_eq!(block.links.len(), 0);
    }
}
