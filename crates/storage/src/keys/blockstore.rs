/// Blockstore keys for IPLD blocks and merkle tree nodes
///
/// These keys are prefixed with 'b' at the store level and handle:
/// - Block storage (CID-based)
/// - Block merge tracking (for CRDT operations)

use crate::corekv::Key;
use cid::Cid;

/// Merge marker prefix byte
pub const MERGE_PREFIX: u8 = b'm';

/// BlockstoreKey: Maps CID to block data
///
/// Structure: Direct CID bytes as key
/// Example: Binary representation of bafyrei...
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockstoreKey {
    /// IPFS Content Identifier (stored as raw bytes)
    pub cid: Cid,
}

impl BlockstoreKey {
    /// Create a new BlockstoreKey
    pub fn new(cid: Cid) -> Self {
        Self { cid }
    }

    /// Create from raw CID bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, cid::Error> {
        let cid = Cid::try_from(bytes)?;
        Ok(Self { cid })
    }
}

impl Key for BlockstoreKey {
    fn bytes(&self) -> Vec<u8> {
        // Use raw CID bytes directly (binary format)
        self.cid.to_bytes()
    }

    fn to_string(&self) -> String {
        // For display/debugging, use string representation
        self.cid.to_string()
    }
}

/// ToMergeIndexKey: Tracks blocks that haven't been merged into permanent store
///
/// Structure: [MERGE_PREFIX_BYTE][CID_BYTES]
/// Example: 0x6D ('m') + CID bytes
///
/// This is used to identify blocks pending merge in the blockstore for CRDT operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToMergeIndexKey {
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl ToMergeIndexKey {
    /// Create a new ToMergeIndexKey
    pub fn new(cid: Cid) -> Self {
        Self { cid }
    }

    /// Create from raw bytes (including merge prefix)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, cid::Error> {
        if bytes.is_empty() || bytes[0] != MERGE_PREFIX {
            return Err(cid::Error::ParsingError);
        }
        let cid = Cid::try_from(&bytes[1..])?;
        Ok(Self { cid })
    }

    /// Get the merge prefix for iteration
    pub fn merge_prefix() -> Vec<u8> {
        vec![MERGE_PREFIX]
    }

    /// Check if a key bytes starts with the merge prefix
    pub fn is_merge_key(bytes: &[u8]) -> bool {
        !bytes.is_empty() && bytes[0] == MERGE_PREFIX
    }
}

impl Key for ToMergeIndexKey {
    fn bytes(&self) -> Vec<u8> {
        // Prefix with merge marker byte
        let mut buf = vec![MERGE_PREFIX];
        buf.extend_from_slice(&self.cid.to_bytes());
        buf
    }

    fn to_string(&self) -> String {
        // For display/debugging
        format!("m/{}", self.cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Test CID (V1, dag-pb, sha2-256)
    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    #[test]
    fn test_blockstore_key() {
        let cid = test_cid();
        let key = BlockstoreKey::new(cid);

        let bytes = key.bytes();
        assert!(!bytes.is_empty());

        // Should be pure CID bytes (no prefix, no string encoding)
        assert_eq!(bytes, cid.to_bytes());

        // Round-trip test
        let key2 = BlockstoreKey::from_bytes(&bytes).unwrap();
        assert_eq!(key, key2);

        // String representation for debugging
        let string = key.to_string();
        assert!(string.starts_with("bafy"));
    }

    #[test]
    fn test_to_merge_index_key() {
        let cid = test_cid();
        let key = ToMergeIndexKey::new(cid);

        let bytes = key.bytes();
        assert!(!bytes.is_empty());

        // Should start with merge prefix
        assert_eq!(bytes[0], MERGE_PREFIX);
        assert_eq!(bytes[0], b'm');

        // Rest should be CID bytes
        assert_eq!(&bytes[1..], cid.to_bytes().as_slice());

        // Round-trip test
        let key2 = ToMergeIndexKey::from_bytes(&bytes).unwrap();
        assert_eq!(key, key2);

        // String representation
        let string = key.to_string();
        assert!(string.starts_with("m/"));
    }

    #[test]
    fn test_merge_key_detection() {
        let cid = test_cid();
        let merge_key = ToMergeIndexKey::new(cid);
        let block_key = BlockstoreKey::new(cid);

        assert!(ToMergeIndexKey::is_merge_key(&merge_key.bytes()));
        assert!(!ToMergeIndexKey::is_merge_key(&block_key.bytes()));
    }

    #[test]
    fn test_merge_prefix() {
        let prefix = ToMergeIndexKey::merge_prefix();
        assert_eq!(prefix, vec![MERGE_PREFIX]);
        assert_eq!(prefix, vec![b'm']);
    }

    #[test]
    fn test_blockstore_key_from_invalid_bytes() {
        let invalid_bytes = vec![0xFF, 0xFF];
        assert!(BlockstoreKey::from_bytes(&invalid_bytes).is_err());
    }

    #[test]
    fn test_to_merge_index_key_from_invalid_bytes() {
        // Missing merge prefix
        let cid = test_cid();
        assert!(ToMergeIndexKey::from_bytes(&cid.to_bytes()).is_err());

        // Wrong prefix
        let mut bytes = vec![b'x'];
        bytes.extend_from_slice(&cid.to_bytes());
        assert!(ToMergeIndexKey::from_bytes(&bytes).is_err());
    }
}
