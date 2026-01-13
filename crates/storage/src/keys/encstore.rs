/// Encstore keys for encrypted block storage
///
/// These keys are prefixed with 'e' at the store level and handle:
/// - Encrypted block storage (CID-based, via IPLD)
use crate::corekv::Key;
use cid::Cid;

/// EncstoreKey: Stores encryption metadata blocks via IPLD
///
/// Structure: Direct CID bytes (same as blockstore)
/// Example: Binary representation of encryption block CID
///
/// The encstore uses the same key format as blockstore but stores
/// encrypted IPLD blocks with encryption metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncstoreKey {
    /// IPFS Content Identifier (stored as raw bytes)
    pub cid: Cid,
}

impl EncstoreKey {
    /// Create a new EncstoreKey
    pub fn new(cid: Cid) -> Self {
        Self { cid }
    }

    /// Create from raw CID bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, cid::Error> {
        let cid = Cid::try_from(bytes)?;
        Ok(Self { cid })
    }
}

impl Key for EncstoreKey {
    fn bytes(&self) -> Vec<u8> {
        // Use raw CID bytes directly (binary format)
        self.cid.to_bytes()
    }

    fn to_string(&self) -> String {
        // For display/debugging, use string representation
        self.cid.to_string()
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
    fn test_encstore_key() {
        let cid = test_cid();
        let key = EncstoreKey::new(cid);

        let bytes = key.bytes();
        assert!(!bytes.is_empty());

        // Should be pure CID bytes (no prefix, no string encoding)
        assert_eq!(bytes, cid.to_bytes());

        // Round-trip test
        let key2 = EncstoreKey::from_bytes(&bytes).unwrap();
        assert_eq!(key, key2);

        // String representation for debugging
        let string = key.to_string();
        assert!(string.starts_with("bafy"));
    }

    #[test]
    fn test_encstore_key_from_invalid_bytes() {
        let invalid_bytes = vec![0xFF, 0xFF];
        assert!(EncstoreKey::from_bytes(&invalid_bytes).is_err());
    }

    #[test]
    fn test_encstore_key_different_cids() {
        let cid1 = test_cid();
        let cid2 =
            Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();

        let key1 = EncstoreKey::new(cid1);
        let key2 = EncstoreKey::new(cid2);

        assert_ne!(key1, key2);
        assert_ne!(key1.bytes(), key2.bytes());
    }
}
