// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Document ID type with content-addressed generation

use cid::Cid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};

/// DocID version constant (matches Go's DocIDV0)
pub const DOC_ID_V0: u16 = 0x01;

/// SDN Namespace UUID for DocID v5 generation (matches Go's SDNNamespaceV0)
/// "c94acbfa-dd53-40d0-97f3-29ce16c333fc"
pub const SDN_NAMESPACE_V0: Uuid = Uuid::from_bytes([
    0xc9, 0x4a, 0xcb, 0xfa, 0xdd, 0x53, 0x40, 0xd0, 0x97, 0xf3, 0x29, 0xce, 0x16, 0xc3, 0x33, 0xfc,
]);

/// Document identifier for DefraDB documents.
///
/// DocID is the root identifier for documents in DefraDB. It consists of:
/// - A version number (currently only v0 is valid)
/// - A UUID derived from the document's content CID
/// - Optionally, the original CID (not always available when parsing from string)
///
/// The string format is: `{base32(version)}-{uuid}`
/// Example: `bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocID {
    version: u16,
    uuid: Uuid,
    #[serde(skip)]
    cid: Option<Cid>,
}

impl DocID {
    /// Create a new DocID v0 from a content CID.
    ///
    /// The UUID is generated using UUID v5 with the SDN namespace and the CID string.
    pub fn new_v0(data_cid: Cid) -> Self {
        let uuid = Uuid::new_v5(&SDN_NAMESPACE_V0, data_cid.to_string().as_bytes());
        Self {
            version: DOC_ID_V0,
            uuid,
            cid: Some(data_cid),
        }
    }

    /// Parse a DocID from its string representation.
    ///
    /// Format: `{base32(version)}-{uuid}`
    pub fn from_string(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        if parts.len() != 2 {
            return Err(Error::MalformedDocID);
        }

        let version_str = parts[0];
        let uuid_str = parts[1];

        // Decode the version from base32
        let (_, version_bytes) = multibase::decode(version_str)?;
        if version_bytes.is_empty() {
            return Err(Error::MalformedDocID);
        }

        // Read version as varint (but for v0 it's just a single byte)
        let version = read_uvarint(&version_bytes).ok_or(Error::MalformedDocID)?;

        // Validate version
        if version != DOC_ID_V0 as u64 {
            return Err(Error::InvalidDocIDVersion(version as u16));
        }

        // Parse UUID
        let uuid = Uuid::parse_str(uuid_str)?;

        Ok(Self {
            version: version as u16,
            uuid,
            cid: None, // CID is not recoverable from string representation
        })
    }

    /// Get the UUID component of this DocID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Get the version of this DocID.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Get the CID if available.
    /// The CID is only available if the DocID was created from a CID,
    /// not if it was parsed from a string.
    pub fn cid(&self) -> Option<&Cid> {
        self.cid.as_ref()
    }

    /// Convert to byte representation.
    /// Format: version (varint) + uuid bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(18); // 2 bytes version + 16 bytes uuid
        write_uvarint(&mut buf, self.version as u64);
        buf.extend_from_slice(self.uuid.as_bytes());
        buf
    }

    /// Parse from byte representation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 17 {
            // At least 1 byte version + 16 bytes uuid
            return Err(Error::MalformedDocID);
        }

        let version = read_uvarint(bytes).ok_or(Error::MalformedDocID)?;
        if version != DOC_ID_V0 as u64 {
            return Err(Error::InvalidDocIDVersion(version as u16));
        }

        // UUID starts after the varint (for v0, varint is 1 byte)
        let uuid_start = varint_size(version);
        if bytes.len() < uuid_start + 16 {
            return Err(Error::MalformedDocID);
        }

        let uuid_bytes: [u8; 16] = bytes[uuid_start..uuid_start + 16]
            .try_into()
            .map_err(|_| Error::MalformedDocID)?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        Ok(Self {
            version: version as u16,
            uuid,
            cid: None,
        })
    }
}

impl std::fmt::Display for DocID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use dynamic buffer sizing based on actual varint requirements
        let size = varint_size(self.version as u64);
        let mut version_buf = vec![0u8; size];
        write_uvarint_to_slice(&mut version_buf, self.version as u64);
        let version_str = multibase::encode(multibase::Base::Base32Lower, &version_buf);

        write!(f, "{}-{}", version_str, self.uuid)
    }
}

impl std::str::FromStr for DocID {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_string(s)
    }
}

// === Varint helpers (matching Go's binary.Uvarint) ===

fn read_uvarint(buf: &[u8]) -> Option<u64> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i == 10 {
            // Overflow
            return None;
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                // Overflow
                return None;
            }
            return Some(x | (b as u64) << s);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    None // Buffer too small
}

fn write_uvarint(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}

fn write_uvarint_to_slice(buf: &mut [u8], mut x: u64) -> usize {
    debug_assert!(
        buf.len() >= varint_size(x),
        "buffer too small for varint: need {} bytes, got {}",
        varint_size(x),
        buf.len()
    );
    let mut i = 0;
    while x >= 0x80 && i < buf.len() {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }
    if i < buf.len() {
        buf[i] = x as u8;
        i += 1;
    }
    i
}

fn varint_size(x: u64) -> usize {
    if x == 0 {
        return 1;
    }
    let mut size = 0;
    let mut v = x;
    while v > 0 {
        size += 1;
        v >>= 7;
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash::Multihash;
    use sha2::{Digest, Sha256};

    const SHA2_256_CODE: u64 = 0x12;

    fn test_cid() -> Cid {
        let mut hasher = Sha256::new();
        hasher.update(b"test document content");
        let hash_bytes = hasher.finalize();
        let mh: Multihash<64> = Multihash::wrap(SHA2_256_CODE, &hash_bytes).unwrap();
        Cid::new_v1(0x55, mh) // 0x55 = raw codec
    }

    #[test]
    fn test_sdn_namespace_matches_go() {
        // Verify the namespace UUID matches Go's SDNNamespaceV0
        assert_eq!(
            SDN_NAMESPACE_V0.to_string(),
            "c94acbfa-dd53-40d0-97f3-29ce16c333fc"
        );
    }

    #[test]
    fn test_new_v0() {
        let cid = test_cid();
        let doc_id = DocID::new_v0(cid.clone());

        assert_eq!(doc_id.version(), DOC_ID_V0);
        assert_eq!(doc_id.cid(), Some(&cid));
        assert!(!doc_id.uuid().is_nil());
    }

    #[test]
    fn test_deterministic_uuid_from_cid() {
        let cid = test_cid();
        let doc_id1 = DocID::new_v0(cid.clone());
        let doc_id2 = DocID::new_v0(cid);

        // Same CID should produce same UUID
        assert_eq!(doc_id1.uuid(), doc_id2.uuid());
    }

    #[test]
    fn test_different_cids_produce_different_uuids() {
        let mut hasher1 = Sha256::new();
        hasher1.update(b"document 1");
        let hash1 = hasher1.finalize();
        let mh1: Multihash<64> = Multihash::wrap(SHA2_256_CODE, &hash1).unwrap();

        let mut hasher2 = Sha256::new();
        hasher2.update(b"document 2");
        let hash2 = hasher2.finalize();
        let mh2: Multihash<64> = Multihash::wrap(SHA2_256_CODE, &hash2).unwrap();

        let cid1 = Cid::new_v1(0x55, mh1);
        let cid2 = Cid::new_v1(0x55, mh2);

        let doc_id1 = DocID::new_v0(cid1);
        let doc_id2 = DocID::new_v0(cid2);

        assert_ne!(doc_id1.uuid(), doc_id2.uuid());
    }

    #[test]
    fn test_string_roundtrip() {
        let cid = test_cid();
        let doc_id = DocID::new_v0(cid);
        let s = doc_id.to_string();

        let parsed = DocID::from_string(&s).unwrap();
        assert_eq!(doc_id.version(), parsed.version());
        assert_eq!(doc_id.uuid(), parsed.uuid());
        // Note: CID is not preserved in string roundtrip
        assert!(parsed.cid().is_none());
    }

    #[test]
    fn test_bytes_roundtrip() {
        let cid = test_cid();
        let doc_id = DocID::new_v0(cid);
        let bytes = doc_id.to_bytes();

        let parsed = DocID::from_bytes(&bytes).unwrap();
        assert_eq!(doc_id.version(), parsed.version());
        assert_eq!(doc_id.uuid(), parsed.uuid());
    }

    #[test]
    fn test_from_str_impl() {
        let cid = test_cid();
        let doc_id = DocID::new_v0(cid);
        let s = doc_id.to_string();

        let parsed: DocID = s.parse().unwrap();
        assert_eq!(doc_id.uuid(), parsed.uuid());
    }

    #[test]
    fn test_invalid_string_no_separator() {
        let result = DocID::from_string("nodash");
        assert!(matches!(result, Err(Error::MalformedDocID)));
    }

    #[test]
    fn test_invalid_string_bad_uuid() {
        let result = DocID::from_string("bae-not-a-uuid");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::UuidParse(_)));
    }

    #[test]
    fn test_varint_helpers() {
        assert_eq!(read_uvarint(&[0x01]), Some(1));
        assert_eq!(read_uvarint(&[0x80, 0x01]), Some(128));
        assert_eq!(varint_size(1), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
    }

    // === Error path tests ===

    #[test]
    fn test_from_bytes_too_short_empty() {
        let result = DocID::from_bytes(&[]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MalformedDocID));
    }

    #[test]
    fn test_from_bytes_too_short_partial() {
        // Only 10 bytes (need at least 17: 1 version + 16 uuid)
        let result =
            DocID::from_bytes(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MalformedDocID));
    }

    #[test]
    fn test_from_bytes_invalid_version() {
        // Version 0x02 is invalid (only 0x01 is valid)
        let mut bytes = vec![0x02];
        bytes.extend_from_slice(&[0x00; 16]); // 16 bytes for UUID
        let result = DocID::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDocIDVersion(2)));
    }

    #[test]
    fn test_from_bytes_version_zero_invalid() {
        // Version 0x00 is also invalid
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&[0x00; 16]);
        let result = DocID::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDocIDVersion(0)));
    }

    #[test]
    fn test_from_string_empty_version() {
        // Empty version part "-uuid-here"
        let result = DocID::from_string("-c94acbfa-dd53-40d0-97f3-29ce16c333fc");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_string_invalid_base32_version() {
        // Invalid base32 characters in version
        let result = DocID::from_string("!!!-c94acbfa-dd53-40d0-97f3-29ce16c333fc");
        assert!(result.is_err());
    }

    #[test]
    fn test_varint_overflow_10_bytes() {
        // 10 continuation bytes would overflow u64
        let bytes = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
        assert_eq!(read_uvarint(&bytes), None);
    }

    #[test]
    fn test_varint_overflow_10_bytes_high_final_byte() {
        // 10 bytes with final byte > 1 would overflow (exceeds u64::MAX)
        // The 10th byte (index 9) must be <= 1 for a valid u64
        let bytes = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
        assert_eq!(read_uvarint(&bytes), None);
    }

    #[test]
    fn test_varint_size_zero() {
        assert_eq!(varint_size(0), 1);
    }

    #[test]
    fn test_varint_size_max_u64() {
        // u64::MAX needs 10 bytes in varint encoding
        assert_eq!(varint_size(u64::MAX), 10);
    }
}
