
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
