//! Document ID type with content-addressed generation

use cid::Cid;
use defra_core::doc_id::ParsedDocId;
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

    /// Create a deterministic DocID v0 from an arbitrary stable seed string.
    ///
    /// This is useful for system-managed documents that need a stable identity
    /// across updates without deriving the ID from the document content itself.
    pub fn new_v0_from_seed(seed: &str) -> Self {
        let uuid = Uuid::new_v5(&SDN_NAMESPACE_V0, seed.as_bytes());
        Self {
            version: DOC_ID_V0,
            uuid,
            cid: None,
        }
    }

    /// Parse a DocID from its string representation.
    ///
    /// Format: `{base32(version)}-{uuid}`
    pub fn from_string(s: &str) -> Result<Self> {
        let parsed = ParsedDocId::from_string(s)?;

        Ok(Self {
            version: parsed.version(),
            uuid: parsed.uuid(),
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
        ParsedDocId::new(self.version, self.uuid)
            .expect("DocID instances always carry a valid version")
            .to_bytes()
    }

    /// Parse from byte representation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let parsed = ParsedDocId::from_bytes(bytes)?;

        Ok(Self {
            version: parsed.version(),
            uuid: parsed.uuid(),
            cid: None,
        })
    }
}

impl std::fmt::Display for DocID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parsed = ParsedDocId::new(self.version, self.uuid).map_err(|_| std::fmt::Error)?;
        write!(f, "{}", parsed)
    }
}

/// Validate that all document IDs have valid format.
///
/// Returns an error on the first invalid ID, matching Go's atomic
/// validation behavior (all or nothing).
pub fn validate_doc_ids(doc_ids: &[String]) -> Result<()> {
    for doc_id in doc_ids {
        DocID::from_string(doc_id)?;
    }
    Ok(())
}

impl std::str::FromStr for DocID {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_string(s)
    }
}
