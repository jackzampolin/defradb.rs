//! Encryption and signature types for blocks.
//!
//! Matches Go's `internal/core/block/encryption.go` and `signature.go`.

use std::fmt;

use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::Result;

use super::block::generate_cid_from_bytes;

/// Encryption metadata block
///
/// Matches Go's `internal/core/block/encryption.go:Encryption`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Encryption {
    /// Document ID bytes
    #[serde(rename = "docID", with = "serde_bytes")]
    pub doc_id: Vec<u8>,

    /// Field name (None for document-level encryption)
    #[serde(rename = "fieldName", default, skip_serializing_if = "Option::is_none")]
    pub field_name: Option<String>,

    /// Encryption key
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
}

impl Encryption {
    /// Create a new encryption block
    pub fn new(doc_id: Vec<u8>, key: Vec<u8>) -> Self {
        Self {
            doc_id,
            field_name: None,
            key,
        }
    }

    /// Create a field-level encryption block
    pub fn new_for_field(doc_id: Vec<u8>, field_name: String, key: Vec<u8>) -> Self {
        Self {
            doc_id,
            field_name: Some(field_name),
            key,
        }
    }

    /// Serialize to DAG-CBOR bytes
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        Ok(serde_ipld_dagcbor::to_vec(self)?)
    }

    /// Deserialize from DAG-CBOR bytes
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        Ok(serde_ipld_dagcbor::from_slice(bytes)?)
    }

    /// Generate CID for this encryption block
    pub fn generate_cid(&self) -> Result<Cid> {
        let bytes = self.to_dag_cbor()?;
        generate_cid_from_bytes(&bytes)
    }
}

/// Signature block
///
/// Matches Go's `internal/core/block/signature.go:Signature`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signature {
    /// Signature header with algorithm and identity
    pub header: SignatureHeader,

    /// Signature value bytes
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

impl Signature {
    /// Create a new signature
    pub fn new(header: SignatureHeader, value: Vec<u8>) -> Self {
        Self { header, value }
    }

    /// Serialize to DAG-CBOR bytes
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        Ok(serde_ipld_dagcbor::to_vec(self)?)
    }

    /// Deserialize from DAG-CBOR bytes
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        Ok(serde_ipld_dagcbor::from_slice(bytes)?)
    }

    /// Generate CID for this signature block
    pub fn generate_cid(&self) -> Result<Cid> {
        let bytes = self.to_dag_cbor()?;
        generate_cid_from_bytes(&bytes)
    }
}

/// Signature header with algorithm type and identity
///
/// Matches Go's `internal/core/block/signature.go:SignatureHeader`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignatureHeader {
    /// Algorithm type: "ES256K" (secp256k1) or "EdDSA" (Ed25519)
    #[serde(rename = "type")]
    pub sig_type: SignatureType,

    /// Signer identity (public key bytes)
    #[serde(with = "serde_bytes")]
    pub identity: Vec<u8>,
}

impl SignatureHeader {
    /// Create a new signature header
    pub fn new(sig_type: SignatureType, identity: Vec<u8>) -> Self {
        Self { sig_type, identity }
    }
}

/// Document status for composite deltas.
///
/// Replaces magic `u8` values (1 = active, 2 = deleted) with a type-safe enum.
/// Serializes as the raw u8 value for Go wire compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum DocumentStatus {
    #[default]
    Active = 1,
    Deleted = 2,
}

impl DocumentStatus {
    /// Convert from a raw u8 value.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(DocumentStatus::Active),
            2 => Some(DocumentStatus::Deleted),
            _ => None,
        }
    }

    /// Convert to the raw u8 value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl Serialize for DocumentStatus {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for DocumentStatus {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let v = u8::deserialize(deserializer)?;
        DocumentStatus::from_u8(v)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid document status: {v}")))
    }
}

impl fmt::Display for DocumentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocumentStatus::Active => write!(f, "Active"),
            DocumentStatus::Deleted => write!(f, "Deleted"),
        }
    }
}

/// Signature algorithm types
///
/// Matches Go's signature type constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignatureType {
    /// ECDSA with secp256k1 curve
    #[serde(rename = "ES256K")]
    ES256K,

    /// ECDSA with secp256r1 (P-256) curve
    ES256,

    /// EdDSA with Ed25519 curve
    EdDSA,

    /// Threshold BLS12-381 (Orbis ring signing)
    BLS,
}
