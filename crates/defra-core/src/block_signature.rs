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
/// Carries only the key: document identity is derived from the genesis
/// composite block CID (Go #4838).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Encryption {
    /// Encryption key
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
}

impl Encryption {
    /// Create a new encryption block
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
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

impl SignatureType {
    /// Whether a Go peer can verify a block signed with this type.
    ///
    /// Go's `getPublicKeyFromSignature` (`internal/core/block/signature.go:186`)
    /// maps only `EdDSA` and `ES256K` to a key type and returns
    /// `ErrUnsupportedPrivKeyType` for anything else, so a block signed with any
    /// other type is rejected during replication rather than merely unverified.
    ///
    /// The match is exhaustive on purpose: a new signature type cannot be added
    /// without deciding, here, whether Go peers can consume it.
    pub fn is_go_verifiable(self) -> bool {
        match self {
            Self::ES256K | Self::EdDSA => true,
            // Rust-only. `BLS` is the Orbis ring extension; `ES256` covers
            // Secure Enclave and other secp256r1 keys.
            Self::BLS | Self::ES256 => false,
        }
    }
}

/// Node policy for emitting block signatures Go peers cannot verify.
///
/// `SignatureType::is_go_verifiable` says which types a Go peer accepts. Signing
/// with any other type produces blocks that replicate between Rust nodes and are
/// refused by Go ones, which is a deployment decision rather than a per-key one,
/// so it is answered once for the process.
///
/// Denied by default: a node that has not said otherwise must not put blocks on
/// the wire that half the network will reject.
pub mod go_verifiable_policy {
    use std::sync::atomic::{AtomicU8, Ordering};

    /// Set by an operator to allow Rust-only signature types.
    pub const ALLOW_ENV: &str = "DEFRA_ALLOW_NON_GO_VERIFIABLE_SIGNING";

    const UNSET: u8 = 0;
    const DENIED: u8 = 1;
    const ALLOWED: u8 = 2;

    static POLICY: AtomicU8 = AtomicU8::new(UNSET);

    /// Allow or deny signing with a type Go peers cannot verify.
    pub fn allow_non_go_verifiable_signing(allow: bool) {
        POLICY.store(if allow { ALLOWED } else { DENIED }, Ordering::Release);
    }

    /// Whether this node may sign blocks Go peers cannot verify.
    ///
    /// Falls back to [`ALLOW_ENV`] the first time it is asked, so an operator can
    /// open the gate without a code change; an explicit call always wins.
    pub fn non_go_verifiable_signing_allowed() -> bool {
        match POLICY.load(Ordering::Acquire) {
            ALLOWED => true,
            DENIED => false,
            _ => {
                let allowed = std::env::var(ALLOW_ENV)
                    .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
                    .unwrap_or(false);
                POLICY.store(if allowed { ALLOWED } else { DENIED }, Ordering::Release);
                allowed
            }
        }
    }

    /// Forget the cached decision. Test-only: the policy is process-global.
    pub fn reset_for_test() {
        POLICY.store(UNSET, Ordering::Release);
    }
}
