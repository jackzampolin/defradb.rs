//! Batch signing and verification over a set of CIDs.
//!
//! Computes a Merkle root from collected CIDs, signs the root with the
//! node's private key, and provides verification of batch signatures.
//!
//! This module is intentionally kept as a live compatibility surface for the
//! Rust HTTP and FFI batch-signing endpoints used by the indexer flow.

use cid::Cid;
use serde::{Deserialize, Serialize};

use defra_core::batch_signing::compute_merkle_root;
use defra_core::signing::SigningConfig;

use crate::keys::{PrivateKey, PublicKey};
use crate::{Ed25519PrivateKey, Ed25519PublicKey, Secp256k1PrivateKey, Secp256k1PublicKey};

/// Errors that can occur during batch signing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BatchSignError {
    #[error("signing key not configured")]
    MissingKey,
    #[error("unsupported key type: {0}")]
    UnsupportedKeyType(String),
    #[error("signing failed: {0}")]
    SigningFailed(String),
}

/// A batch signature covering a set of CIDs via their Merkle root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSignature {
    /// Signature algorithm: "EdDSA" or "ES256K"
    pub sig_type: String,
    /// Hex-encoded public key of the signer
    pub identity: String,
    /// Raw signature bytes over the Merkle root
    #[serde(with = "hex_bytes")]
    pub value: Vec<u8>,
    /// The 32-byte Merkle root that was signed
    #[serde(with = "hex_bytes")]
    pub merkle_root: Vec<u8>,
    /// Number of CIDs in the batch
    pub cid_count: usize,
}

/// Sign a batch of CIDs using the provided signing config.
pub fn sign_batch(cids: &[Cid], config: &SigningConfig) -> Result<BatchSignature, BatchSignError> {
    let root = compute_merkle_root(cids).ok_or(BatchSignError::MissingKey)?;

    let (sig_type, sig_bytes) = match config.key_type.as_str() {
        "ed25519" => {
            let key = Ed25519PrivateKey::from_bytes(&config.private_key_bytes).map_err(|e| {
                BatchSignError::SigningFailed(format!("invalid ed25519 key: {}", e))
            })?;
            let sig = key.sign(&root).map_err(|e| {
                BatchSignError::SigningFailed(format!("ed25519 sign failed: {}", e))
            })?;
            ("EdDSA".to_string(), sig)
        }
        "secp256k1" => {
            let key = Secp256k1PrivateKey::from_bytes(&config.private_key_bytes).map_err(|e| {
                BatchSignError::SigningFailed(format!("invalid secp256k1 key: {}", e))
            })?;
            let sig = key.sign(&root).map_err(|e| {
                BatchSignError::SigningFailed(format!("secp256k1 sign failed: {}", e))
            })?;
            ("ES256K".to_string(), sig)
        }
        other => return Err(BatchSignError::UnsupportedKeyType(other.to_string())),
    };

    Ok(BatchSignature {
        sig_type,
        identity: config.public_key_hex.clone(),
        value: sig_bytes,
        merkle_root: root.to_vec(),
        cid_count: cids.len(),
    })
}

/// Verify a batch signature against a set of CIDs.
pub fn verify_batch_signature(sig: &BatchSignature, cids: &[Cid]) -> Result<bool, String> {
    let root = compute_merkle_root(cids).ok_or_else(|| "cannot verify empty batch".to_string())?;

    if root.as_slice() != sig.merkle_root.as_slice() {
        return Ok(false);
    }

    let pub_bytes =
        hex::decode(&sig.identity).map_err(|e| format!("invalid identity hex: {}", e))?;

    match sig.sig_type.as_str() {
        "EdDSA" => {
            let pubkey = Ed25519PublicKey::from_bytes(&pub_bytes)
                .map_err(|e| format!("invalid ed25519 public key: {}", e))?;
            pubkey
                .verify(&root, &sig.value)
                .map_err(|e| format!("ed25519 verify failed: {}", e))
        }
        "ES256K" => {
            let pubkey = Secp256k1PublicKey::from_bytes(&pub_bytes)
                .map_err(|e| format!("invalid secp256k1 public key: {}", e))?;
            pubkey
                .verify(&root, &sig.value)
                .map_err(|e| format!("secp256k1 verify failed: {}", e))
        }
        other => Err(format!("unsupported sig type: {}", other)),
    }
}

/// Serde helper for hex-encoding Vec<u8> fields.
mod hex_bytes {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        hex::decode(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::Key;
    use cid::multihash::{Code, MultihashDigest};

    fn make_cid(data: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x71, hash)
    }

    fn test_ed25519_config() -> SigningConfig {
        let private_key = crate::keys::generation::generate_ed25519().unwrap();
        let public_key = private_key.public_key();
        SigningConfig {
            key_type: "ed25519".to_string(),
            private_key_bytes: private_key.raw_owned(),
            public_key_bytes: public_key.raw_owned(),
            public_key_hex: public_key.to_hex_string(),
            remote_signer: None,
            signing_authorization: None,
        }
    }

    fn test_secp256k1_config() -> SigningConfig {
        let private_key = crate::keys::generation::generate_secp256k1().unwrap();
        let public_key = private_key.public_key();
        SigningConfig {
            key_type: "secp256k1".to_string(),
            private_key_bytes: private_key.raw_owned(),
            public_key_bytes: public_key.raw_owned(),
            public_key_hex: public_key.to_hex_string(),
            remote_signer: None,
            signing_authorization: None,
        }
    }

    #[test]
    fn sign_verify_ed25519_roundtrip() {
        let config = test_ed25519_config();
        let cids = vec![make_cid(b"a"), make_cid(b"b"), make_cid(b"c")];
        let sig = sign_batch(&cids, &config).unwrap();
        assert_eq!(sig.sig_type, "EdDSA");
        assert_eq!(sig.cid_count, 3);
        assert!(verify_batch_signature(&sig, &cids).unwrap());
    }

    #[test]
    fn sign_verify_secp256k1_roundtrip() {
        let config = test_secp256k1_config();
        let cids = vec![make_cid(b"x"), make_cid(b"y")];
        let sig = sign_batch(&cids, &config).unwrap();
        assert_eq!(sig.sig_type, "ES256K");
        assert!(verify_batch_signature(&sig, &cids).unwrap());
    }

    #[test]
    fn verify_fails_wrong_cids() {
        let config = test_ed25519_config();
        let cids = vec![make_cid(b"a"), make_cid(b"b")];
        let sig = sign_batch(&cids, &config).unwrap();
        let wrong_cids = vec![make_cid(b"a"), make_cid(b"c")];
        assert!(!verify_batch_signature(&sig, &wrong_cids).unwrap());
    }

    #[test]
    fn sign_empty_fails() {
        let config = test_ed25519_config();
        assert!(sign_batch(&[], &config).is_err());
    }

    #[test]
    fn batch_signature_json_roundtrip() {
        let config = test_ed25519_config();
        let cids = vec![make_cid(b"json")];
        let sig = sign_batch(&cids, &config).unwrap();
        let json = serde_json::to_string(&sig).unwrap();
        let deserialized: BatchSignature = serde_json::from_str(&json).unwrap();
        assert_eq!(sig.sig_type, deserialized.sig_type);
        assert_eq!(sig.identity, deserialized.identity);
        assert_eq!(sig.value, deserialized.value);
        assert_eq!(sig.merkle_root, deserialized.merkle_root);
        assert_eq!(sig.cid_count, deserialized.cid_count);
    }
}
