//! Signed Merkle proofs with cryptographic attestation.

use defra_core::block::{Signature, SignatureHeader, SignatureType};
use defra_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::keys::{PrivateKey, PublicKey};
use crate::types::KeyType;

use super::proof::MerkleProof;

/// A signed Merkle proof with cryptographic attestation
///
/// The signature covers the entire proof content (leaf_cid, root_cid, and path).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedMerkleProof {
    /// The underlying Merkle proof
    pub proof: MerkleProof,

    /// Signature over the proof
    pub signature: Signature,
}

impl SignedMerkleProof {
    /// Create a signed proof from a proof and private key
    ///
    /// The identity field is stored as hex-encoded public key bytes for
    /// wire compatibility with Go DefraDB.
    pub fn sign(proof: MerkleProof, private_key: &dyn PrivateKey) -> Result<Self> {
        let proof_bytes = proof.to_dag_cbor()?;
        let sig_value = private_key.sign(&proof_bytes)?;

        let sig_type = match private_key.key_type() {
            KeyType::Ed25519 => SignatureType::EdDSA,
            KeyType::Secp256k1 => SignatureType::ES256K,
            KeyType::Secp256r1 => SignatureType::ES256,
            KeyType::Bls12381 => SignatureType::BLS,
        };

        let public_key = private_key.public_key();
        // Go stores identity as hex-encoded public key string bytes
        let identity = public_key.to_hex_string().into_bytes();
        let header = SignatureHeader::new(sig_type, identity);

        Ok(Self {
            proof,
            signature: Signature::new(header, sig_value),
        })
    }

    /// Verify the signature and proof validity
    ///
    /// Returns true only if both the signature is valid AND the proof is valid.
    /// The public key type must match the signature algorithm.
    pub fn verify(&self, public_key: &dyn PublicKey) -> Result<bool> {
        // Validate key type matches signature algorithm
        #[allow(unreachable_patterns)]
        let expected_key_type = match self.signature.header.sig_type {
            SignatureType::EdDSA => KeyType::Ed25519,
            SignatureType::ES256K => KeyType::Secp256k1,
            SignatureType::ES256 => KeyType::Secp256r1,
            SignatureType::BLS => KeyType::Bls12381,
            _ => {
                return Err(Error::Crypto(format!(
                    "unsupported signature type: {:?}",
                    self.signature.header.sig_type
                )));
            }
        };
        if public_key.key_type() != expected_key_type {
            return Err(Error::Crypto(format!(
                "Key type mismatch: signature requires {:?}, got {:?}",
                expected_key_type,
                public_key.key_type()
            )));
        }

        // Verify the identity in the signature matches the provided key
        // Identity is stored as hex-encoded public key string bytes
        let expected_identity = public_key.to_hex_string().into_bytes();
        if self.signature.header.identity != expected_identity {
            return Ok(false);
        }

        // Verify the signature
        let proof_bytes = self.proof.to_dag_cbor()?;
        if !public_key.verify(&proof_bytes, &self.signature.value)? {
            return Ok(false);
        }

        // Verify the proof itself
        self.proof.verify()
    }

    /// Verify using the embedded public key from the signature
    ///
    /// Extracts the public key from the signature header and verifies.
    pub fn verify_with_embedded_key(&self) -> Result<bool> {
        let public_key = extract_public_key_from_signature(&self.signature)?;

        // Verify the signature
        let proof_bytes = self.proof.to_dag_cbor()?;
        if !public_key.verify(&proof_bytes, &self.signature.value)? {
            return Ok(false);
        }

        // Verify the proof itself
        self.proof.verify()
    }

    /// Serialize to DAG-CBOR
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Deserialize from DAG-CBOR
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }
}

/// Extract public key from a signature header
///
/// The identity field is stored as hex-encoded public key string bytes
/// for wire compatibility with Go DefraDB.
fn extract_public_key_from_signature(sig: &Signature) -> Result<Box<dyn PublicKey>> {
    #[allow(unreachable_patterns)]
    let key_type = match sig.header.sig_type {
        SignatureType::EdDSA => KeyType::Ed25519,
        SignatureType::ES256K => KeyType::Secp256k1,
        SignatureType::ES256 => KeyType::Secp256r1,
        SignatureType::BLS => KeyType::Bls12381,
        _ => {
            return Err(Error::Crypto(format!(
                "unsupported signature type: {:?}",
                sig.header.sig_type
            )));
        }
    };

    // Identity is stored as hex-encoded string bytes
    let hex_string = String::from_utf8(sig.header.identity.clone()).map_err(|e| {
        Error::Crypto(format!(
            "Invalid identity encoding in signature header: {}",
            e
        ))
    })?;

    crate::keys::generation::public_key_from_string(key_type, &hex_string)
}

/// Verify a signed proof without blockstore access
pub fn verify_signed_proof(proof: &SignedMerkleProof) -> Result<bool> {
    proof.verify_with_embedded_key()
}
