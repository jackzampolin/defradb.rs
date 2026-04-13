//! BLS12-381 public key implementation (verification only)
//!
//! BLS12-381 threshold keys are managed by Orbis rings. This module provides
//! public key support for verifying BLS signatures on blocks. Private key
//! operations are handled remotely by the Orbis ring's threshold signing service.
//!
//! Uses the min_pk variant: G1 for public keys (48 bytes compressed),
//! G2 for signatures (96 bytes compressed).

use defra_core::Result;

use crate::error::crypto_error;
use crate::keys::{Key, PublicKey};
use crate::types::KeyType;

const DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

/// BLS12-381 public key wrapper (G1, 48 bytes compressed)
#[derive(Clone, Debug)]
pub struct BlsPublicKey {
    key: blst::min_pk::PublicKey,
    raw_bytes: Vec<u8>,
}

impl PartialEq for BlsPublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.raw_bytes == other.raw_bytes
    }
}

impl Eq for BlsPublicKey {}

impl BlsPublicKey {
    /// Create from compressed G1 public key bytes (48 bytes)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(crypto_error("BLS12-381 public key cannot be empty"));
        }
        let key = blst::min_pk::PublicKey::from_bytes(bytes)
            .map_err(|e| crypto_error(format!("invalid BLS12-381 public key: {:?}", e)))?;
        Ok(Self {
            key,
            raw_bytes: bytes.to_vec(),
        })
    }
}

impl Key for BlsPublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Bls12381 {
            return false;
        }
        self.raw_bytes == other.raw()
    }

    fn raw(&self) -> Vec<u8> {
        self.raw_bytes.clone()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Bls12381
    }
}

impl PublicKey for BlsPublicKey {
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<bool> {
        let sig = blst::min_pk::Signature::from_bytes(signature)
            .map_err(|e| crypto_error(format!("invalid BLS12-381 signature: {:?}", e)))?;
        let err = sig.verify(true, data, DST, &[], &self.key, true);
        if err == blst::BLST_ERROR::BLST_SUCCESS {
            Ok(true)
        } else {
            Err(crypto_error(format!(
                "BLS12-381 signature verification failed: {:?}",
                err
            )))
        }
    }

    fn did(&self) -> Result<String> {
        crate::did::create_did_key(KeyType::Bls12381, &self.raw_bytes)
    }
}
