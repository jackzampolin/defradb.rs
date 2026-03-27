//! RawIdentity implementation

use crypto::keys::{Key, PrivateKey, PublicKey};
use crypto::{
    Ed25519PrivateKey, Ed25519PublicKey, KeyType, Secp256k1PrivateKey, Secp256k1PublicKey,
    Secp256r1PrivateKey, Secp256r1PublicKey,
};

use crate::did::Did;
use crate::error::Error;
use crate::key_type::IdentityKeyType;
use crate::{FullIdentity, Identity, Result};

/// A concrete identity implementation backed by raw key material.
///
/// RawIdentity stores the private key and caches the derived public key.
/// It supports Ed25519, secp256k1, and secp256r1 key types.
pub struct RawIdentity {
    inner: IdentityInner,
}

/// Internal enum to hold different key types without dynamic dispatch.
enum IdentityInner {
    Ed25519 {
        private_key: Ed25519PrivateKey,
        public_key: Ed25519PublicKey,
    },
    Secp256k1 {
        private_key: Secp256k1PrivateKey,
        public_key: Secp256k1PublicKey,
    },
    Secp256r1 {
        private_key: Secp256r1PrivateKey,
        public_key: Secp256r1PublicKey,
    },
}

impl RawIdentity {
    /// Creates a new RawIdentity from an Ed25519 private key.
    pub fn from_ed25519(private_key: Ed25519PrivateKey) -> Result<Self> {
        let public_key_box = private_key.public_key();
        let public_key_raw = public_key_box.raw();
        let public_key = Ed25519PublicKey::from_bytes(&public_key_raw).map_err(|e| {
            Error::PublicKeyDerivation(format!("Ed25519 public key derivation failed: {}", e))
        })?;

        Ok(Self {
            inner: IdentityInner::Ed25519 {
                private_key,
                public_key,
            },
        })
    }

    /// Creates a new RawIdentity from a secp256k1 private key.
    pub fn from_secp256k1(private_key: Secp256k1PrivateKey) -> Result<Self> {
        let public_key_box = private_key.public_key();
        let public_key_raw = public_key_box.raw();
        let public_key = Secp256k1PublicKey::from_bytes(&public_key_raw).map_err(|e| {
            Error::PublicKeyDerivation(format!("secp256k1 public key derivation failed: {}", e))
        })?;

        Ok(Self {
            inner: IdentityInner::Secp256k1 {
                private_key,
                public_key,
            },
        })
    }

    /// Creates a new RawIdentity from a secp256r1 (P-256) private key.
    pub fn from_secp256r1(private_key: Secp256r1PrivateKey) -> Result<Self> {
        let public_key_box = private_key.public_key();
        let public_key_raw = public_key_box.raw();
        let public_key = Secp256r1PublicKey::from_bytes(&public_key_raw).map_err(|e| {
            Error::PublicKeyDerivation(format!("secp256r1 public key derivation failed: {}", e))
        })?;

        Ok(Self {
            inner: IdentityInner::Secp256r1 {
                private_key,
                public_key,
            },
        })
    }

    /// Creates a new RawIdentity from any PrivateKey implementation.
    pub fn from_private_key<P: PrivateKey + 'static>(private_key: P) -> Result<Self> {
        match private_key.key_type() {
            KeyType::Ed25519 => {
                let raw_bytes = private_key.raw();
                let ed25519_key = Ed25519PrivateKey::from_bytes(&raw_bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Ed25519, e.to_string()))?;
                Self::from_ed25519(ed25519_key)
            }
            KeyType::Secp256k1 => {
                let raw_bytes = private_key.raw();
                let secp256k1_key = Secp256k1PrivateKey::from_bytes(&raw_bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Secp256k1, e.to_string()))?;
                Self::from_secp256k1(secp256k1_key)
            }
            KeyType::Secp256r1 => {
                let raw_bytes = private_key.raw();
                let secp256r1_key = Secp256r1PrivateKey::from_bytes(&raw_bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Secp256r1, e.to_string()))?;
                Self::from_secp256r1(secp256r1_key)
            }
            KeyType::Bls12381 => Err(Error::UnsupportedKeyType(private_key.key_type())),
            _ => Err(Error::UnsupportedKeyType(private_key.key_type())),
        }
    }

    /// Creates a RawIdentity from raw private key bytes.
    pub fn from_bytes(key_type: KeyType, bytes: &[u8]) -> Result<Self> {
        match key_type {
            KeyType::Ed25519 => {
                let private_key = Ed25519PrivateKey::from_bytes(bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Ed25519, e.to_string()))?;
                Self::from_ed25519(private_key)
            }
            KeyType::Secp256k1 => {
                let private_key = Secp256k1PrivateKey::from_bytes(bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Secp256k1, e.to_string()))?;
                Self::from_secp256k1(private_key)
            }
            KeyType::Secp256r1 => {
                let private_key = Secp256r1PrivateKey::from_bytes(bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Secp256r1, e.to_string()))?;
                Self::from_secp256r1(private_key)
            }
            KeyType::Bls12381 => Err(Error::UnsupportedKeyType(key_type)),
            _ => Err(Error::UnsupportedKeyType(key_type)),
        }
    }

    /// Creates a RawIdentity from raw private key bytes using the type-safe key type.
    pub fn from_identity_key_type(key_type: IdentityKeyType, bytes: &[u8]) -> Result<Self> {
        match key_type {
            IdentityKeyType::Ed25519 => {
                let private_key = Ed25519PrivateKey::from_bytes(bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Ed25519, e.to_string()))?;
                Self::from_ed25519(private_key)
            }
            IdentityKeyType::Secp256k1 => {
                let private_key = Secp256k1PrivateKey::from_bytes(bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Secp256k1, e.to_string()))?;
                Self::from_secp256k1(private_key)
            }
            IdentityKeyType::Secp256r1 => {
                let private_key = Secp256r1PrivateKey::from_bytes(bytes)
                    .map_err(|e| Error::InvalidKeyBytes(KeyType::Secp256r1, e.to_string()))?;
                Self::from_secp256r1(private_key)
            }
        }
    }

    /// Returns the key type of this identity as a `crypto::KeyType`.
    pub fn key_type(&self) -> KeyType {
        match &self.inner {
            IdentityInner::Ed25519 { .. } => KeyType::Ed25519,
            IdentityInner::Secp256k1 { .. } => KeyType::Secp256k1,
            IdentityInner::Secp256r1 { .. } => KeyType::Secp256r1,
        }
    }

    /// Returns the key type of this identity as an `IdentityKeyType`.
    pub fn identity_key_type(&self) -> IdentityKeyType {
        match &self.inner {
            IdentityInner::Ed25519 { .. } => IdentityKeyType::Ed25519,
            IdentityInner::Secp256k1 { .. } => IdentityKeyType::Secp256k1,
            IdentityInner::Secp256r1 { .. } => IdentityKeyType::Secp256r1,
        }
    }

    /// Returns the raw private key bytes.
    pub fn private_key_bytes(&self) -> Vec<u8> {
        match &self.inner {
            IdentityInner::Ed25519 { private_key, .. } => private_key.raw(),
            IdentityInner::Secp256k1 { private_key, .. } => private_key.raw(),
            IdentityInner::Secp256r1 { private_key, .. } => private_key.raw(),
        }
    }

    /// Returns the raw public key bytes.
    pub fn public_key_bytes(&self) -> Vec<u8> {
        match &self.inner {
            IdentityInner::Ed25519 { public_key, .. } => public_key.raw(),
            IdentityInner::Secp256k1 { public_key, .. } => public_key.raw(),
            IdentityInner::Secp256r1 { public_key, .. } => public_key.raw(),
        }
    }
}

impl Identity for RawIdentity {
    fn pub_key(&self) -> &dyn PublicKey {
        match &self.inner {
            IdentityInner::Ed25519 { public_key, .. } => public_key,
            IdentityInner::Secp256k1 { public_key, .. } => public_key,
            IdentityInner::Secp256r1 { public_key, .. } => public_key,
        }
    }

    fn did(&self) -> Result<Did> {
        let did_string = self
            .pub_key()
            .did()
            .map_err(|e| Error::InvalidDid(format!("failed to derive DID: {}", e)))?;
        Ok(Did::new_unchecked(did_string))
    }
}

impl FullIdentity for RawIdentity {
    fn priv_key(&self) -> &dyn PrivateKey {
        match &self.inner {
            IdentityInner::Ed25519 { private_key, .. } => private_key,
            IdentityInner::Secp256k1 { private_key, .. } => private_key,
            IdentityInner::Secp256r1 { private_key, .. } => private_key,
        }
    }
}

impl std::fmt::Debug for RawIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let did_display = match self.did() {
            Ok(did) => did.to_string(),
            Err(e) => format!("<DID derivation failed: {}>", e),
        };
        f.debug_struct("RawIdentity")
            .field("key_type", &self.key_type())
            .field("did", &did_display)
            .finish_non_exhaustive()
    }
}
