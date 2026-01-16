//! RawIdentity implementation

use crypto::keys::{Key, PrivateKey, PublicKey};
use crypto::{
    Ed25519PrivateKey, Ed25519PublicKey, KeyType, Secp256k1PrivateKey, Secp256k1PublicKey,
};

use crate::did::Did;
use crate::error::Error;
use crate::key_type::IdentityKeyType;
use crate::{FullIdentity, Identity, Result};

/// A concrete identity implementation backed by raw key material.
///
/// RawIdentity stores the private key and caches the derived public key.
/// It supports both Ed25519 and secp256k1 key types.
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
}

impl RawIdentity {
    /// Creates a new RawIdentity from an Ed25519 private key.
    ///
    /// # Errors
    ///
    /// Returns an error if the public key cannot be derived from the private key.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the public key cannot be derived from the private key.
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

    /// Creates a new RawIdentity from any PrivateKey implementation.
    ///
    /// This is the primary constructor that automatically handles different key types.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key type is not supported (secp256r1)
    /// - The key bytes are invalid
    /// - The public key cannot be derived
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
            KeyType::Secp256r1 => Err(Error::UnsupportedKeyType(KeyType::Secp256r1)),
        }
    }

    /// Creates a RawIdentity from raw private key bytes.
    ///
    /// # Parameters
    /// * `key_type` - The type of key (Ed25519 or Secp256k1)
    /// * `bytes` - The raw private key bytes
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key type is not supported (secp256r1)
    /// - The key bytes are invalid for the specified type
    /// - The public key cannot be derived
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
            KeyType::Secp256r1 => Err(Error::UnsupportedKeyType(KeyType::Secp256r1)),
        }
    }

    /// Creates a RawIdentity from raw private key bytes using the type-safe key type.
    ///
    /// This is the preferred constructor when you have an `IdentityKeyType`,
    /// as it provides compile-time safety that the key type is supported.
    ///
    /// # Parameters
    /// * `key_type` - The identity key type (Ed25519 or Secp256k1)
    /// * `bytes` - The raw private key bytes
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key bytes are invalid for the specified type
    /// - The public key cannot be derived
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
        }
    }

    /// Returns the key type of this identity as a `crypto::KeyType`.
    pub fn key_type(&self) -> KeyType {
        match &self.inner {
            IdentityInner::Ed25519 { .. } => KeyType::Ed25519,
            IdentityInner::Secp256k1 { .. } => KeyType::Secp256k1,
        }
    }

    /// Returns the key type of this identity as an `IdentityKeyType`.
    ///
    /// This is the preferred method when working with identity-specific code,
    /// as it provides compile-time guarantees that the key type is supported.
    pub fn identity_key_type(&self) -> IdentityKeyType {
        match &self.inner {
            IdentityInner::Ed25519 { .. } => IdentityKeyType::Ed25519,
            IdentityInner::Secp256k1 { .. } => IdentityKeyType::Secp256k1,
        }
    }

    /// Returns the raw private key bytes.
    ///
    /// Use with caution - private key material should be protected.
    pub fn private_key_bytes(&self) -> Vec<u8> {
        match &self.inner {
            IdentityInner::Ed25519 { private_key, .. } => private_key.raw(),
            IdentityInner::Secp256k1 { private_key, .. } => private_key.raw(),
        }
    }

    /// Returns the raw public key bytes.
    pub fn public_key_bytes(&self) -> Vec<u8> {
        match &self.inner {
            IdentityInner::Ed25519 { public_key, .. } => public_key.raw(),
            IdentityInner::Secp256k1 { public_key, .. } => public_key.raw(),
        }
    }
}

impl Identity for RawIdentity {
    fn pub_key(&self) -> &dyn PublicKey {
        match &self.inner {
            IdentityInner::Ed25519 { public_key, .. } => public_key,
            IdentityInner::Secp256k1 { public_key, .. } => public_key,
        }
    }

    fn did(&self) -> Result<Did> {
        let did_string = self
            .pub_key()
            .did()
            .map_err(|e| Error::InvalidDid(format!("failed to derive DID: {}", e)))?;
        // Use unchecked since pub_key().did() always returns valid did:key format
        Ok(Did::new_unchecked(did_string))
    }
}

impl FullIdentity for RawIdentity {
    fn priv_key(&self) -> &dyn PrivateKey {
        match &self.inner {
            IdentityInner::Ed25519 { private_key, .. } => private_key,
            IdentityInner::Secp256k1 { private_key, .. } => private_key,
        }
    }
    // sign() uses default implementation from FullIdentity trait
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

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::{generate_ed25519, generate_secp256k1};

    #[test]
    fn test_from_ed25519() {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_ed25519(key).unwrap();

        assert_eq!(identity.key_type(), KeyType::Ed25519);
        assert!(identity.did().unwrap().as_str().starts_with("did:key:"));
    }

    #[test]
    fn test_from_secp256k1() {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_secp256k1(key).unwrap();

        assert_eq!(identity.key_type(), KeyType::Secp256k1);
        assert!(identity.did().unwrap().as_str().starts_with("did:key:"));
    }

    #[test]
    fn test_from_bytes_ed25519() {
        let key = generate_ed25519().unwrap();
        let bytes = key.raw();

        let identity = RawIdentity::from_bytes(KeyType::Ed25519, &bytes).unwrap();
        assert_eq!(identity.key_type(), KeyType::Ed25519);
    }

    #[test]
    fn test_from_bytes_secp256k1() {
        let key = generate_secp256k1().unwrap();
        let bytes = key.raw();

        let identity = RawIdentity::from_bytes(KeyType::Secp256k1, &bytes).unwrap();
        assert_eq!(identity.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_from_bytes_invalid() {
        let result = RawIdentity::from_bytes(KeyType::Ed25519, &[0u8; 32]);
        assert!(result.is_err(), "Should fail with invalid Ed25519 key");
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidKeyBytes(KeyType::Ed25519, _)
        ));

        let result = RawIdentity::from_bytes(KeyType::Secp256k1, &[0u8; 16]);
        assert!(result.is_err(), "Should fail with invalid secp256k1 key");
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidKeyBytes(KeyType::Secp256k1, _)
        ));
    }

    #[test]
    fn test_from_bytes_secp256r1_unsupported() {
        let result = RawIdentity::from_bytes(KeyType::Secp256r1, &[0u8; 32]);
        assert!(result.is_err(), "secp256r1 should not be supported");
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedKeyType(KeyType::Secp256r1)
        ));
    }

    #[test]
    fn test_public_key_bytes_consistency() {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let bytes1 = identity.public_key_bytes();
        let bytes2 = identity.pub_key().raw();

        assert_eq!(
            bytes1, bytes2,
            "public_key_bytes should match pub_key().raw()"
        );
    }

    #[test]
    fn test_private_key_bytes_roundtrip() {
        let key = generate_ed25519().unwrap();
        let identity1 = RawIdentity::from_private_key(key).unwrap();

        let bytes = identity1.private_key_bytes();
        let identity2 = RawIdentity::from_bytes(KeyType::Ed25519, &bytes).unwrap();

        assert_eq!(
            identity1.did().unwrap(),
            identity2.did().unwrap(),
            "Roundtrip should preserve identity"
        );
    }

    #[test]
    fn test_debug_impl() {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let debug_str = format!("{:?}", identity);
        assert!(debug_str.contains("RawIdentity"));
        assert!(debug_str.contains("Ed25519"));
        assert!(debug_str.contains("did:key:"));
    }

    #[test]
    fn test_sign_with_ed25519() {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_ed25519(key).unwrap();

        let message = b"test message";
        let signature = identity.sign(message).unwrap();

        assert_eq!(signature.len(), 64, "Ed25519 signature should be 64 bytes");

        let verified = identity.pub_key().verify(message, &signature).unwrap();
        assert!(verified);
    }

    #[test]
    fn test_sign_with_secp256k1() {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_secp256k1(key).unwrap();

        let message = b"test message";
        let signature = identity.sign(message).unwrap();

        // DER signatures vary in length
        assert!(signature.len() >= 70 && signature.len() <= 73);

        let verified = identity.pub_key().verify(message, &signature).unwrap();
        assert!(verified);
    }

    #[test]
    fn test_priv_key_trait_method() {
        let key = generate_ed25519().unwrap();
        let expected_bytes = key.raw();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let priv_key = identity.priv_key();
        assert_eq!(priv_key.key_type(), KeyType::Ed25519);
        assert_eq!(priv_key.raw(), expected_bytes);
    }

    #[test]
    fn test_identity_key_type_ed25519() {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        assert_eq!(identity.identity_key_type(), IdentityKeyType::Ed25519);
    }

    #[test]
    fn test_identity_key_type_secp256k1() {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        assert_eq!(identity.identity_key_type(), IdentityKeyType::Secp256k1);
    }

    #[test]
    fn test_from_identity_key_type_ed25519() {
        let key = generate_ed25519().unwrap();
        let bytes = key.raw();

        let identity =
            RawIdentity::from_identity_key_type(IdentityKeyType::Ed25519, &bytes).unwrap();
        assert_eq!(identity.identity_key_type(), IdentityKeyType::Ed25519);
        assert_eq!(identity.key_type(), KeyType::Ed25519);
    }

    #[test]
    fn test_from_identity_key_type_secp256k1() {
        let key = generate_secp256k1().unwrap();
        let bytes = key.raw();

        let identity =
            RawIdentity::from_identity_key_type(IdentityKeyType::Secp256k1, &bytes).unwrap();
        assert_eq!(identity.identity_key_type(), IdentityKeyType::Secp256k1);
        assert_eq!(identity.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_key_type_and_identity_key_type_consistent() {
        let ed25519_identity = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();
        assert_eq!(
            ed25519_identity.key_type(),
            ed25519_identity.identity_key_type().to_crypto_key_type()
        );

        let secp256k1_identity =
            RawIdentity::from_private_key(generate_secp256k1().unwrap()).unwrap();
        assert_eq!(
            secp256k1_identity.key_type(),
            secp256k1_identity.identity_key_type().to_crypto_key_type()
        );
    }
}
