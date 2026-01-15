// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Transaction signer backed by keyring
//!
//! Provides signing operations using keys stored in a keyring.
//! The key is fetched from the keyring on each sign request rather than
//! being cached, minimizing the risk of key material being leaked via
//! memory paging.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::Keyring;

/// A signer that fetches keys from a keyring on demand.
///
/// This signer does not cache the private key, instead fetching it from
/// the keyring each time it's needed. This minimizes the risk of key
/// material being leaked through memory paging or other side channels.
pub struct KeyringSigner {
    keyring: Arc<dyn Keyring>,
    key_name: String,
    key_type: KeyType,
}

/// Key type for determining how to interpret key bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// secp256k1 ECDSA key (32 bytes)
    Secp256k1,
    /// Ed25519 key (32 bytes)
    Ed25519,
}

impl KeyringSigner {
    /// Create a new keyring signer.
    ///
    /// The key must already exist in the keyring. Call `verify_key` to ensure
    /// the key exists and is of the expected type.
    pub fn new(keyring: Arc<dyn Keyring>, key_name: impl Into<String>, key_type: KeyType) -> Self {
        Self {
            keyring,
            key_name: key_name.into(),
            key_type,
        }
    }

    /// Verify that the key exists in the keyring and can be loaded.
    pub fn verify_key(&self) -> Result<()> {
        let key_bytes = self.keyring.get(&self.key_name)?;
        let expected_len = match self.key_type {
            KeyType::Secp256k1 => 32,
            KeyType::Ed25519 => 32,
        };
        if key_bytes.len() != expected_len {
            return Err(Error::Decryption(format!(
                "invalid key length: expected {} bytes, got {}",
                expected_len,
                key_bytes.len()
            )));
        }
        Ok(())
    }

    /// Get the key name.
    pub fn key_name(&self) -> &str {
        &self.key_name
    }

    /// Get the key type.
    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    /// Get the raw key bytes from the keyring.
    ///
    /// Returns an error if the key doesn't exist or can't be decrypted.
    pub fn get_key_bytes(&self) -> Result<Vec<u8>> {
        self.keyring.get(&self.key_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileKeyring;
    use std::sync::Arc;

    #[test]
    fn test_keyring_signer_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a 32-byte key
        let key_data = [0u8; 32];
        keyring.set("test-key", &key_data).unwrap();

        let signer = KeyringSigner::new(keyring, "test-key", KeyType::Secp256k1);
        assert_eq!(signer.key_name(), "test-key");
        assert_eq!(signer.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_keyring_signer_verify_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a 32-byte key
        let key_data = [0u8; 32];
        keyring.set("valid-key", &key_data).unwrap();

        let signer = KeyringSigner::new(keyring, "valid-key", KeyType::Ed25519);
        assert!(signer.verify_key().is_ok());
    }

    #[test]
    fn test_keyring_signer_verify_key_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        let signer = KeyringSigner::new(keyring, "nonexistent", KeyType::Secp256k1);
        let result = signer.verify_key();
        assert!(result.is_err());
    }

    #[test]
    fn test_keyring_signer_verify_key_wrong_length() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a key with wrong length (16 bytes instead of 32)
        let key_data = [0u8; 16];
        keyring.set("short-key", &key_data).unwrap();

        let signer = KeyringSigner::new(keyring, "short-key", KeyType::Secp256k1);
        let result = signer.verify_key();
        assert!(result.is_err());
    }

    #[test]
    fn test_keyring_signer_get_key_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        let key_data: Vec<u8> = (0..32).collect();
        keyring.set("my-key", &key_data).unwrap();

        let signer = KeyringSigner::new(keyring, "my-key", KeyType::Secp256k1);
        let retrieved = signer.get_key_bytes().unwrap();
        assert_eq!(retrieved, key_data);
    }
}
