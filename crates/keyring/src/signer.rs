// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Key handle backed by keyring
//!
//! Provides access to keys stored in a keyring with on-demand fetching.
//! The key is fetched from the keyring on each request rather than
//! being cached, minimizing the risk of key material being leaked via
//! memory paging.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::key_name::KeyName;
use crate::Keyring;

/// A handle to a key stored in a keyring.
///
/// `KeyHandle` provides on-demand access to cryptographic keys without caching
/// the key material in memory. This minimizes the risk of key material being
/// leaked through memory paging or other side channels.
///
/// The handle stores a reference to the keyring and the key name, fetching
/// the actual key bytes only when needed.
///
/// # Example
///
/// ```ignore
/// use keyring::{FileKeyring, KeyHandle, KeyType, KeyName};
/// use std::sync::Arc;
///
/// let keyring = Arc::new(FileKeyring::open("./keys", b"password")?);
/// keyring.set("my-key", &[0u8; 64])?;
///
/// // Create a verified handle (validates key exists and has correct length)
/// let handle = KeyHandle::new_verified(keyring, "my-key", KeyType::Ed25519)?;
///
/// // Get key bytes on demand
/// let key_bytes = handle.get_key_bytes()?;
/// ```
pub struct KeyHandle {
    keyring: Arc<dyn Keyring>,
    key_name: KeyName,
    key_type: KeyType,
}

/// Key type for determining how to interpret key bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// secp256k1 ECDSA key (32 bytes scalar)
    Secp256k1,
    /// Ed25519 key (64 bytes: 32-byte seed + 32-byte public key)
    ///
    /// This matches Go's ed25519.PrivateKey type which stores the full keypair.
    Ed25519,
}

impl KeyType {
    /// Get the expected key length in bytes for this key type.
    pub fn expected_length(&self) -> usize {
        match self {
            KeyType::Secp256k1 => 32,
            KeyType::Ed25519 => 64,
        }
    }
}

impl KeyHandle {
    /// Create a new key handle without verification.
    ///
    /// The key may not exist or may have incorrect length. Call `verify_key`
    /// to validate, or use `new_verified` for immediate validation.
    pub fn new(
        keyring: Arc<dyn Keyring>,
        key_name: impl Into<String>,
        key_type: KeyType,
    ) -> Result<Self> {
        let key_name = KeyName::new(key_name)?;
        Ok(Self {
            keyring,
            key_name,
            key_type,
        })
    }

    /// Create a new key handle with immediate verification.
    ///
    /// This constructor validates that the key exists in the keyring and
    /// has the expected length for the specified key type. Returns an error
    /// if the key is missing or has incorrect length.
    ///
    /// This is the recommended constructor when you need to ensure the key
    /// is valid before proceeding.
    pub fn new_verified(
        keyring: Arc<dyn Keyring>,
        key_name: impl Into<String>,
        key_type: KeyType,
    ) -> Result<Self> {
        let handle = Self::new(keyring, key_name, key_type)?;
        handle.verify_key()?;
        Ok(handle)
    }

    /// Verify that the key exists in the keyring and has the correct length.
    ///
    /// Returns an error if the key doesn't exist or has incorrect length
    /// for the specified key type.
    pub fn verify_key(&self) -> Result<()> {
        let key_bytes = self.keyring.get(self.key_name.as_str())?;
        let expected_len = self.key_type.expected_length();
        if key_bytes.len() != expected_len {
            return Err(Error::Decryption(format!(
                "invalid key length for {:?}: expected {} bytes, got {}",
                self.key_type,
                expected_len,
                key_bytes.len()
            )));
        }
        Ok(())
    }

    /// Get the key name.
    pub fn key_name(&self) -> &KeyName {
        &self.key_name
    }

    /// Get the key type.
    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    /// Get the raw key bytes from the keyring.
    ///
    /// Returns an error if the key doesn't exist or can't be decrypted.
    /// The key is fetched fresh from the keyring on each call.
    pub fn get_key_bytes(&self) -> Result<Vec<u8>> {
        self.keyring.get(self.key_name.as_str())
    }
}

/// Type alias for backward compatibility
#[deprecated(since = "0.2.0", note = "Renamed to KeyHandle")]
pub type KeyringSigner = KeyHandle;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileKeyring;
    use std::sync::Arc;

    #[test]
    fn test_key_handle_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a 32-byte key
        let key_data = [0u8; 32];
        keyring.set("test-key", &key_data).unwrap();

        let handle = KeyHandle::new(keyring, "test-key", KeyType::Secp256k1).unwrap();
        assert_eq!(handle.key_name().as_str(), "test-key");
        assert_eq!(handle.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_key_handle_new_verified() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring: Arc<dyn crate::Keyring> =
            Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a 64-byte Ed25519 key (full keypair)
        let key_data = [0u8; 64];
        keyring.set("valid-key", &key_data).unwrap();

        // new_verified should succeed
        let handle =
            KeyHandle::new_verified(Arc::clone(&keyring), "valid-key", KeyType::Ed25519).unwrap();
        assert_eq!(handle.key_name().as_str(), "valid-key");

        // new_verified should fail for missing key
        let result = KeyHandle::new_verified(Arc::clone(&keyring), "missing", KeyType::Ed25519);
        assert!(result.is_err());

        // new_verified should fail for wrong length
        keyring.set("wrong-length", &[0u8; 32]).unwrap();
        let result = KeyHandle::new_verified(keyring, "wrong-length", KeyType::Ed25519);
        assert!(result.is_err());
    }

    #[test]
    fn test_key_handle_verify_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a 64-byte Ed25519 key (full keypair)
        let key_data = [0u8; 64];
        keyring.set("valid-key", &key_data).unwrap();

        let handle = KeyHandle::new(keyring, "valid-key", KeyType::Ed25519).unwrap();
        assert!(handle.verify_key().is_ok());
    }

    #[test]
    fn test_key_handle_verify_key_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        let handle = KeyHandle::new(keyring, "nonexistent", KeyType::Secp256k1).unwrap();
        let result = handle.verify_key();
        assert!(result.is_err());
    }

    #[test]
    fn test_key_handle_verify_key_wrong_length_secp256k1() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a key with wrong length (16 bytes instead of 32 for secp256k1)
        let key_data = [0u8; 16];
        keyring.set("short-key", &key_data).unwrap();

        let handle = KeyHandle::new(keyring, "short-key", KeyType::Secp256k1).unwrap();
        let result = handle.verify_key();
        assert!(result.is_err());
    }

    #[test]
    fn test_key_handle_verify_key_wrong_length_ed25519() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Store a key with wrong length (32 bytes instead of 64 for Ed25519)
        let key_data = [0u8; 32];
        keyring.set("short-ed25519-key", &key_data).unwrap();

        let handle = KeyHandle::new(keyring, "short-ed25519-key", KeyType::Ed25519).unwrap();
        let result = handle.verify_key();
        assert!(result.is_err());
    }

    #[test]
    fn test_key_handle_get_key_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        let key_data: Vec<u8> = (0..32).collect();
        keyring.set("my-key", &key_data).unwrap();

        let handle = KeyHandle::new(keyring, "my-key", KeyType::Secp256k1).unwrap();
        let retrieved = handle.get_key_bytes().unwrap();
        assert_eq!(retrieved, key_data);
    }

    #[test]
    fn test_key_handle_invalid_key_name() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = Arc::new(FileKeyring::open(temp_dir.path(), b"password").unwrap());

        // Invalid key names should fail at construction
        let result = KeyHandle::new(keyring, "../escape", KeyType::Secp256k1);
        assert!(matches!(result, Err(Error::InvalidKeyName(_))));
    }

    #[test]
    fn test_key_type_expected_length() {
        assert_eq!(KeyType::Secp256k1.expected_length(), 32);
        assert_eq!(KeyType::Ed25519.expected_length(), 64);
    }
}
