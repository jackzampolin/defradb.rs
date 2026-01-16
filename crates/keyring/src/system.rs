// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! System keyring using OS-provided key management
//!
//! Uses the operating system's native keyring service:
//! - macOS: Keychain
//! - Linux: Secret Service (GNOME Keyring, KWallet)
//! - Windows: Credential Manager

use crate::error::{Error, Result};
use crate::keyring::Keyring;

use keyring_crate::Entry;

/// System keyring that uses the OS-provided key management service.
///
/// Keys are stored using base64 encoding since OS keyrings typically
/// expect string values. This matches Go DefraDB's SystemKeyring behavior.
pub struct SystemKeyring {
    service: String,
}

impl SystemKeyring {
    /// Opens the system keyring with the given service name.
    ///
    /// The service name is used to namespace keys in the OS keyring.
    pub fn open(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
        }
    }

    fn entry(&self, name: &str) -> std::result::Result<Entry, keyring_crate::Error> {
        Entry::new(&self.service, name)
    }
}

impl Keyring for SystemKeyring {
    fn set(&self, name: &str, key: &[u8]) -> Result<()> {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(key);

        let entry = self
            .entry(name)
            .map_err(|e| Error::Encryption(format!("failed to create entry: {}", e)))?;

        entry
            .set_password(&encoded)
            .map_err(|e| Error::Encryption(format!("failed to store key: {}", e)))
    }

    fn get(&self, name: &str) -> Result<Vec<u8>> {
        use base64::Engine;

        let entry = self
            .entry(name)
            .map_err(|e| Error::Decryption(format!("failed to create entry: {}", e)))?;

        let encoded = entry.get_password().map_err(|e| match e {
            keyring_crate::Error::NoEntry => Error::NotFound(name.to_string()),
            _ => Error::Decryption(format!("failed to retrieve key: {}", e)),
        })?;

        base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .map_err(|e| Error::Decryption(format!("invalid base64: {}", e)))
    }

    fn delete(&self, name: &str) -> Result<()> {
        let entry = self
            .entry(name)
            .map_err(|e| Error::Encryption(format!("failed to create entry: {}", e)))?;

        entry.delete_credential().map_err(|e| match e {
            keyring_crate::Error::NoEntry => Error::NotFound(name.to_string()),
            _ => Error::Io(std::io::Error::other(format!(
                "failed to delete key: {}",
                e
            ))),
        })
    }

    fn list(&self) -> Result<Vec<String>> {
        Err(Error::SystemKeyringListNotSupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SERVICE: &str = "defradb-rust-test";

    // These tests interact with the actual OS keyring.
    // Run with: cargo test -p keyring -- --ignored

    #[test]
    #[ignore]
    fn test_system_keyring_set_get() {
        let keyring = SystemKeyring::open(TEST_SERVICE);
        let key_name = "test-key-set-get";

        // Clean up from previous runs
        let _ = keyring.delete(key_name);

        // Set and get
        keyring.set(key_name, b"test-data").unwrap();
        let retrieved = keyring.get(key_name).unwrap();
        assert_eq!(retrieved, b"test-data");

        // Cleanup
        keyring.delete(key_name).unwrap();
    }

    #[test]
    #[ignore]
    fn test_system_keyring_get_not_found() {
        let keyring = SystemKeyring::open(TEST_SERVICE);
        let result = keyring.get("nonexistent-key-12345");
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[test]
    #[ignore]
    fn test_system_keyring_delete() {
        let keyring = SystemKeyring::open(TEST_SERVICE);
        let key_name = "test-key-delete";

        keyring.set(key_name, b"to-delete").unwrap();
        keyring.delete(key_name).unwrap();

        let result = keyring.get(key_name);
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[test]
    #[ignore]
    fn test_system_keyring_delete_not_found() {
        let keyring = SystemKeyring::open(TEST_SERVICE);
        let result = keyring.delete("nonexistent-key-delete-12345");
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[test]
    #[ignore]
    fn test_system_keyring_list_not_supported() {
        let keyring = SystemKeyring::open(TEST_SERVICE);
        let result = keyring.list();
        assert!(matches!(result, Err(Error::SystemKeyringListNotSupported)));
    }

    #[test]
    #[ignore]
    fn test_system_keyring_binary_data() {
        let keyring = SystemKeyring::open(TEST_SERVICE);
        let key_name = "test-key-binary";

        // Clean up
        let _ = keyring.delete(key_name);

        // Binary data with all byte values 0-255
        let binary_data: Vec<u8> = (0u8..=255).collect();
        keyring.set(key_name, &binary_data).unwrap();

        let retrieved = keyring.get(key_name).unwrap();
        assert_eq!(retrieved, binary_data);

        // Cleanup
        keyring.delete(key_name).unwrap();
    }

    #[test]
    #[ignore]
    fn test_system_keyring_overwrite() {
        let keyring = SystemKeyring::open(TEST_SERVICE);
        let key_name = "test-key-overwrite";

        // Clean up
        let _ = keyring.delete(key_name);

        // Set initial value
        keyring.set(key_name, b"first-value").unwrap();
        assert_eq!(keyring.get(key_name).unwrap(), b"first-value");

        // Overwrite
        keyring.set(key_name, b"second-value").unwrap();
        assert_eq!(keyring.get(key_name).unwrap(), b"second-value");

        // Cleanup
        keyring.delete(key_name).unwrap();
    }
}
