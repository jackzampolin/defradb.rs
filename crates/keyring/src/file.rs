// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! File-based keyring with JWE encryption
//!
//! Uses raw JWE compact serialization for Go DefraDB compatibility.
//! The Go implementation uses github.com/lestrrat-go/jwx/v2/jwe with
//! PBES2_HS512_A256KW algorithm.

use std::fs;
use std::path::{Path, PathBuf};

use josekit::jwe::{self, JweHeader, PBES2_HS512_A256KW};

use crate::error::{Error, Result};
use crate::keyring::Keyring;

/// Content encryption algorithm - matches Go jwx default for PBES2_HS512_A256KW
const CONTENT_ENCRYPTION: &str = "A128CBC-HS256";

/// File-based keyring that stores keys in encrypted files using JWE.
///
/// Each key is stored as a separate file in the directory, encrypted using
/// PBES2-HS512-A256KW (Password-Based Encryption Scheme 2 with HMAC-SHA-512
/// and AES-256-KW for key wrapping) with A128CBC-HS256 content encryption.
///
/// The file format is compatible with Go DefraDB's file keyring.
pub struct FileKeyring {
    dir: PathBuf,
    password: Vec<u8>,
}

impl FileKeyring {
    /// Opens or creates a file keyring in the given directory.
    ///
    /// The directory will be created if it does not exist.
    pub fn open(dir: impl AsRef<Path>, password: impl Into<Vec<u8>>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            password: password.into(),
        })
    }

    fn key_path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut header = JweHeader::new();
        header.set_content_encryption(CONTENT_ENCRYPTION);

        let encrypter = PBES2_HS512_A256KW
            .encrypter_from_bytes(&self.password)
            .map_err(|e| Error::Encryption(format!("failed to create encrypter: {}", e)))?;

        let token = jwe::serialize_compact(data, &header, &encrypter)
            .map_err(|e| Error::Encryption(format!("failed to encrypt: {}", e)))?;

        Ok(token.into_bytes())
    }

    fn decrypt(&self, cipher: &[u8]) -> Result<Vec<u8>> {
        let token = std::str::from_utf8(cipher)
            .map_err(|e| Error::Decryption(format!("invalid token format: {}", e)))?;

        let decrypter = PBES2_HS512_A256KW
            .decrypter_from_bytes(&self.password)
            .map_err(|e| Error::Decryption(format!("failed to create decrypter: {}", e)))?;

        let (data, _header) = jwe::deserialize_compact(token, &decrypter)
            .map_err(|e| Error::Decryption(format!("failed to decrypt: {}", e)))?;

        Ok(data)
    }
}

impl Keyring for FileKeyring {
    fn set(&self, name: &str, key: &[u8]) -> Result<()> {
        let cipher = self.encrypt(key)?;
        fs::write(self.key_path(name), cipher)?;
        Ok(())
    }

    fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = self.key_path(name);
        let cipher = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound(name.to_string())
            } else {
                Error::Io(e)
            }
        })?;
        self.decrypt(&cipher)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let path = self.key_path(name);
        fs::remove_file(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound(name.to_string())
            } else {
                Error::Io(e)
            }
        })
    }

    fn list(&self) -> Result<Vec<String>> {
        let entries = fs::read_dir(&self.dir)?;
        let mut keys = Vec::new();
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    keys.push(name.to_string());
                }
            }
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_keyring_set_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"test-password").unwrap();

        let key_data = b"my-secret-key-data";
        keyring.set("test-key", key_data).unwrap();

        let retrieved = keyring.get("test-key").unwrap();
        assert_eq!(retrieved, key_data);
    }

    #[test]
    fn test_file_keyring_get_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"test-password").unwrap();

        let result = keyring.get("nonexistent");
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[test]
    fn test_file_keyring_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"test-password").unwrap();

        keyring.set("to-delete", b"some-data").unwrap();
        assert!(keyring.get("to-delete").is_ok());

        keyring.delete("to-delete").unwrap();
        assert!(matches!(keyring.get("to-delete"), Err(Error::NotFound(_))));
    }

    #[test]
    fn test_file_keyring_delete_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"test-password").unwrap();

        let result = keyring.delete("nonexistent");
        assert!(matches!(result, Err(Error::NotFound(_))));
    }

    #[test]
    fn test_file_keyring_list() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"test-password").unwrap();

        keyring.set("key1", b"data1").unwrap();
        keyring.set("key2", b"data2").unwrap();
        keyring.set("key3", b"data3").unwrap();

        let mut keys = keyring.list().unwrap();
        keys.sort();
        assert_eq!(keys, vec!["key1", "key2", "key3"]);
    }

    #[test]
    fn test_file_keyring_list_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"test-password").unwrap();

        let keys = keyring.list().unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_file_keyring_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"test-password").unwrap();

        keyring.set("key", b"original").unwrap();
        keyring.set("key", b"updated").unwrap();

        let retrieved = keyring.get("key").unwrap();
        assert_eq!(retrieved, b"updated");
    }

    #[test]
    fn test_file_keyring_wrong_password() {
        let temp_dir = tempfile::tempdir().unwrap();

        let keyring1 = FileKeyring::open(temp_dir.path(), b"password1").unwrap();
        keyring1.set("key", b"secret").unwrap();

        let keyring2 = FileKeyring::open(temp_dir.path(), b"password2").unwrap();
        let result = keyring2.get("key");
        assert!(matches!(result, Err(Error::Decryption(_))));
    }

    #[test]
    fn test_jwe_format_go_compatible() {
        // Verify JWE compact serialization format (5 base64url parts separated by dots)
        let temp_dir = tempfile::tempdir().unwrap();
        let keyring = FileKeyring::open(temp_dir.path(), b"secret").unwrap();

        keyring.set("peer_key", b"abc").unwrap();

        // Read raw file content
        let cipher = std::fs::read(temp_dir.path().join("peer_key")).unwrap();
        let token = std::str::from_utf8(&cipher).unwrap();

        // JWE compact serialization has 5 parts: header.encrypted_key.iv.ciphertext.tag
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 5, "JWE compact should have 5 parts");

        // Verify header contains expected algorithm
        use base64::Engine;
        let header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[0])
            .unwrap();
        let header: serde_json::Value = serde_json::from_slice(&header_bytes).unwrap();

        assert_eq!(header["alg"], "PBES2-HS512+A256KW");
        assert_eq!(header["enc"], "A128CBC-HS256");
    }
}
