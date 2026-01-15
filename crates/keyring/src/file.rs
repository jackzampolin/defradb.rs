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

use std::fs;
use std::path::{Path, PathBuf};

use josekit::jwe::{JweHeader, PBES2_HS512_A256KW};
use josekit::jwt::{self, JwtPayload};

use crate::error::{Error, Result};
use crate::keyring::Keyring;

/// File-based keyring that stores keys in encrypted files using JWE.
///
/// Each key is stored as a separate file in the directory, encrypted using
/// PBES2-HS512-A256KW (Password-Based Encryption Scheme 2 with HMAC-SHA-512
/// and AES-256-KW for key wrapping).
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
        header.set_content_encryption("A256GCM");

        let encrypter = PBES2_HS512_A256KW
            .encrypter_from_bytes(&self.password)
            .map_err(|e| Error::Encryption(format!("failed to create encrypter: {}", e)))?;

        let mut payload = JwtPayload::new();
        payload
            .set_claim("key", Some(serde_json::json!(base64_encode(data))))
            .map_err(|e| Error::Encryption(format!("failed to set payload: {}", e)))?;

        let token = jwt::encode_with_encrypter(&payload, &header, &encrypter)
            .map_err(|e| Error::Encryption(format!("failed to encrypt: {}", e)))?;

        Ok(token.into_bytes())
    }

    fn decrypt(&self, cipher: &[u8]) -> Result<Vec<u8>> {
        let token = std::str::from_utf8(cipher)
            .map_err(|e| Error::Decryption(format!("invalid token format: {}", e)))?;

        let decrypter = PBES2_HS512_A256KW
            .decrypter_from_bytes(&self.password)
            .map_err(|e| Error::Decryption(format!("failed to create decrypter: {}", e)))?;

        let (payload, _header) = jwt::decode_with_decrypter(token, &decrypter)
            .map_err(|e| Error::Decryption(format!("failed to decrypt: {}", e)))?;

        let key_b64 = payload
            .claim("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Decryption("missing key claim".to_string()))?;

        base64_decode(key_b64).map_err(|e| Error::Decryption(format!("invalid base64: {}", e)))
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

fn base64_encode(data: &[u8]) -> String {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut encoder =
            base64::write::EncoderWriter::new(&mut buf, &base64::engine::general_purpose::STANDARD);
        encoder.write_all(data).unwrap();
    }
    String::from_utf8(buf).unwrap()
}

fn base64_decode(s: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s)
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
}
