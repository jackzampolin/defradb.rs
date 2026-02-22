//! Tests for key handle functionality

use std::sync::Arc;

use keyring::{Error, FileKeyring, KeyHandle, KeyType, Keyring};

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
    let keyring: Arc<dyn Keyring> =
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
    assert_eq!(&retrieved[..], key_data.as_slice());
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
