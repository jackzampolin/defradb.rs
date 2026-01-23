
//! Tests for system keyring (OS-provided key management)
//!
//! These tests interact with the actual OS keyring.
//! Run with: cargo test -p keyring -- --ignored

use keyring::{Error, Keyring, SystemKeyring};

const TEST_SERVICE: &str = "defradb-rust-test";

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
