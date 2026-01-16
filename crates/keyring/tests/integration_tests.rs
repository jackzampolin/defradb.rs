// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Integration tests for keyring functionality

use std::sync::Arc;
use std::thread;

use keyring::{Error, FileKeyring, KeyType, Keyring, KeyringSigner};

/// Test the complete lifecycle of a key in the file keyring
#[test]
fn test_file_keyring_full_lifecycle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"integration-test-password").unwrap();

    // Initially empty
    let keys = keyring.list().unwrap();
    assert!(keys.is_empty());

    // Create multiple keys
    keyring
        .set("peer_key", b"peer-key-data-32-bytes-exactly!")
        .unwrap();
    keyring
        .set("node_key", b"node-key-data-32-bytes-exactly!")
        .unwrap();
    keyring
        .set("encryption_key", b"enc-key-data-32-bytes-exactly!!")
        .unwrap();

    // Verify list
    let mut keys = keyring.list().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["encryption_key", "node_key", "peer_key"]);

    // Verify retrieval
    assert_eq!(
        keyring.get("peer_key").unwrap(),
        b"peer-key-data-32-bytes-exactly!"
    );
    assert_eq!(
        keyring.get("node_key").unwrap(),
        b"node-key-data-32-bytes-exactly!"
    );

    // Delete one key
    keyring.delete("node_key").unwrap();

    // Verify it's gone
    assert!(matches!(keyring.get("node_key"), Err(Error::NotFound(_))));

    // Others still exist
    let mut keys = keyring.list().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["encryption_key", "peer_key"]);
}

/// Test that keys persist across keyring reopening
#[test]
fn test_file_keyring_persistence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let password = b"persistence-test-password";

    // Create keyring and store key
    {
        let keyring = FileKeyring::open(temp_dir.path(), password).unwrap();
        keyring
            .set("persistent_key", b"this-data-should-persist!!!!")
            .unwrap();
    }

    // Reopen keyring and verify key exists
    {
        let keyring = FileKeyring::open(temp_dir.path(), password).unwrap();
        let data = keyring.get("persistent_key").unwrap();
        assert_eq!(data, b"this-data-should-persist!!!!");
    }
}

/// Test concurrent access to the file keyring
#[test]
fn test_file_keyring_concurrent_access() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let password = b"concurrent-test-password";

    // Pre-create some keys
    let keyring = FileKeyring::open(&path, password).unwrap();
    for i in 0..10 {
        keyring
            .set(&format!("key_{}", i), format!("data_{}", i).as_bytes())
            .unwrap();
    }
    drop(keyring);

    // Spawn multiple threads reading concurrently
    let handles: Vec<_> = (0..5)
        .map(|thread_id| {
            let path = path.clone();
            thread::spawn(move || {
                let keyring = FileKeyring::open(&path, password).unwrap();
                for i in 0..10 {
                    let key_name = format!("key_{}", i);
                    let expected = format!("data_{}", i);
                    let data = keyring.get(&key_name).unwrap();
                    assert_eq!(
                        data,
                        expected.as_bytes(),
                        "Thread {} failed to read key {}",
                        thread_id,
                        key_name
                    );
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

/// Test KeyringSigner with actual key operations
#[test]
fn test_keyring_signer_integration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring: Arc<dyn Keyring> =
        Arc::new(FileKeyring::open(temp_dir.path(), b"signer-test").unwrap());

    // Store a 32-byte secp256k1 private key (random bytes for testing)
    let secp256k1_key: Vec<u8> = (0..32).collect();
    keyring.set("secp256k1_key", &secp256k1_key).unwrap();

    // Store a 32-byte ed25519 private key
    let ed25519_key: Vec<u8> = (32..64).collect();
    keyring.set("ed25519_key", &ed25519_key).unwrap();

    // Create signers
    let secp_signer = KeyringSigner::new(Arc::clone(&keyring), "secp256k1_key", KeyType::Secp256k1);
    let ed_signer = KeyringSigner::new(Arc::clone(&keyring), "ed25519_key", KeyType::Ed25519);

    // Verify keys exist and are correct length
    assert!(secp_signer.verify_key().is_ok());
    assert!(ed_signer.verify_key().is_ok());

    // Retrieve keys multiple times (simulating multiple sign operations)
    for _ in 0..10 {
        let key = secp_signer.get_key_bytes().unwrap();
        assert_eq!(key, secp256k1_key);

        let key = ed_signer.get_key_bytes().unwrap();
        assert_eq!(key, ed25519_key);
    }
}

/// Test that wrong password fails gracefully
#[test]
fn test_file_keyring_wrong_password_error() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create keyring with one password
    {
        let keyring = FileKeyring::open(temp_dir.path(), b"correct-password").unwrap();
        keyring.set("secret", b"sensitive-data-here!!!!!").unwrap();
    }

    // Try to read with wrong password
    {
        let keyring = FileKeyring::open(temp_dir.path(), b"wrong-password").unwrap();
        let result = keyring.get("secret");
        assert!(matches!(result, Err(Error::Decryption(_))));
    }
}

/// Test binary data (non-UTF8) keys
#[test]
fn test_file_keyring_binary_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"binary-test").unwrap();

    // Store binary data with all byte values
    let binary_data: Vec<u8> = (0u8..=255).collect();
    keyring.set("binary_key", &binary_data).unwrap();

    let retrieved = keyring.get("binary_key").unwrap();
    assert_eq!(retrieved, binary_data);
}

/// Test empty key data
#[test]
fn test_file_keyring_empty_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"empty-test").unwrap();

    keyring.set("empty_key", b"").unwrap();
    let retrieved = keyring.get("empty_key").unwrap();
    assert!(retrieved.is_empty());
}

/// Test large key data
#[test]
fn test_file_keyring_large_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"large-test").unwrap();

    // 1MB of data
    let large_data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    keyring.set("large_key", &large_data).unwrap();

    let retrieved = keyring.get("large_key").unwrap();
    assert_eq!(retrieved.len(), large_data.len());
    assert_eq!(retrieved, large_data);
}

/// Test special characters in key names
#[test]
fn test_file_keyring_special_key_names() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"special-names-test").unwrap();

    // Various key names that might cause issues
    let test_cases = vec![
        ("simple", b"data1".as_slice()),
        ("with-dashes", b"data2"),
        ("with_underscores", b"data3"),
        ("with.dots", b"data4"),
        ("MixedCase", b"data5"),
        ("123numeric", b"data6"),
    ];

    for (name, data) in &test_cases {
        keyring.set(name, data).unwrap();
    }

    for (name, expected) in &test_cases {
        let retrieved = keyring.get(name).unwrap();
        assert_eq!(&retrieved[..], *expected, "Failed for key: {}", name);
    }
}
