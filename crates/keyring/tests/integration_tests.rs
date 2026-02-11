//! Integration tests for keyring functionality

use std::sync::Arc;
use std::thread;

use keyring::{Error, FileKeyring, KeyHandle, KeyType, Keyring};

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

/// Test KeyHandle with actual key operations
#[test]
fn test_key_handle_integration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring: Arc<dyn Keyring> =
        Arc::new(FileKeyring::open(temp_dir.path(), b"handle-test").unwrap());

    // Store a 32-byte secp256k1 private key (random bytes for testing)
    let secp256k1_key: Vec<u8> = (0..32).collect();
    keyring.set("secp256k1_key", &secp256k1_key).unwrap();

    // Store a 64-byte ed25519 private key (full keypair: seed + public key)
    let ed25519_key: Vec<u8> = (0..64).collect();
    keyring.set("ed25519_key", &ed25519_key).unwrap();

    // Create handles using new_verified (validates at construction)
    let secp_handle =
        KeyHandle::new_verified(Arc::clone(&keyring), "secp256k1_key", KeyType::Secp256k1).unwrap();
    let ed_handle =
        KeyHandle::new_verified(Arc::clone(&keyring), "ed25519_key", KeyType::Ed25519).unwrap();

    // Retrieve keys multiple times (simulating multiple operations)
    for _ in 0..10 {
        let key = secp_handle.get_key_bytes().unwrap();
        assert_eq!(key, secp256k1_key);

        let key = ed_handle.get_key_bytes().unwrap();
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

/// Test that path traversal attacks are prevented
#[test]
fn test_file_keyring_path_traversal_prevention() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"path-traversal-test").unwrap();

    // These should all be rejected with InvalidKeyName error
    let dangerous_names = vec![
        "../escape",
        "..\\escape",
        "/absolute/path",
        "sub/dir/key",
        ".",
        "..",
        "key\0name",
    ];

    for name in dangerous_names {
        let result = keyring.set(name, b"data");
        assert!(
            matches!(result, Err(Error::InvalidKeyName(_))),
            "Expected InvalidKeyName error for name: {:?}, got: {:?}",
            name,
            result
        );
    }
}

/// Test that corrupted encrypted files are detected
#[test]
fn test_file_keyring_corrupted_file_detection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"corruption-test").unwrap();

    keyring.set("secret", b"sensitive-data-here").unwrap();

    // Corrupt the file by modifying bytes in the ciphertext portion
    let path = temp_dir.path().join("secret");
    let mut data = std::fs::read(&path).unwrap();

    // Find a position in the ciphertext (after the header, which is the first base64 segment)
    // JWE format: header.encrypted_key.iv.ciphertext.tag
    // Corrupt somewhere in the middle
    if data.len() > 50 {
        data[50] ^= 0xFF; // Flip bits
    }
    std::fs::write(&path, data).unwrap();

    // Should fail with decryption error, not return garbage
    let result = keyring.get("secret");
    assert!(
        matches!(result, Err(Error::Decryption(_))),
        "Expected Decryption error, got: {:?}",
        result
    );
}

/// Test that truncated encrypted files are handled gracefully
#[test]
fn test_file_keyring_truncated_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"truncation-test").unwrap();

    keyring.set("key", b"data-to-truncate").unwrap();

    // Truncate the file to half its size
    let path = temp_dir.path().join("key");
    let data = std::fs::read(&path).unwrap();
    std::fs::write(&path, &data[..data.len() / 2]).unwrap();

    let result = keyring.get("key");
    assert!(
        result.is_err(),
        "Expected error for truncated file, got: {:?}",
        result
    );
}

/// Test that non-JWE file content is handled gracefully
#[test]
fn test_file_keyring_non_jwe_content() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"non-jwe-test").unwrap();

    // Write raw binary (non-JWE) data directly to a key file
    let path = temp_dir.path().join("binary_garbage");
    std::fs::write(&path, [0xFF, 0xFE, 0x00, 0x01]).unwrap();

    let result = keyring.get("binary_garbage");
    assert!(
        matches!(result, Err(Error::Decryption(_))),
        "Expected Decryption error for non-JWE content, got: {:?}",
        result
    );
}

/// Test that empty key names are rejected
#[test]
fn test_file_keyring_empty_key_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"empty-name-test").unwrap();

    let result = keyring.set("", b"data");
    assert!(
        matches!(result, Err(Error::InvalidKeyName(_))),
        "Expected InvalidKeyName error for empty name, got: {:?}",
        result
    );
}

/// Test KeyHandle with key deleted after creation
#[test]
fn test_key_handle_key_deleted_after_creation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let keyring: Arc<dyn Keyring> =
        Arc::new(FileKeyring::open(temp_dir.path(), b"handle-delete-test").unwrap());

    // Store a 64-byte Ed25519 key
    keyring.set("ephemeral_key", &[0u8; 64]).unwrap();

    let handle =
        KeyHandle::new_verified(Arc::clone(&keyring), "ephemeral_key", KeyType::Ed25519).unwrap();

    // Delete the key
    keyring.delete("ephemeral_key").unwrap();

    // Handle should gracefully handle missing key
    let result = handle.get_key_bytes();
    assert!(
        matches!(result, Err(Error::NotFound(_))),
        "Expected NotFound error, got: {:?}",
        result
    );
}

/// Test directory permissions on Unix
#[cfg(unix)]
#[test]
fn test_file_keyring_directory_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let keyring_path = temp_dir.path().join("secure_keyring");

    let _keyring = FileKeyring::open(&keyring_path, b"permissions-test").unwrap();

    // Verify directory has restrictive permissions (0700)
    let metadata = std::fs::metadata(&keyring_path).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o700,
        "Expected directory permissions 0700, got {:o}",
        mode
    );
}

/// Test key file permissions on Unix
#[cfg(unix)]
#[test]
fn test_file_keyring_key_file_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(temp_dir.path(), b"file-permissions-test").unwrap();

    keyring.set("secure_key", b"secret-data").unwrap();

    // Verify key file has restrictive permissions (0600)
    let key_path = temp_dir.path().join("secure_key");
    let metadata = std::fs::metadata(&key_path).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o600,
        "Expected key file permissions 0600, got {:o}",
        mode
    );
}
