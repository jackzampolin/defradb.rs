//! X25519/HKDF, ECIES, AES-GCM cross-implementation tests for Go compatibility.
//!
//! These test vectors were generated from the Go DefraDB implementation
//! to ensure the Rust crypto implementation produces identical outputs.

use crypto::encryption::aes::{decrypt_aes, encrypt_aes};
use crypto::encryption::ecies::{decrypt_ecies, encrypt_ecies, EciesOptions};
use crypto::encryption::nonce::USE_DETERMINISTIC_NONCE;
use crypto::keys::ed25519::Ed25519PrivateKey;
use crypto::keys::{Key, PrivateKey};
use hkdf::Hkdf;
use serial_test::serial;
use sha2::Sha256;
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

// ===== X25519/ECIES Test Vectors =====
const X25519_SENDER_PRIVATE: [u8; 32] = [
    0x77, 0x07, 0x6d, 0x0a, 0x73, 0x18, 0xa5, 0x7d, 0x3c, 0x16, 0xc1, 0x72, 0x51, 0xb2, 0x66,
    0x45, 0xdf, 0x4c, 0x2f, 0x87, 0xeb, 0xc0, 0x99, 0x2a, 0xb1, 0x77, 0xfb, 0xa5, 0x1d, 0xb9,
    0x2c, 0x2a,
];
const X25519_SENDER_PUBLIC: [u8; 32] = [
    0x85, 0x20, 0xf0, 0x09, 0x89, 0x30, 0xa7, 0x54, 0x74, 0x8b, 0x7d, 0xdc, 0xb4, 0x3e, 0xf7,
    0x5a, 0x0d, 0xbf, 0x3a, 0x0d, 0x26, 0x38, 0x1a, 0xf4, 0xeb, 0xa4, 0xa9, 0x8e, 0xaa, 0x9b,
    0x4e, 0x6a,
];
const X25519_RECIPIENT_PRIVATE: [u8; 32] = [
    0x5d, 0xab, 0x08, 0x7e, 0x62, 0x4a, 0x8a, 0x4b, 0x79, 0xe1, 0x7f, 0x8b, 0x83, 0x80, 0x0e,
    0xe6, 0x6f, 0x3b, 0xb1, 0x29, 0x26, 0x18, 0xb6, 0xfd, 0x1c, 0x2f, 0x8b, 0x27, 0xff, 0x88,
    0xe0, 0xeb,
];
const X25519_RECIPIENT_PUBLIC: [u8; 32] = [
    0xde, 0x9e, 0xdb, 0x7d, 0x7b, 0x7d, 0xc1, 0xb4, 0xd3, 0x5b, 0x61, 0xc2, 0xec, 0xe4, 0x35,
    0x37, 0x3f, 0x83, 0x43, 0xc8, 0x5b, 0x78, 0x67, 0x4d, 0xad, 0xfc, 0x7e, 0x14, 0x6f, 0x88,
    0x2b, 0x4f,
];
const X25519_SHARED_SECRET: [u8; 32] = [
    0x4a, 0x5d, 0x9d, 0x5b, 0xa4, 0xce, 0x2d, 0xe1, 0x72, 0x8e, 0x3b, 0xf4, 0x80, 0x35, 0x0f,
    0x25, 0xe0, 0x7e, 0x21, 0xc9, 0x47, 0xd1, 0x9e, 0x33, 0x76, 0xf0, 0x9b, 0x3c, 0x1e, 0x16,
    0x17, 0x42,
];
const HKDF_AES_KEY: [u8; 32] = [
    0xea, 0x1d, 0x8a, 0x20, 0xf4, 0x76, 0xd1, 0xe1, 0xec, 0x95, 0x2c, 0xa4, 0x27, 0x08, 0xb8,
    0xf7, 0x16, 0x1c, 0xe7, 0xc8, 0x1e, 0xad, 0xf9, 0x7e, 0x52, 0x0e, 0x2b, 0x40, 0x33, 0x3d,
    0xec, 0xd5,
];
const HKDF_HMAC_KEY: [u8; 32] = [
    0x66, 0x98, 0xbc, 0x97, 0xa8, 0xce, 0x75, 0x06, 0x84, 0x9b, 0xe3, 0x20, 0x17, 0x5a, 0x48,
    0x32, 0xc5, 0xce, 0x24, 0x62, 0xe9, 0xc3, 0x0c, 0xd4, 0x30, 0x0b, 0x04, 0xa2, 0x8d, 0x75,
    0xbf, 0xa5,
];

const ECIES_PLAINTEXT: &[u8] = b"Hello, World!";
const ECIES_CIPHERTEXT: &[u8] = &[
    0x85, 0x20, 0xf0, 0x09, 0x89, 0x30, 0xa7, 0x54, 0x74, 0x8b, 0x7d, 0xdc, 0xb4, 0x3e, 0xf7,
    0x5a, 0x0d, 0xbf, 0x3a, 0x0d, 0x26, 0x38, 0x1a, 0xf4, 0xeb, 0xa4, 0xa9, 0x8e, 0xaa, 0x9b,
    0x4e, 0x6a, 0x64, 0x65, 0x74, 0x65, 0x72, 0x6d, 0x69, 0x6e, 0x69, 0x73, 0x74, 0x69, 0x06,
    0x37, 0x0d, 0xcb, 0x50, 0x3d, 0x96, 0x30, 0x77, 0xa7, 0x3b, 0x9b, 0x53, 0xc6, 0x2c, 0x18,
    0x02, 0xf5, 0x1f, 0xf1, 0xf3, 0x82, 0xdc, 0xef, 0xbf, 0x09, 0x7d, 0x97, 0xd2, 0xf6, 0xe7,
    0x67, 0x51, 0x20, 0x28, 0x1c, 0x9c, 0x2f, 0x22, 0xd6, 0x48, 0x94, 0x57, 0x4b, 0x44, 0xf8,
    0x1e, 0xea, 0x6d, 0xc5, 0x8d, 0x32, 0x3d, 0xca, 0xcc, 0xeb, 0x66, 0x7d, 0xe8, 0x3d, 0x7b,
];

// ===== AES-GCM Test Vectors =====
const AES_KEY: [u8; 32] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
    0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
    0x1e, 0x1f,
];
const AES_PLAINTEXT: &[u8] = b"Secret message";
const AES_AAD: &[u8] = b"additional data";
const AES_NONCE: [u8; 12] = [
    0x64, 0x65, 0x74, 0x65, 0x72, 0x6d, 0x69, 0x6e, 0x69, 0x73, 0x74, 0x69,
];
const AES_CIPHERTEXT_WITH_NONCE: &[u8] = &[
    0x64, 0x65, 0x74, 0x65, 0x72, 0x6d, 0x69, 0x6e, 0x69, 0x73, 0x74, 0x69, 0x3a, 0x52, 0xfb,
    0x62, 0x7c, 0x15, 0x83, 0xc4, 0x6c, 0xa9, 0x0b, 0xc5, 0x5d, 0xbd, 0xa5, 0x2c, 0xc5, 0x79,
    0xa9, 0x64, 0x8c, 0xc1, 0x81, 0xa2, 0xa1, 0xbc, 0xd4, 0xb0, 0x49, 0xe3,
];


// ===== X25519/HKDF Compatibility Tests =====

#[test]
fn test_x25519_public_key_derivation_matches_go() {
    let sender_private = StaticSecret::from(X25519_SENDER_PRIVATE);
    let sender_public = X25519PublicKey::from(&sender_private);

    assert_eq!(
        sender_public.as_bytes(),
        &X25519_SENDER_PUBLIC,
        "X25519 public key derivation should match Go"
    );

    let recipient_private = StaticSecret::from(X25519_RECIPIENT_PRIVATE);
    let recipient_public = X25519PublicKey::from(&recipient_private);

    assert_eq!(
        recipient_public.as_bytes(),
        &X25519_RECIPIENT_PUBLIC,
        "X25519 recipient public key should match Go"
    );
}

#[test]
fn test_x25519_shared_secret_matches_go() {
    let sender_private = StaticSecret::from(X25519_SENDER_PRIVATE);
    let recipient_public = X25519PublicKey::from(X25519_RECIPIENT_PUBLIC);

    let shared_secret = sender_private.diffie_hellman(&recipient_public);

    assert_eq!(
        shared_secret.as_bytes(),
        &X25519_SHARED_SECRET,
        "X25519 shared secret should match Go"
    );
}

#[test]
fn test_hkdf_key_derivation_matches_go() {
    // Use the shared secret from Go
    let hkdf = Hkdf::<Sha256>::new(None, &X25519_SHARED_SECRET);

    let mut keys = [0u8; 64];
    hkdf.expand(&[], &mut keys).unwrap();

    let aes_key = &keys[..32];
    let hmac_key = &keys[32..];

    assert_eq!(
        aes_key, &HKDF_AES_KEY,
        "HKDF AES key derivation should match Go"
    );
    assert_eq!(
        hmac_key, &HKDF_HMAC_KEY,
        "HKDF HMAC key derivation should match Go"
    );
}

// ===== ECIES Compatibility Tests =====

#[test]
fn test_ecies_decrypt_go_ciphertext() {
    let recipient_private = StaticSecret::from(X25519_RECIPIENT_PRIVATE);

    let options = EciesOptions::builder().prepend_public_key(true).build();

    let decrypted = decrypt_ecies(ECIES_CIPHERTEXT, &recipient_private, options)
        .expect("Should decrypt Go ECIES ciphertext");

    assert_eq!(
        decrypted, ECIES_PLAINTEXT,
        "Decrypted plaintext should match Go"
    );
}

#[test]
#[serial]
fn test_ecies_encrypt_matches_go_with_deterministic_nonce() {
    // Enable deterministic nonce mode
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let sender_private = StaticSecret::from(X25519_SENDER_PRIVATE);
    let recipient_public = X25519PublicKey::from(X25519_RECIPIENT_PUBLIC);

    let options = EciesOptions::builder()
        .with_private_key(sender_private)
        .prepend_public_key(true)
        .build();

    let ciphertext = encrypt_ecies(ECIES_PLAINTEXT, &recipient_public, options)
        .expect("Should encrypt with ECIES");

    // Restore random nonce mode
    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        ciphertext, ECIES_CIPHERTEXT,
        "ECIES ciphertext should match Go with deterministic nonce"
    );
}

#[test]
fn test_ecies_bidirectional_round_trip() {
    // This test verifies that Rust-encrypted data can be decrypted by Rust
    // (and by extension, Go, since the ciphertext format is identical).
    // This is the critical bidirectional compatibility test.

    let long_message = vec![b'A'; 1000];
    let test_messages: Vec<&[u8]> = vec![
        b"Hello, World!",
        b"",                             // Empty message
        &[0x00, 0x01, 0x02, 0xff, 0xfe], // Binary data with null bytes
        &long_message,                   // Longer message
    ];

    for msg in test_messages {
        let recipient_private = StaticSecret::from(X25519_RECIPIENT_PRIVATE);
        let recipient_public = X25519PublicKey::from(X25519_RECIPIENT_PUBLIC);

        // Encrypt with Rust (random ephemeral key, random nonce - production mode)
        let encrypt_options = EciesOptions::builder().prepend_public_key(true).build();
        let ciphertext = encrypt_ecies(msg, &recipient_public, encrypt_options)
            .expect("Should encrypt with ECIES");

        // Decrypt with Rust (simulating Go decryption of Rust ciphertext)
        let decrypt_options = EciesOptions::builder().prepend_public_key(true).build();
        let decrypted = decrypt_ecies(&ciphertext, &recipient_private, decrypt_options)
            .expect("Should decrypt Rust ECIES ciphertext");

        assert_eq!(
            decrypted,
            msg,
            "Bidirectional round-trip should preserve message (len={})",
            msg.len()
        );
    }
}

#[test]
#[serial]
fn test_ecies_ciphertext_format_matches_go() {
    // Verify the ciphertext structure matches what Go expects:
    // [32-byte ephemeral public key][12-byte nonce][ciphertext][16-byte auth tag][32-byte HMAC]

    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let sender_private = StaticSecret::from(X25519_SENDER_PRIVATE);
    let recipient_public = X25519PublicKey::from(X25519_RECIPIENT_PUBLIC);

    let plaintext = b"test";
    let options = EciesOptions::builder()
        .with_private_key(sender_private)
        .prepend_public_key(true)
        .build();

    let ciphertext =
        encrypt_ecies(plaintext, &recipient_public, options).expect("Should encrypt");

    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    // Expected structure:
    // 32 (ephemeral pubkey) + 12 (nonce) + 4 (plaintext) + 16 (auth tag) + 32 (HMAC) = 96
    let expected_len = 32 + 12 + plaintext.len() + 16 + 32;
    assert_eq!(
        ciphertext.len(),
        expected_len,
        "ECIES ciphertext should have correct structure length"
    );

    // First 32 bytes should be sender's public key
    assert_eq!(
        &ciphertext[..32],
        &X25519_SENDER_PUBLIC,
        "Ciphertext should start with sender's ephemeral public key"
    );
}

// ===== AES-GCM Compatibility Tests =====

#[test]
fn test_aes_decrypt_go_ciphertext() {
    let decrypted = decrypt_aes(None, AES_CIPHERTEXT_WITH_NONCE, &AES_KEY, AES_AAD)
        .expect("Should decrypt Go AES ciphertext");

    assert_eq!(decrypted, AES_PLAINTEXT, "Decrypted AES should match Go");
}

#[test]
#[serial]
fn test_aes_encrypt_matches_go_with_deterministic_nonce() {
    // Enable deterministic nonce mode
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let (ciphertext, nonce) =
        encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt with AES");

    // Restore random nonce mode
    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        nonce, AES_NONCE,
        "Deterministic nonce should match Go test nonce"
    );
    assert_eq!(
        ciphertext, AES_CIPHERTEXT_WITH_NONCE,
        "AES ciphertext should match Go with deterministic nonce"
    );
}

#[test]
fn test_deterministic_nonce_value() {
    // Verify our deterministic nonce matches Go's generateTestNonce()
    // Go: []byte("deterministic nonce for testing")[:12]
    let expected = b"deterministi";
    assert_eq!(
        &AES_NONCE, expected,
        "Deterministic nonce should be first 12 bytes of Go test string"
    );
}

// ===== AES-GCM Edge Case Tests =====

#[test]
#[serial]
fn test_aes_empty_plaintext() {
    // Empty plaintext should encrypt/decrypt correctly
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let empty_plaintext: &[u8] = b"";
    let (ciphertext, _nonce) = encrypt_aes(empty_plaintext, &AES_KEY, AES_AAD, true)
        .expect("Should encrypt empty plaintext");

    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    // Ciphertext should only contain nonce (12) + auth tag (16) = 28 bytes
    assert_eq!(
        ciphertext.len(),
        28,
        "Empty plaintext should produce 28-byte ciphertext"
    );

    // Decrypt and verify
    let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD)
        .expect("Should decrypt empty plaintext");
    assert_eq!(
        decrypted, empty_plaintext,
        "Decrypted empty plaintext should match"
    );
}

#[test]
#[serial]
fn test_aes_empty_aad() {
    // Empty AAD should work correctly
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let empty_aad: &[u8] = b"";
    let (ciphertext, _nonce) = encrypt_aes(AES_PLAINTEXT, &AES_KEY, empty_aad, true)
        .expect("Should encrypt with empty AAD");

    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, empty_aad)
        .expect("Should decrypt with empty AAD");
    assert_eq!(
        decrypted, AES_PLAINTEXT,
        "Decrypted with empty AAD should match"
    );
}

#[test]
fn test_aes_large_plaintext() {
    // 1MB plaintext should encrypt/decrypt correctly
    let large_plaintext = vec![0xABu8; 1024 * 1024]; // 1MB of 0xAB bytes

    let (ciphertext, _nonce) = encrypt_aes(&large_plaintext, &AES_KEY, AES_AAD, true)
        .expect("Should encrypt large plaintext");

    // Expected: 12 (nonce) + 1MB (ciphertext) + 16 (tag) = 1048604 bytes
    assert_eq!(
        ciphertext.len(),
        12 + large_plaintext.len() + 16,
        "Large plaintext ciphertext should have correct length"
    );

    let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD)
        .expect("Should decrypt large plaintext");
    assert_eq!(
        decrypted, large_plaintext,
        "Decrypted large plaintext should match"
    );
}

#[test]
fn test_aes_binary_plaintext() {
    // Binary data with null bytes should work correctly
    let binary_plaintext: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00, 0x7f, 0x80];

    let (ciphertext, _nonce) = encrypt_aes(binary_plaintext, &AES_KEY, AES_AAD, true)
        .expect("Should encrypt binary plaintext");

    let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD)
        .expect("Should decrypt binary plaintext");
    assert_eq!(
        decrypted, binary_plaintext,
        "Decrypted binary plaintext should match"
    );
}

#[test]
#[serial]
fn test_aes_wrong_key_fails() {
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let (ciphertext, _nonce) =
        encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    // Try to decrypt with wrong key
    let wrong_key: [u8; 32] = [0xFF; 32];
    let result = decrypt_aes(None, &ciphertext, &wrong_key, AES_AAD);
    assert!(result.is_err(), "Decryption with wrong key should fail");
}

#[test]
#[serial]
fn test_aes_wrong_aad_fails() {
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let (ciphertext, _nonce) =
        encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    // Try to decrypt with wrong AAD
    let wrong_aad: &[u8] = b"wrong additional data";
    let result = decrypt_aes(None, &ciphertext, &AES_KEY, wrong_aad);
    assert!(result.is_err(), "Decryption with wrong AAD should fail");
}

#[test]
#[serial]
fn test_aes_tampered_ciphertext_fails() {
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let (mut ciphertext, _nonce) =
        encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    // Tamper with ciphertext (flip a bit in the encrypted portion)
    if ciphertext.len() > 15 {
        ciphertext[15] ^= 0x01;
    }

    let result = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD);
    assert!(
        result.is_err(),
        "Decryption of tampered ciphertext should fail"
    );
}

#[test]
#[serial]
fn test_aes_truncated_ciphertext_fails() {
    USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

    let (ciphertext, _nonce) =
        encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

    USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

    // Truncate the ciphertext (remove auth tag)
    let truncated = &ciphertext[..ciphertext.len() - 5];

    let result = decrypt_aes(None, truncated, &AES_KEY, AES_AAD);
    assert!(
        result.is_err(),
        "Decryption of truncated ciphertext should fail"
    );
}

