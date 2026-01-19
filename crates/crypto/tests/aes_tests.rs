//! Integration tests for AES-256-GCM encryption/decryption

use crypto::encryption::aes::{decrypt_aes, encrypt_aes};
use crypto::keys::generation::generate_aes256;
use crypto::types::AES_NONCE_SIZE;

#[test]
fn test_encrypt_decrypt_round_trip() {
    let key = generate_aes256().unwrap();
    let plaintext = b"test message";
    let aad = b"additional data";

    // Encrypt with nonce not prepended
    let (ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, false).unwrap();

    // Decrypt with separate nonce
    let decrypted = decrypt_aes(Some(&nonce), &ciphertext, &key, aad).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_encrypt_decrypt_with_prepended_nonce() {
    let key = generate_aes256().unwrap();
    let plaintext = b"test message";
    let aad = b"context";

    // Encrypt with nonce prepended
    let (ciphertext, _nonce) = encrypt_aes(plaintext, &key, aad, true).unwrap();

    // Decrypt with nonce from ciphertext
    let decrypted = decrypt_aes(None, &ciphertext, &key, aad).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_encrypt_decrypt_empty_aad() {
    let key = generate_aes256().unwrap();
    let plaintext = b"test message";

    let (ciphertext, nonce) = encrypt_aes(plaintext, &key, &[], false).unwrap();
    let decrypted = decrypt_aes(Some(&nonce), &ciphertext, &key, &[]).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_decrypt_with_wrong_key_fails() {
    let key1 = generate_aes256().unwrap();
    let key2 = generate_aes256().unwrap();
    let plaintext = b"test message";

    let (ciphertext, nonce) = encrypt_aes(plaintext, &key1, &[], false).unwrap();
    let result = decrypt_aes(Some(&nonce), &ciphertext, &key2, &[]);
    assert!(result.is_err());
}

#[test]
fn test_decrypt_with_wrong_aad_fails() {
    let key = generate_aes256().unwrap();
    let plaintext = b"test message";
    let aad1 = b"correct aad";
    let aad2 = b"wrong aad";

    let (ciphertext, nonce) = encrypt_aes(plaintext, &key, aad1, false).unwrap();
    let result = decrypt_aes(Some(&nonce), &ciphertext, &key, aad2);
    assert!(result.is_err());
}

#[test]
fn test_decrypt_with_tampered_ciphertext_fails() {
    let key = generate_aes256().unwrap();
    let plaintext = b"test message";

    let (mut ciphertext, nonce) = encrypt_aes(plaintext, &key, &[], false).unwrap();

    // Tamper with ciphertext
    if !ciphertext.is_empty() {
        ciphertext[0] ^= 0xFF;
    }

    let result = decrypt_aes(Some(&nonce), &ciphertext, &key, &[]);
    assert!(result.is_err());
}

#[test]
fn test_encrypt_with_invalid_key_size() {
    let key = vec![0u8; 16]; // Wrong size (should be 32)
    let plaintext = b"test";

    let result = encrypt_aes(plaintext, &key, &[], false);
    assert!(result.is_err());
}

#[test]
fn test_decrypt_with_invalid_key_size() {
    let key = vec![0u8; 16]; // Wrong size
    let ciphertext = vec![0u8; 32];

    let result = decrypt_aes(None, &ciphertext, &key, &[]);
    assert!(result.is_err());
}

#[test]
fn test_decrypt_ciphertext_too_short() {
    let key = generate_aes256().unwrap();
    let ciphertext = vec![0u8; 5]; // Too short to contain nonce

    let result = decrypt_aes(None, &ciphertext, &key, &[]);
    assert!(result.is_err());
}

#[test]
fn test_nonce_prepending() {
    let key = generate_aes256().unwrap();
    let plaintext = b"test";

    let (ciphertext_with_nonce, nonce) = encrypt_aes(plaintext, &key, &[], true).unwrap();

    // Nonce should be prepended (first 12 bytes)
    assert_eq!(&ciphertext_with_nonce[..12], &nonce[..]);

    // Should be able to decrypt
    let decrypted = decrypt_aes(None, &ciphertext_with_nonce, &key, &[]).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_encrypt_decrypt_empty_plaintext() {
    let key = generate_aes256().unwrap();
    let plaintext = b"";
    let aad = b"metadata";

    // AES-GCM should handle empty plaintext (authentication only)
    let (ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, false).unwrap();

    // Ciphertext should not be empty (contains authentication tag)
    assert!(
        !ciphertext.is_empty(),
        "Ciphertext should contain auth tag even for empty plaintext"
    );

    let decrypted = decrypt_aes(Some(&nonce), &ciphertext, &key, aad).unwrap();
    assert_eq!(
        plaintext,
        &decrypted[..],
        "Should decrypt empty plaintext correctly"
    );
}

#[test]
fn test_decrypt_empty_ciphertext_fails() {
    let key = generate_aes256().unwrap();
    let empty_ciphertext = b"";

    // Decrypting completely empty ciphertext should fail
    let result = decrypt_aes(None, empty_ciphertext, &key, &[]);
    assert!(result.is_err(), "Empty ciphertext should fail to decrypt");
}

#[test]
fn test_decrypt_only_nonce_fails() {
    let key = generate_aes256().unwrap();
    let only_nonce = vec![0u8; AES_NONCE_SIZE];

    // Ciphertext containing only nonce (no encrypted data) should fail
    let result = decrypt_aes(None, &only_nonce, &key, &[]);
    assert!(result.is_err(), "Nonce without ciphertext should fail");
}

#[test]
fn test_decrypt_invalid_nonce_size() {
    let key = generate_aes256().unwrap();
    let ciphertext = vec![0u8; 20];
    let wrong_size_nonce = vec![0u8; 16]; // Should be 12 bytes

    let result = decrypt_aes(Some(&wrong_size_nonce), &ciphertext, &key, &[]);
    assert!(result.is_err(), "Wrong nonce size should fail");
}

#[test]
fn test_aes_tampered_auth_tag_fails() {
    let key = generate_aes256().unwrap();
    let plaintext = b"sensitive message";
    let aad = b"context";

    let (mut ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, false).unwrap();

    // AES-GCM auth tag is the last 16 bytes of ciphertext
    let ct_len = ciphertext.len();
    assert!(ct_len >= 16, "Ciphertext should contain auth tag");

    // Tamper with the last byte of auth tag
    ciphertext[ct_len - 1] ^= 0x01;

    let result = decrypt_aes(Some(&nonce), &ciphertext, &key, aad);
    assert!(
        result.is_err(),
        "Tampered auth tag should cause decryption failure"
    );
}

#[test]
fn test_aes_tampered_ciphertext_body_fails() {
    let key = generate_aes256().unwrap();
    let plaintext = b"sensitive message";
    let aad = b"context";

    let (mut ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, false).unwrap();

    // Tamper with the first byte of actual ciphertext (not the auth tag)
    assert!(
        ciphertext.len() > 16,
        "Should have ciphertext before auth tag"
    );
    ciphertext[0] ^= 0x01;

    let result = decrypt_aes(Some(&nonce), &ciphertext, &key, aad);
    assert!(
        result.is_err(),
        "Tampered ciphertext body should cause auth failure"
    );
}

#[test]
fn test_encrypt_decrypt_large_plaintext_1mb() {
    // Test with 1MB plaintext to verify large data handling
    let key = generate_aes256().unwrap();
    let large_plaintext = vec![0xABu8; 1024 * 1024]; // 1MB
    let aad = b"context for large data";

    let (ciphertext, nonce) = encrypt_aes(&large_plaintext, &key, aad, false).unwrap();

    // Ciphertext should be plaintext + 16 bytes auth tag
    assert_eq!(
        ciphertext.len(),
        large_plaintext.len() + 16,
        "Ciphertext should be plaintext length + 16 byte auth tag"
    );

    let decrypted = decrypt_aes(Some(&nonce), &ciphertext, &key, aad).unwrap();
    assert_eq!(
        large_plaintext.len(),
        decrypted.len(),
        "Decrypted length should match original"
    );
    assert_eq!(
        large_plaintext, decrypted,
        "Decrypted data should match original"
    );
}

#[test]
fn test_encrypt_decrypt_large_plaintext_10mb() {
    // Test with 10MB plaintext for memory/performance verification
    let key = generate_aes256().unwrap();
    let large_plaintext = vec![0xCDu8; 10 * 1024 * 1024]; // 10MB
    let aad = b"large data context";

    let (ciphertext, _nonce) = encrypt_aes(&large_plaintext, &key, aad, true).unwrap();

    // With prepended nonce: nonce (12) + ciphertext (10MB + 16 auth tag)
    assert_eq!(
        ciphertext.len(),
        12 + large_plaintext.len() + 16,
        "Ciphertext with nonce should be 12 + plaintext + 16"
    );

    let decrypted = decrypt_aes(None, &ciphertext, &key, aad).unwrap();
    assert_eq!(
        large_plaintext, decrypted,
        "Large plaintext should decrypt correctly"
    );
}

#[test]
fn test_encrypt_decrypt_varied_byte_patterns() {
    // Test with varied byte patterns to ensure no pattern-dependent bugs
    let key = generate_aes256().unwrap();
    let aad = b"varied pattern test";

    // Pattern 1: Sequential bytes (0, 1, 2, ..., 255, 0, 1, ...)
    let sequential: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    let (ct1, _) = encrypt_aes(&sequential, &key, aad, true).unwrap();
    let dec1 = decrypt_aes(None, &ct1, &key, aad).unwrap();
    assert_eq!(sequential, dec1, "Sequential pattern should round-trip");

    // Pattern 2: Alternating high/low bytes
    let alternating: Vec<u8> = (0..100_000)
        .map(|i| if i % 2 == 0 { 0x00 } else { 0xFF })
        .collect();
    let (ct2, _) = encrypt_aes(&alternating, &key, aad, true).unwrap();
    let dec2 = decrypt_aes(None, &ct2, &key, aad).unwrap();
    assert_eq!(alternating, dec2, "Alternating pattern should round-trip");

    // Pattern 3: Pseudo-random from seed (deterministic)
    let mut prng_data: Vec<u8> = Vec::with_capacity(100_000);
    let mut seed: u32 = 0xDEADBEEF;
    for _ in 0..100_000 {
        seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
        prng_data.push((seed >> 16) as u8);
    }
    let (ct3, _) = encrypt_aes(&prng_data, &key, aad, true).unwrap();
    let dec3 = decrypt_aes(None, &ct3, &key, aad).unwrap();
    assert_eq!(prng_data, dec3, "Pseudo-random pattern should round-trip");

    // Pattern 4: Repeated blocks (compression-like pattern)
    let block = b"ABCDEFGH12345678"; // 16-byte AES block size
    let repeated: Vec<u8> = block.iter().cycle().take(100_000).cloned().collect();
    let (ct4, _) = encrypt_aes(&repeated, &key, aad, true).unwrap();
    let dec4 = decrypt_aes(None, &ct4, &key, aad).unwrap();
    assert_eq!(repeated, dec4, "Repeated block pattern should round-trip");
}

#[test]
fn test_aes_gcm_auth_tag_size() {
    // Verify AES-GCM produces expected ciphertext size with 16-byte auth tag
    let key = generate_aes256().unwrap();

    let test_sizes = [0, 1, 15, 16, 17, 100, 1000];

    for size in test_sizes {
        let plaintext = vec![0x42u8; size];
        let (ciphertext, _nonce) = encrypt_aes(&plaintext, &key, &[], false).unwrap();

        assert_eq!(
            ciphertext.len(),
            size + 16,
            "Ciphertext for {} byte plaintext should be {} bytes (+ 16 byte auth tag)",
            size,
            size + 16
        );
    }
}

#[test]
fn test_nonce_uniqueness_per_encryption() {
    // Verify that random nonces are unique per encryption call
    let key = generate_aes256().unwrap();
    let plaintext = b"same message for all encryptions";
    let aad = b"context";

    // Collect nonces
    let mut nonces: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for i in 0..100 {
        let (_ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, false).unwrap();
        assert!(
            nonces.insert(nonce),
            "Nonce collision detected at iteration {} - PRNG may be weak",
            i
        );
    }

    assert_eq!(nonces.len(), 100, "Should have 100 unique nonces");
}
