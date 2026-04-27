//! Integration tests for ECIES encryption/decryption

use crypto::encryption::ecies::{decrypt_ecies, encrypt_ecies, EciesOptions};
use crypto::keys::generation::generate_x25519;
use crypto::types::{AES_KEY_SIZE, HMAC_KEY_SIZE, HMAC_SIZE, X25519_PUBLIC_KEY_SIZE};
use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

#[test]
fn test_ecies_encrypt_decrypt_default() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"test message";

    // Default options: prepend public key
    let options_enc = EciesOptions::builder().prepend_public_key(true).build();

    let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    let options_dec = EciesOptions::builder().prepend_public_key(true).build();

    let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_ecies_with_aad() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"test message";
    let aad = b"context data";

    let options_enc = EciesOptions::builder()
        .with_aad(aad.to_vec())
        .prepend_public_key(true)
        .build();

    let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    let options_dec = EciesOptions::builder()
        .with_aad(aad.to_vec())
        .prepend_public_key(true)
        .build();

    let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_ecies_wrong_aad_fails() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"test message";

    let options_enc = EciesOptions::builder()
        .with_aad(b"correct".to_vec())
        .prepend_public_key(true)
        .build();

    let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    let options_dec = EciesOptions::builder()
        .with_aad(b"wrong".to_vec())
        .prepend_public_key(true)
        .build();

    let result = decrypt_ecies(&ciphertext, &private_key, options_dec);
    assert!(result.is_err());
}

#[test]
fn test_ecies_tampered_ciphertext_fails() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"test message";

    let options_enc = EciesOptions::builder().prepend_public_key(true).build();

    let mut ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    // Tamper with ciphertext (but not HMAC)
    if ciphertext.len() > 50 {
        ciphertext[40] ^= 0xFF;
    }

    let options_dec = EciesOptions::builder().prepend_public_key(true).build();

    let result = decrypt_ecies(&ciphertext, &private_key, options_dec);
    assert!(result.is_err());
}

#[test]
fn test_ecies_custom_private_key() {
    let recipient_key = generate_x25519().unwrap();
    let recipient_pub = PublicKey::from(&recipient_key);

    let sender_key = generate_x25519().unwrap();
    let sender_pub = PublicKey::from(&sender_key);
    let plaintext = b"test message";

    // Encrypt with custom sender key, don't prepend public key
    let options_enc = EciesOptions::builder()
        .with_private_key(sender_key)
        .prepend_public_key(false)
        .build();

    let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

    // Should be shorter without 32-byte public key prepended
    assert!(ciphertext.len() < 100);

    // Now test decryption with separate public key
    let options_dec = EciesOptions::builder()
        .with_public_key_bytes(sender_pub.as_bytes().to_vec())
        .build();

    let decrypted = decrypt_ecies(&ciphertext, &recipient_key, options_dec).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_ecies_tampered_hmac_fails() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"test message";

    let options_enc = EciesOptions::builder().prepend_public_key(true).build();

    let mut ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    // Tamper with HMAC (last 32 bytes)
    let len = ciphertext.len();
    if len > 32 {
        ciphertext[len - 10] ^= 0xFF; // Flip a bit in the HMAC
    }

    let options_dec = EciesOptions::builder().prepend_public_key(true).build();

    let result = decrypt_ecies(&ciphertext, &private_key, options_dec);
    assert!(
        result.is_err(),
        "Tampered HMAC should cause decryption to fail"
    );
}

#[test]
fn test_ecies_wrong_separate_public_key_fails() {
    let recipient_key = generate_x25519().unwrap();
    let recipient_pub = PublicKey::from(&recipient_key);

    let sender_key = generate_x25519().unwrap();
    let wrong_key = generate_x25519().unwrap();
    let wrong_pub = PublicKey::from(&wrong_key);

    let plaintext = b"test message";

    // Encrypt without prepending public key
    let options_enc = EciesOptions::builder()
        .with_private_key(sender_key)
        .prepend_public_key(false)
        .build();

    let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

    // Try to decrypt with wrong public key
    let options_dec = EciesOptions::builder()
        .with_public_key_bytes(wrong_pub.as_bytes().to_vec())
        .build();

    let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);
    assert!(
        result.is_err(),
        "Wrong public key should cause decryption to fail"
    );
}

#[test]
fn test_ecies_output_format() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"test";

    let options = EciesOptions::builder().prepend_public_key(true).build();

    let ciphertext = encrypt_ecies(plaintext, &public_key, options).unwrap();

    // Format: [ephemeral_pub (32) | nonce (12) | encrypted (varies) | HMAC (32)]
    assert!(ciphertext.len() >= X25519_PUBLIC_KEY_SIZE + 12 + HMAC_SIZE);
}

#[test]
fn test_ecies_empty_plaintext() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"";

    let options_enc = EciesOptions::builder().prepend_public_key(true).build();

    let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    let options_dec = EciesOptions::builder().prepend_public_key(true).build();

    let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
    assert_eq!(plaintext, &decrypted[..]);
}

#[test]
fn test_ecies_ciphertext_only_public_key_no_data() {
    let private_key = generate_x25519().unwrap();

    // Ciphertext with only ephemeral public key (32 bytes), no encrypted data, no HMAC
    let malformed_ct = vec![0u8; X25519_PUBLIC_KEY_SIZE];

    let options = EciesOptions::builder().prepend_public_key(true).build();
    let result = decrypt_ecies(&malformed_ct, &private_key, options);

    assert!(
        result.is_err(),
        "Should fail: ciphertext too short (only ephemeral key)"
    );
}

#[test]
fn test_ecies_rejects_payload_below_go_min_ciphertext_size() {
    let private_key = generate_x25519().unwrap();
    let mut malformed_ct = vec![0u8; X25519_PUBLIC_KEY_SIZE];
    malformed_ct.push(1);
    malformed_ct.extend_from_slice(&[0u8; HMAC_SIZE]);

    let options = EciesOptions::builder().prepend_public_key(true).build();
    let error = decrypt_ecies(&malformed_ct, &private_key, options)
        .expect_err("payload smaller than Go minCipherTextSize must fail");

    assert!(
        error.to_string().contains("encrypted payload is 1 bytes"),
        "unexpected error: {error}"
    );
}

#[test]
fn test_ecies_ciphertext_no_hmac() {
    let private_key = generate_x25519().unwrap();

    // Ciphertext with ephemeral key + some data but no HMAC
    let malformed_ct = vec![0u8; X25519_PUBLIC_KEY_SIZE + 16]; // Missing HMAC_SIZE bytes

    let options = EciesOptions::builder().prepend_public_key(true).build();
    let result = decrypt_ecies(&malformed_ct, &private_key, options);

    assert!(
        result.is_err(),
        "Should fail: ciphertext too short (missing HMAC)"
    );
}

#[test]
fn test_ecies_invalid_separate_public_key_sizes() {
    let recipient_key = generate_x25519().unwrap();
    let recipient_pub = PublicKey::from(&recipient_key);
    let sender_key = generate_x25519().unwrap();
    let plaintext = b"test";

    // Encrypt without prepending key
    let options_enc = EciesOptions::builder()
        .with_private_key(sender_key)
        .prepend_public_key(false)
        .build();
    let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

    // Try to decrypt with wrong-sized public key (31 bytes)
    let wrong_size_key = vec![0u8; 31];
    let options_dec = EciesOptions::builder()
        .with_public_key_bytes(wrong_size_key)
        .build();
    let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);
    assert!(result.is_err(), "31-byte ephemeral key should be rejected");

    // Try with 33 bytes
    let wrong_size_key = vec![0u8; 33];
    let options_dec = EciesOptions::builder()
        .with_public_key_bytes(wrong_size_key)
        .build();
    let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);
    assert!(result.is_err(), "33-byte ephemeral key should be rejected");
}

#[test]
fn test_ecies_wrong_recipient_key_fails() {
    let correct_key = generate_x25519().unwrap();
    let correct_pub = PublicKey::from(&correct_key);
    let wrong_key = generate_x25519().unwrap();
    let plaintext = b"sensitive data";

    let options_enc = EciesOptions::builder().prepend_public_key(true).build();
    let ciphertext = encrypt_ecies(plaintext, &correct_pub, options_enc).unwrap();

    // Try to decrypt with wrong recipient key
    let options_dec = EciesOptions::builder().prepend_public_key(true).build();
    let result = decrypt_ecies(&ciphertext, &wrong_key, options_dec);

    assert!(
        result.is_err(),
        "Wrong recipient key should cause HMAC verification failure"
    );
}

#[test]
fn test_encrypt_with_weak_public_key() {
    let plaintext = b"test data";
    let zero_pub = PublicKey::from([0u8; 32]);

    let options_enc = EciesOptions::builder().prepend_public_key(true).build();
    let ciphertext = encrypt_ecies(plaintext, &zero_pub, options_enc).unwrap();

    // Decryption with a random private key should fail (HMAC mismatch)
    let random_private = generate_x25519().unwrap();
    let options_dec = EciesOptions::builder().prepend_public_key(true).build();
    let result = decrypt_ecies(&ciphertext, &random_private, options_dec);

    assert!(
        result.is_err(),
        "Decryption with wrong key should fail HMAC verification"
    );
}

#[test]
fn test_encrypt_no_prepend_without_ephemeral_private_key() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"test data";

    // Encrypt without prepending public key
    let options_enc = EciesOptions::builder().prepend_public_key(false).build();
    let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    // Try to decrypt with prepend_public_key=true (expecting prepended key but there isn't one)
    let options_dec = EciesOptions::builder().prepend_public_key(true).build();
    let result = decrypt_ecies(&ciphertext, &private_key, options_dec);

    assert!(
        result.is_err(),
        "Decryption should fail when expecting prepended key but none exists"
    );
}

#[test]
fn test_decrypt_with_invalid_private_key() {
    let correct_key = generate_x25519().unwrap();
    let correct_pub = PublicKey::from(&correct_key);
    let plaintext = b"test data";

    // Encrypt with correct key
    let options_enc = EciesOptions::builder().prepend_public_key(true).build();
    let ciphertext = encrypt_ecies(plaintext, &correct_pub, options_enc).unwrap();

    // Create a weak private key (all zeros)
    let weak_key = StaticSecret::from([0u8; 32]);

    // Try to decrypt with weak private key
    let options_dec = EciesOptions::builder().prepend_public_key(true).build();
    let result = decrypt_ecies(&ciphertext, &weak_key, options_dec);

    assert!(
        result.is_err(),
        "Decryption with weak/wrong private key should fail"
    );
}

#[test]
fn test_verify_ephemeral_public_key_in_ciphertext() {
    let sender_private = generate_x25519().unwrap();
    let sender_public = PublicKey::from(&sender_private);

    let recipient_private = generate_x25519().unwrap();
    let recipient_public = PublicKey::from(&recipient_private);

    let plaintext = b"test data";

    // Encrypt with custom sender private key and prepend public key
    let options_enc = EciesOptions::builder()
        .with_private_key(sender_private)
        .prepend_public_key(true)
        .build();
    let ciphertext = encrypt_ecies(plaintext, &recipient_public, options_enc).unwrap();

    // Verify the first 32 bytes are the sender's ephemeral public key
    assert!(
        ciphertext.len() >= X25519_PUBLIC_KEY_SIZE,
        "Ciphertext should contain ephemeral public key"
    );
    let prepended_key = &ciphertext[..X25519_PUBLIC_KEY_SIZE];
    assert_eq!(
        prepended_key,
        sender_public.as_bytes(),
        "Prepended public key should match sender's public key"
    );

    // Verify decryption works
    let options_dec = EciesOptions::builder().prepend_public_key(true).build();
    let decrypted = decrypt_ecies(&ciphertext, &recipient_private, options_dec).unwrap();
    assert_eq!(decrypted, plaintext, "Decrypted data should match original");
}

#[test]
fn test_ecies_encryption_with_empty_plaintext() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let plaintext = b"";

    let options_enc = EciesOptions::builder().prepend_public_key(true).build();
    let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

    // Ciphertext should still contain ephemeral key + HMAC even for empty plaintext
    assert!(
        ciphertext.len() >= X25519_PUBLIC_KEY_SIZE + HMAC_SIZE,
        "Empty plaintext should still produce ciphertext with ephemeral key and HMAC"
    );

    let options_dec = EciesOptions::builder().prepend_public_key(true).build();
    let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
    assert_eq!(
        decrypted, plaintext,
        "Should decrypt empty plaintext correctly"
    );
}

#[test]
fn test_ecies_aad_mismatch_with_separate_key() {
    let sender_key = generate_x25519().unwrap();
    let sender_pub = PublicKey::from(&sender_key);
    let recipient_key = generate_x25519().unwrap();
    let recipient_pub = PublicKey::from(&recipient_key);
    let plaintext = b"test";

    let aad_enc = b"context1".to_vec();
    let aad_dec = b"context2".to_vec();

    // Encrypt with AAD and custom sender key (prepend=false so key goes separately)
    let options_enc = EciesOptions::builder()
        .with_private_key(sender_key)
        .with_aad(aad_enc)
        .prepend_public_key(false)
        .build();
    let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

    // Decrypt with different AAD and separate ephemeral public key
    let options_dec = EciesOptions::builder()
        .with_public_key_bytes(sender_pub.as_bytes().to_vec())
        .with_aad(aad_dec)
        .prepend_public_key(false)
        .build();
    let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);

    assert!(
        result.is_err(),
        "AAD mismatch should cause decryption failure"
    );
}

#[test]
fn test_ecies_large_plaintext_1mb() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let large_plaintext = vec![0xABu8; 1024 * 1024]; // 1MB

    let options_enc = EciesOptions::builder().prepend_public_key(true).build();

    let ciphertext = encrypt_ecies(&large_plaintext, &public_key, options_enc).unwrap();

    // Expected size: ephemeral_pub(32) + nonce(12) + ciphertext(1MB + 16 auth tag) + HMAC(32)
    let expected_min_size = X25519_PUBLIC_KEY_SIZE + 12 + large_plaintext.len() + 16 + HMAC_SIZE;
    assert_eq!(
        ciphertext.len(),
        expected_min_size,
        "ECIES ciphertext size should be ephemeral_pub + nonce + plaintext + auth_tag + HMAC"
    );

    let options_dec = EciesOptions::builder().prepend_public_key(true).build();

    let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
    assert_eq!(
        large_plaintext.len(),
        decrypted.len(),
        "Decrypted length should match"
    );
    assert_eq!(
        large_plaintext, decrypted,
        "1MB plaintext should decrypt correctly"
    );
}

#[test]
fn test_ecies_large_plaintext_5mb_with_aad() {
    let private_key = generate_x25519().unwrap();
    let public_key = PublicKey::from(&private_key);
    let large_plaintext = vec![0xCDu8; 5 * 1024 * 1024]; // 5MB
    let aad = b"large document encryption context".to_vec();

    let options_enc = EciesOptions::builder()
        .with_aad(aad.clone())
        .prepend_public_key(true)
        .build();

    let ciphertext = encrypt_ecies(&large_plaintext, &public_key, options_enc).unwrap();

    let options_dec = EciesOptions::builder()
        .with_aad(aad)
        .prepend_public_key(true)
        .build();

    let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
    assert_eq!(
        large_plaintext, decrypted,
        "5MB plaintext with AAD should decrypt correctly"
    );
}

#[test]
fn test_ecies_large_plaintext_without_prepend() {
    let sender_key = generate_x25519().unwrap();
    let sender_pub = PublicKey::from(&sender_key);
    let recipient_key = generate_x25519().unwrap();
    let recipient_pub = PublicKey::from(&recipient_key);
    let large_plaintext = vec![0xEFu8; 2 * 1024 * 1024]; // 2MB

    let options_enc = EciesOptions::builder()
        .with_private_key(sender_key)
        .prepend_public_key(false)
        .build();

    let ciphertext = encrypt_ecies(&large_plaintext, &recipient_pub, options_enc).unwrap();

    // Without prepended key: nonce(12) + ciphertext(2MB + 16 auth tag) + HMAC(32)
    let expected_size = 12 + large_plaintext.len() + 16 + HMAC_SIZE;
    assert_eq!(
        ciphertext.len(),
        expected_size,
        "ECIES without prepend should be nonce + plaintext + auth_tag + HMAC"
    );

    let options_dec = EciesOptions::builder()
        .with_public_key_bytes(sender_pub.as_bytes().to_vec())
        .build();

    let decrypted = decrypt_ecies(&ciphertext, &recipient_key, options_dec).unwrap();
    assert_eq!(
        large_plaintext, decrypted,
        "2MB plaintext should decrypt correctly with separate key"
    );
}

#[test]
fn test_ecies_hkdf_key_derivation() {
    // Test HKDF key derivation with known inputs to verify Go compatibility
    let alice_private_bytes: [u8; 32] = [
        0x77, 0x07, 0x6d, 0x0a, 0x73, 0x18, 0xa5, 0x7d, 0x3c, 0x16, 0xc1, 0x72, 0x51, 0xb2, 0x66,
        0x45, 0xdf, 0x4c, 0x2f, 0x87, 0xeb, 0xc0, 0x99, 0x2a, 0xb1, 0x77, 0xfb, 0xa5, 0x1d, 0xb9,
        0x2c, 0x2a,
    ];
    let bob_private_bytes: [u8; 32] = [
        0x5d, 0xab, 0x08, 0x7e, 0x62, 0x4a, 0x8a, 0x4b, 0x79, 0xe1, 0x7f, 0x8b, 0x83, 0x80, 0x0e,
        0xe6, 0x6f, 0x3b, 0xb1, 0x29, 0x26, 0x18, 0xb6, 0xfd, 0x1c, 0x2f, 0x8b, 0x27, 0xff, 0x88,
        0xe0, 0xeb,
    ];

    let alice_private = StaticSecret::from(alice_private_bytes);
    let bob_private = StaticSecret::from(bob_private_bytes);
    let bob_public = PublicKey::from(&bob_private);

    // Compute shared secret (Alice encrypting to Bob)
    let shared_secret = alice_private.diffie_hellman(&bob_public);

    // Derive keys using HKDF-SHA256 with empty salt and info (matches Go)
    let hkdf = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());
    let mut keys = [0u8; AES_KEY_SIZE + HMAC_KEY_SIZE];
    hkdf.expand(&[], &mut keys).unwrap();

    let aes_key = &keys[..AES_KEY_SIZE];
    let hmac_key = &keys[AES_KEY_SIZE..];

    // Verify derived keys are deterministic
    assert_eq!(aes_key.len(), AES_KEY_SIZE, "AES key should be 32 bytes");
    assert_eq!(hmac_key.len(), HMAC_KEY_SIZE, "HMAC key should be 32 bytes");
    assert_ne!(aes_key, hmac_key, "AES and HMAC keys should be different");

    // Verify keys are non-trivial
    assert!(
        !aes_key.iter().all(|&b| b == 0),
        "AES key should not be all zeros"
    );
    assert!(
        !hmac_key.iter().all(|&b| b == 0),
        "HMAC key should not be all zeros"
    );

    // Re-derive to ensure determinism
    let hkdf2 = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());
    let mut keys2 = [0u8; AES_KEY_SIZE + HMAC_KEY_SIZE];
    hkdf2.expand(&[], &mut keys2).unwrap();

    assert_eq!(keys, keys2, "HKDF derivation should be deterministic");

    // Test full ECIES flow with known keys
    let plaintext = b"test message for key derivation";
    let options_enc = EciesOptions::builder()
        .with_private_key(alice_private_bytes.into())
        .prepend_public_key(true)
        .build();

    let ciphertext1 = encrypt_ecies(plaintext, &bob_public, options_enc).unwrap();

    // Decrypt should succeed with Bob's private key
    let options_dec = EciesOptions::builder().prepend_public_key(true).build();
    let decrypted = decrypt_ecies(&ciphertext1, &bob_private, options_dec).unwrap();
    assert_eq!(
        plaintext.to_vec(),
        decrypted,
        "Decryption should recover original plaintext"
    );
}
