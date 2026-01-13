//! Cross-implementation test vectors for Go compatibility
//!
//! These test vectors were generated from the Go DefraDB implementation
//! to ensure the Rust crypto implementation produces identical outputs.
//!
//! Run the Go test to regenerate vectors:
//!   cd defradb && go test -v -run TestGenerateVectors ./crypto/

#[cfg(test)]
mod tests {
    use crate::encryption::aes::{decrypt_aes, encrypt_aes};
    use crate::encryption::ecies::{decrypt_ecies, encrypt_ecies, EciesOptions};
    use crate::encryption::nonce::USE_DETERMINISTIC_NONCE;
    use crate::keys::ed25519::{Ed25519PrivateKey, Ed25519PublicKey};
    use crate::keys::secp256k1::{Secp256k1PrivateKey, Secp256k1PublicKey};
    use crate::keys::{PrivateKey, PublicKey};
    use hkdf::Hkdf;
    use sha2::Sha256;
    use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

    // ===== Ed25519 Test Vectors =====
    const ED25519_SEED: [u8; 32] = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c,
        0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae,
        0x7f, 0x60,
    ];
    const ED25519_PRIVATE_KEY: [u8; 64] = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c,
        0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae,
        0x7f, 0x60, 0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9,
        0x64, 0x07, 0x3a, 0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68,
        0xf7, 0x07, 0x51, 0x1a,
    ];
    const ED25519_PUBLIC_KEY: [u8; 32] = [
        0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07,
        0x3a, 0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07,
        0x51, 0x1a,
    ];
    const ED25519_TEST_MESSAGE: &[u8] = b"test message";
    const ED25519_SIGNATURE: [u8; 64] = [
        0x98, 0xa3, 0x9e, 0xc1, 0x1a, 0x0d, 0xfb, 0xbf, 0xdb, 0xd7, 0xa7, 0xe2, 0x39, 0x4b, 0x2b,
        0x83, 0xa1, 0x65, 0x86, 0xe9, 0x21, 0x00, 0xbc, 0xb9, 0xbe, 0x67, 0x2d, 0xdf, 0xba, 0x3e,
        0x7a, 0xcb, 0x86, 0x1c, 0x94, 0xd6, 0xad, 0x4c, 0xf6, 0xe3, 0xe6, 0x01, 0x36, 0xca, 0x14,
        0x1f, 0xc4, 0xf2, 0xf1, 0xbe, 0x0c, 0x1b, 0x8e, 0xf0, 0xbe, 0xa1, 0x2a, 0xee, 0x76, 0xf0,
        0x07, 0xa4, 0xc3, 0x0a,
    ];
    const ED25519_DID: &str = "did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw";

    // ===== secp256k1 Test Vectors =====
    const SECP256K1_PRIVATE_KEY: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];
    #[allow(dead_code)]
    const SECP256K1_PUBLIC_KEY_COMPRESSED: [u8; 33] = [
        0x02, 0x84, 0xbf, 0x75, 0x62, 0x26, 0x2b, 0xbd, 0x69, 0x40, 0x08, 0x57, 0x48, 0xf3, 0xbe,
        0x6a, 0xfa, 0x52, 0xae, 0x31, 0x71, 0x55, 0x18, 0x1e, 0xce, 0x31, 0xb6, 0x63, 0x51, 0xcc,
        0xff, 0xa4, 0xb0,
    ];
    #[allow(dead_code)]
    const SECP256K1_PUBLIC_KEY_UNCOMPRESSED: [u8; 65] = [
        0x04, 0x84, 0xbf, 0x75, 0x62, 0x26, 0x2b, 0xbd, 0x69, 0x40, 0x08, 0x57, 0x48, 0xf3, 0xbe,
        0x6a, 0xfa, 0x52, 0xae, 0x31, 0x71, 0x55, 0x18, 0x1e, 0xce, 0x31, 0xb6, 0x63, 0x51, 0xcc,
        0xff, 0xa4, 0xb0, 0x8c, 0xc4, 0x3d, 0x63, 0xb2, 0x85, 0x9d, 0x46, 0x9f, 0xee, 0x15, 0xf3,
        0x1c, 0x9e, 0xdb, 0x53, 0x24, 0x26, 0x6e, 0x6f, 0xd0, 0x40, 0x7e, 0x87, 0x38, 0x2d, 0x60,
        0xfc, 0x45, 0x11, 0xac, 0xd8,
    ];
    const SECP256K1_TEST_MESSAGE: &[u8] = b"test message";
    const SECP256K1_DID: &str =
        "did:key:z7r8or8ecagY9LD87s54K2arcXmgmw6bUhyvq83RrnB2hJiUb2ug5YGAk1ZUaimewnoLL1ZGzXuTCnWRSrRZgR3v2PLPH";

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

    // ===== Ed25519 Compatibility Tests =====

    #[test]
    fn test_ed25519_private_key_from_go_bytes() {
        let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY)
            .expect("Should parse Go Ed25519 private key");

        // Verify public key derivation matches Go
        let public_key = private_key.public_key();
        assert_eq!(
            public_key.raw(),
            ED25519_PUBLIC_KEY.to_vec(),
            "Derived public key should match Go"
        );
    }

    #[test]
    fn test_ed25519_signature_matches_go() {
        let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();

        // Sign the same message
        let signature = private_key.sign(ED25519_TEST_MESSAGE).unwrap();

        // Ed25519 is deterministic - same key + message = same signature
        assert_eq!(
            signature, ED25519_SIGNATURE,
            "Ed25519 signature should match Go"
        );
    }

    #[test]
    fn test_ed25519_signature_verification_from_go() {
        let public_key =
            Ed25519PublicKey::from_bytes(&ED25519_PUBLIC_KEY).expect("Should parse Go public key");

        // Verify Go-generated signature
        let valid = public_key
            .verify(ED25519_TEST_MESSAGE, &ED25519_SIGNATURE)
            .unwrap();
        assert!(valid, "Should verify Go-generated Ed25519 signature");
    }

    #[test]
    fn test_ed25519_did_matches_go() {
        let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();
        let public_key = private_key.public_key();

        let did = public_key.did().unwrap();
        assert_eq!(did, ED25519_DID, "Ed25519 DID should match Go");
    }

    // ===== secp256k1 Compatibility Tests =====

    #[test]
    fn test_secp256k1_private_key_from_go_bytes() {
        let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY)
            .expect("Should parse Go secp256k1 private key");

        // Verify public key derivation (compressed format)
        let public_key = private_key.public_key();
        assert_eq!(
            public_key.raw(),
            SECP256K1_PUBLIC_KEY_COMPRESSED.to_vec(),
            "Derived compressed public key should match Go"
        );
    }

    #[test]
    fn test_secp256k1_signature_verification_from_go() {
        // Both Go (dcrd/secp256k1) and Rust (k256) use RFC 6979 deterministic ECDSA,
        // so the same key + message produces identical signatures.
        let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY)
            .expect("Should parse Go private key");

        let public_key = Secp256k1PublicKey::from_bytes(&SECP256K1_PUBLIC_KEY_COMPRESSED)
            .expect("Should parse Go public key");

        // Known DER signature from Go (deterministic via RFC 6979)
        let go_signature: &[u8] = &[
            0x30, 0x44, 0x02, 0x20, 0x3d, 0x46, 0x09, 0xf4, 0xd7, 0x62, 0x05, 0xd3, 0x49, 0x16,
            0x0f, 0xf7, 0x90, 0x4c, 0xf9, 0x14, 0x38, 0xe0, 0xbb, 0x5f, 0x9b, 0x98, 0x42, 0xc2,
            0x8b, 0x4e, 0x9d, 0xe7, 0x6b, 0x28, 0x36, 0xf8, 0x02, 0x20, 0x2e, 0xe2, 0x7f, 0x4e,
            0x70, 0x62, 0x1e, 0x98, 0x55, 0xd7, 0x92, 0x68, 0xaf, 0x70, 0x95, 0x46, 0x18, 0x05,
            0x34, 0x19, 0x99, 0x0a, 0x6c, 0x09, 0xcf, 0x71, 0x52, 0xc5, 0x30, 0x15, 0x6a, 0xf0,
        ];

        // Verify Rust produces the same signature (RFC 6979 deterministic)
        let rust_signature = private_key.sign(SECP256K1_TEST_MESSAGE).unwrap();
        assert_eq!(
            rust_signature, go_signature,
            "Rust secp256k1 signature should match Go (both use RFC 6979)"
        );

        // Verify the signature
        let valid = public_key
            .verify(SECP256K1_TEST_MESSAGE, go_signature)
            .unwrap();
        assert!(valid, "Should verify Go-generated secp256k1 signature");
    }

    #[test]
    fn test_secp256k1_did_matches_go() {
        let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();
        let public_key = private_key.public_key();

        let did = public_key.did().unwrap();
        assert_eq!(did, SECP256K1_DID, "secp256k1 DID should match Go");
    }

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

    // ===== AES-GCM Compatibility Tests =====

    #[test]
    fn test_aes_decrypt_go_ciphertext() {
        let decrypted = decrypt_aes(None, AES_CIPHERTEXT_WITH_NONCE, &AES_KEY, AES_AAD)
            .expect("Should decrypt Go AES ciphertext");

        assert_eq!(decrypted, AES_PLAINTEXT, "Decrypted AES should match Go");
    }

    #[test]
    fn test_aes_encrypt_matches_go_with_deterministic_nonce() {
        // Enable deterministic nonce mode
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let (ciphertext, nonce) = encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true)
            .expect("Should encrypt with AES");

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
}
