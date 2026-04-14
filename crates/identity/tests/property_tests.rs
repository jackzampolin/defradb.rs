//! Property-based tests for identity operations.
//!
//! These tests verify key invariants of the identity system using randomly
//! generated inputs to ensure robustness across edge cases.

use crypto::keys::Key;
use crypto::{generate_ed25519, generate_secp256k1, KeyType};
use identity::{FullIdentity, Identity, RawIdentity};
use proptest::prelude::*;

// ===== Sign/Verify Roundtrip Properties =====

proptest! {
    #[test]
    fn prop_ed25519_sign_verify_roundtrip(data: Vec<u8>) {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let signature = identity.sign(&data).unwrap();
        let verified = identity.pub_key().verify(&data, &signature).unwrap();

        prop_assert!(verified, "Signature must verify for original message");
    }

    #[test]
    fn prop_secp256k1_sign_verify_roundtrip(data: Vec<u8>) {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let signature = identity.sign(&data).unwrap();
        let verified = identity.pub_key().verify(&data, &signature).unwrap();

        prop_assert!(verified, "Signature must verify for original message");
    }
}

// ===== DID Determinism Properties =====

proptest! {
    #[test]
    fn prop_ed25519_did_is_deterministic(_seed in any::<[u8; 32]>()) {
        // Generate two identities from the same seed
        // Note: Ed25519 private key is 64 bytes (seed + public key)
        // We generate a key and use its bytes to ensure consistency
        let key1 = generate_ed25519().unwrap();
        let bytes = key1.raw();

        let identity1 = RawIdentity::from_bytes(KeyType::Ed25519, bytes).unwrap();
        let identity2 = RawIdentity::from_bytes(KeyType::Ed25519, bytes).unwrap();

        let did1 = identity1.did().unwrap();
        let did2 = identity2.did().unwrap();

        prop_assert_eq!(did1, did2, "Same key bytes must produce same DID");
    }

    #[test]
    fn prop_secp256k1_did_is_deterministic(_seed in any::<[u8; 32]>()) {
        let key1 = generate_secp256k1().unwrap();
        let bytes = key1.raw();

        let identity1 = RawIdentity::from_bytes(KeyType::Secp256k1, bytes).unwrap();
        let identity2 = RawIdentity::from_bytes(KeyType::Secp256k1, bytes).unwrap();

        let did1 = identity1.did().unwrap();
        let did2 = identity2.did().unwrap();

        prop_assert_eq!(did1, did2, "Same key bytes must produce same DID");
    }
}

// ===== Signature Uniqueness Properties =====

proptest! {
    #[test]
    fn prop_different_messages_have_different_signatures(
        msg1 in any::<Vec<u8>>(),
        msg2 in any::<Vec<u8>>()
    ) {
        prop_assume!(msg1 != msg2);

        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let sig1 = identity.sign(&msg1).unwrap();
        let sig2 = identity.sign(&msg2).unwrap();

        prop_assert_ne!(sig1, sig2, "Different messages should produce different signatures");
    }

    #[test]
    fn prop_signature_not_valid_for_different_message(
        original_msg in any::<Vec<u8>>(),
        different_msg in any::<Vec<u8>>()
    ) {
        prop_assume!(original_msg != different_msg);

        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let signature = identity.sign(&original_msg).unwrap();

        // Signature should NOT verify for a different message
        let valid_for_different = identity.pub_key().verify(&different_msg, &signature).unwrap();

        prop_assert!(!valid_for_different, "Signature must not verify for different message");
    }
}

// ===== Key Isolation Properties =====

proptest! {
    #[test]
    fn prop_different_keys_cannot_verify_each_others_signatures(data: Vec<u8>) {
        let key1 = generate_ed25519().unwrap();
        let key2 = generate_ed25519().unwrap();

        let identity1 = RawIdentity::from_private_key(key1).unwrap();
        let identity2 = RawIdentity::from_private_key(key2).unwrap();

        // Sign with identity1
        let signature = identity1.sign(&data).unwrap();

        // Verify with identity2 (should fail)
        let valid = identity2.pub_key().verify(&data, &signature).unwrap();

        prop_assert!(!valid, "Different identity must not verify another's signature");
    }
}

// ===== Key Type Consistency Properties =====

proptest! {
    #[test]
    fn prop_ed25519_key_type_is_consistent(_dummy in any::<u8>()) {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        prop_assert_eq!(identity.key_type(), KeyType::Ed25519);
        prop_assert_eq!(identity.pub_key().key_type(), KeyType::Ed25519);
        prop_assert_eq!(identity.priv_key().key_type(), KeyType::Ed25519);
    }

    #[test]
    fn prop_secp256k1_key_type_is_consistent(_dummy in any::<u8>()) {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        prop_assert_eq!(identity.key_type(), KeyType::Secp256k1);
        prop_assert_eq!(identity.pub_key().key_type(), KeyType::Secp256k1);
        prop_assert_eq!(identity.priv_key().key_type(), KeyType::Secp256k1);
    }
}

// ===== DID Format Properties =====

proptest! {
    #[test]
    fn prop_did_starts_with_did_key(_dummy in any::<u8>()) {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let did = identity.did().unwrap();
        prop_assert!(did.as_str().starts_with("did:key:"), "DID must start with 'did:key:'");
    }

    #[test]
    fn prop_ed25519_did_has_correct_multibase_prefix(_dummy in any::<u8>()) {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let did = identity.did().unwrap();
        // Ed25519 DIDs start with "did:key:z6Mk" (z = base58btc, 6Mk = ed25519-pub multicodec)
        prop_assert!(
            did.as_str().starts_with("did:key:z6Mk"),
            "Ed25519 DID must have correct multibase/multicodec prefix"
        );
    }

    #[test]
    fn prop_secp256k1_did_has_correct_multibase_prefix(_dummy in any::<u8>()) {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let did = identity.did().unwrap();
        // secp256k1 DIDs start with "did:key:z7r8" (z = base58btc, uses uncompressed key)
        prop_assert!(
            did.as_str().starts_with("did:key:z7r8"),
            "secp256k1 DID must have correct multibase/multicodec prefix (uncompressed key)"
        );
    }
}

// ===== Private Key Bytes Roundtrip Properties =====

proptest! {
    #[test]
    fn prop_ed25519_private_key_roundtrip(_dummy in any::<u8>()) {
        let key = generate_ed25519().unwrap();
        let identity1 = RawIdentity::from_private_key(key).unwrap();

        let bytes = identity1.private_key_bytes();
        let identity2 = RawIdentity::from_bytes(KeyType::Ed25519, &bytes).unwrap();

        prop_assert_eq!(
            identity1.did().unwrap(),
            identity2.did().unwrap(),
            "Roundtrip must preserve identity"
        );
    }

    #[test]
    fn prop_secp256k1_private_key_roundtrip(_dummy in any::<u8>()) {
        let key = generate_secp256k1().unwrap();
        let identity1 = RawIdentity::from_private_key(key).unwrap();

        let bytes = identity1.private_key_bytes();
        let identity2 = RawIdentity::from_bytes(KeyType::Secp256k1, &bytes).unwrap();

        prop_assert_eq!(
            identity1.did().unwrap(),
            identity2.did().unwrap(),
            "Roundtrip must preserve identity"
        );
    }
}

// ===== Signature Length Properties =====

proptest! {
    #[test]
    fn prop_ed25519_signature_is_64_bytes(data: Vec<u8>) {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let signature = identity.sign(&data).unwrap();
        prop_assert_eq!(signature.len(), 64, "Ed25519 signature must be 64 bytes");
    }

    #[test]
    fn prop_secp256k1_signature_is_der_encoded(data: Vec<u8>) {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let signature = identity.sign(&data).unwrap();

        // DER-encoded ECDSA signatures are typically 70-72 bytes
        prop_assert!(
            signature.len() >= 68 && signature.len() <= 73,
            "secp256k1 DER signature should be 68-73 bytes, got {}",
            signature.len()
        );

        // DER format starts with 0x30 (SEQUENCE tag)
        prop_assert_eq!(
            signature[0], 0x30,
            "secp256k1 signature must start with DER SEQUENCE tag"
        );
    }
}

// ===== Signature Determinism Properties =====

proptest! {
    #[test]
    fn prop_ed25519_signatures_are_deterministic(data: Vec<u8>) {
        let key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let sig1 = identity.sign(&data).unwrap();
        let sig2 = identity.sign(&data).unwrap();

        prop_assert_eq!(sig1, sig2, "Ed25519 signatures must be deterministic");
    }

    #[test]
    fn prop_secp256k1_signatures_are_deterministic(data: Vec<u8>) {
        let key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(key).unwrap();

        let sig1 = identity.sign(&data).unwrap();
        let sig2 = identity.sign(&data).unwrap();

        prop_assert_eq!(sig1, sig2, "secp256k1 signatures must be deterministic (RFC 6979)");
    }
}
