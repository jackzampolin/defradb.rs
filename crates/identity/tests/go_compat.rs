//! Go DefraDB compatibility tests for identity operations.
//!
//! These tests verify that the identity crate produces identical DIDs and signatures
//! as the Go DefraDB implementation, ensuring cross-implementation compatibility.

use crypto::{Ed25519PrivateKey, KeyType, Secp256k1PrivateKey};
use identity::{FullIdentity, Identity, RawIdentity};

// Test vectors from Go DefraDB (crates/crypto/src/go_compat.rs)
// These ensure Rust produces identical output to Go.

const ED25519_PRIVATE_KEY: [u8; 64] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
    0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07, 0x3a,
    0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07, 0x51, 0x1a,
];
const ED25519_DID: &str = "did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw";
const ED25519_TEST_MESSAGE: &[u8] = b"test message";
const ED25519_SIGNATURE: [u8; 64] = [
    0x98, 0xa3, 0x9e, 0xc1, 0x1a, 0x0d, 0xfb, 0xbf, 0xdb, 0xd7, 0xa7, 0xe2, 0x39, 0x4b, 0x2b, 0x83,
    0xa1, 0x65, 0x86, 0xe9, 0x21, 0x00, 0xbc, 0xb9, 0xbe, 0x67, 0x2d, 0xdf, 0xba, 0x3e, 0x7a, 0xcb,
    0x86, 0x1c, 0x94, 0xd6, 0xad, 0x4c, 0xf6, 0xe3, 0xe6, 0x01, 0x36, 0xca, 0x14, 0x1f, 0xc4, 0xf2,
    0xf1, 0xbe, 0x0c, 0x1b, 0x8e, 0xf0, 0xbe, 0xa1, 0x2a, 0xee, 0x76, 0xf0, 0x07, 0xa4, 0xc3, 0x0a,
];

const SECP256K1_PRIVATE_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];
const SECP256K1_DID: &str =
    "did:key:z7r8or8ecagY9LD87s54K2arcXmgmw6bUhyvq83RrnB2hJiUb2ug5YGAk1ZUaimewnoLL1ZGzXuTCnWRSrRZgR3v2PLPH";

// Unicode test message used in Go tests
const UNICODE_MESSAGE: &str = "Hello, 世界! 🌍 Привет мир";
const ED25519_UNICODE_SIGNATURE: [u8; 64] = [
    0x47, 0xcb, 0x3b, 0x89, 0x42, 0x41, 0x3e, 0x3e, 0x5c, 0xb8, 0x21, 0xdc, 0x85, 0xce, 0xc2, 0xed,
    0xdd, 0x1b, 0x54, 0x51, 0xe4, 0x77, 0x17, 0x3b, 0xa6, 0x74, 0x63, 0x6d, 0x1b, 0x12, 0x2c, 0xa2,
    0xfe, 0xed, 0x69, 0xa3, 0xa3, 0x81, 0xd5, 0x2f, 0x52, 0xc6, 0x7c, 0xa6, 0x4b, 0x03, 0x53, 0x17,
    0x7f, 0x6a, 0x0f, 0xed, 0xbc, 0x0c, 0x9b, 0xbe, 0xea, 0x47, 0x0e, 0x06, 0xa4, 0xea, 0x9e, 0x06,
];
const SECP256K1_UNICODE_SIGNATURE: &[u8] = &[
    0x30, 0x44, 0x02, 0x20, 0x17, 0xac, 0xd0, 0xf1, 0x54, 0x41, 0x0f, 0x34, 0x6c, 0x02, 0xdb, 0xea,
    0xf9, 0x4c, 0xac, 0x64, 0x7b, 0x3f, 0x64, 0x18, 0x23, 0x01, 0xe1, 0x2c, 0x90, 0xa9, 0x9f, 0x0a,
    0x61, 0xec, 0x48, 0xf8, 0x02, 0x20, 0x6b, 0x72, 0x9e, 0xea, 0x68, 0x2c, 0x91, 0x7f, 0x48, 0x7e,
    0xc3, 0x66, 0xbd, 0x62, 0x06, 0xe6, 0xd5, 0xc5, 0x5f, 0x89, 0x8f, 0xf9, 0x62, 0x50, 0x2f, 0xaf,
    0x2a, 0x21, 0x41, 0x5e, 0x38, 0x7b,
];

// ===== DID Compatibility Tests =====

#[test]
fn test_ed25519_did_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();
    let identity = RawIdentity::from_ed25519(private_key).unwrap();

    let did = identity.did().unwrap();
    assert_eq!(
        did, ED25519_DID,
        "Ed25519 DID must match Go DefraDB output exactly"
    );
}

#[test]
fn test_secp256k1_did_matches_go() {
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();
    let identity = RawIdentity::from_secp256k1(private_key).unwrap();

    let did = identity.did().unwrap();
    assert_eq!(
        did, SECP256K1_DID,
        "secp256k1 DID must match Go DefraDB output exactly (uses uncompressed public key)"
    );
}

// ===== Signature Compatibility Tests =====

#[test]
fn test_ed25519_signature_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();
    let identity = RawIdentity::from_ed25519(private_key).unwrap();

    let signature = identity.sign(ED25519_TEST_MESSAGE).unwrap();
    assert_eq!(
        signature.as_slice(),
        &ED25519_SIGNATURE,
        "Ed25519 signature must match Go DefraDB output exactly (deterministic)"
    );
}

#[test]
fn test_ed25519_unicode_signature_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();
    let identity = RawIdentity::from_ed25519(private_key).unwrap();

    let signature = identity.sign(UNICODE_MESSAGE.as_bytes()).unwrap();
    assert_eq!(
        signature.as_slice(),
        &ED25519_UNICODE_SIGNATURE,
        "Ed25519 Unicode signature must match Go output"
    );
}

#[test]
fn test_secp256k1_signature_matches_go() {
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();
    let identity = RawIdentity::from_secp256k1(private_key).unwrap();

    // secp256k1 uses RFC 6979 for deterministic signatures
    let signature = identity.sign(UNICODE_MESSAGE.as_bytes()).unwrap();
    assert_eq!(
        signature.as_slice(),
        SECP256K1_UNICODE_SIGNATURE,
        "secp256k1 signature must match Go DefraDB output (RFC 6979 deterministic)"
    );
}

// ===== Cross-verification Tests =====

#[test]
fn test_ed25519_can_verify_go_signature() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();
    let identity = RawIdentity::from_ed25519(private_key).unwrap();

    // Verify a signature that was generated by Go
    let verified = identity
        .pub_key()
        .verify(ED25519_TEST_MESSAGE, &ED25519_SIGNATURE)
        .unwrap();
    assert!(
        verified,
        "Rust should verify Go-generated Ed25519 signature"
    );
}

#[test]
fn test_secp256k1_can_verify_go_signature() {
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();
    let identity = RawIdentity::from_secp256k1(private_key).unwrap();

    // Verify a signature that was generated by Go
    let verified = identity
        .pub_key()
        .verify(UNICODE_MESSAGE.as_bytes(), SECP256K1_UNICODE_SIGNATURE)
        .unwrap();
    assert!(
        verified,
        "Rust should verify Go-generated secp256k1 signature"
    );
}

// ===== from_bytes Compatibility Tests =====

#[test]
fn test_from_bytes_ed25519_produces_correct_did() {
    let identity = RawIdentity::from_bytes(KeyType::Ed25519, &ED25519_PRIVATE_KEY).unwrap();

    assert_eq!(identity.key_type(), KeyType::Ed25519);
    assert_eq!(
        identity.did().unwrap(),
        ED25519_DID,
        "from_bytes should produce same DID as from_ed25519"
    );
}

#[test]
fn test_from_bytes_secp256k1_produces_correct_did() {
    let identity = RawIdentity::from_bytes(KeyType::Secp256k1, &SECP256K1_PRIVATE_KEY).unwrap();

    assert_eq!(identity.key_type(), KeyType::Secp256k1);
    assert_eq!(
        identity.did().unwrap(),
        SECP256K1_DID,
        "from_bytes should produce same DID as from_secp256k1"
    );
}

// ===== Key Type Consistency Tests =====

#[test]
fn test_identity_key_type_matches_underlying_key() {
    let ed25519_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();
    let ed25519_identity = RawIdentity::from_ed25519(ed25519_key).unwrap();
    assert_eq!(ed25519_identity.key_type(), KeyType::Ed25519);
    assert_eq!(ed25519_identity.pub_key().key_type(), KeyType::Ed25519);
    assert_eq!(ed25519_identity.priv_key().key_type(), KeyType::Ed25519);

    let secp256k1_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();
    let secp256k1_identity = RawIdentity::from_secp256k1(secp256k1_key).unwrap();
    assert_eq!(secp256k1_identity.key_type(), KeyType::Secp256k1);
    assert_eq!(secp256k1_identity.pub_key().key_type(), KeyType::Secp256k1);
    assert_eq!(secp256k1_identity.priv_key().key_type(), KeyType::Secp256k1);
}

// ===== Private Key Bytes Roundtrip Tests =====

#[test]
fn test_ed25519_private_key_bytes_match_input() {
    let identity = RawIdentity::from_bytes(KeyType::Ed25519, &ED25519_PRIVATE_KEY).unwrap();

    let extracted_bytes = identity.private_key_bytes();
    assert_eq!(
        extracted_bytes.as_slice(),
        &ED25519_PRIVATE_KEY,
        "private_key_bytes should return original key bytes"
    );
}

#[test]
fn test_secp256k1_private_key_bytes_match_input() {
    let identity = RawIdentity::from_bytes(KeyType::Secp256k1, &SECP256K1_PRIVATE_KEY).unwrap();

    let extracted_bytes = identity.private_key_bytes();
    assert_eq!(
        extracted_bytes.as_slice(),
        &SECP256K1_PRIVATE_KEY,
        "private_key_bytes should return original key bytes"
    );
}

// ===== Signature Determinism Tests =====

#[test]
fn test_ed25519_signature_is_deterministic() {
    let identity = RawIdentity::from_bytes(KeyType::Ed25519, &ED25519_PRIVATE_KEY).unwrap();

    let sig1 = identity.sign(b"determinism test").unwrap();
    let sig2 = identity.sign(b"determinism test").unwrap();

    assert_eq!(sig1, sig2, "Ed25519 signatures should be deterministic");
}

#[test]
fn test_secp256k1_signature_is_deterministic() {
    let identity = RawIdentity::from_bytes(KeyType::Secp256k1, &SECP256K1_PRIVATE_KEY).unwrap();

    let sig1 = identity.sign(b"determinism test").unwrap();
    let sig2 = identity.sign(b"determinism test").unwrap();

    assert_eq!(
        sig1, sig2,
        "secp256k1 signatures should be deterministic (RFC 6979)"
    );
}
