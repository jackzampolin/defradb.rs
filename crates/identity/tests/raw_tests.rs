//! Integration tests for RawIdentity.
//!
//! Tests for the RawIdentity type which provides a concrete implementation
//! of the Identity and FullIdentity traits.

use crypto::keys::Key;
use crypto::{generate_ed25519, generate_secp256k1, generate_secp256r1, KeyType};
use identity::{FullIdentity, Identity, IdentityKeyType, RawIdentity};

#[test]
fn test_from_ed25519() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_ed25519(key).unwrap();

    assert_eq!(identity.key_type(), KeyType::Ed25519);
    assert!(identity.did().unwrap().as_str().starts_with("did:key:"));
}

#[test]
fn test_from_secp256k1() {
    let key = generate_secp256k1().unwrap();
    let identity = RawIdentity::from_secp256k1(key).unwrap();

    assert_eq!(identity.key_type(), KeyType::Secp256k1);
    assert!(identity.did().unwrap().as_str().starts_with("did:key:"));
}

#[test]
fn test_from_bytes_ed25519() {
    let key = generate_ed25519().unwrap();
    let bytes = key.raw();

    let identity = RawIdentity::from_bytes(KeyType::Ed25519, bytes).unwrap();
    assert_eq!(identity.key_type(), KeyType::Ed25519);
}

#[test]
fn test_from_bytes_secp256k1() {
    let key = generate_secp256k1().unwrap();
    let bytes = key.raw();

    let identity = RawIdentity::from_bytes(KeyType::Secp256k1, bytes).unwrap();
    assert_eq!(identity.key_type(), KeyType::Secp256k1);
}

#[test]
fn test_from_bytes_invalid() {
    let result = RawIdentity::from_bytes(KeyType::Ed25519, &[0u8; 32]);
    assert!(result.is_err(), "Should fail with invalid Ed25519 key");
    assert!(matches!(
        result.unwrap_err(),
        identity::Error::InvalidKeyBytes(KeyType::Ed25519, _)
    ));

    let result = RawIdentity::from_bytes(KeyType::Secp256k1, &[0u8; 16]);
    assert!(result.is_err(), "Should fail with invalid secp256k1 key");
    assert!(matches!(
        result.unwrap_err(),
        identity::Error::InvalidKeyBytes(KeyType::Secp256k1, _)
    ));
}

#[test]
fn test_from_secp256r1() {
    let key = crypto::generate_secp256r1().unwrap();
    let identity = RawIdentity::from_secp256r1(key).unwrap();
    assert_eq!(identity.key_type(), KeyType::Secp256r1);
    assert!(!identity.public_key_bytes().is_empty());
}

#[test]
fn test_from_bytes_secp256r1() {
    let key = crypto::generate_secp256r1().unwrap();
    let key_bytes = key.raw();
    let identity = RawIdentity::from_bytes(KeyType::Secp256r1, key_bytes).unwrap();
    assert_eq!(identity.key_type(), KeyType::Secp256r1);
}

#[test]
fn test_public_key_bytes_consistency() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();

    let bytes1 = identity.public_key_bytes();
    let bytes2 = identity.pub_key().raw();

    assert_eq!(
        bytes1, bytes2,
        "public_key_bytes should match pub_key().raw()"
    );
}

#[test]
fn test_private_key_bytes_roundtrip() {
    let key = generate_ed25519().unwrap();
    let identity1 = RawIdentity::from_private_key(key).unwrap();

    let bytes = identity1.private_key_bytes();
    let identity2 = RawIdentity::from_bytes(KeyType::Ed25519, &bytes).unwrap();

    assert_eq!(
        identity1.did().unwrap(),
        identity2.did().unwrap(),
        "Roundtrip should preserve identity"
    );
}

#[test]
fn test_debug_impl() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();

    let debug_str = format!("{:?}", identity);
    assert!(debug_str.contains("RawIdentity"));
    assert!(debug_str.contains("Ed25519"));
    assert!(debug_str.contains("did:key:"));
}

#[test]
fn test_sign_with_ed25519() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_ed25519(key).unwrap();

    let message = b"test message";
    let signature = identity.sign(message).unwrap();

    assert_eq!(signature.len(), 64, "Ed25519 signature should be 64 bytes");

    let verified = identity.pub_key().verify(message, &signature).unwrap();
    assert!(verified);
}

#[test]
fn test_sign_with_secp256k1() {
    let key = generate_secp256k1().unwrap();
    let identity = RawIdentity::from_secp256k1(key).unwrap();

    let message = b"test message";
    let signature = identity.sign(message).unwrap();

    // DER-encoded ECDSA signatures vary in length (typically 68-73 bytes)
    assert!(
        signature.len() >= 68 && signature.len() <= 73,
        "unexpected DER signature length: {}",
        signature.len()
    );

    let verified = identity.pub_key().verify(message, &signature).unwrap();
    assert!(verified);
}

#[test]
fn test_sign_with_secp256r1() {
    let key = generate_secp256r1().unwrap();
    let identity = RawIdentity::from_secp256r1(key).unwrap();

    let message = b"test message";
    let signature = identity.sign(message).unwrap();

    // DER signatures vary in length
    assert!(signature.len() >= 68 && signature.len() <= 72);

    let verified = identity.pub_key().verify(message, &signature).unwrap();
    assert!(verified);
}

#[test]
fn test_priv_key_trait_method() {
    let key = generate_ed25519().unwrap();
    let expected_bytes = key.raw_owned();
    let identity = RawIdentity::from_private_key(key).unwrap();

    let priv_key = identity.priv_key();
    assert_eq!(priv_key.key_type(), KeyType::Ed25519);
    assert_eq!(priv_key.raw(), expected_bytes);
}

#[test]
fn test_identity_key_type_ed25519() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();

    assert_eq!(identity.identity_key_type(), IdentityKeyType::Ed25519);
}

#[test]
fn test_identity_key_type_secp256k1() {
    let key = generate_secp256k1().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();

    assert_eq!(identity.identity_key_type(), IdentityKeyType::Secp256k1);
}

#[test]
fn test_from_identity_key_type_ed25519() {
    let key = generate_ed25519().unwrap();
    let bytes = key.raw();

    let identity = RawIdentity::from_identity_key_type(IdentityKeyType::Ed25519, bytes).unwrap();
    assert_eq!(identity.identity_key_type(), IdentityKeyType::Ed25519);
    assert_eq!(identity.key_type(), KeyType::Ed25519);
}

#[test]
fn test_from_identity_key_type_secp256k1() {
    let key = generate_secp256k1().unwrap();
    let bytes = key.raw();

    let identity = RawIdentity::from_identity_key_type(IdentityKeyType::Secp256k1, bytes).unwrap();
    assert_eq!(identity.identity_key_type(), IdentityKeyType::Secp256k1);
    assert_eq!(identity.key_type(), KeyType::Secp256k1);
}

#[test]
fn test_key_type_and_identity_key_type_consistent() {
    let ed25519_identity = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();
    assert_eq!(
        ed25519_identity.key_type(),
        ed25519_identity.identity_key_type().to_crypto_key_type()
    );

    let secp256k1_identity = RawIdentity::from_private_key(generate_secp256k1().unwrap()).unwrap();
    assert_eq!(
        secp256k1_identity.key_type(),
        secp256k1_identity.identity_key_type().to_crypto_key_type()
    );
}
