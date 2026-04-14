//! Integration tests for identity crate core functionality.
//!
//! Tests for the core identity traits (Identity, FullIdentity) and RawIdentity
//! construction from various key types.

use crypto::{generate_ed25519, generate_secp256k1, KeyType};
use identity::{FullIdentity, Identity, RawIdentity};

#[test]
fn test_raw_identity_from_ed25519() {
    let private_key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let did = identity.did().unwrap();
    assert!(did.as_str().starts_with("did:key:"));
}

#[test]
fn test_raw_identity_from_secp256k1() {
    let private_key = generate_secp256k1().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let did = identity.did().unwrap();
    assert!(did.as_str().starts_with("did:key:"));
}

#[test]
fn test_raw_identity_sign_verify() {
    let private_key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let message = b"test message";
    let signature = identity.sign(message).unwrap();

    let verified = identity.pub_key().verify(message, &signature).unwrap();
    assert!(verified);
}

#[test]
fn test_raw_identity_did_deterministic() {
    let private_key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let did1 = identity.did().unwrap();
    let did2 = identity.did().unwrap();
    assert_eq!(did1, did2, "DID should be deterministic");
}

#[test]
fn test_identity_key_type() {
    let ed25519_key = generate_ed25519().unwrap();
    let ed25519_identity = RawIdentity::from_private_key(ed25519_key).unwrap();
    assert_eq!(ed25519_identity.key_type(), KeyType::Ed25519);

    let secp256k1_key = generate_secp256k1().unwrap();
    let secp256k1_identity = RawIdentity::from_private_key(secp256k1_key).unwrap();
    assert_eq!(secp256k1_identity.key_type(), KeyType::Secp256k1);
}

#[test]
fn test_raw_identity_secp256k1_sign_verify() {
    let private_key = generate_secp256k1().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let message = b"test message for secp256k1";
    let signature = identity.sign(message).unwrap();

    let verified = identity.pub_key().verify(message, &signature).unwrap();
    assert!(verified);
}

#[test]
fn test_different_identities_have_different_dids() {
    let identity1 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();
    let identity2 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();

    let did1 = identity1.did().unwrap();
    let did2 = identity2.did().unwrap();

    assert_ne!(did1, did2, "Different keys should produce different DIDs");
}

#[test]
fn test_signature_not_reusable() {
    let identity = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();

    let message1 = b"first message";
    let message2 = b"second message";

    let signature = identity.sign(message1).unwrap();

    let valid_for_msg1 = identity.pub_key().verify(message1, &signature).unwrap();
    let err = identity.pub_key().verify(message2, &signature).unwrap_err();

    assert!(
        valid_for_msg1,
        "Signature should verify for original message"
    );
    assert!(
        err.to_string()
            .contains("Ed25519 signature verification failed"),
        "unexpected error: {}",
        err
    );
}

#[test]
fn test_wrong_key_verification_fails() {
    let identity1 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();
    let identity2 = RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap();

    let message = b"test message";
    let signature = identity1.sign(message).unwrap();

    let err = identity2.pub_key().verify(message, &signature).unwrap_err();
    assert!(
        err.to_string()
            .contains("Ed25519 signature verification failed"),
        "unexpected error: {}",
        err
    );
}
