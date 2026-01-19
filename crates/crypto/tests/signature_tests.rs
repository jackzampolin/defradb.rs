//! Integration tests for digital signatures

use crypto::keys::generation::{generate_ed25519, generate_secp256k1};
use crypto::keys::{PrivateKey, PublicKey};

// ===== Ed25519 Signature Tests =====

#[test]
fn test_ed25519_sign_and_verify() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let message = b"test message";

    let signature = private_key.sign(message).unwrap();
    assert_eq!(signature.len(), 64, "Ed25519 signature should be 64 bytes");

    let valid = public_key.verify(message, &signature).unwrap();
    assert!(valid, "Signature should verify");
}

#[test]
fn test_ed25519_wrong_message_fails() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let message = b"original message";
    let wrong_message = b"wrong message";

    let signature = private_key.sign(message).unwrap();

    let valid = public_key.verify(wrong_message, &signature).unwrap();
    assert!(!valid, "Signature should not verify for wrong message");
}

#[test]
fn test_ed25519_wrong_key_fails() {
    let private_key1 = generate_ed25519().unwrap();
    let private_key2 = generate_ed25519().unwrap();
    let wrong_public_key = private_key2.public_key();
    let message = b"test message";

    let signature = private_key1.sign(message).unwrap();

    let valid = wrong_public_key.verify(message, &signature).unwrap();
    assert!(!valid, "Signature should not verify with wrong key");
}

#[test]
fn test_ed25519_tampered_signature_fails() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let message = b"test message";

    let mut signature = private_key.sign(message).unwrap();

    // Tamper with the signature
    signature[0] ^= 0xFF;

    let valid = public_key.verify(message, &signature).unwrap();
    assert!(!valid, "Tampered signature should not verify");
}

#[test]
fn test_ed25519_signature_deterministic() {
    let private_key = generate_ed25519().unwrap();
    let message = b"test message";

    let sig1 = private_key.sign(message).unwrap();
    let sig2 = private_key.sign(message).unwrap();

    assert_eq!(sig1, sig2, "Ed25519 signatures should be deterministic");
}

// ===== secp256k1 Signature Tests =====

#[test]
fn test_secp256k1_sign_and_verify() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();
    let message = b"test message";

    let signature = private_key.sign(message).unwrap();
    // DER-encoded signature varies in length (typically 70-72 bytes)
    assert!(
        signature.len() >= 68 && signature.len() <= 72,
        "DER signature should be 68-72 bytes, got {}",
        signature.len()
    );

    let valid = public_key.verify(message, &signature).unwrap();
    assert!(valid, "Signature should verify");
}

#[test]
fn test_secp256k1_wrong_message_fails() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();
    let message = b"original message";
    let wrong_message = b"wrong message";

    let signature = private_key.sign(message).unwrap();

    let valid = public_key.verify(wrong_message, &signature).unwrap();
    assert!(!valid, "Signature should not verify for wrong message");
}

#[test]
fn test_secp256k1_wrong_key_fails() {
    let private_key1 = generate_secp256k1().unwrap();
    let private_key2 = generate_secp256k1().unwrap();
    let wrong_public_key = private_key2.public_key();
    let message = b"test message";

    let signature = private_key1.sign(message).unwrap();

    let valid = wrong_public_key.verify(message, &signature).unwrap();
    assert!(!valid, "Signature should not verify with wrong key");
}

#[test]
fn test_secp256k1_tampered_signature_fails() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();
    let message = b"test message";

    let mut signature = private_key.sign(message).unwrap();

    // Tamper with the signature (skip the DER length byte at index 1)
    if signature.len() > 10 {
        signature[10] ^= 0xFF;
    }

    let result = public_key.verify(message, &signature);
    // Tampered DER signature might fail to parse or fail verification
    if let Ok(valid) = result {
        assert!(!valid, "Tampered signature should not verify");
    }
    // If it returns an error, that's also acceptable (invalid DER format)
}

#[test]
fn test_secp256k1_empty_message() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();
    let message = b"";

    let signature = private_key.sign(message).unwrap();
    let valid = public_key.verify(message, &signature).unwrap();
    assert!(valid, "Empty message signature should verify");
}

#[test]
fn test_ed25519_empty_message() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let message = b"";

    let signature = private_key.sign(message).unwrap();
    let valid = public_key.verify(message, &signature).unwrap();
    assert!(valid, "Empty message signature should verify");
}

#[test]
fn test_secp256k1_large_message() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();
    let message = vec![0xABu8; 1024 * 1024]; // 1MB message

    let signature = private_key.sign(&message).unwrap();
    let valid = public_key.verify(&message, &signature).unwrap();
    assert!(valid, "Large message signature should verify");
}

#[test]
fn test_ed25519_large_message() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let message = vec![0xCDu8; 1024 * 1024]; // 1MB message

    let signature = private_key.sign(&message).unwrap();
    let valid = public_key.verify(&message, &signature).unwrap();
    assert!(valid, "Large message signature should verify");
}
