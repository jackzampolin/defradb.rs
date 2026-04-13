//! Integration tests for digital signatures

use crypto::keys::generation::{generate_ed25519, generate_secp256k1, generate_secp256r1};
use crypto::keys::{PrivateKey, PublicKey};

fn assert_verify_err(result: crypto::Result<bool>, expected: &str) {
    let err = result.expect_err("verification should return Err");
    assert!(
        err.to_string().contains(expected),
        "expected error containing {:?}, got {:?}",
        expected,
        err
    );
}

#[cfg(not(target_arch = "wasm32"))]
const BLS_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

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

    assert_verify_err(
        public_key.verify(wrong_message, &signature),
        "Ed25519 signature verification failed",
    );
}

#[test]
fn test_ed25519_wrong_key_fails() {
    let private_key1 = generate_ed25519().unwrap();
    let private_key2 = generate_ed25519().unwrap();
    let wrong_public_key = private_key2.public_key();
    let message = b"test message";

    let signature = private_key1.sign(message).unwrap();

    assert_verify_err(
        wrong_public_key.verify(message, &signature),
        "Ed25519 signature verification failed",
    );
}

#[test]
fn test_ed25519_tampered_signature_fails() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let message = b"test message";

    let mut signature = private_key.sign(message).unwrap();

    // Tamper with the signature
    signature[0] ^= 0xFF;

    assert_verify_err(
        public_key.verify(message, &signature),
        "Ed25519 signature verification failed",
    );
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

    assert_verify_err(
        public_key.verify(wrong_message, &signature),
        "secp256k1 signature verification failed",
    );
}

#[test]
fn test_secp256k1_wrong_key_fails() {
    let private_key1 = generate_secp256k1().unwrap();
    let private_key2 = generate_secp256k1().unwrap();
    let wrong_public_key = private_key2.public_key();
    let message = b"test message";

    let signature = private_key1.sign(message).unwrap();

    assert_verify_err(
        wrong_public_key.verify(message, &signature),
        "secp256k1 signature verification failed",
    );
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
    let err = result.expect_err("tampered signature should return Err");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("invalid secp256k1 DER signature")
            || err_msg.contains("secp256k1 signature verification failed"),
        "unexpected error: {}",
        err_msg
    );
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

// ===== secp256r1 (P-256) Signature Tests =====

#[test]
fn test_secp256r1_sign_and_verify() {
    let private_key = generate_secp256r1().unwrap();
    let public_key = private_key.public_key();
    let message = b"test message";

    let signature = private_key.sign(message).unwrap();
    assert!(
        signature.len() >= 68 && signature.len() <= 72,
        "DER signature should be 68-72 bytes, got {}",
        signature.len()
    );

    let valid = public_key.verify(message, &signature).unwrap();
    assert!(valid, "Signature should verify");
}

#[test]
fn test_secp256r1_wrong_message_fails() {
    let private_key = generate_secp256r1().unwrap();
    let public_key = private_key.public_key();
    let message = b"original message";
    let wrong_message = b"wrong message";

    let signature = private_key.sign(message).unwrap();

    assert_verify_err(
        public_key.verify(wrong_message, &signature),
        "secp256r1 signature verification failed",
    );
}

#[test]
fn test_secp256r1_wrong_key_fails() {
    let private_key1 = generate_secp256r1().unwrap();
    let private_key2 = generate_secp256r1().unwrap();
    let wrong_public_key = private_key2.public_key();
    let message = b"test message";

    let signature = private_key1.sign(message).unwrap();

    assert_verify_err(
        wrong_public_key.verify(message, &signature),
        "secp256r1 signature verification failed",
    );
}

#[test]
fn test_secp256r1_tampered_signature_fails() {
    let private_key = generate_secp256r1().unwrap();
    let public_key = private_key.public_key();
    let message = b"test message";

    let mut signature = private_key.sign(message).unwrap();

    if signature.len() > 10 {
        signature[10] ^= 0xFF;
    }

    let result = public_key.verify(message, &signature);
    let err = result.expect_err("tampered signature should return Err");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("invalid secp256r1 DER signature")
            || err_msg.contains("secp256r1 signature verification failed"),
        "unexpected error: {}",
        err_msg
    );
}

#[test]
fn test_secp256r1_empty_message() {
    let private_key = generate_secp256r1().unwrap();
    let public_key = private_key.public_key();
    let message = b"";

    let signature = private_key.sign(message).unwrap();
    let valid = public_key.verify(message, &signature).unwrap();
    assert!(valid, "Empty message signature should verify");
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn test_bls_sign_and_verify() {
    let mut ikm = [7u8; 32];
    let secret_key = blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap();
    let public_key = crypto::BlsPublicKey::from_bytes(&secret_key.sk_to_pk().compress()).unwrap();
    let message = b"test message";

    let signature = secret_key.sign(message, BLS_DST, &[]).compress().to_vec();
    let valid = public_key.verify(message, &signature).unwrap();

    assert!(valid, "BLS signature should verify");
    ikm.fill(0);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn test_bls_invalid_signatures_return_err() {
    let mut ikm = [9u8; 32];
    let secret_key = blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap();
    let public_key = crypto::BlsPublicKey::from_bytes(&secret_key.sk_to_pk().compress()).unwrap();
    let message = b"test message";

    let malformed = vec![0u8; 95];
    let malformed_err = public_key
        .verify(message, &malformed)
        .expect_err("malformed BLS signature should return Err");
    assert!(
        malformed_err
            .to_string()
            .contains("invalid BLS12-381 signature"),
        "unexpected error: {}",
        malformed_err
    );

    let mut tampered = secret_key.sign(message, BLS_DST, &[]).compress().to_vec();
    tampered[0] ^= 0x01;
    let tampered_err = public_key
        .verify(message, &tampered)
        .expect_err("tampered BLS signature should return Err");
    let tampered_msg = tampered_err.to_string();
    assert!(
        tampered_msg.contains("invalid BLS12-381 signature")
            || tampered_msg.contains("BLS12-381 signature verification failed"),
        "unexpected error: {}",
        tampered_msg
    );

    ikm.fill(0);
}
