//! Go Compatibility Tests for secp256r1 (P-256) Signatures
//!
//! Addresses security audit findings:
//! - 01-13: secp256r1 systematic compat gaps — byte-equality signing, low-S normalization
//! - 01-04: secp256r1 Go signature S-normalization gap
//!
//! Key findings documented here:
//! - Rust p256 crate uses RFC 6979 → deterministic signing
//! - Go crypto/ecdsa uses random k → non-deterministic; Go-generated signatures may have high-S
//! - Rust verification normalizes S via `normalize_s()` → accepts both high-S and low-S Go sigs
//! - secp256r1 is NOT used for IPLD block signing (only secp256k1 and Ed25519 are used there)

use crypto::keys::secp256r1::{Secp256r1PrivateKey, Secp256r1PublicKey};
use crypto::keys::{PrivateKey, PublicKey};

// Test key: same as SECP256R1_PRIVATE_KEY in go_compat_keys.rs
const PRIVATE_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];
const PUBLIC_KEY_COMPRESSED: [u8; 33] = [
    0x02, 0x51, 0x5c, 0x3d, 0x6e, 0xb9, 0xe3, 0x96, 0xb9, 0x04, 0xd3, 0xfe, 0xca, 0x7f, 0x54, 0xfd,
    0xcd, 0x0c, 0xc1, 0xe9, 0x97, 0xbf, 0x37, 0x5d, 0xca, 0x51, 0x5a, 0xd0, 0xa6, 0xc3, 0xb4, 0x03,
    0x5f,
];

// Go-generated DER signature for "test message" — has HIGH-S value.
// Go's crypto/ecdsa uses random k (non-deterministic); the S value is not normalized.
// Rust verification must accept this by normalizing S before checking.
// S value: 0xe72e19dc...b44aec > N/2 (N/2 starts with 0x7fff...)
const GO_SIG_HIGH_S: &[u8] = &[
    0x30, 0x45, 0x02, 0x20, 0x3f, 0x7a, 0xdd, 0x48, 0x1c, 0x52, 0x73, 0x86, 0x5d, 0x4b, 0x15, 0xba,
    0xbb, 0xa8, 0x09, 0xc8, 0xf5, 0x7c, 0x69, 0x37, 0xe1, 0x6f, 0x5d, 0xc0, 0x4c, 0xa7, 0x22, 0xcb,
    0x34, 0x5c, 0xed, 0xd7, 0x02, 0x21, 0x00, 0xe7, 0x2e, 0x19, 0xdc, 0xa1, 0xf8, 0xfa, 0x94, 0x6e,
    0xd8, 0xf1, 0xc5, 0x18, 0x7c, 0xb7, 0xaa, 0x3b, 0x7d, 0xe3, 0x17, 0x72, 0xba, 0xab, 0xdd, 0x93,
    0x86, 0x6d, 0xba, 0xff, 0xb4, 0x4a, 0xec,
];

// Go-generated DER signature for empty message — low-S.
const GO_SIG_EMPTY_MSG: &[u8] = &[
    0x30, 0x45, 0x02, 0x21, 0x00, 0xa3, 0xe3, 0x82, 0xd5, 0xbb, 0x5a, 0x15, 0x47, 0xd6, 0x79, 0x66,
    0x50, 0x31, 0x4d, 0x4f, 0xf8, 0xfe, 0x4e, 0xd7, 0x89, 0x78, 0xd8, 0xf2, 0x02, 0x29, 0x37, 0x27,
    0x2f, 0xae, 0xba, 0x87, 0xb3, 0x02, 0x20, 0x75, 0x5f, 0x43, 0x99, 0x3b, 0xb3, 0x5b, 0xb7, 0x41,
    0x75, 0x3c, 0xea, 0x8a, 0x2d, 0x16, 0x26, 0x13, 0x76, 0x75, 0xa6, 0x65, 0xaf, 0x06, 0xf2, 0xb1,
    0xaa, 0x48, 0xf0, 0xc3, 0x42, 0xde, 0xca,
];

// Go-generated DER signature for binary message [0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00]
const GO_SIG_BINARY_MSG: &[u8] = &[
    0x30, 0x46, 0x02, 0x21, 0x00, 0xfd, 0x7b, 0x34, 0x1f, 0xf7, 0x4b, 0xf0, 0x31, 0x5a, 0x90, 0x26,
    0x39, 0xf0, 0xaf, 0xb4, 0x28, 0x1a, 0x8b, 0x42, 0x9f, 0x61, 0xe6, 0x10, 0x30, 0xc4, 0xd3, 0xc8,
    0x7a, 0xf1, 0x0c, 0xfe, 0xac, 0x02, 0x21, 0x00, 0xa8, 0x9f, 0xd0, 0xc0, 0xc2, 0xdf, 0x07, 0xd6,
    0xef, 0xb2, 0xf9, 0x66, 0x0d, 0xfc, 0xc8, 0x0f, 0xd1, 0x01, 0x9a, 0xd2, 0x7c, 0xe8, 0x59, 0x8c,
    0x2f, 0xba, 0x16, 0x16, 0x36, 0x50, 0x3b, 0x30,
];

// Go-generated DER signature for 1024-byte message ("A" * 1024)
const GO_SIG_1KB_MSG: &[u8] = &[
    0x30, 0x45, 0x02, 0x21, 0x00, 0xb2, 0xaa, 0x12, 0x1d, 0xaa, 0xd5, 0xa4, 0xc7, 0x89, 0x7d, 0x56,
    0xc8, 0x2f, 0xc6, 0x57, 0x82, 0xfe, 0xf4, 0x19, 0xc0, 0x3d, 0xdc, 0xfd, 0xe2, 0xf7, 0xe5, 0xd7,
    0xbf, 0x33, 0xde, 0xce, 0x7c, 0x02, 0x20, 0x52, 0x0a, 0x2e, 0xde, 0xdd, 0xe8, 0xaf, 0x9e, 0x87,
    0xa4, 0xc3, 0x16, 0x08, 0xbe, 0x2e, 0xc5, 0xe0, 0xce, 0x24, 0x31, 0xbc, 0x38, 0x4d, 0xd9, 0x94,
    0xf3, 0x9c, 0xe3, 0x54, 0xfd, 0x20, 0xa4,
];

fn extract_s_from_der(der: &[u8]) -> &[u8] {
    let mut pos = 2usize;
    if der[1] & 0x80 != 0 {
        pos += (der[1] & 0x7f) as usize;
    }
    assert_eq!(der[pos], 0x02);
    pos += 1;
    let r_len = der[pos] as usize;
    pos += 1 + r_len;
    assert_eq!(der[pos], 0x02);
    pos += 1;
    let s_len = der[pos] as usize;
    pos += 1;
    &der[pos..pos + s_len]
}

fn is_low_s_p256(s_bytes: &[u8]) -> bool {
    // P-256 curve order N/2 (first byte is 0x7f):
    // N/2 = 0x7fffffff800000007fffffffffffffffde737d56d38bcf4279dce5617e3192a8
    let stripped = if s_bytes.first() == Some(&0x00) {
        &s_bytes[1..]
    } else {
        s_bytes
    };
    stripped[0] <= 0x7f
}

// ===== Determinism Tests =====

#[test]
fn test_rust_secp256r1_signing_is_deterministic() {
    // Rust p256 uses RFC 6979 → deterministic. Same key + message → identical DER sig.
    let pk = Secp256r1PrivateKey::from_bytes(&PRIVATE_KEY).unwrap();

    let messages: &[&[u8]] = &[b"hello", b"test message", b"", &[0x00, 0x01, 0xff]];
    for msg in messages {
        let sig1 = pk.sign(msg).unwrap();
        let sig2 = pk.sign(msg).unwrap();
        assert_eq!(sig1, sig2, "Rust secp256r1 signing must be deterministic");
    }
}

// ===== S-Normalization Tests =====

#[test]
fn test_rust_verifier_accepts_go_high_s_signature() {
    // Go's crypto/ecdsa may produce high-S signatures for secp256r1.
    // Our Rust verifier normalizes S before checking, so it must accept these.
    let pub_key = Secp256r1PublicKey::from_bytes(&PUBLIC_KEY_COMPRESSED).expect("valid public key");

    let valid = pub_key
        .verify(b"test message", GO_SIG_HIGH_S)
        .expect("verification should not error");

    assert!(
        valid,
        "Rust must accept Go-generated high-S secp256r1 signature after S-normalization"
    );
}

#[test]
fn test_go_sig_has_high_s_value() {
    // Document that the Go test vector actually has high-S (S > N/2).
    let s = extract_s_from_der(GO_SIG_HIGH_S);
    assert!(
        !is_low_s_p256(s),
        "Test vector GO_SIG_HIGH_S should have S > N/2 (high-S). First S byte: 0x{:02x}",
        if s[0] == 0x00 { s[1] } else { s[0] }
    );
}

#[test]
fn test_rust_produces_low_s_for_same_message() {
    // Rust RFC 6979 produces low-S; Go produced high-S for the same message.
    // Both are valid; Rust verifier accepts both.
    let pk = Secp256r1PrivateKey::from_bytes(&PRIVATE_KEY).unwrap();
    let rust_sig = pk.sign(b"test message").unwrap();

    let s = extract_s_from_der(&rust_sig);
    assert!(
        is_low_s_p256(s),
        "Rust secp256r1 signature should have low-S. First S byte: 0x{:02x}",
        if s[0] == 0x00 { s[1] } else { s[0] }
    );

    // The Rust signature differs from Go's because Go uses random k
    assert_ne!(
        rust_sig.as_slice(),
        GO_SIG_HIGH_S,
        "Rust and Go produce different (both valid) signatures for secp256r1"
    );
}

#[test]
fn test_rust_sig_verifies_with_go_public_key() {
    // Rust signature over "test message" must verify with the same public key
    let pk = Secp256r1PrivateKey::from_bytes(&PRIVATE_KEY).unwrap();
    let pub_key = Secp256r1PublicKey::from_bytes(&PUBLIC_KEY_COMPRESSED).expect("valid public key");

    let rust_sig = pk.sign(b"test message").unwrap();
    let valid = pub_key.verify(b"test message", &rust_sig).unwrap();

    assert!(valid, "Rust-generated secp256r1 signature must self-verify");
}

// ===== Go Signature Verification Tests =====

#[test]
fn test_verify_go_sig_empty_message() {
    let pub_key = Secp256r1PublicKey::from_bytes(&PUBLIC_KEY_COMPRESSED).expect("valid public key");

    let valid = pub_key.verify(b"", GO_SIG_EMPTY_MSG).unwrap();
    assert!(
        valid,
        "Rust must verify Go secp256r1 signature for empty message"
    );
}

#[test]
fn test_verify_go_sig_binary_message() {
    let pub_key = Secp256r1PublicKey::from_bytes(&PUBLIC_KEY_COMPRESSED).expect("valid public key");

    let valid = pub_key
        .verify(
            &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00],
            GO_SIG_BINARY_MSG,
        )
        .unwrap();
    assert!(
        valid,
        "Rust must verify Go secp256r1 signature for binary message"
    );
}

#[test]
fn test_verify_go_sig_1kb_message() {
    let pub_key = Secp256r1PublicKey::from_bytes(&PUBLIC_KEY_COMPRESSED).expect("valid public key");

    let msg = vec![b'A'; 1024];
    let valid = pub_key.verify(&msg, GO_SIG_1KB_MSG).unwrap();
    assert!(
        valid,
        "Rust must verify Go secp256r1 signature for 1KB message"
    );
}

// ===== Wrong Message / Tamper Tests =====

#[test]
fn test_go_sig_rejected_for_wrong_message() {
    let pub_key = Secp256r1PublicKey::from_bytes(&PUBLIC_KEY_COMPRESSED).expect("valid public key");

    let valid = pub_key.verify(b"wrong message", GO_SIG_HIGH_S).unwrap();
    assert!(!valid, "Go signature must not verify against wrong message");
}

#[test]
fn test_tampered_go_sig_rejected() {
    let pub_key = Secp256r1PublicKey::from_bytes(&PUBLIC_KEY_COMPRESSED).expect("valid public key");

    let mut tampered = GO_SIG_HIGH_S.to_vec();
    tampered[20] ^= 0x01;

    let valid = pub_key.verify(b"test message", &tampered).unwrap();
    assert!(!valid, "Tampered Go signature must not verify");
}

// ===== All Go Signatures Have Consistent S Behavior =====

#[test]
fn test_all_rust_signatures_have_low_s() {
    let pk = Secp256r1PrivateKey::from_bytes(&PRIVATE_KEY).unwrap();

    let messages: &[&[u8]] = &[
        b"",
        b"test message",
        &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00],
    ];
    for msg in messages {
        let sig = pk.sign(msg).unwrap();
        let s = extract_s_from_der(&sig);
        assert!(
            is_low_s_p256(s),
            "All Rust secp256r1 signatures must have low-S. msg={:?}",
            msg
        );
    }
}
