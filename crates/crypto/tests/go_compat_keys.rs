//! Ed25519, secp256k1, secp256r1 cross-implementation tests for Go compatibility.
//!
//! These test vectors were generated from the Go DefraDB implementation
//! to ensure the Rust crypto implementation produces identical outputs.

use k256::ecdsa::Signature;
use crypto::keys::ed25519::{Ed25519PrivateKey, Ed25519PublicKey};
use crypto::keys::secp256k1::{Secp256k1PrivateKey, Secp256k1PublicKey};
use crypto::keys::secp256r1::Secp256r1PublicKey;
use crypto::keys::{Key, PrivateKey, PublicKey};

// ===== Ed25519 Test Vectors =====
const ED25519_PRIVATE_KEY: [u8; 64] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
    0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07, 0x3a,
    0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07, 0x51, 0x1a,
];
const ED25519_PUBLIC_KEY: [u8; 32] = [
    0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07, 0x3a,
    0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07, 0x51, 0x1a,
];
const ED25519_TEST_MESSAGE: &[u8] = b"test message";
const ED25519_SIGNATURE: [u8; 64] = [
    0x98, 0xa3, 0x9e, 0xc1, 0x1a, 0x0d, 0xfb, 0xbf, 0xdb, 0xd7, 0xa7, 0xe2, 0x39, 0x4b, 0x2b, 0x83,
    0xa1, 0x65, 0x86, 0xe9, 0x21, 0x00, 0xbc, 0xb9, 0xbe, 0x67, 0x2d, 0xdf, 0xba, 0x3e, 0x7a, 0xcb,
    0x86, 0x1c, 0x94, 0xd6, 0xad, 0x4c, 0xf6, 0xe3, 0xe6, 0x01, 0x36, 0xca, 0x14, 0x1f, 0xc4, 0xf2,
    0xf1, 0xbe, 0x0c, 0x1b, 0x8e, 0xf0, 0xbe, 0xa1, 0x2a, 0xee, 0x76, 0xf0, 0x07, 0xa4, 0xc3, 0x0a,
];
const ED25519_DID: &str = "did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw";

// ===== Ed25519 Edge Case Test Vectors =====
// Empty message signature
const ED25519_EMPTY_MESSAGE: &[u8] = b"";
const ED25519_EMPTY_MESSAGE_SIGNATURE: [u8; 64] = [
    0xe5, 0x56, 0x43, 0x00, 0xc3, 0x60, 0xac, 0x72, 0x90, 0x86, 0xe2, 0xcc, 0x80, 0x6e, 0x82, 0x8a,
    0x84, 0x87, 0x7f, 0x1e, 0xb8, 0xe5, 0xd9, 0x74, 0xd8, 0x73, 0xe0, 0x65, 0x22, 0x49, 0x01, 0x55,
    0x5f, 0xb8, 0x82, 0x15, 0x90, 0xa3, 0x3b, 0xac, 0xc6, 0x1e, 0x39, 0x70, 0x1c, 0xf9, 0xb4, 0x6b,
    0xd2, 0x5b, 0xf5, 0xf0, 0x59, 0x5b, 0xbe, 0x24, 0x65, 0x51, 0x41, 0x43, 0x8e, 0x7a, 0x10, 0x0b,
];
// Binary message with null bytes
const ED25519_BINARY_MESSAGE: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00];
const ED25519_BINARY_MESSAGE_SIGNATURE: [u8; 64] = [
    0x1d, 0x00, 0x16, 0x1e, 0x72, 0xf5, 0x6f, 0x9a, 0x20, 0x39, 0x0a, 0x53, 0xad, 0x64, 0xc5, 0xec,
    0x36, 0x0c, 0x6a, 0xae, 0xb8, 0x2a, 0x27, 0xa8, 0x82, 0x00, 0x27, 0x31, 0xc1, 0x41, 0xe2, 0x76,
    0x77, 0x73, 0xac, 0xd2, 0xbc, 0x31, 0xae, 0x66, 0xd7, 0x7f, 0x09, 0x91, 0xdf, 0x20, 0x45, 0x26,
    0xad, 0x28, 0xe4, 0xac, 0x2b, 0x37, 0x37, 0xaf, 0xc6, 0x9d, 0x1c, 0x0f, 0xce, 0x0e, 0x76, 0x03,
];
// Single byte message
const ED25519_SINGLE_BYTE_MESSAGE: &[u8] = &[0x42];
const ED25519_SINGLE_BYTE_SIGNATURE: [u8; 64] = [
    0x48, 0x8f, 0x92, 0x77, 0xcb, 0x20, 0x29, 0xaa, 0xfd, 0x34, 0x38, 0x6e, 0xc5, 0xf3, 0xf1, 0xea,
    0x09, 0x28, 0x89, 0x6a, 0x36, 0x31, 0x54, 0x09, 0xa2, 0xc0, 0x7f, 0xbf, 0x89, 0xc6, 0x7c, 0xf9,
    0xa0, 0x94, 0xe2, 0xf3, 0xdc, 0x40, 0xe5, 0x12, 0x5c, 0x73, 0x33, 0x38, 0x69, 0x62, 0x47, 0x19,
    0x3e, 0xb9, 0x15, 0x63, 0x4b, 0x1e, 0x8a, 0x39, 0x63, 0x08, 0x68, 0xda, 0x57, 0xe3, 0xff, 0x06,
];

// ===== Unicode/UTF-8 Message Test Vectors =====
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
// Note: secp256r1 Unicode signature is non-deterministic, stored for verification test
const SECP256R1_UNICODE_SIGNATURE: &[u8] = &[
    0x30, 0x44, 0x02, 0x20, 0x04, 0xd8, 0x8f, 0xa0, 0x61, 0x04, 0x18, 0xb8, 0xe3, 0xea, 0x11, 0x15,
    0x77, 0x86, 0x94, 0x4c, 0x3e, 0x8a, 0xd2, 0x09, 0x38, 0x81, 0xec, 0x21, 0x35, 0x00, 0x44, 0x72,
    0x4c, 0x22, 0xee, 0x64, 0x02, 0x20, 0x47, 0x01, 0x89, 0xb1, 0x10, 0x56, 0xd8, 0x2c, 0x06, 0xd4,
    0x91, 0xe4, 0x77, 0x28, 0xcd, 0x06, 0xc4, 0x02, 0x18, 0x7e, 0x2f, 0x8b, 0xd4, 0x04, 0xee, 0x44,
    0x41, 0xcc, 0xe0, 0xd8, 0xc9, 0xee,
];

// ===== Long Message Test Vectors =====
// 1KB message signature (message is 1024 'A' characters)
const ED25519_1KB_MESSAGE_SIGNATURE: [u8; 64] = [
    0x57, 0xc5, 0x3a, 0x0a, 0x6f, 0x75, 0xc4, 0x58, 0xac, 0xa5, 0xd7, 0x5e, 0xfa, 0x93, 0x1e, 0x4f,
    0x0f, 0xea, 0xb7, 0x03, 0xc2, 0x27, 0x9d, 0x4d, 0x42, 0xdd, 0x1a, 0xd0, 0x87, 0x70, 0x83, 0x95,
    0xa9, 0x08, 0xbb, 0x66, 0xbc, 0xdc, 0x95, 0xb3, 0xb0, 0x1e, 0x38, 0x64, 0x5c, 0xe0, 0x68, 0xc0,
    0x47, 0x88, 0x32, 0xd8, 0xd4, 0x81, 0xf9, 0x26, 0xb6, 0xdb, 0xdd, 0x0d, 0xd9, 0x91, 0x63, 0x0c,
];
const SECP256K1_1KB_MESSAGE_SIGNATURE: &[u8] = &[
    0x30, 0x44, 0x02, 0x20, 0x1e, 0x4d, 0xf4, 0x9b, 0xb5, 0x9c, 0x4a, 0xc1, 0x98, 0x96, 0xa3, 0x4a,
    0x48, 0x43, 0xa3, 0xa9, 0x3b, 0x2e, 0x91, 0xa8, 0x27, 0xbb, 0xbf, 0x91, 0xea, 0x03, 0x47, 0x28,
    0x05, 0xc6, 0x04, 0xcc, 0x02, 0x20, 0x78, 0x18, 0x30, 0xe2, 0x37, 0xd7, 0x35, 0x7f, 0x19, 0x85,
    0x90, 0x13, 0xcb, 0x5a, 0xdc, 0x9b, 0xf3, 0xee, 0x2e, 0x19, 0x62, 0x6d, 0x24, 0xbe, 0x14, 0x7e,
    0x2e, 0x0c, 0x2d, 0xe5, 0x4c, 0x22,
];

// ===== secp256k1 Test Vectors =====
const SECP256K1_PRIVATE_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];
#[allow(dead_code)]
const SECP256K1_PUBLIC_KEY_COMPRESSED: [u8; 33] = [
    0x02, 0x84, 0xbf, 0x75, 0x62, 0x26, 0x2b, 0xbd, 0x69, 0x40, 0x08, 0x57, 0x48, 0xf3, 0xbe, 0x6a,
    0xfa, 0x52, 0xae, 0x31, 0x71, 0x55, 0x18, 0x1e, 0xce, 0x31, 0xb6, 0x63, 0x51, 0xcc, 0xff, 0xa4,
    0xb0,
];
#[allow(dead_code)]
const SECP256K1_PUBLIC_KEY_UNCOMPRESSED: [u8; 65] = [
    0x04, 0x84, 0xbf, 0x75, 0x62, 0x26, 0x2b, 0xbd, 0x69, 0x40, 0x08, 0x57, 0x48, 0xf3, 0xbe, 0x6a,
    0xfa, 0x52, 0xae, 0x31, 0x71, 0x55, 0x18, 0x1e, 0xce, 0x31, 0xb6, 0x63, 0x51, 0xcc, 0xff, 0xa4,
    0xb0, 0x8c, 0xc4, 0x3d, 0x63, 0xb2, 0x85, 0x9d, 0x46, 0x9f, 0xee, 0x15, 0xf3, 0x1c, 0x9e, 0xdb,
    0x53, 0x24, 0x26, 0x6e, 0x6f, 0xd0, 0x40, 0x7e, 0x87, 0x38, 0x2d, 0x60, 0xfc, 0x45, 0x11, 0xac,
    0xd8,
];
const SECP256K1_TEST_MESSAGE: &[u8] = b"test message";
const SECP256K1_DID: &str =
    "did:key:z7r8or8ecagY9LD87s54K2arcXmgmw6bUhyvq83RrnB2hJiUb2ug5YGAk1ZUaimewnoLL1ZGzXuTCnWRSrRZgR3v2PLPH";
const SECP256K1_CURVE_ORDER: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
];

// ===== secp256r1 (P-256) Test Vectors =====

fn parse_der_integer(input: &[u8], offset: &mut usize) -> Vec<u8> {
    assert_eq!(input[*offset], 0x02, "DER integer tag expected");
    *offset += 1;

    let len = input[*offset] as usize;
    *offset += 1;

    let value = input[*offset..*offset + len].to_vec();
    *offset += len;
    value
}

fn left_pad_to_32(bytes: &[u8]) -> [u8; 32] {
    assert!(
        bytes.len() <= 32,
        "Expected integer to fit in 32 bytes, got {}",
        bytes.len()
    );

    let mut padded = [0u8; 32];
    padded[32 - bytes.len()..].copy_from_slice(bytes);
    padded
}

fn subtract_32(lhs: &[u8; 32], rhs: &[u8; 32]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut borrow = 0u16;

    for idx in (0..32).rev() {
        let lhs = lhs[idx] as u16;
        let rhs = rhs[idx] as u16 + borrow;

        if lhs >= rhs {
            out[idx] = (lhs - rhs) as u8;
            borrow = 0;
        } else {
            out[idx] = ((lhs + 256) - rhs) as u8;
            borrow = 1;
        }
    }

    assert_eq!(borrow, 0, "underflow while subtracting curve scalars");
    out
}

fn trim_leading_zeroes(bytes: &[u8; 32]) -> Vec<u8> {
    let first_non_zero = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len().saturating_sub(1));
    bytes[first_non_zero..].to_vec()
}

fn encode_der_integer(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 3);
    out.push(0x02);

    let needs_leading_zero = bytes.first().is_some_and(|byte| byte & 0x80 != 0);
    let len = bytes.len() + usize::from(needs_leading_zero);
    out.push(len as u8);

    if needs_leading_zero {
        out.push(0x00);
    }

    out.extend_from_slice(bytes);
    out
}

fn to_high_s_der(signature: &[u8]) -> Vec<u8> {
    assert_eq!(signature[0], 0x30, "DER sequence tag expected");

    let mut offset = 2;
    let r = parse_der_integer(signature, &mut offset);
    let s = parse_der_integer(signature, &mut offset);

    let s = left_pad_to_32(&s);
    let high_s = subtract_32(&SECP256K1_CURVE_ORDER, &s);
    let high_s = trim_leading_zeroes(&high_s);

    let r = encode_der_integer(&r);
    let high_s = encode_der_integer(&high_s);

    let mut der = Vec::with_capacity(2 + r.len() + high_s.len());
    der.push(0x30);
    der.push((r.len() + high_s.len()) as u8);
    der.extend_from_slice(&r);
    der.extend_from_slice(&high_s);
    der
}
// Generated from Go for Rust compatibility testing
#[allow(dead_code)]
const SECP256R1_PRIVATE_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
];
const SECP256R1_PUBLIC_KEY_COMPRESSED: [u8; 33] = [
    0x02, 0x51, 0x5c, 0x3d, 0x6e, 0xb9, 0xe3, 0x96, 0xb9, 0x04, 0xd3, 0xfe, 0xca, 0x7f, 0x54, 0xfd,
    0xcd, 0x0c, 0xc1, 0xe9, 0x97, 0xbf, 0x37, 0x5d, 0xca, 0x51, 0x5a, 0xd0, 0xa6, 0xc3, 0xb4, 0x03,
    0x5f,
];
#[allow(dead_code)]
const SECP256R1_PUBLIC_KEY_UNCOMPRESSED: [u8; 65] = [
    0x04, 0x51, 0x5c, 0x3d, 0x6e, 0xb9, 0xe3, 0x96, 0xb9, 0x04, 0xd3, 0xfe, 0xca, 0x7f, 0x54, 0xfd,
    0xcd, 0x0c, 0xc1, 0xe9, 0x97, 0xbf, 0x37, 0x5d, 0xca, 0x51, 0x5a, 0xd0, 0xa6, 0xc3, 0xb4, 0x03,
    0x5f, 0x45, 0x36, 0xbe, 0x3a, 0x50, 0xf3, 0x18, 0xfb, 0xf9, 0xa5, 0x47, 0x59, 0x02, 0xa2, 0x21,
    0x50, 0x2b, 0xef, 0x0d, 0x57, 0xe0, 0x8c, 0x53, 0xb2, 0xcc, 0x0a, 0x56, 0xf1, 0x7d, 0x9f, 0x93,
    0x54,
];
const SECP256R1_TEST_MESSAGE: &[u8] = b"test message";
// Note: secp256r1 signatures are NOT deterministic (unlike secp256k1 with RFC 6979)
// This specific signature was generated by Go and is used to verify Rust can verify it
const SECP256R1_SIGNATURE: &[u8] = &[
    0x30, 0x45, 0x02, 0x20, 0x3f, 0x7a, 0xdd, 0x48, 0x1c, 0x52, 0x73, 0x86, 0x5d, 0x4b, 0x15, 0xba,
    0xbb, 0xa8, 0x09, 0xc8, 0xf5, 0x7c, 0x69, 0x37, 0xe1, 0x6f, 0x5d, 0xc0, 0x4c, 0xa7, 0x22, 0xcb,
    0x34, 0x5c, 0xed, 0xd7, 0x02, 0x21, 0x00, 0xe7, 0x2e, 0x19, 0xdc, 0xa1, 0xf8, 0xfa, 0x94, 0x6e,
    0xd8, 0xf1, 0xc5, 0x18, 0x7c, 0xb7, 0xaa, 0x3b, 0x7d, 0xe3, 0x17, 0x72, 0xba, 0xab, 0xdd, 0x93,
    0x86, 0x6d, 0xba, 0xff, 0xb4, 0x4a, 0xec,
];
const SECP256R1_DID: &str =
    "did:key:z4oJ8bQWmdRbkhWsbC85S7BkLD7dfZ2tm3eZ2mtA6C4j3Si19XLij1UD1qzaFYQM9fC7x1Yh2PMdnGkM8PoBnndDLwzHH";

// ===== secp256r1 Edge Case Test Vectors =====
// Empty message signature (using same key as SECP256R1_PRIVATE_KEY)
const SECP256R1_EMPTY_MESSAGE: &[u8] = b"";
const SECP256R1_EMPTY_MESSAGE_SIGNATURE: &[u8] = &[
    0x30, 0x45, 0x02, 0x21, 0x00, 0xa3, 0xe3, 0x82, 0xd5, 0xbb, 0x5a, 0x15, 0x47, 0xd6, 0x79, 0x66,
    0x50, 0x31, 0x4d, 0x4f, 0xf8, 0xfe, 0x4e, 0xd7, 0x89, 0x78, 0xd8, 0xf2, 0x02, 0x29, 0x37, 0x27,
    0x2f, 0xae, 0xba, 0x87, 0xb3, 0x02, 0x20, 0x75, 0x5f, 0x43, 0x99, 0x3b, 0xb3, 0x5b, 0xb7, 0x41,
    0x75, 0x3c, 0xea, 0x8a, 0x2d, 0x16, 0x26, 0x13, 0x76, 0x75, 0xa6, 0x65, 0xaf, 0x06, 0xf2, 0xb1,
    0xaa, 0x48, 0xf0, 0xc3, 0x42, 0xde, 0xca,
];
// Binary message with null bytes
const SECP256R1_BINARY_MESSAGE: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00];
const SECP256R1_BINARY_MESSAGE_SIGNATURE: &[u8] = &[
    0x30, 0x46, 0x02, 0x21, 0x00, 0xfd, 0x7b, 0x34, 0x1f, 0xf7, 0x4b, 0xf0, 0x31, 0x5a, 0x90, 0x26,
    0x39, 0xf0, 0xaf, 0xb4, 0x28, 0x1a, 0x8b, 0x42, 0x9f, 0x61, 0xe6, 0x10, 0x30, 0xc4, 0xd3, 0xc8,
    0x7a, 0xf1, 0x0c, 0xfe, 0xac, 0x02, 0x21, 0x00, 0xa8, 0x9f, 0xd0, 0xc0, 0xc2, 0xdf, 0x07, 0xd6,
    0xef, 0xb2, 0xf9, 0x66, 0x0d, 0xfc, 0xc8, 0x0f, 0xd1, 0x01, 0x9a, 0xd2, 0x7c, 0xe8, 0x59, 0x8c,
    0x2f, 0xba, 0x16, 0x16, 0x36, 0x50, 0x3b, 0x30,
];
// 1KB message (1024 'A' characters)
const SECP256R1_1KB_MESSAGE_SIGNATURE: &[u8] = &[
    0x30, 0x45, 0x02, 0x21, 0x00, 0xb2, 0xaa, 0x12, 0x1d, 0xaa, 0xd5, 0xa4, 0xc7, 0x89, 0x7d, 0x56,
    0xc8, 0x2f, 0xc6, 0x57, 0x82, 0xfe, 0xf4, 0x19, 0xc0, 0x3d, 0xdc, 0xfd, 0xe2, 0xf7, 0xe5, 0xd7,
    0xbf, 0x33, 0xde, 0xce, 0x7c, 0x02, 0x20, 0x52, 0x0a, 0x2e, 0xde, 0xdd, 0xe8, 0xaf, 0x9e, 0x87,
    0xa4, 0xc3, 0x16, 0x08, 0xbe, 0x2e, 0xc5, 0xe0, 0xce, 0x24, 0x31, 0xbc, 0x38, 0x4d, 0xd9, 0x94,
    0xf3, 0x9c, 0xe3, 0x54, 0xfd, 0x20, 0xa4,
];

// ===== secp256k1 Edge Case Test Vectors =====
// Empty message signature (using same key as SECP256K1_PRIVATE_KEY)
const SECP256K1_EMPTY_MESSAGE: &[u8] = b"";
const SECP256K1_EMPTY_MESSAGE_SIGNATURE: &[u8] = &[
    0x30, 0x44, 0x02, 0x20, 0x0c, 0x3d, 0xec, 0xb3, 0x81, 0x70, 0x9d, 0x58, 0xc4, 0x3f, 0x8d, 0x6e,
    0x18, 0x89, 0x7d, 0xed, 0x02, 0x87, 0x44, 0x4e, 0x0a, 0xbc, 0xed, 0xa8, 0xa7, 0xb6, 0x48, 0x1b,
    0x48, 0x71, 0xc6, 0x4d, 0x02, 0x20, 0x36, 0x67, 0xf8, 0xa8, 0x34, 0x2f, 0x13, 0x69, 0x07, 0x68,
    0xf3, 0xcb, 0xee, 0x0f, 0xf5, 0xa1, 0x08, 0xd7, 0x05, 0xd7, 0x1e, 0xc1, 0xac, 0x9a, 0x7c, 0x24,
    0x2c, 0xd6, 0xe1, 0x98, 0xbc, 0x07,
];
// Binary message with null bytes
const SECP256K1_BINARY_MESSAGE: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00];
const SECP256K1_BINARY_MESSAGE_SIGNATURE: &[u8] = &[
    0x30, 0x44, 0x02, 0x20, 0x58, 0xc4, 0x3d, 0xad, 0x4c, 0xfd, 0xd4, 0xff, 0x6c, 0xbf, 0xab, 0x3c,
    0x04, 0xf4, 0xb2, 0x28, 0xed, 0x19, 0xf4, 0x53, 0xd2, 0xdb, 0xbb, 0xda, 0xf8, 0x1b, 0x1c, 0x8b,
    0x76, 0x8e, 0x00, 0xbc, 0x02, 0x20, 0x4b, 0x2d, 0x12, 0x34, 0x05, 0x10, 0xed, 0xe2, 0x4e, 0xcb,
    0x6b, 0x00, 0xcd, 0xa1, 0x49, 0x07, 0xa0, 0x77, 0x3d, 0x19, 0x5c, 0xc3, 0x98, 0x4a, 0x84, 0xb8,
    0xca, 0x0b, 0xb8, 0xa0, 0xb3, 0x52,
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

#[test]
fn test_parse_go_ed25519_did() {
    use crypto::did::parse_did_key;
    use crypto::types::KeyType;

    // Parse Go-generated DID
    let (key_type, public_key_bytes) = parse_did_key(ED25519_DID).unwrap();

    // Verify key type
    assert_eq!(key_type, KeyType::Ed25519);

    // Verify public key bytes match the expected Ed25519 public key
    assert_eq!(public_key_bytes, ED25519_PUBLIC_KEY.to_vec());
}

// ===== Ed25519 Edge Case Tests =====

#[test]
fn test_ed25519_empty_message_signature_from_go() {
    let public_key =
        Ed25519PublicKey::from_bytes(&ED25519_PUBLIC_KEY).expect("Should parse Go public key");

    let valid = public_key
        .verify(ED25519_EMPTY_MESSAGE, &ED25519_EMPTY_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated Ed25519 empty message signature"
    );
}

#[test]
fn test_ed25519_empty_message_signature_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();

    let rust_signature = private_key.sign(ED25519_EMPTY_MESSAGE).unwrap();
    assert_eq!(
        rust_signature.as_slice(),
        ED25519_EMPTY_MESSAGE_SIGNATURE,
        "Rust Ed25519 empty message signature should match Go"
    );
}

#[test]
fn test_ed25519_binary_message_signature_from_go() {
    let public_key =
        Ed25519PublicKey::from_bytes(&ED25519_PUBLIC_KEY).expect("Should parse Go public key");

    let valid = public_key
        .verify(ED25519_BINARY_MESSAGE, &ED25519_BINARY_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated Ed25519 binary message signature"
    );
}

#[test]
fn test_ed25519_binary_message_signature_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();

    let rust_signature = private_key.sign(ED25519_BINARY_MESSAGE).unwrap();
    assert_eq!(
        rust_signature.as_slice(),
        ED25519_BINARY_MESSAGE_SIGNATURE,
        "Rust Ed25519 binary message signature should match Go"
    );
}

#[test]
fn test_ed25519_single_byte_signature_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();

    let rust_signature = private_key.sign(ED25519_SINGLE_BYTE_MESSAGE).unwrap();
    assert_eq!(
        rust_signature.as_slice(),
        ED25519_SINGLE_BYTE_SIGNATURE,
        "Rust Ed25519 single byte signature should match Go"
    );
}

// ===== Unicode/UTF-8 Message Tests =====

#[test]
fn test_ed25519_unicode_message_signature_from_go() {
    let public_key =
        Ed25519PublicKey::from_bytes(&ED25519_PUBLIC_KEY).expect("Should parse Go public key");

    let valid = public_key
        .verify(UNICODE_MESSAGE.as_bytes(), &ED25519_UNICODE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated Ed25519 Unicode signature"
    );
}

#[test]
fn test_ed25519_unicode_message_signature_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();

    let rust_signature = private_key.sign(UNICODE_MESSAGE.as_bytes()).unwrap();
    assert_eq!(
        rust_signature.as_slice(),
        ED25519_UNICODE_SIGNATURE,
        "Rust Ed25519 Unicode signature should match Go"
    );
}

#[test]
fn test_secp256k1_unicode_message_signature_from_go() {
    let public_key = Secp256k1PublicKey::from_bytes(&SECP256K1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let valid = public_key
        .verify(UNICODE_MESSAGE.as_bytes(), SECP256K1_UNICODE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated secp256k1 Unicode signature"
    );
}

#[test]
fn test_secp256k1_unicode_message_signature_matches_go() {
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();

    let rust_signature = private_key.sign(UNICODE_MESSAGE.as_bytes()).unwrap();
    assert_eq!(
        rust_signature, SECP256K1_UNICODE_SIGNATURE,
        "Rust secp256k1 Unicode signature should match Go"
    );
}

#[test]
fn test_secp256r1_unicode_message_signature_from_go() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let valid = public_key
        .verify(UNICODE_MESSAGE.as_bytes(), SECP256R1_UNICODE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated secp256r1 Unicode signature"
    );
}

// ===== Long Message Tests =====

#[test]
fn test_ed25519_1kb_message_signature_from_go() {
    let public_key =
        Ed25519PublicKey::from_bytes(&ED25519_PUBLIC_KEY).expect("Should parse Go public key");

    // Generate 1KB message (1024 'A' characters)
    let message = vec![b'A'; 1024];

    let valid = public_key
        .verify(&message, &ED25519_1KB_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated Ed25519 1KB message signature"
    );
}

#[test]
fn test_ed25519_1kb_message_signature_matches_go() {
    let private_key = Ed25519PrivateKey::from_bytes(&ED25519_PRIVATE_KEY).unwrap();

    let message = vec![b'A'; 1024];
    let rust_signature = private_key.sign(&message).unwrap();
    assert_eq!(
        rust_signature.as_slice(),
        ED25519_1KB_MESSAGE_SIGNATURE,
        "Rust Ed25519 1KB signature should match Go"
    );
}

#[test]
fn test_secp256k1_1kb_message_signature_from_go() {
    let public_key = Secp256k1PublicKey::from_bytes(&SECP256K1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let message = vec![b'A'; 1024];

    let valid = public_key
        .verify(&message, SECP256K1_1KB_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated secp256k1 1KB message signature"
    );
}

#[test]
fn test_secp256k1_1kb_message_signature_matches_go() {
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();

    let message = vec![b'A'; 1024];
    let rust_signature = private_key.sign(&message).unwrap();
    assert_eq!(
        rust_signature, SECP256K1_1KB_MESSAGE_SIGNATURE,
        "Rust secp256k1 1KB signature should match Go"
    );
}

// ===== Signature Low-S Normalization Tests =====
// Both Go (btcd/btcec) and Rust (k256) produce low-S normalized signatures
// This is important for malleability protection (BIP-62, BIP-146)

#[test]
fn test_secp256k1_signature_is_low_s_normalized() {
    // The secp256k1 curve order N
    // N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
    // Half N = N/2 = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0

    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();

    // Sign multiple messages and verify all signatures have low-S values
    let messages = [
        b"test message".as_slice(),
        b"another message".as_slice(),
        &[0u8; 32],
        &[0xffu8; 32],
    ];

    for msg in messages {
        let signature = private_key.sign(msg).unwrap();

        // DER format: 0x30 <len> 0x02 <r_len> <r> 0x02 <s_len> <s>
        // Extract S value from DER signature
        assert!(signature.len() >= 8, "Signature too short");
        assert_eq!(signature[0], 0x30, "Not a DER sequence");

        let r_len = signature[3] as usize;
        let s_offset = 4 + r_len + 1; // Skip: 0x30 <len> 0x02 <r_len> <r> 0x02
        let s_len = signature[s_offset] as usize;
        let s_bytes = &signature[s_offset + 1..s_offset + 1 + s_len];

        // S should be positive (no leading 0x00 needed for positive)
        // If S starts with 0x00, it means the value would be negative without it
        // Low-S means S <= N/2, which for secp256k1 means S[0] should be <= 0x7F
        // (after stripping any leading zeros that are just for DER positivity)
        let s_first_significant = if s_bytes[0] == 0x00 && s_bytes.len() > 1 {
            s_bytes[1]
        } else {
            s_bytes[0]
        };

        // For low-S, the first significant byte should indicate a value <= N/2
        // N/2 starts with 0x7F..., so valid low-S values have first byte <= 0x7F
        assert!(
            s_first_significant <= 0x7F,
            "Signature S value is not low-S normalized for message {:?}. First S byte: 0x{:02x}",
            msg,
            s_first_significant
        );
    }
}

#[test]
fn test_secp256k1_go_signatures_are_low_s() {
    // Verify all Go-generated signatures in our test vectors are low-S normalized

    let signatures: &[&[u8]] = &[
        SECP256K1_EMPTY_MESSAGE_SIGNATURE,
        SECP256K1_BINARY_MESSAGE_SIGNATURE,
        SECP256K1_UNICODE_SIGNATURE,
        SECP256K1_1KB_MESSAGE_SIGNATURE,
    ];

    for sig in signatures {
        let r_len = sig[3] as usize;
        let s_offset = 4 + r_len + 1;
        let s_len = sig[s_offset] as usize;
        let s_bytes = &sig[s_offset + 1..s_offset + 1 + s_len];

        let s_first_significant = if s_bytes[0] == 0x00 && s_bytes.len() > 1 {
            s_bytes[1]
        } else {
            s_bytes[0]
        };

        assert!(
            s_first_significant <= 0x7F,
            "Go signature is not low-S normalized. First S byte: 0x{:02x}",
            s_first_significant
        );
    }
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
        0x30, 0x44, 0x02, 0x20, 0x3d, 0x46, 0x09, 0xf4, 0xd7, 0x62, 0x05, 0xd3, 0x49, 0x16, 0x0f,
        0xf7, 0x90, 0x4c, 0xf9, 0x14, 0x38, 0xe0, 0xbb, 0x5f, 0x9b, 0x98, 0x42, 0xc2, 0x8b, 0x4e,
        0x9d, 0xe7, 0x6b, 0x28, 0x36, 0xf8, 0x02, 0x20, 0x2e, 0xe2, 0x7f, 0x4e, 0x70, 0x62, 0x1e,
        0x98, 0x55, 0xd7, 0x92, 0x68, 0xaf, 0x70, 0x95, 0x46, 0x18, 0x05, 0x34, 0x19, 0x99, 0x0a,
        0x6c, 0x09, 0xcf, 0x71, 0x52, 0xc5, 0x30, 0x15, 0x6a, 0xf0,
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
fn test_secp256k1_high_s_signature_is_rejected() {
    let public_key = Secp256k1PublicKey::from_bytes(&SECP256K1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let low_s_signature: &[u8] = &[
        0x30, 0x44, 0x02, 0x20, 0x3d, 0x46, 0x09, 0xf4, 0xd7, 0x62, 0x05, 0xd3, 0x49, 0x16, 0x0f,
        0xf7, 0x90, 0x4c, 0xf9, 0x14, 0x38, 0xe0, 0xbb, 0x5f, 0x9b, 0x98, 0x42, 0xc2, 0x8b, 0x4e,
        0x9d, 0xe7, 0x6b, 0x28, 0x36, 0xf8, 0x02, 0x20, 0x2e, 0xe2, 0x7f, 0x4e, 0x70, 0x62, 0x1e,
        0x98, 0x55, 0xd7, 0x92, 0x68, 0xaf, 0x70, 0x95, 0x46, 0x18, 0x05, 0x34, 0x19, 0x99, 0x0a,
        0x6c, 0x09, 0xcf, 0x71, 0x52, 0xc5, 0x30, 0x15, 0x6a, 0xf0,
    ];

    let high_s_signature = to_high_s_der(low_s_signature);
    assert_ne!(high_s_signature, low_s_signature);
    let high_s = Signature::from_der(&high_s_signature).expect("High-S variant should parse");
    assert!(
        high_s.normalize_s().is_some(),
        "Derived signature must actually be high-S"
    );
    assert_eq!(
        high_s
            .normalize_s()
            .expect("High-S signature should normalize")
            .to_der()
            .as_bytes(),
        low_s_signature,
        "High-S variant should be mathematically equivalent to the known-valid low-S signature"
    );

    let valid = public_key
        .verify(SECP256K1_TEST_MESSAGE, &high_s_signature)
        .unwrap();

    assert!(
        !valid,
        "Verifier must reject high-S secp256k1 signatures instead of normalizing them"
    );
}

#[test]
fn test_secp256k1_did_matches_go() {
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY).unwrap();
    let public_key = private_key.public_key();

    let did = public_key.did().unwrap();
    assert_eq!(did, SECP256K1_DID, "secp256k1 DID should match Go");
}

#[test]
fn test_parse_go_secp256k1_did() {
    use crypto::did::parse_did_key;
    use crypto::types::KeyType;

    // Parse Go-generated DID
    let (key_type, public_key_bytes) = parse_did_key(SECP256K1_DID).unwrap();

    // Verify key type
    assert_eq!(key_type, KeyType::Secp256k1);

    // secp256k1 DID uses uncompressed key (65 bytes)
    assert_eq!(public_key_bytes.len(), 65);
    assert_eq!(public_key_bytes, SECP256K1_PUBLIC_KEY_UNCOMPRESSED.to_vec());
}

#[test]
fn test_secp256k1_empty_message_signature_from_go() {
    let public_key = Secp256k1PublicKey::from_bytes(&SECP256K1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let valid = public_key
        .verify(SECP256K1_EMPTY_MESSAGE, SECP256K1_EMPTY_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated secp256k1 empty message signature"
    );
}

#[test]
fn test_secp256k1_binary_message_signature_from_go() {
    let public_key = Secp256k1PublicKey::from_bytes(&SECP256K1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let valid = public_key
        .verify(SECP256K1_BINARY_MESSAGE, SECP256K1_BINARY_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Should verify Go-generated secp256k1 binary message signature"
    );
}

#[test]
fn test_secp256k1_empty_message_signature_matches_go() {
    // Verify Rust produces the same signature as Go for empty message (RFC 6979 deterministic)
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY)
        .expect("Should parse Go private key");

    let rust_signature = private_key.sign(SECP256K1_EMPTY_MESSAGE).unwrap();
    assert_eq!(
        rust_signature, SECP256K1_EMPTY_MESSAGE_SIGNATURE,
        "Rust secp256k1 empty message signature should match Go"
    );
}

#[test]
fn test_secp256k1_binary_message_signature_matches_go() {
    // Verify Rust produces the same signature as Go for binary message (RFC 6979 deterministic)
    let private_key = Secp256k1PrivateKey::from_bytes(&SECP256K1_PRIVATE_KEY)
        .expect("Should parse Go private key");

    let rust_signature = private_key.sign(SECP256K1_BINARY_MESSAGE).unwrap();
    assert_eq!(
        rust_signature, SECP256K1_BINARY_MESSAGE_SIGNATURE,
        "Rust secp256k1 binary message signature should match Go"
    );
}

// ===== secp256r1 (P-256) Compatibility Tests =====

#[test]
fn test_secp256r1_public_key_from_go_bytes_compressed() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go secp256r1 compressed public key");

    // Verify it stored as compressed format
    assert_eq!(
        public_key.raw(),
        SECP256R1_PUBLIC_KEY_COMPRESSED.to_vec(),
        "secp256r1 public key should be in compressed format"
    );
}

#[test]
fn test_secp256r1_public_key_from_go_bytes_uncompressed() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_UNCOMPRESSED)
        .expect("Should parse Go secp256r1 uncompressed public key");

    // Uncompressed input should be stored as compressed
    assert_eq!(
        public_key.raw(),
        SECP256R1_PUBLIC_KEY_COMPRESSED.to_vec(),
        "secp256r1 uncompressed key should convert to compressed format"
    );
}

#[test]
fn test_secp256r1_signature_verification_from_go() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    // Verify Go-generated signature
    let valid = public_key
        .verify(SECP256R1_TEST_MESSAGE, SECP256R1_SIGNATURE)
        .unwrap();
    assert!(valid, "Should verify Go-generated secp256r1 signature");
}

#[test]
fn test_secp256r1_did_matches_go() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let did = public_key.did().unwrap();
    assert_eq!(did, SECP256R1_DID, "secp256r1 DID should match Go");
}

#[test]
fn test_parse_go_secp256r1_did() {
    use crypto::did::parse_did_key;
    use crypto::types::KeyType;

    // Parse Go-generated DID
    let (key_type, public_key_bytes) = parse_did_key(SECP256R1_DID).unwrap();

    // Verify key type
    assert_eq!(key_type, KeyType::Secp256r1);

    // secp256r1 DID uses uncompressed key (65 bytes)
    assert_eq!(public_key_bytes.len(), 65);
    assert_eq!(public_key_bytes, SECP256R1_PUBLIC_KEY_UNCOMPRESSED.to_vec());
}

#[test]
fn test_secp256r1_signature_verification_rejects_wrong_message() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    // Verify Go-generated signature fails with wrong message
    let valid = public_key
        .verify(b"wrong message", SECP256R1_SIGNATURE)
        .unwrap();
    assert!(!valid, "Should reject signature with wrong message");
}

#[test]
fn test_secp256r1_signature_verification_rejects_tampered_signature() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    // Tamper with signature
    let mut tampered_sig = SECP256R1_SIGNATURE.to_vec();
    tampered_sig[20] ^= 0x01;

    let valid = public_key
        .verify(SECP256R1_TEST_MESSAGE, &tampered_sig)
        .unwrap();
    assert!(!valid, "Should reject tampered signature");
}

#[test]
fn test_secp256r1_empty_message_signature_from_go() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let valid = public_key
        .verify(SECP256R1_EMPTY_MESSAGE, SECP256R1_EMPTY_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Rust should verify Go-generated secp256r1 empty message signature"
    );
}

#[test]
fn test_secp256r1_binary_message_signature_from_go() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    let valid = public_key
        .verify(SECP256R1_BINARY_MESSAGE, SECP256R1_BINARY_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Rust should verify Go-generated secp256r1 binary message signature"
    );
}

#[test]
fn test_secp256r1_1kb_message_signature_from_go() {
    let public_key = Secp256r1PublicKey::from_bytes(&SECP256R1_PUBLIC_KEY_COMPRESSED)
        .expect("Should parse Go public key");

    // 1KB message (1024 'A' characters)
    let message: Vec<u8> = vec![b'A'; 1024];

    let valid = public_key
        .verify(&message, SECP256R1_1KB_MESSAGE_SIGNATURE)
        .unwrap();
    assert!(
        valid,
        "Rust should verify Go-generated secp256r1 1KB message signature"
    );
}
