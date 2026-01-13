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
    use crate::keys::secp256r1::Secp256r1PublicKey;
    use crate::keys::{Key, PrivateKey, PublicKey};
    use hkdf::Hkdf;
    use serial_test::serial;
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

    // ===== Ed25519 Edge Case Test Vectors =====
    // Empty message signature
    const ED25519_EMPTY_MESSAGE: &[u8] = b"";
    const ED25519_EMPTY_MESSAGE_SIGNATURE: [u8; 64] = [
        0xe5, 0x56, 0x43, 0x00, 0xc3, 0x60, 0xac, 0x72, 0x90, 0x86, 0xe2, 0xcc, 0x80, 0x6e, 0x82,
        0x8a, 0x84, 0x87, 0x7f, 0x1e, 0xb8, 0xe5, 0xd9, 0x74, 0xd8, 0x73, 0xe0, 0x65, 0x22, 0x49,
        0x01, 0x55, 0x5f, 0xb8, 0x82, 0x15, 0x90, 0xa3, 0x3b, 0xac, 0xc6, 0x1e, 0x39, 0x70, 0x1c,
        0xf9, 0xb4, 0x6b, 0xd2, 0x5b, 0xf5, 0xf0, 0x59, 0x5b, 0xbe, 0x24, 0x65, 0x51, 0x41, 0x43,
        0x8e, 0x7a, 0x10, 0x0b,
    ];
    // Binary message with null bytes
    const ED25519_BINARY_MESSAGE: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00];
    const ED25519_BINARY_MESSAGE_SIGNATURE: [u8; 64] = [
        0x1d, 0x00, 0x16, 0x1e, 0x72, 0xf5, 0x6f, 0x9a, 0x20, 0x39, 0x0a, 0x53, 0xad, 0x64, 0xc5,
        0xec, 0x36, 0x0c, 0x6a, 0xae, 0xb8, 0x2a, 0x27, 0xa8, 0x82, 0x00, 0x27, 0x31, 0xc1, 0x41,
        0xe2, 0x76, 0x77, 0x73, 0xac, 0xd2, 0xbc, 0x31, 0xae, 0x66, 0xd7, 0x7f, 0x09, 0x91, 0xdf,
        0x20, 0x45, 0x26, 0xad, 0x28, 0xe4, 0xac, 0x2b, 0x37, 0x37, 0xaf, 0xc6, 0x9d, 0x1c, 0x0f,
        0xce, 0x0e, 0x76, 0x03,
    ];
    // Single byte message
    const ED25519_SINGLE_BYTE_MESSAGE: &[u8] = &[0x42];
    const ED25519_SINGLE_BYTE_SIGNATURE: [u8; 64] = [
        0x48, 0x8f, 0x92, 0x77, 0xcb, 0x20, 0x29, 0xaa, 0xfd, 0x34, 0x38, 0x6e, 0xc5, 0xf3, 0xf1,
        0xea, 0x09, 0x28, 0x89, 0x6a, 0x36, 0x31, 0x54, 0x09, 0xa2, 0xc0, 0x7f, 0xbf, 0x89, 0xc6,
        0x7c, 0xf9, 0xa0, 0x94, 0xe2, 0xf3, 0xdc, 0x40, 0xe5, 0x12, 0x5c, 0x73, 0x33, 0x38, 0x69,
        0x62, 0x47, 0x19, 0x3e, 0xb9, 0x15, 0x63, 0x4b, 0x1e, 0x8a, 0x39, 0x63, 0x08, 0x68, 0xda,
        0x57, 0xe3, 0xff, 0x06,
    ];

    // ===== Unicode/UTF-8 Message Test Vectors =====
    const UNICODE_MESSAGE: &str = "Hello, 世界! 🌍 Привет мир";
    const ED25519_UNICODE_SIGNATURE: [u8; 64] = [
        0x47, 0xcb, 0x3b, 0x89, 0x42, 0x41, 0x3e, 0x3e, 0x5c, 0xb8, 0x21, 0xdc, 0x85, 0xce, 0xc2,
        0xed, 0xdd, 0x1b, 0x54, 0x51, 0xe4, 0x77, 0x17, 0x3b, 0xa6, 0x74, 0x63, 0x6d, 0x1b, 0x12,
        0x2c, 0xa2, 0xfe, 0xed, 0x69, 0xa3, 0xa3, 0x81, 0xd5, 0x2f, 0x52, 0xc6, 0x7c, 0xa6, 0x4b,
        0x03, 0x53, 0x17, 0x7f, 0x6a, 0x0f, 0xed, 0xbc, 0x0c, 0x9b, 0xbe, 0xea, 0x47, 0x0e, 0x06,
        0xa4, 0xea, 0x9e, 0x06,
    ];
    const SECP256K1_UNICODE_SIGNATURE: &[u8] = &[
        0x30, 0x44, 0x02, 0x20, 0x17, 0xac, 0xd0, 0xf1, 0x54, 0x41, 0x0f, 0x34, 0x6c, 0x02, 0xdb,
        0xea, 0xf9, 0x4c, 0xac, 0x64, 0x7b, 0x3f, 0x64, 0x18, 0x23, 0x01, 0xe1, 0x2c, 0x90, 0xa9,
        0x9f, 0x0a, 0x61, 0xec, 0x48, 0xf8, 0x02, 0x20, 0x6b, 0x72, 0x9e, 0xea, 0x68, 0x2c, 0x91,
        0x7f, 0x48, 0x7e, 0xc3, 0x66, 0xbd, 0x62, 0x06, 0xe6, 0xd5, 0xc5, 0x5f, 0x89, 0x8f, 0xf9,
        0x62, 0x50, 0x2f, 0xaf, 0x2a, 0x21, 0x41, 0x5e, 0x38, 0x7b,
    ];
    // Note: secp256r1 Unicode signature is non-deterministic, stored for verification test
    const SECP256R1_UNICODE_SIGNATURE: &[u8] = &[
        0x30, 0x44, 0x02, 0x20, 0x04, 0xd8, 0x8f, 0xa0, 0x61, 0x04, 0x18, 0xb8, 0xe3, 0xea, 0x11,
        0x15, 0x77, 0x86, 0x94, 0x4c, 0x3e, 0x8a, 0xd2, 0x09, 0x38, 0x81, 0xec, 0x21, 0x35, 0x00,
        0x44, 0x72, 0x4c, 0x22, 0xee, 0x64, 0x02, 0x20, 0x47, 0x01, 0x89, 0xb1, 0x10, 0x56, 0xd8,
        0x2c, 0x06, 0xd4, 0x91, 0xe4, 0x77, 0x28, 0xcd, 0x06, 0xc4, 0x02, 0x18, 0x7e, 0x2f, 0x8b,
        0xd4, 0x04, 0xee, 0x44, 0x41, 0xcc, 0xe0, 0xd8, 0xc9, 0xee,
    ];

    // ===== Long Message Test Vectors =====
    // 1KB message signature (message is 1024 'A' characters)
    const ED25519_1KB_MESSAGE_SIGNATURE: [u8; 64] = [
        0x57, 0xc5, 0x3a, 0x0a, 0x6f, 0x75, 0xc4, 0x58, 0xac, 0xa5, 0xd7, 0x5e, 0xfa, 0x93, 0x1e,
        0x4f, 0x0f, 0xea, 0xb7, 0x03, 0xc2, 0x27, 0x9d, 0x4d, 0x42, 0xdd, 0x1a, 0xd0, 0x87, 0x70,
        0x83, 0x95, 0xa9, 0x08, 0xbb, 0x66, 0xbc, 0xdc, 0x95, 0xb3, 0xb0, 0x1e, 0x38, 0x64, 0x5c,
        0xe0, 0x68, 0xc0, 0x47, 0x88, 0x32, 0xd8, 0xd4, 0x81, 0xf9, 0x26, 0xb6, 0xdb, 0xdd, 0x0d,
        0xd9, 0x91, 0x63, 0x0c,
    ];
    const SECP256K1_1KB_MESSAGE_SIGNATURE: &[u8] = &[
        0x30, 0x44, 0x02, 0x20, 0x1e, 0x4d, 0xf4, 0x9b, 0xb5, 0x9c, 0x4a, 0xc1, 0x98, 0x96, 0xa3,
        0x4a, 0x48, 0x43, 0xa3, 0xa9, 0x3b, 0x2e, 0x91, 0xa8, 0x27, 0xbb, 0xbf, 0x91, 0xea, 0x03,
        0x47, 0x28, 0x05, 0xc6, 0x04, 0xcc, 0x02, 0x20, 0x78, 0x18, 0x30, 0xe2, 0x37, 0xd7, 0x35,
        0x7f, 0x19, 0x85, 0x90, 0x13, 0xcb, 0x5a, 0xdc, 0x9b, 0xf3, 0xee, 0x2e, 0x19, 0x62, 0x6d,
        0x24, 0xbe, 0x14, 0x7e, 0x2e, 0x0c, 0x2d, 0xe5, 0x4c, 0x22,
    ];

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

    // ===== secp256r1 (P-256) Test Vectors =====
    // Generated from Go for Rust compatibility testing
    #[allow(dead_code)]
    const SECP256R1_PRIVATE_KEY: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];
    const SECP256R1_PUBLIC_KEY_COMPRESSED: [u8; 33] = [
        0x02, 0x51, 0x5c, 0x3d, 0x6e, 0xb9, 0xe3, 0x96, 0xb9, 0x04, 0xd3, 0xfe, 0xca, 0x7f, 0x54,
        0xfd, 0xcd, 0x0c, 0xc1, 0xe9, 0x97, 0xbf, 0x37, 0x5d, 0xca, 0x51, 0x5a, 0xd0, 0xa6, 0xc3,
        0xb4, 0x03, 0x5f,
    ];
    #[allow(dead_code)]
    const SECP256R1_PUBLIC_KEY_UNCOMPRESSED: [u8; 65] = [
        0x04, 0x51, 0x5c, 0x3d, 0x6e, 0xb9, 0xe3, 0x96, 0xb9, 0x04, 0xd3, 0xfe, 0xca, 0x7f, 0x54,
        0xfd, 0xcd, 0x0c, 0xc1, 0xe9, 0x97, 0xbf, 0x37, 0x5d, 0xca, 0x51, 0x5a, 0xd0, 0xa6, 0xc3,
        0xb4, 0x03, 0x5f, 0x45, 0x36, 0xbe, 0x3a, 0x50, 0xf3, 0x18, 0xfb, 0xf9, 0xa5, 0x47, 0x59,
        0x02, 0xa2, 0x21, 0x50, 0x2b, 0xef, 0x0d, 0x57, 0xe0, 0x8c, 0x53, 0xb2, 0xcc, 0x0a, 0x56,
        0xf1, 0x7d, 0x9f, 0x93, 0x54,
    ];
    const SECP256R1_TEST_MESSAGE: &[u8] = b"test message";
    // Note: secp256r1 signatures are NOT deterministic (unlike secp256k1 with RFC 6979)
    // This specific signature was generated by Go and is used to verify Rust can verify it
    const SECP256R1_SIGNATURE: &[u8] = &[
        0x30, 0x45, 0x02, 0x20, 0x3f, 0x7a, 0xdd, 0x48, 0x1c, 0x52, 0x73, 0x86, 0x5d, 0x4b, 0x15,
        0xba, 0xbb, 0xa8, 0x09, 0xc8, 0xf5, 0x7c, 0x69, 0x37, 0xe1, 0x6f, 0x5d, 0xc0, 0x4c, 0xa7,
        0x22, 0xcb, 0x34, 0x5c, 0xed, 0xd7, 0x02, 0x21, 0x00, 0xe7, 0x2e, 0x19, 0xdc, 0xa1, 0xf8,
        0xfa, 0x94, 0x6e, 0xd8, 0xf1, 0xc5, 0x18, 0x7c, 0xb7, 0xaa, 0x3b, 0x7d, 0xe3, 0x17, 0x72,
        0xba, 0xab, 0xdd, 0x93, 0x86, 0x6d, 0xba, 0xff, 0xb4, 0x4a, 0xec,
    ];
    const SECP256R1_DID: &str =
        "did:key:z4oJ8bQWmdRbkhWsbC85S7BkLD7dfZ2tm3eZ2mtA6C4j3Si19XLij1UD1qzaFYQM9fC7x1Yh2PMdnGkM8PoBnndDLwzHH";

    // ===== secp256r1 Edge Case Test Vectors =====
    // Empty message signature (using same key as SECP256R1_PRIVATE_KEY)
    const SECP256R1_EMPTY_MESSAGE: &[u8] = b"";
    const SECP256R1_EMPTY_MESSAGE_SIGNATURE: &[u8] = &[
        0x30, 0x45, 0x02, 0x21, 0x00, 0xa3, 0xe3, 0x82, 0xd5, 0xbb, 0x5a, 0x15, 0x47, 0xd6, 0x79,
        0x66, 0x50, 0x31, 0x4d, 0x4f, 0xf8, 0xfe, 0x4e, 0xd7, 0x89, 0x78, 0xd8, 0xf2, 0x02, 0x29,
        0x37, 0x27, 0x2f, 0xae, 0xba, 0x87, 0xb3, 0x02, 0x20, 0x75, 0x5f, 0x43, 0x99, 0x3b, 0xb3,
        0x5b, 0xb7, 0x41, 0x75, 0x3c, 0xea, 0x8a, 0x2d, 0x16, 0x26, 0x13, 0x76, 0x75, 0xa6, 0x65,
        0xaf, 0x06, 0xf2, 0xb1, 0xaa, 0x48, 0xf0, 0xc3, 0x42, 0xde, 0xca,
    ];
    // Binary message with null bytes
    const SECP256R1_BINARY_MESSAGE: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00];
    const SECP256R1_BINARY_MESSAGE_SIGNATURE: &[u8] = &[
        0x30, 0x46, 0x02, 0x21, 0x00, 0xfd, 0x7b, 0x34, 0x1f, 0xf7, 0x4b, 0xf0, 0x31, 0x5a, 0x90,
        0x26, 0x39, 0xf0, 0xaf, 0xb4, 0x28, 0x1a, 0x8b, 0x42, 0x9f, 0x61, 0xe6, 0x10, 0x30, 0xc4,
        0xd3, 0xc8, 0x7a, 0xf1, 0x0c, 0xfe, 0xac, 0x02, 0x21, 0x00, 0xa8, 0x9f, 0xd0, 0xc0, 0xc2,
        0xdf, 0x07, 0xd6, 0xef, 0xb2, 0xf9, 0x66, 0x0d, 0xfc, 0xc8, 0x0f, 0xd1, 0x01, 0x9a, 0xd2,
        0x7c, 0xe8, 0x59, 0x8c, 0x2f, 0xba, 0x16, 0x16, 0x36, 0x50, 0x3b, 0x30,
    ];
    // 1KB message (1024 'A' characters)
    const SECP256R1_1KB_MESSAGE_SIGNATURE: &[u8] = &[
        0x30, 0x45, 0x02, 0x21, 0x00, 0xb2, 0xaa, 0x12, 0x1d, 0xaa, 0xd5, 0xa4, 0xc7, 0x89, 0x7d,
        0x56, 0xc8, 0x2f, 0xc6, 0x57, 0x82, 0xfe, 0xf4, 0x19, 0xc0, 0x3d, 0xdc, 0xfd, 0xe2, 0xf7,
        0xe5, 0xd7, 0xbf, 0x33, 0xde, 0xce, 0x7c, 0x02, 0x20, 0x52, 0x0a, 0x2e, 0xde, 0xdd, 0xe8,
        0xaf, 0x9e, 0x87, 0xa4, 0xc3, 0x16, 0x08, 0xbe, 0x2e, 0xc5, 0xe0, 0xce, 0x24, 0x31, 0xbc,
        0x38, 0x4d, 0xd9, 0x94, 0xf3, 0x9c, 0xe3, 0x54, 0xfd, 0x20, 0xa4,
    ];

    // ===== secp256k1 Edge Case Test Vectors =====
    // Empty message signature (using same key as SECP256K1_PRIVATE_KEY)
    const SECP256K1_EMPTY_MESSAGE: &[u8] = b"";
    const SECP256K1_EMPTY_MESSAGE_SIGNATURE: &[u8] = &[
        0x30, 0x44, 0x02, 0x20, 0x0c, 0x3d, 0xec, 0xb3, 0x81, 0x70, 0x9d, 0x58, 0xc4, 0x3f, 0x8d,
        0x6e, 0x18, 0x89, 0x7d, 0xed, 0x02, 0x87, 0x44, 0x4e, 0x0a, 0xbc, 0xed, 0xa8, 0xa7, 0xb6,
        0x48, 0x1b, 0x48, 0x71, 0xc6, 0x4d, 0x02, 0x20, 0x36, 0x67, 0xf8, 0xa8, 0x34, 0x2f, 0x13,
        0x69, 0x07, 0x68, 0xf3, 0xcb, 0xee, 0x0f, 0xf5, 0xa1, 0x08, 0xd7, 0x05, 0xd7, 0x1e, 0xc1,
        0xac, 0x9a, 0x7c, 0x24, 0x2c, 0xd6, 0xe1, 0x98, 0xbc, 0x07,
    ];
    // Binary message with null bytes
    const SECP256K1_BINARY_MESSAGE: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00];
    const SECP256K1_BINARY_MESSAGE_SIGNATURE: &[u8] = &[
        0x30, 0x44, 0x02, 0x20, 0x58, 0xc4, 0x3d, 0xad, 0x4c, 0xfd, 0xd4, 0xff, 0x6c, 0xbf, 0xab,
        0x3c, 0x04, 0xf4, 0xb2, 0x28, 0xed, 0x19, 0xf4, 0x53, 0xd2, 0xdb, 0xbb, 0xda, 0xf8, 0x1b,
        0x1c, 0x8b, 0x76, 0x8e, 0x00, 0xbc, 0x02, 0x20, 0x4b, 0x2d, 0x12, 0x34, 0x05, 0x10, 0xed,
        0xe2, 0x4e, 0xcb, 0x6b, 0x00, 0xcd, 0xa1, 0x49, 0x07, 0xa0, 0x77, 0x3d, 0x19, 0x5c, 0xc3,
        0x98, 0x4a, 0x84, 0xb8, 0xca, 0x0b, 0xb8, 0xa0, 0xb3, 0x52,
    ];

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

    #[test]
    fn test_parse_go_ed25519_did() {
        use crate::did::parse_did_key;
        use crate::types::KeyType;

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

    #[test]
    fn test_parse_go_secp256k1_did() {
        use crate::did::parse_did_key;
        use crate::types::KeyType;

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
        use crate::did::parse_did_key;
        use crate::types::KeyType;

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
    #[serial]
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

    #[test]
    fn test_ecies_bidirectional_round_trip() {
        // This test verifies that Rust-encrypted data can be decrypted by Rust
        // (and by extension, Go, since the ciphertext format is identical).
        // This is the critical bidirectional compatibility test.

        let long_message = vec![b'A'; 1000];
        let test_messages: Vec<&[u8]> = vec![
            b"Hello, World!",
            b"",                             // Empty message
            &[0x00, 0x01, 0x02, 0xff, 0xfe], // Binary data with null bytes
            &long_message,                   // Longer message
        ];

        for msg in test_messages {
            let recipient_private = StaticSecret::from(X25519_RECIPIENT_PRIVATE);
            let recipient_public = X25519PublicKey::from(X25519_RECIPIENT_PUBLIC);

            // Encrypt with Rust (random ephemeral key, random nonce - production mode)
            let encrypt_options = EciesOptions::builder().prepend_public_key(true).build();
            let ciphertext = encrypt_ecies(msg, &recipient_public, encrypt_options)
                .expect("Should encrypt with ECIES");

            // Decrypt with Rust (simulating Go decryption of Rust ciphertext)
            let decrypt_options = EciesOptions::builder().prepend_public_key(true).build();
            let decrypted = decrypt_ecies(&ciphertext, &recipient_private, decrypt_options)
                .expect("Should decrypt Rust ECIES ciphertext");

            assert_eq!(
                decrypted,
                msg,
                "Bidirectional round-trip should preserve message (len={})",
                msg.len()
            );
        }
    }

    #[test]
    #[serial]
    fn test_ecies_ciphertext_format_matches_go() {
        // Verify the ciphertext structure matches what Go expects:
        // [32-byte ephemeral public key][12-byte nonce][ciphertext][16-byte auth tag][32-byte HMAC]

        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let sender_private = StaticSecret::from(X25519_SENDER_PRIVATE);
        let recipient_public = X25519PublicKey::from(X25519_RECIPIENT_PUBLIC);

        let plaintext = b"test";
        let options = EciesOptions::builder()
            .with_private_key(sender_private)
            .prepend_public_key(true)
            .build();

        let ciphertext =
            encrypt_ecies(plaintext, &recipient_public, options).expect("Should encrypt");

        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        // Expected structure:
        // 32 (ephemeral pubkey) + 12 (nonce) + 4 (plaintext) + 16 (auth tag) + 32 (HMAC) = 96
        let expected_len = 32 + 12 + plaintext.len() + 16 + 32;
        assert_eq!(
            ciphertext.len(),
            expected_len,
            "ECIES ciphertext should have correct structure length"
        );

        // First 32 bytes should be sender's public key
        assert_eq!(
            &ciphertext[..32],
            &X25519_SENDER_PUBLIC,
            "Ciphertext should start with sender's ephemeral public key"
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
    #[serial]
    fn test_aes_encrypt_matches_go_with_deterministic_nonce() {
        // Enable deterministic nonce mode
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let (ciphertext, nonce) =
            encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt with AES");

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

    // ===== AES-GCM Edge Case Tests =====

    #[test]
    #[serial]
    fn test_aes_empty_plaintext() {
        // Empty plaintext should encrypt/decrypt correctly
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let empty_plaintext: &[u8] = b"";
        let (ciphertext, _nonce) = encrypt_aes(empty_plaintext, &AES_KEY, AES_AAD, true)
            .expect("Should encrypt empty plaintext");

        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        // Ciphertext should only contain nonce (12) + auth tag (16) = 28 bytes
        assert_eq!(
            ciphertext.len(),
            28,
            "Empty plaintext should produce 28-byte ciphertext"
        );

        // Decrypt and verify
        let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD)
            .expect("Should decrypt empty plaintext");
        assert_eq!(
            decrypted, empty_plaintext,
            "Decrypted empty plaintext should match"
        );
    }

    #[test]
    #[serial]
    fn test_aes_empty_aad() {
        // Empty AAD should work correctly
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let empty_aad: &[u8] = b"";
        let (ciphertext, _nonce) = encrypt_aes(AES_PLAINTEXT, &AES_KEY, empty_aad, true)
            .expect("Should encrypt with empty AAD");

        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, empty_aad)
            .expect("Should decrypt with empty AAD");
        assert_eq!(
            decrypted, AES_PLAINTEXT,
            "Decrypted with empty AAD should match"
        );
    }

    #[test]
    fn test_aes_large_plaintext() {
        // 1MB plaintext should encrypt/decrypt correctly
        let large_plaintext = vec![0xABu8; 1024 * 1024]; // 1MB of 0xAB bytes

        let (ciphertext, _nonce) = encrypt_aes(&large_plaintext, &AES_KEY, AES_AAD, true)
            .expect("Should encrypt large plaintext");

        // Expected: 12 (nonce) + 1MB (ciphertext) + 16 (tag) = 1048604 bytes
        assert_eq!(
            ciphertext.len(),
            12 + large_plaintext.len() + 16,
            "Large plaintext ciphertext should have correct length"
        );

        let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD)
            .expect("Should decrypt large plaintext");
        assert_eq!(
            decrypted, large_plaintext,
            "Decrypted large plaintext should match"
        );
    }

    #[test]
    fn test_aes_binary_plaintext() {
        // Binary data with null bytes should work correctly
        let binary_plaintext: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00, 0x7f, 0x80];

        let (ciphertext, _nonce) = encrypt_aes(binary_plaintext, &AES_KEY, AES_AAD, true)
            .expect("Should encrypt binary plaintext");

        let decrypted = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD)
            .expect("Should decrypt binary plaintext");
        assert_eq!(
            decrypted, binary_plaintext,
            "Decrypted binary plaintext should match"
        );
    }

    #[test]
    #[serial]
    fn test_aes_wrong_key_fails() {
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let (ciphertext, _nonce) =
            encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        // Try to decrypt with wrong key
        let wrong_key: [u8; 32] = [0xFF; 32];
        let result = decrypt_aes(None, &ciphertext, &wrong_key, AES_AAD);
        assert!(result.is_err(), "Decryption with wrong key should fail");
    }

    #[test]
    #[serial]
    fn test_aes_wrong_aad_fails() {
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let (ciphertext, _nonce) =
            encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        // Try to decrypt with wrong AAD
        let wrong_aad: &[u8] = b"wrong additional data";
        let result = decrypt_aes(None, &ciphertext, &AES_KEY, wrong_aad);
        assert!(result.is_err(), "Decryption with wrong AAD should fail");
    }

    #[test]
    #[serial]
    fn test_aes_tampered_ciphertext_fails() {
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let (mut ciphertext, _nonce) =
            encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        // Tamper with ciphertext (flip a bit in the encrypted portion)
        if ciphertext.len() > 15 {
            ciphertext[15] ^= 0x01;
        }

        let result = decrypt_aes(None, &ciphertext, &AES_KEY, AES_AAD);
        assert!(
            result.is_err(),
            "Decryption of tampered ciphertext should fail"
        );
    }

    #[test]
    #[serial]
    fn test_aes_truncated_ciphertext_fails() {
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let (ciphertext, _nonce) =
            encrypt_aes(AES_PLAINTEXT, &AES_KEY, AES_AAD, true).expect("Should encrypt");

        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        // Truncate the ciphertext (remove auth tag)
        let truncated = &ciphertext[..ciphertext.len() - 5];

        let result = decrypt_aes(None, truncated, &AES_KEY, AES_AAD);
        assert!(
            result.is_err(),
            "Decryption of truncated ciphertext should fail"
        );
    }
}
