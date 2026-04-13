//! Integration tests for key generation, serialization, and operations

use crypto::keys::generation::{
    generate_aes256, generate_ed25519, generate_key, generate_secp256k1, generate_x25519,
    private_key_from_bytes, private_key_from_string, public_key_from_bytes, public_key_from_string,
};
use crypto::keys::{Key, PrivateKey};
use crypto::types::KeyType;

// ===== Key Generation Tests =====

#[test]
fn test_generate_secp256k1() {
    let key = generate_secp256k1().unwrap();
    assert_eq!(key.key_type(), KeyType::Secp256k1);
    assert_eq!(key.raw().len(), 32);
}

#[test]
fn test_generate_ed25519() {
    let key = generate_ed25519().unwrap();
    assert_eq!(key.key_type(), KeyType::Ed25519);
    assert_eq!(key.raw().len(), 64);
}

#[test]
fn test_private_key_raw_returns_stable_borrow() {
    let key = generate_ed25519().unwrap();
    let first = key.raw();
    let second = key.raw();

    assert!(std::ptr::eq(first.as_ptr(), second.as_ptr()));
    assert_eq!(first, second);
}

#[test]
fn test_generate_key_secp256k1() {
    let key = generate_key(KeyType::Secp256k1).unwrap();
    assert_eq!(key.key_type(), KeyType::Secp256k1);
}

#[test]
fn test_generate_key_ed25519() {
    let key = generate_key(KeyType::Ed25519).unwrap();
    assert_eq!(key.key_type(), KeyType::Ed25519);
}

#[test]
fn test_generate_key_secp256r1() {
    let key = generate_key(KeyType::Secp256r1).unwrap();
    assert_eq!(key.key_type(), KeyType::Secp256r1);
}

#[test]
fn test_generate_x25519() {
    let key = generate_x25519().unwrap();
    let public = x25519_dalek::PublicKey::from(&key);
    assert_eq!(public.as_bytes().len(), 32);
}

#[test]
fn test_generate_aes256() {
    let key = generate_aes256().unwrap();
    assert_eq!(key.len(), 32);

    // Generate multiple keys to ensure they're different (random)
    let key2 = generate_aes256().unwrap();
    assert_ne!(key, key2);
}

// ===== Private Key Deserialization Tests =====

#[test]
fn test_private_key_from_bytes_secp256k1() {
    let original = generate_secp256k1().unwrap();
    let bytes = original.raw();

    let parsed = private_key_from_bytes(KeyType::Secp256k1, bytes).unwrap();
    assert_eq!(parsed.raw(), bytes);
}

#[test]
fn test_private_key_from_bytes_ed25519() {
    let original = generate_ed25519().unwrap();
    let bytes = original.raw();

    let parsed = private_key_from_bytes(KeyType::Ed25519, bytes).unwrap();
    assert_eq!(parsed.raw(), bytes);
}

#[test]
fn test_private_key_from_string() {
    let original = generate_secp256k1().unwrap();
    let hex_str = original.to_hex_string();

    let parsed = private_key_from_string(KeyType::Secp256k1, &hex_str).unwrap();
    assert_eq!(parsed.raw(), original.raw());
}

#[test]
fn test_public_key_from_bytes() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();
    let bytes = public_key.raw();

    let parsed = public_key_from_bytes(KeyType::Secp256k1, bytes).unwrap();
    assert_eq!(parsed.raw(), bytes);
}

#[test]
fn test_public_key_from_string() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let hex_str = public_key.to_hex_string();

    let parsed = public_key_from_string(KeyType::Ed25519, &hex_str).unwrap();
    assert_eq!(parsed.raw(), public_key.raw());
}

#[test]
fn test_private_key_public_key_returns_cached_reference() {
    let private_key = generate_secp256k1().unwrap();
    let first = private_key.public_key() as *const dyn crypto::PublicKey;
    let second = private_key.public_key() as *const dyn crypto::PublicKey;

    assert_eq!(first, second);
}

#[test]
fn test_private_key_from_string_invalid_hex() {
    // Invalid hex characters
    let result = private_key_from_string(KeyType::Secp256k1, "gggggggg");
    assert!(result.is_err(), "Invalid hex characters should fail");

    // Non-hex characters
    let result = private_key_from_string(KeyType::Secp256k1, "xyz123");
    assert!(result.is_err(), "Non-hex characters should fail");

    // Odd length hex string
    let result = private_key_from_string(KeyType::Secp256k1, "abc");
    assert!(result.is_err(), "Odd-length hex should fail");

    // Empty string
    let result = private_key_from_string(KeyType::Secp256k1, "");
    assert!(result.is_err(), "Empty hex string should fail");
}

#[test]
fn test_public_key_from_string_invalid_hex() {
    // Invalid hex characters
    let result = public_key_from_string(KeyType::Ed25519, "gggggggg");
    assert!(result.is_err(), "Invalid hex characters should fail");

    // Valid hex but wrong length for Ed25519 public key (needs 32 bytes = 64 hex chars)
    let result = public_key_from_string(KeyType::Ed25519, "abcd1234");
    assert!(result.is_err(), "Wrong length hex should fail");

    // Empty string
    let result = public_key_from_string(KeyType::Ed25519, "");
    assert!(result.is_err(), "Empty hex string should fail");
}

#[test]
fn test_private_key_from_string_wrong_length() {
    // Valid hex but wrong length for secp256k1 (needs 32 bytes = 64 hex chars)
    let short_hex = "0123456789abcdef"; // Only 8 bytes
    let result = private_key_from_string(KeyType::Secp256k1, short_hex);
    assert!(result.is_err(), "Short key should fail");

    // Valid hex but wrong length for Ed25519 (needs 64 bytes = 128 hex chars)
    let wrong_length_hex = "0123456789abcdef0123456789abcdef"; // Only 16 bytes
    let result = private_key_from_string(KeyType::Ed25519, wrong_length_hex);
    assert!(result.is_err(), "Wrong length should fail");
}

#[test]
fn test_private_key_from_weak_bytes() {
    // All-zero key should be rejected
    let zero_key = vec![0u8; 32];
    let result = private_key_from_bytes(KeyType::Secp256k1, &zero_key);
    assert!(result.is_err(), "All-zero key should be rejected");
    match result {
        Err(e) => assert!(
            e.to_string().contains("all zeros"),
            "Error should mention weak key"
        ),
        Ok(_) => panic!("Should have failed for all-zero key"),
    }

    // All-ones key should be rejected
    let ones_key = vec![0xFFu8; 32];
    let result = private_key_from_bytes(KeyType::Secp256k1, &ones_key);
    assert!(result.is_err(), "All-ones key should be rejected");
    match result {
        Err(e) => assert!(
            e.to_string().contains("all ones"),
            "Error should mention weak key"
        ),
        Ok(_) => panic!("Should have failed for all-ones key"),
    }

    // Same for Ed25519
    let zero_key = vec![0u8; 64];
    let result = private_key_from_bytes(KeyType::Ed25519, &zero_key);
    assert!(result.is_err(), "All-zero Ed25519 key should be rejected");

    let ones_key = vec![0xFFu8; 64];
    let result = private_key_from_bytes(KeyType::Ed25519, &ones_key);
    assert!(result.is_err(), "All-ones Ed25519 key should be rejected");
}

// ===== Key Deserialization Tests (from Go keys_test.go) =====

#[test]
fn test_private_key_from_bytes_valid_secp256k1() {
    let original = generate_secp256k1().unwrap();
    let key_bytes = original.raw();

    let parsed = private_key_from_bytes(KeyType::Secp256k1, key_bytes).unwrap();

    assert_eq!(
        parsed.key_type(),
        KeyType::Secp256k1,
        "Key type should match"
    );
    assert_eq!(parsed.raw(), key_bytes, "Raw bytes should match");

    // Verify public keys match
    let original_pub = original.public_key();
    let parsed_pub = parsed.public_key();
    assert_eq!(
        original_pub.raw(),
        parsed_pub.raw(),
        "Public keys should match"
    );
}

#[test]
fn test_private_key_from_bytes_valid_ed25519() {
    let original = generate_ed25519().unwrap();
    let key_bytes = original.raw();

    let parsed = private_key_from_bytes(KeyType::Ed25519, key_bytes).unwrap();

    assert_eq!(parsed.key_type(), KeyType::Ed25519, "Key type should match");
    assert_eq!(parsed.raw(), key_bytes, "Raw bytes should match");

    // Verify public keys match
    let original_pub = original.public_key();
    let parsed_pub = parsed.public_key();
    assert_eq!(
        original_pub.raw(),
        parsed_pub.raw(),
        "Public keys should match"
    );
}

#[test]
fn test_private_key_from_bytes_invalid_secp256k1_length() {
    let short_key = vec![1u8; 16]; // Too short (should be 32)
    let result = private_key_from_bytes(KeyType::Secp256k1, &short_key);
    assert!(result.is_err(), "Short secp256k1 key should fail");

    let long_key = vec![1u8; 48]; // Too long
    let result = private_key_from_bytes(KeyType::Secp256k1, &long_key);
    assert!(result.is_err(), "Long secp256k1 key should fail");
}

#[test]
fn test_private_key_from_bytes_invalid_ed25519_length() {
    let short_key = vec![1u8; 32]; // Too short (should be 64)
    let result = private_key_from_bytes(KeyType::Ed25519, &short_key);
    assert!(result.is_err(), "Short Ed25519 key should fail");

    let long_key = vec![1u8; 96]; // Too long
    let result = private_key_from_bytes(KeyType::Ed25519, &long_key);
    assert!(result.is_err(), "Long Ed25519 key should fail");
}

#[test]
fn test_private_key_from_bytes_secp256r1() {
    let generated = generate_key(KeyType::Secp256r1).unwrap();
    let key_bytes = generated.raw();
    let key = private_key_from_bytes(KeyType::Secp256r1, key_bytes).unwrap();
    assert_eq!(key.key_type(), KeyType::Secp256r1);
    assert_eq!(key.raw(), key_bytes);
}

#[test]
fn test_private_key_from_string_valid_secp256k1() {
    let original = generate_secp256k1().unwrap();
    let hex_string = original.to_hex_string();

    let parsed = private_key_from_string(KeyType::Secp256k1, &hex_string).unwrap();

    assert_eq!(
        parsed.key_type(),
        KeyType::Secp256k1,
        "Key type should match"
    );
    assert_eq!(parsed.raw(), original.raw(), "Raw bytes should match");
}

#[test]
fn test_private_key_from_string_valid_ed25519() {
    let original = generate_ed25519().unwrap();
    let hex_string = original.to_hex_string();

    let parsed = private_key_from_string(KeyType::Ed25519, &hex_string).unwrap();

    assert_eq!(parsed.key_type(), KeyType::Ed25519, "Key type should match");
    assert_eq!(parsed.raw(), original.raw(), "Raw bytes should match");
}

#[test]
fn test_public_key_from_bytes_valid_secp256k1() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();
    let key_bytes = public_key.raw();

    let parsed = public_key_from_bytes(KeyType::Secp256k1, key_bytes).unwrap();

    assert_eq!(
        parsed.key_type(),
        KeyType::Secp256k1,
        "Key type should match"
    );
    assert_eq!(parsed.raw(), key_bytes, "Raw bytes should match");
}

#[test]
fn test_public_key_from_bytes_valid_ed25519() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let key_bytes = public_key.raw();

    let parsed = public_key_from_bytes(KeyType::Ed25519, key_bytes).unwrap();

    assert_eq!(parsed.key_type(), KeyType::Ed25519, "Key type should match");
    assert_eq!(parsed.raw(), key_bytes, "Raw bytes should match");
}

#[test]
fn test_public_key_from_bytes_secp256r1_rejects_invalid() {
    // All 0xFF with 0x02 prefix - X coordinate exceeds field prime
    let mut invalid_all_ff = vec![0x02u8; 33];
    invalid_all_ff[1..].fill(0xFF);
    let result = public_key_from_bytes(KeyType::Secp256r1, &invalid_all_ff);
    assert!(
        result.is_err(),
        "X coordinate exceeding field prime should be rejected"
    );

    // Invalid prefix byte (0x05 is not valid)
    let mut invalid_prefix = vec![0x05u8; 33];
    invalid_prefix[1..].fill(0x01);
    let result = public_key_from_bytes(KeyType::Secp256r1, &invalid_prefix);
    assert!(result.is_err(), "Invalid prefix 0x05 should be rejected");

    // Wrong length (32 bytes instead of 33 for compressed)
    let wrong_length = vec![0x02u8; 32];
    let result = public_key_from_bytes(KeyType::Secp256r1, &wrong_length);
    assert!(result.is_err(), "Wrong length should be rejected");
}

#[test]
fn test_public_key_from_string_valid() {
    let secp_private = generate_secp256k1().unwrap();
    let secp_public = secp_private.public_key();
    let secp_hex = secp_public.to_hex_string();

    let parsed_secp = public_key_from_string(KeyType::Secp256k1, &secp_hex).unwrap();
    assert_eq!(
        parsed_secp.raw(),
        secp_public.raw(),
        "Secp256k1 public key should match"
    );

    let ed_private = generate_ed25519().unwrap();
    let ed_public = ed_private.public_key();
    let ed_hex = ed_public.to_hex_string();

    let parsed_ed = public_key_from_string(KeyType::Ed25519, &ed_hex).unwrap();
    assert_eq!(
        parsed_ed.raw(),
        ed_public.raw(),
        "Ed25519 public key should match"
    );
}

#[test]
fn test_public_key_from_string_invalid_secp256k1_data() {
    let invalid_hex = "deadbeef"; // Valid hex but invalid key data
    let result = public_key_from_string(KeyType::Secp256k1, invalid_hex);
    assert!(result.is_err(), "Invalid secp256k1 key data should fail");
}

#[test]
fn test_public_key_from_string_invalid_ed25519_length() {
    let short_hex = "deadbeef"; // Valid hex but wrong length (need 32 bytes = 64 hex chars)
    let result = public_key_from_string(KeyType::Ed25519, short_hex);
    assert!(result.is_err(), "Invalid Ed25519 key length should fail");
}
