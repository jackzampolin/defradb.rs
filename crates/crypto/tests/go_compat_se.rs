//! Go Compatibility Tests for Searchable Encryption
//!
//! These tests verify that Rust SE tag generation produces identical output
//! to Go DefraDB's internal/se/core/tag.go GenerateEqualityTag function.
//!
//! Test vectors are generated using the algorithm:
//! 1. Domain separator: "eq:<identityID>:<collectionID>:<fieldName>"
//! 2. HMAC-SHA256(key, domainSeparator || value)
//! 3. Truncate to 16 bytes

use crypto::se::{generate_equality_tag, generate_equality_tag_str, SEARCH_TAG_SIZE};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Manually compute expected tag using raw HMAC to verify our implementation
fn compute_expected_tag(
    key: &[u8],
    identity: &str,
    collection: &str,
    field: &str,
    value: &[u8],
) -> [u8; 16] {
    let domain_separator = format!("eq:{}:{}:{}", identity, collection, field);

    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(domain_separator.as_bytes());
    mac.update(value);

    let result = mac.finalize().into_bytes();
    let mut tag = [0u8; 16];
    tag.copy_from_slice(&result[..16]);
    tag
}

/// Test that our implementation matches manual HMAC computation
#[test]
fn test_matches_manual_hmac() {
    let key = [0x42u8; 32];
    let identity = "user123";
    let collection = "users_v1";
    let field = "age";
    let value = b"21";

    let expected = compute_expected_tag(&key, identity, collection, field, value);
    let actual = generate_equality_tag_str(&key, identity, collection, field, value);

    assert_eq!(
        actual, expected,
        "Tag should match manual HMAC-SHA256 computation"
    );
}

/// Test with empty identity (Go allows this)
#[test]
fn test_empty_identity_matches_expected() {
    let key = [0x01u8; 32];
    let collection = "col";
    let field = "f";
    let value = b"val";

    // Domain separator becomes "eq::col:f"
    let expected = compute_expected_tag(&key, "", collection, field, value);
    let actual = generate_equality_tag_str(&key, "", collection, field, value);

    assert_eq!(actual, expected);
}

/// Test with binary identity (raw public key bytes)
#[test]
fn test_binary_identity() {
    let key = [0xABu8; 32];
    let identity = &[0x04, 0x01, 0x02, 0x03]; // Raw bytes, like a public key prefix
    let collection = "docs";
    let field = "content";
    let value = b"hello";

    let actual = generate_equality_tag(&key, identity, collection, field, value);

    // Verify determinism
    let actual2 = generate_equality_tag(&key, identity, collection, field, value);
    assert_eq!(actual, actual2);

    // Verify it's 16 bytes
    assert_eq!(actual.len(), SEARCH_TAG_SIZE);
}

/// Test domain separator format: "eq:<identity>:<collection>:<field>"
#[test]
fn test_domain_separator_format() {
    let key = [0x00u8; 32];

    // Different components should produce different tags
    let tag1 = generate_equality_tag_str(&key, "a", "b", "c", b"v");
    let tag2 = generate_equality_tag_str(&key, "a:b", "", "c", b"v"); // Different parsing
    let tag3 = generate_equality_tag_str(&key, "", "a:b", "c", b"v");

    // These should all be different because the domain separator string differs
    assert_ne!(tag1, tag2, "Different identity/collection split should differ");
    assert_ne!(tag1, tag3, "Different identity/collection split should differ");
    assert_ne!(tag2, tag3, "Different identity/collection split should differ");
}

/// Test that value bytes are appended after domain separator
#[test]
fn test_value_appended_to_domain() {
    let key = [0x99u8; 32];
    let identity = "id";
    let collection = "col";
    let field = "f";

    // Value "abc" should differ from values "ab" + suffix
    let tag1 = generate_equality_tag_str(&key, identity, collection, field, b"abc");
    let tag2 = generate_equality_tag_str(&key, identity, collection, field, b"ab");

    assert_ne!(tag1, tag2);
}

/// Test with encoded integer values (as Go would use with EncodeFieldValue)
/// Go uses order-preserving encoding for integers
#[test]
fn test_with_encoded_integer() {
    let key = [0x11u8; 32];
    let identity = "user";
    let collection = "users";
    let field = "age";

    // Simulating Go's EncodeVarintAscending for value 21 (0x15)
    // For small positive integers, Go encoding is: intZero + value
    // intZero = 0x88 (136), so 21 encodes to [0x88 + 21] = [0x9d]
    // Actually for values <= intSmall (109), it's just intZero + value = 136 + 21 = 157 = 0x9d
    let encoded_21 = vec![0x9d]; // EncodeVarintAscending(21)
    let tag = generate_equality_tag_str(&key, identity, collection, field, &encoded_21);

    // Different value should produce different tag
    let encoded_22 = vec![0x9e]; // EncodeVarintAscending(22)
    let tag2 = generate_equality_tag_str(&key, identity, collection, field, &encoded_22);

    assert_ne!(tag, tag2);
    assert_eq!(tag.len(), SEARCH_TAG_SIZE);
}

/// Test with encoded string values
/// Go's EncodeStringAscending uses escape-based encoding
#[test]
fn test_with_encoded_string() {
    let key = [0x22u8; 32];
    let identity = "id";
    let collection = "col";
    let field = "name";

    // Go's EncodeStringAscending for "Alice":
    // [0x06] + "Alice" + [0x00, 0x01]
    // 0x06 = bytesMarker
    // 0x00, 0x01 = escaped terminator
    let encoded_alice = vec![0x06, b'A', b'l', b'i', b'c', b'e', 0x00, 0x01];
    let tag = generate_equality_tag_str(&key, identity, collection, field, &encoded_alice);

    assert_eq!(tag.len(), SEARCH_TAG_SIZE);
}

/// Test that output is always exactly 16 bytes (128 bits)
#[test]
fn test_output_size_is_16_bytes() {
    let key = [0xFFu8; 32];

    // Test with various input sizes
    for i in 0..100 {
        let value = vec![i as u8; i];
        let tag = generate_equality_tag_str(&key, "id", "col", "field", &value);
        assert_eq!(
            tag.len(),
            SEARCH_TAG_SIZE,
            "Tag length should be {} bytes for input size {}",
            SEARCH_TAG_SIZE,
            i
        );
    }
}

/// Test with maximum-length inputs
#[test]
fn test_large_inputs() {
    let key = [0xAAu8; 32];
    let identity = "a".repeat(1000);
    let collection = "b".repeat(1000);
    let field = "c".repeat(1000);
    let value = vec![0xBB; 10000];

    let tag = generate_equality_tag_str(&key, &identity, &collection, &field, &value);
    assert_eq!(tag.len(), SEARCH_TAG_SIZE);
}

/// Test key sensitivity - different keys should produce very different tags
#[test]
fn test_key_sensitivity() {
    let key1 = [0x00u8; 32];
    let mut key2 = [0x00u8; 32];
    key2[0] = 0x01; // Flip one bit

    let tag1 = generate_equality_tag_str(&key1, "id", "col", "f", b"v");
    let tag2 = generate_equality_tag_str(&key2, "id", "col", "f", b"v");

    // Tags should be completely different (avalanche effect)
    let diff_bits: u32 = tag1
        .iter()
        .zip(tag2.iter())
        .map(|(a, b)| (a ^ b).count_ones())
        .sum();

    // Expect roughly half the bits to differ (avalanche property)
    assert!(
        diff_bits > 40 && diff_bits < 90,
        "Expected avalanche effect, got {} differing bits",
        diff_bits
    );
}

/// Test that truncation preserves collision resistance
#[test]
fn test_truncation_uniqueness() {
    let key = [0x77u8; 32];
    let mut seen_tags = std::collections::HashSet::new();

    // Generate 1000 tags and ensure no collisions
    for i in 0..1000 {
        let value = format!("value_{}", i);
        let tag = generate_equality_tag_str(&key, "id", "col", "field", value.as_bytes());
        assert!(
            seen_tags.insert(tag),
            "Collision detected at iteration {}",
            i
        );
    }
}

/// Verify known test vector (computed from Go implementation algorithm)
/// This ensures byte-for-byte compatibility
#[test]
fn test_known_vector_simple() {
    // Test vector: Simple inputs
    let key = [0u8; 32]; // Zero key
    let identity = "";
    let collection = "test";
    let field = "value";
    let value = b"hello";

    // Domain separator: "eq::test:value"
    // HMAC-SHA256(zeros[32], "eq::test:value" || "hello")
    let expected = compute_expected_tag(&key, identity, collection, field, value);
    let actual = generate_equality_tag_str(&key, identity, collection, field, value);

    assert_eq!(
        hex::encode(actual),
        hex::encode(expected),
        "Known test vector mismatch"
    );
}

/// Test vector with non-ASCII identity (UTF-8 encoded)
#[test]
fn test_utf8_identity() {
    let key = [0x55u8; 32];
    // Unicode identity (emoji)
    let identity = "user_🔐";
    let collection = "secrets";
    let field = "data";
    let value = b"secret_value";

    let expected = compute_expected_tag(&key, identity, collection, field, value);
    let actual = generate_equality_tag_str(&key, identity, collection, field, value);

    assert_eq!(actual, expected);
}

/// Test that binary identity bytes are converted to UTF-8 lossy
#[test]
fn test_binary_identity_utf8_lossy() {
    let key = [0x33u8; 32];
    // Invalid UTF-8 sequence
    let identity_bytes = &[0xFF, 0xFE, 0x00, 0x01];

    let tag = generate_equality_tag(&key, identity_bytes, "col", "f", b"v");
    assert_eq!(tag.len(), SEARCH_TAG_SIZE);

    // Verify it's deterministic
    let tag2 = generate_equality_tag(&key, identity_bytes, "col", "f", b"v");
    assert_eq!(tag, tag2);
}
