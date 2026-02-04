//! Search tag generation for searchable encryption.
//!
//! This module provides deterministic search tag generation using HMAC-SHA256.
//! Tags enable equality search on encrypted fields without revealing the actual values.
//!
//! Matches Go's internal/se/core/tag.go

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Size of the search tag in bytes (16 bytes = 128 bits).
///
/// HMAC output is truncated to 16 bytes for storage and network efficiency.
/// 128 bits provides good collision resistance even with billions of documents.
pub const SEARCH_TAG_SIZE: usize = 16;

/// Generate an equality search tag for a field value.
///
/// Creates a deterministic search tag using HMAC-SHA256, enabling equality queries
/// without decrypting the data. The same value always produces the same tag,
/// allowing the remote node to match documents without knowing the actual data.
///
/// # Security Note
///
/// This function generates the SAME tag for the same value across ALL documents
/// in the collection for the same identity. This enables efficient equality search
/// but reveals when multiple documents share the same field value (frequency analysis).
/// For fields with low cardinality (e.g., boolean, status codes), consider the
/// privacy implications.
///
/// # Algorithm
///
/// The tag is computed as:
/// ```text
/// HMAC-SHA256(key, "eq:identity:collectionID:fieldName" || value)[:16]
/// ```
///
/// # Domain Separation
///
/// - `"eq"` indicates equality search (vs future range/prefix)
/// - `identity_id` isolates tags per identity on shared remote nodes
/// - `collection_id` ensures tags are unique per collection
/// - `field_name` ensures tags are unique per field
///
/// This prevents cross-identity, cross-field and cross-collection tag collisions.
///
/// # Arguments
///
/// * `key` - 32-byte AES-256 key for HMAC computation
/// * `identity_id` - Identity's public key bytes (isolates tags per user)
/// * `collection_id` - Unique collection identifier
/// * `field_name` - Name of the indexed field
/// * `value` - Pre-encoded field value bytes (using order-preserving encoding)
///
/// # Returns
///
/// A 16-byte deterministic search tag.
///
/// # Example
///
/// ```
/// use crypto::se::generate_equality_tag;
///
/// let key = [0u8; 32];
/// let identity = b"user123";
/// let collection = "users_v1";
/// let field = "age";
/// let value = &[0x88, 0x15]; // Encoded integer 21
///
/// let tag = generate_equality_tag(&key, identity, collection, field, value);
/// assert_eq!(tag.len(), 16);
/// ```
pub fn generate_equality_tag(
    key: &[u8],
    identity_id: &[u8],
    collection_id: &str,
    field_name: &str,
    value: &[u8],
) -> [u8; SEARCH_TAG_SIZE] {
    // Build domain separator: "eq:<identityID>:<collectionID>:<fieldName>"
    // Use String::from_utf8_lossy for identity since it may be raw bytes
    let identity_str = String::from_utf8_lossy(identity_id);
    let domain_separator = format!("eq:{}:{}:{}", identity_str, collection_id, field_name);

    // Create HMAC-SHA256 instance
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");

    // Feed domain separator and value
    mac.update(domain_separator.as_bytes());
    mac.update(value);

    // Finalize and truncate to 16 bytes
    let result = mac.finalize();
    let full_tag = result.into_bytes();

    let mut tag = [0u8; SEARCH_TAG_SIZE];
    tag.copy_from_slice(&full_tag[..SEARCH_TAG_SIZE]);
    tag
}

/// Generate an equality search tag using a string identity.
///
/// Convenience wrapper for `generate_equality_tag` when identity is a string.
pub fn generate_equality_tag_str(
    key: &[u8],
    identity_id: &str,
    collection_id: &str,
    field_name: &str,
    value: &[u8],
) -> [u8; SEARCH_TAG_SIZE] {
    generate_equality_tag(
        key,
        identity_id.as_bytes(),
        collection_id,
        field_name,
        value,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_length() {
        let key = [0u8; 32];
        let tag = generate_equality_tag(&key, b"identity", "collection", "field", b"value");
        assert_eq!(tag.len(), SEARCH_TAG_SIZE);
    }

    #[test]
    fn test_deterministic() {
        let key = [1u8; 32];
        let tag1 = generate_equality_tag(&key, b"id", "col", "field", b"value");
        let tag2 = generate_equality_tag(&key, b"id", "col", "field", b"value");
        assert_eq!(tag1, tag2);
    }

    #[test]
    fn test_different_values_different_tags() {
        let key = [1u8; 32];
        let tag1 = generate_equality_tag(&key, b"id", "col", "field", b"value1");
        let tag2 = generate_equality_tag(&key, b"id", "col", "field", b"value2");
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn test_different_fields_different_tags() {
        let key = [1u8; 32];
        let tag1 = generate_equality_tag(&key, b"id", "col", "field1", b"value");
        let tag2 = generate_equality_tag(&key, b"id", "col", "field2", b"value");
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn test_different_collections_different_tags() {
        let key = [1u8; 32];
        let tag1 = generate_equality_tag(&key, b"id", "col1", "field", b"value");
        let tag2 = generate_equality_tag(&key, b"id", "col2", "field", b"value");
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn test_different_identities_different_tags() {
        let key = [1u8; 32];
        let tag1 = generate_equality_tag(&key, b"id1", "col", "field", b"value");
        let tag2 = generate_equality_tag(&key, b"id2", "col", "field", b"value");
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn test_different_keys_different_tags() {
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];
        let tag1 = generate_equality_tag(&key1, b"id", "col", "field", b"value");
        let tag2 = generate_equality_tag(&key2, b"id", "col", "field", b"value");
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn test_empty_identity() {
        // Empty identity should still produce valid tag
        let key = [1u8; 32];
        let tag = generate_equality_tag(&key, b"", "col", "field", b"value");
        assert_eq!(tag.len(), SEARCH_TAG_SIZE);
    }

    #[test]
    fn test_empty_value() {
        // Empty value should still produce valid tag
        let key = [1u8; 32];
        let tag = generate_equality_tag(&key, b"id", "col", "field", b"");
        assert_eq!(tag.len(), SEARCH_TAG_SIZE);
    }

    #[test]
    fn test_string_wrapper() {
        let key = [1u8; 32];
        let tag1 = generate_equality_tag(&key, b"identity", "col", "field", b"value");
        let tag2 = generate_equality_tag_str(&key, "identity", "col", "field", b"value");
        assert_eq!(tag1, tag2);
    }
}
