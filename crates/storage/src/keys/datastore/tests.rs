use super::*;
use crate::corekv::Key;
use crate::keys::utils::{InstanceType, SEPARATOR};

#[test]
fn test_datastore_key() {
    let key = DataStoreKey::new(1, InstanceType::Value, 42, "fieldname");

    let bytes = key.bytes();
    assert!(!bytes.is_empty());
    assert!(bytes[0] == SEPARATOR);

    let string = key.to_string();
    assert!(string.contains("/1/v/"));
    assert!(string.contains("fieldname"));
}

#[test]
fn test_primary_datastore_key() {
    let key = PrimaryDataStoreKey::new(1, 42);

    let bytes = key.bytes();
    let string = key.to_string();

    assert_eq!(string, "/1/pk/42");
    assert_eq!(bytes, [&[0x2f, 0x01][..], b"/pk/", &[0x2a]].concat());
}

#[test]
fn test_index_datastore_key() {
    use document::NormalValue;

    let key = IndexDataStoreKey::new(
        1,
        2,
        vec![
            IndexedField::ascending(NormalValue::String("valueA".to_string())),
            IndexedField::ascending(NormalValue::String("valueB".to_string())),
        ],
    );

    let bytes = key.bytes();
    assert!(!bytes.is_empty());
    assert!(bytes[0] == SEPARATOR);

    let string = key.to_string();
    assert!(string.contains("/1/2/"));
}

#[test]
fn test_index_datastore_key_sort_order() {
    use document::NormalValue;

    // Test that keys with different values maintain sort order
    let key1 = IndexDataStoreKey::new(1, 1, vec![IndexedField::ascending(NormalValue::Int(1))]);
    let key2 = IndexDataStoreKey::new(1, 1, vec![IndexedField::ascending(NormalValue::Int(2))]);
    let key3 = IndexDataStoreKey::new(1, 1, vec![IndexedField::ascending(NormalValue::Int(3))]);

    let bytes1 = key1.bytes();
    let bytes2 = key2.bytes();
    let bytes3 = key3.bytes();

    assert!(bytes1 < bytes2, "key1 should be < key2");
    assert!(bytes2 < bytes3, "key2 should be < key3");
}

#[test]
fn test_index_datastore_key_descending() {
    use document::NormalValue;

    // Test that descending order reverses sort
    let key1 = IndexDataStoreKey::new(1, 1, vec![IndexedField::descending(NormalValue::Int(1))]);
    let key2 = IndexDataStoreKey::new(1, 1, vec![IndexedField::descending(NormalValue::Int(2))]);

    let bytes1 = key1.bytes();
    let bytes2 = key2.bytes();

    // In descending order, larger value should have smaller key bytes
    assert!(bytes1 > bytes2, "descending: key1 should be > key2");
}

#[test]
fn test_index_datastore_key_composite() {
    use document::NormalValue;

    // Test composite index (multiple fields)
    let key = IndexDataStoreKey::new(
        1,
        1,
        vec![
            IndexedField::ascending(NormalValue::String("alice".to_string())),
            IndexedField::descending(NormalValue::Int(25)),
        ],
    );

    let bytes = key.bytes();
    assert!(!bytes.is_empty());

    // Same first field, different second field
    let key_a = IndexDataStoreKey::new(
        1,
        1,
        vec![
            IndexedField::ascending(NormalValue::String("alice".to_string())),
            IndexedField::descending(NormalValue::Int(30)),
        ],
    );
    let key_b = IndexDataStoreKey::new(
        1,
        1,
        vec![
            IndexedField::ascending(NormalValue::String("alice".to_string())),
            IndexedField::descending(NormalValue::Int(20)),
        ],
    );

    // With descending second field: 30 should come before 20 in sort order
    assert!(key_a.bytes() < key_b.bytes());
}

#[test]
fn test_datastore_se() {
    let key = DatastoreSE::new("col1", "idx1", vec![0xa1, 0xb2, 0xc3], "docid123");

    let string = key.to_string();
    assert!(string.starts_with("/se/"));
    assert!(string.contains("col1"));
    assert!(string.contains("idx1"));
    assert!(string.contains("a1b2c3")); // hex encoded
    assert!(string.contains("docid123"));
}

#[test]
fn test_view_cache_key() {
    let key = ViewCacheKey::new(1, 5);

    let string = key.to_string();
    assert_eq!(string, "/collection/vi/1/5");

    let bytes = key.bytes();
    assert!(!bytes.is_empty());
}

#[test]
fn test_datastore_key_prefixes() {
    let prefix = DataStoreKey::collection_prefix(1);
    assert!(!prefix.is_empty());

    let prefix = DataStoreKey::collection_instance_prefix(1, InstanceType::Value);
    assert!(!prefix.is_empty());

    let prefix = DataStoreKey::document_prefix(1, InstanceType::Value, 42);
    assert!(!prefix.is_empty());
}

#[test]
fn test_version_key() {
    let key = DataStoreKey::version_key(1, InstanceType::Value, 42);

    assert!(key.is_version_field());
    assert_eq!(key.field_id, DATASTORE_DOC_VERSION_FIELD_ID);
    assert_eq!(key.field_id, "v");

    let string = key.to_string();
    assert!(string.ends_with("/v"), "Version key should end with /v");
    assert!(string.contains("/1/v/"));
}

#[test]
fn test_is_version_field() {
    let version_key = DataStoreKey::new(1, InstanceType::Value, 42, DATASTORE_DOC_VERSION_FIELD_ID);
    assert!(version_key.is_version_field());

    let regular_key = DataStoreKey::new(1, InstanceType::Value, 42, "name");
    assert!(!regular_key.is_version_field());
}

#[test]
fn test_version_field_id_constant() {
    // Verify the constant matches Go's DATASTORE_DOC_VERSION_FIELD_ID
    assert_eq!(DATASTORE_DOC_VERSION_FIELD_ID, "v");
}
