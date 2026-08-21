use db::merge::se::coordinator::*;
use document::NormalValue;
use schema::EncryptedIndexDescription;
use schema::EncryptedIndexType;
use zeroize::Zeroizing;

#[test]
fn test_coordinator_creation() {
    let enc_key = vec![1u8; 32];
    let coordinator = SECoordinator::with_key(enc_key.clone());

    assert_eq!(coordinator.enc_key(), &enc_key);
    assert!(coordinator.identity_pubkey().is_none());
}

#[test]
fn test_coordinator_with_identity() {
    let config = SECoordinatorConfig {
        enc_key: Zeroizing::new(vec![1u8; 32]),
        identity_pubkey: Some(b"user_pubkey".to_vec()),
    };
    let coordinator = SECoordinator::new(config);

    assert!(coordinator.identity_pubkey().is_some());
    assert_eq!(coordinator.identity_pubkey().unwrap(), b"user_pubkey");
}

#[test]
fn test_coordinator_with_key_and_identity() {
    let coordinator = SECoordinator::with_key_and_identity(vec![1u8; 32], b"user_pubkey".to_vec());

    assert_eq!(coordinator.enc_key(), &[1u8; 32]);
    assert_eq!(coordinator.identity_pubkey().unwrap(), b"user_pubkey");
}

#[test]
fn test_generate_artifacts() {
    let coordinator = SECoordinator::with_key(vec![2u8; 32]);

    let encrypted_indexes = vec![EncryptedIndexDescription::new("age")];

    let mut field_values = std::collections::HashMap::new();
    field_values.insert("age".to_string(), NormalValue::Int(25));
    field_values.insert("name".to_string(), NormalValue::String("Test".to_string()));

    let artifacts = coordinator
        .generate_artifacts("users_v1", "bae123", &encrypted_indexes, &[], &field_values)
        .unwrap();

    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].index_id, "age");
    assert_eq!(artifacts[0].search_tag.len(), 16);
}

#[test]
fn test_to_field_queries() {
    let coordinator = SECoordinator::with_key(vec![3u8; 32]);

    let queries = vec![FieldValueQuery::equality("age", NormalValue::Int(30))];

    let field_queries = coordinator.to_field_queries("users_v1", &queries).unwrap();

    assert_eq!(field_queries.len(), 1);
    assert_eq!(field_queries[0].field_name, "age");
    assert_eq!(field_queries[0].search_tag.len(), 16);
}

#[test]
fn test_field_value_query_equality() {
    let q = FieldValueQuery::equality("status", NormalValue::String("active".to_string()));

    assert_eq!(q.field_name, "status");
    assert_eq!(q.index_desc.field_name, "status");
    assert_eq!(q.index_desc.index_type, EncryptedIndexType::Equality);
}
