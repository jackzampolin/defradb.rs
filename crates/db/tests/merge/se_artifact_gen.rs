use db::merge::se::artifact_gen::*;
use document::NormalValue;
use schema::EncryptedIndexDescription;

#[test]
fn test_generate_field_artifact_equality() {
    let collection_id = "users_v1";
    let doc_id = "bae123";
    let enc_idx = EncryptedIndexDescription::new("age");
    let field_value = NormalValue::Int(21);
    let identity = b"user_pubkey";
    let enc_key = [0u8; 32];

    let artifact = generate_field_artifact(
        collection_id,
        doc_id,
        &enc_idx,
        &field_value,
        Some(identity),
        &enc_key,
    )
    .unwrap();

    assert_eq!(artifact.collection_id, collection_id);
    assert_eq!(artifact.doc_id, doc_id);
    assert_eq!(artifact.index_id, "age");
    assert_eq!(artifact.search_tag.len(), 16);
}

#[test]
fn test_generate_field_artifact_deterministic() {
    let collection_id = "users_v1";
    let doc_id = "bae123";
    let enc_idx = EncryptedIndexDescription::new("name");
    let field_value = NormalValue::String("Alice".to_string());
    let enc_key = [1u8; 32];

    let artifact1 = generate_field_artifact(
        collection_id,
        doc_id,
        &enc_idx,
        &field_value,
        None,
        &enc_key,
    )
    .unwrap();

    let artifact2 = generate_field_artifact(
        collection_id,
        doc_id,
        &enc_idx,
        &field_value,
        None,
        &enc_key,
    )
    .unwrap();

    assert_eq!(artifact1.search_tag, artifact2.search_tag);
}

#[test]
fn test_generate_doc_artifacts() {
    let collection_id = "users_v1";
    let doc_id = "bae456";
    let encrypted_indexes = vec![
        EncryptedIndexDescription::new("age"),
        EncryptedIndexDescription::new("city"),
    ];

    let mut field_values = std::collections::HashMap::new();
    field_values.insert("age".to_string(), NormalValue::Int(30));
    field_values.insert("city".to_string(), NormalValue::String("NYC".to_string()));
    field_values.insert("name".to_string(), NormalValue::String("Bob".to_string()));

    let enc_key = [2u8; 32];

    // Generate for all encrypted fields
    let artifacts = generate_doc_artifacts(
        collection_id,
        doc_id,
        &encrypted_indexes,
        &[],
        &field_values,
        None,
        &enc_key,
    )
    .unwrap();

    assert_eq!(artifacts.len(), 2);

    // Generate for specific field only
    let artifacts = generate_doc_artifacts(
        collection_id,
        doc_id,
        &encrypted_indexes,
        &["age".to_string()],
        &field_values,
        None,
        &enc_key,
    )
    .unwrap();

    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].index_id, "age");
}

#[test]
fn test_generate_doc_artifacts_no_encrypted_indexes() {
    let artifacts = generate_doc_artifacts(
        "col",
        "doc",
        &[],
        &[],
        &std::collections::HashMap::new(),
        None,
        &[0u8; 32],
    )
    .unwrap();

    assert!(artifacts.is_empty());
}

#[test]
fn test_different_identities_different_tags() {
    let collection_id = "users_v1";
    let doc_id = "bae123";
    let enc_idx = EncryptedIndexDescription::new("secret");
    let field_value = NormalValue::String("data".to_string());
    let enc_key = [3u8; 32];

    let artifact1 = generate_field_artifact(
        collection_id,
        doc_id,
        &enc_idx,
        &field_value,
        Some(b"user1"),
        &enc_key,
    )
    .unwrap();

    let artifact2 = generate_field_artifact(
        collection_id,
        doc_id,
        &enc_idx,
        &field_value,
        Some(b"user2"),
        &enc_key,
    )
    .unwrap();

    // Different identities should produce different tags
    assert_ne!(artifact1.search_tag, artifact2.search_tag);
}
