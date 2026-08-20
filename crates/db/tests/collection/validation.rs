use db::Collection;
use db::Error;
use document::Document;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use schema::ScalarKind;

fn filtered_collection() -> Collection {
    Collection::new(CollectionVersion::new(
        "AgentDoc",
        "agent_doc_version",
        "agent_doc_collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "agent_did", FieldKind::string()).as_immutable(),
            FieldDescription::new("3", "body", FieldKind::string()),
        ],
    ))
}

#[test]
fn immutable_field_validation_allows_other_field_changes() {
    let collection = filtered_collection();
    let old_doc =
        Document::from_json_str(r#"{"agent_did":"did:key:z6M","body":"before"}"#).unwrap();
    let new_doc = Document::from_json_str(r#"{"agent_did":"did:key:z6M","body":"after"}"#).unwrap();

    collection
        .validate_immutable_fields_unchanged(&old_doc, &new_doc)
        .unwrap();
}

#[test]
fn immutable_field_validation_rejects_key_changes() {
    let collection = filtered_collection();
    let old_doc =
        Document::from_json_str(r#"{"agent_did":"did:key:z6M","body":"before"}"#).unwrap();
    let new_doc =
        Document::from_json_str(r#"{"agent_did":"did:key:other","body":"before"}"#).unwrap();

    let result = collection.validate_immutable_fields_unchanged(&old_doc, &new_doc);
    assert!(
        matches!(result, Err(Error::InvalidDocument(message)) if message.contains("agent_did"))
    );
}

#[test]
fn non_nillable_field_rejects_null_and_missing_values() {
    let collection = Collection::new(CollectionVersion::new(
        "Event",
        "event_version",
        "event_collection",
        vec![FieldDescription::new(
            "1",
            "createdAt",
            FieldKind::Scalar(ScalarKind::NonNillableDateTime),
        )],
    ));

    let valid = Document::from_json_str(r#"{"createdAt":"2024-01-01T00:00:00Z"}"#).unwrap();
    collection.validate_document(&valid).unwrap();

    let null = Document::from_json_str(r#"{"createdAt":null}"#).unwrap();
    let error = collection.validate_document(&null).unwrap_err().to_string();
    assert!(error.contains("null value provided for non-nillable field. Name: createdAt"));

    let missing = Document::from_json_str("{}").unwrap();
    let error = collection
        .validate_document(&missing)
        .unwrap_err()
        .to_string();
    assert!(error.contains("value not provided for non-nillable field. Name: createdAt"));
}
