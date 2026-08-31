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

#[tokio::test]
async fn atomic_creation_rejects_an_unknown_relation_target() {
    let database = db::DB::open(storage::backends::RegolithStore::in_memory().unwrap())
        .await
        .unwrap();
    let author = FieldDescription::new("1", "author", FieldKind::named("Missing", false))
        .with_relation_name("missing_posts")
        .as_primary();
    let posts = CollectionVersion::new("Post", "v-post", "c-post", vec![author]);

    let error = database
        .create_collections_atomic(vec![posts])
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("no type found for given name. Field: author, Kind: Missing"));
}

#[tokio::test]
async fn creation_rejects_an_invalid_default_value() {
    let database = db::DB::open(storage::backends::RegolithStore::in_memory().unwrap())
        .await
        .unwrap();
    let count = FieldDescription::new("1", "count", FieldKind::int())
        .with_default(serde_json::json!("not-an-int"));
    let collection = CollectionVersion::new("Counter", "v-counter", "c-counter", vec![count]);

    let error = database.create_collection(collection).await.unwrap_err();

    assert!(error.to_string().contains(
        "default field value is invalid. Collection: Counter, Inner: Field 'count' has incompatible type"
    ));
}

#[tokio::test]
async fn creation_accepts_supported_scalar_defaults() {
    let database = db::DB::open(storage::backends::RegolithStore::in_memory().unwrap())
        .await
        .unwrap();
    let collections = query::parse_sdl(
        r#"
        type Defaults {
            active: Boolean @default(value: true)
            created: DateTime @default(value: "2000-07-23T03:00:00Z")
            name: String @default(value: "Bob")
            age: Int @default(value: 40)
            points: Float @default(value: 10)
            metadata: JSON @default(value: "{\"one\":1}")
            image: Blob @default(value: "ff0099")
        }
        "#,
    )
    .unwrap();

    database
        .create_collections_atomic(collections)
        .await
        .unwrap();
}
