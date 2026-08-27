use db::error::commit_query_error;
use db::error::index_write_query_error;
use db::error::Error;

#[test]
fn document_at_key_preserves_display_message() {
    let error = Error::document_at_key(
        b"doc-key",
        document::Error::CborDecode("bad cbor".to_string()),
    );

    assert!(matches!(error, Error::DocumentAtKey { .. }));
    assert_eq!(
        error.to_string(),
        "failed to deserialize document at key \"doc-key\": CBOR decode error: bad cbor"
    );
}

#[test]
fn collection_schema_json_preserves_display_message() {
    let source = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
    let error = Error::collection_schema_json(
        "failed to deserialize schema for collection 'users'",
        source,
    );

    assert!(matches!(error, Error::CollectionSchemaJson { .. }));
    assert!(error
        .to_string()
        .starts_with("failed to deserialize schema for collection 'users': "));
}

#[test]
fn lens_config_json_preserves_display_message() {
    let source = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
    let error = Error::lens_config_json("failed to serialize lens config", source);

    assert!(matches!(error, Error::LensConfigJson { .. }));
    assert!(error
        .to_string()
        .starts_with("failed to serialize lens config: "));
}

#[test]
fn text_decode_preserves_display_message() {
    let source = String::from_utf8(vec![0x80]).unwrap_err();
    let error = Error::text_decode("invalid version encoding", source);

    assert!(matches!(error, Error::TextDecode { .. }));
    assert!(error.to_string().starts_with("invalid version encoding: "));
}

#[test]
fn index_write_query_error_preserves_unique_constraint_message() {
    let error = Error::Storage(storage::Error::UniqueConstraintViolation);

    assert!(error.is_unique_constraint_violation());
    assert!(matches!(
        index_write_query_error("create", error),
        query::error::QueryError::Execution(message)
            if message == storage::corekv::UNIQUE_CONSTRAINT_VIOLATION_MESSAGE
    ));
}

#[test]
fn commit_query_error_preserves_transaction_conflict_type() {
    let error = Error::Storage(storage::Error::TxnConflict);

    assert!(matches!(
        commit_query_error(error),
        query::error::QueryError::TransactionConflict(_)
    ));
}
