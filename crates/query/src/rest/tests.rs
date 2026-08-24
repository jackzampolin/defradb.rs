use super::gql::{build_filtered_delete_mutation, json_to_graphql_input};
use super::*;
use crate::error::QueryError;
use serde_json::json;

#[test]
fn test_json_to_graphql_null() {
    assert_eq!(json_to_graphql_input(&json!(null)).unwrap(), "null");
}

#[test]
fn test_json_to_graphql_bool() {
    assert_eq!(json_to_graphql_input(&json!(true)).unwrap(), "true");
    assert_eq!(json_to_graphql_input(&json!(false)).unwrap(), "false");
}

#[test]
fn test_json_to_graphql_number() {
    assert_eq!(json_to_graphql_input(&json!(42)).unwrap(), "42");
    assert_eq!(json_to_graphql_input(&json!(-17)).unwrap(), "-17");
    assert_eq!(json_to_graphql_input(&json!(3.15)).unwrap(), "3.15");
}

#[test]
fn test_json_to_graphql_simple_string() {
    assert_eq!(json_to_graphql_input(&json!("hello")).unwrap(), "\"hello\"");
    assert_eq!(json_to_graphql_input(&json!("")).unwrap(), "\"\"");
}

#[test]
fn test_json_to_graphql_string_with_quotes() {
    assert_eq!(
        json_to_graphql_input(&json!("Hello \"World\"")).unwrap(),
        "\"Hello \\\"World\\\"\""
    );
}

#[test]
fn test_json_to_graphql_string_with_backslashes() {
    assert_eq!(
        json_to_graphql_input(&json!("path\\to\\file")).unwrap(),
        "\"path\\\\to\\\\file\""
    );
}

#[test]
fn test_json_to_graphql_string_with_newlines() {
    assert_eq!(
        json_to_graphql_input(&json!("line1\nline2")).unwrap(),
        "\"line1\\nline2\""
    );
}

#[test]
fn test_json_to_graphql_string_with_carriage_return() {
    assert_eq!(
        json_to_graphql_input(&json!("line1\rline2")).unwrap(),
        "\"line1\\rline2\""
    );
}

#[test]
fn test_json_to_graphql_string_with_tabs() {
    assert_eq!(
        json_to_graphql_input(&json!("col1\tcol2")).unwrap(),
        "\"col1\\tcol2\""
    );
}

#[test]
fn test_json_to_graphql_string_with_backspace() {
    assert_eq!(
        json_to_graphql_input(&json!("a\u{0008}b")).unwrap(),
        "\"a\\bb\""
    );
}

#[test]
fn test_json_to_graphql_string_with_form_feed() {
    assert_eq!(
        json_to_graphql_input(&json!("a\u{000c}b")).unwrap(),
        "\"a\\fb\""
    );
}

#[test]
fn test_json_to_graphql_string_with_other_controls() {
    // JSON (and Go valueToGQL) use \uXXXX for controls without a short escape.
    assert_eq!(
        json_to_graphql_input(&json!("a\u{0000}b")).unwrap(),
        "\"a\\u0000b\""
    );
    assert_eq!(
        json_to_graphql_input(&json!("a\u{0007}b")).unwrap(),
        "\"a\\u0007b\""
    );
}

#[test]
fn test_json_to_graphql_string_with_mixed_escapes() {
    assert_eq!(
        json_to_graphql_input(&json!("line1\nline2\t\"quoted\"\r\\end")).unwrap(),
        "\"line1\\nline2\\t\\\"quoted\\\"\\r\\\\end\""
    );
}

#[test]
fn test_json_to_graphql_array_empty() {
    assert_eq!(json_to_graphql_input(&json!([])).unwrap(), "[]");
}

#[test]
fn test_json_to_graphql_array_simple() {
    assert_eq!(
        json_to_graphql_input(&json!([1, 2, 3])).unwrap(),
        "[1, 2, 3]"
    );
}

#[test]
fn test_json_to_graphql_array_mixed() {
    assert_eq!(
        json_to_graphql_input(&json!(["hello", 42, true, null])).unwrap(),
        "[\"hello\", 42, true, null]"
    );
}

#[test]
fn test_json_to_graphql_array_nested() {
    assert_eq!(
        json_to_graphql_input(&json!([[1, 2], [3, 4]])).unwrap(),
        "[[1, 2], [3, 4]]"
    );
}

#[test]
fn test_json_to_graphql_object_simple() {
    let result = json_to_graphql_input(&json!({"name": "Alice", "age": 30})).unwrap();
    assert!(
        result == "{name: \"Alice\", age: 30}" || result == "{age: 30, name: \"Alice\"}",
        "Unexpected result: {}",
        result
    );
}

#[test]
fn test_json_to_graphql_object_nested() {
    let result = json_to_graphql_input(&json!({"user": {"name": "Bob"}})).unwrap();
    assert_eq!(result, "{user: {name: \"Bob\"}}");
}

#[test]
fn test_json_to_graphql_object_with_array() {
    let result = json_to_graphql_input(&json!({"tags": ["a", "b"]})).unwrap();
    assert_eq!(result, "{tags: [\"a\", \"b\"]}");
}

#[test]
fn test_json_to_graphql_complex_nested() {
    let value = json!({
        "user": {
            "name": "Alice\nSmith",
            "tags": ["admin", "user"],
            "active": true
        }
    });
    let result = json_to_graphql_input(&value).unwrap();
    assert!(result.contains("name: \"Alice\\nSmith\""));
    assert!(result.contains("tags: [\"admin\", \"user\"]"));
    assert!(result.contains("active: true"));
}

#[test]
fn test_json_to_graphql_unicode() {
    assert_eq!(
        json_to_graphql_input(&json!("héllo 世界")).unwrap(),
        "\"héllo 世界\""
    );
}

#[test]
fn test_rest_error_display() {
    let err = RestError::collection_not_found("Users");
    assert_eq!(err.to_string(), "collection not found: Users");

    let err = RestError::document_not_found("bae-123");
    assert_eq!(err.to_string(), "document not found: bae-123");

    let err = RestError::invalid_doc_id("invalid");
    assert_eq!(err.to_string(), "invalid document ID: invalid");

    let err = RestError::invalid_input("missing field");
    assert_eq!(err.to_string(), "invalid input: missing field");

    let err = RestError::permission_denied("access denied");
    assert_eq!(err.to_string(), "permission denied: access denied");

    let err = RestError::internal("storage failure");
    assert_eq!(err.to_string(), "internal error: storage failure");
}

#[test]
fn test_rest_error_from_query_error() {
    let err = QueryError::collection_not_found("Users");
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::CollectionNotFound(_)));

    let err = QueryError::DocumentNotFound("bae-123".into());
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::DocumentNotFound(_)));

    let err = QueryError::parse("unexpected token");
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::InvalidInput(_)));
    assert!(rest_err.to_string().contains("parse error"));

    let err = QueryError::invalid_filter("bad condition");
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::InvalidInput(_)));
    assert!(rest_err.to_string().contains("invalid filter"));

    let err = QueryError::unknown_field("foo");
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::InvalidInput(_)));
    assert!(rest_err.to_string().contains("foo"));

    let err = QueryError::TypeMismatch {
        expected: "String".into(),
        actual: "Int".into(),
    };
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::InvalidInput(_)));
    assert!(rest_err.to_string().contains("type mismatch"));

    let err = QueryError::RequiredFieldMissing("name".into());
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::InvalidInput(_)));
    assert!(rest_err.to_string().contains("required field missing"));

    let err = QueryError::permission_denied("not authorized");
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::PermissionDenied(_)));

    let err = QueryError::acp_registration_failed("bae-123", "policy error");
    let rest_err: RestError = err.into();
    assert!(matches!(rest_err, RestError::PermissionDenied(_)));
    assert!(rest_err.to_string().contains("ACP registration failed"));
}

// ---------------------------------------------------------------------------
// Object keys are the trust boundary
// ---------------------------------------------------------------------------

/// Keys are written into the mutation unquoted, so a key carrying GraphQL
/// punctuation would close the argument list and let a caller append
/// operations of its own. This is the check that it cannot.
#[test]
fn a_key_that_could_escape_the_document_is_refused() {
    let hostile = [
        json!({"a) { _docID } } mutation { delete_Other(filter: {": 1}),
        json!({"name\"": 1}),
        json!({"na me": 1}),
        json!({"": 1}),
        json!({"1name": 1}),
        json!({"name#comment": 1}),
        json!({"a$b": 1}),
    ];
    for value in hostile {
        assert!(
            json_to_graphql_input(&value).is_err(),
            "{value} should be refused"
        );
    }
}

/// The refusal has to reach nested keys too, or a filter one level down is
/// still an open door.
#[test]
fn a_hostile_key_is_refused_at_any_depth() {
    assert!(json_to_graphql_input(&json!({"user": {"na)me": 1}})).is_err());
    assert!(json_to_graphql_input(&json!({"users": [{"na)me": 1}]})).is_err());
    assert!(json_to_graphql_input(&json!([[{"na)me": 1}]])).is_err());
}

/// Nothing legitimate is turned away: a key that is not a GraphQL Name cannot
/// address a schema field in the first place.
#[test]
fn real_field_names_still_encode() {
    for value in [
        json!({"name": "Alice"}),
        json!({"_docID": "bae-1"}),
        json!({"age_2": 1}),
        json!({"_": 1}),
        json!({"A1": 1}),
        json!({"user": {"address": {"city": "Berlin"}}}),
    ] {
        assert!(
            json_to_graphql_input(&value).is_ok(),
            "{value} should encode"
        );
    }
}

/// String *values* are still free-form; only keys are constrained.
#[test]
fn hostile_text_in_a_value_is_escaped_not_refused() {
    let encoded = json_to_graphql_input(&json!({"name": "a) { _docID } } mutation {"})).unwrap();
    assert_eq!(
        encoded, r#"{name: "a) { _docID } } mutation {"}"#,
        "a value is a quoted StringValue, so it cannot escape"
    );
}

/// The filter reaches the document builder verbatim, so the refusal has to
/// hold at the builder the real REST implementation calls, not only at the
/// encoder underneath it.
#[test]
fn a_filtered_delete_refuses_a_hostile_filter_key() {
    let hostile = json!({"a) { _docID } } mutation { delete_Other(filter: {": 1});
    assert!(build_filtered_delete_mutation("Users", &hostile).is_err());

    let ordinary = json!({"name": {"_eq": "Alice"}});
    assert_eq!(
        build_filtered_delete_mutation("Users", &ordinary).unwrap(),
        r#"mutation { delete_Users(filter: {name: {_eq: "Alice"}}) { _docID } }"#
    );
}
