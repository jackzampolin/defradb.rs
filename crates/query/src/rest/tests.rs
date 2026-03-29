use super::*;
use crate::error::QueryError;
use serde_json::json;
use serde_json::Value as JsonValue;

fn json_to_graphql(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t");
            format!("\"{}\"", escaped)
        }
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_graphql).collect();
            format!("[{}]", items.join(", "))
        }
        JsonValue::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", k, json_to_graphql(v)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}

#[test]
fn test_json_to_graphql_null() {
    assert_eq!(json_to_graphql(&json!(null)), "null");
}

#[test]
fn test_json_to_graphql_bool() {
    assert_eq!(json_to_graphql(&json!(true)), "true");
    assert_eq!(json_to_graphql(&json!(false)), "false");
}

#[test]
fn test_json_to_graphql_number() {
    assert_eq!(json_to_graphql(&json!(42)), "42");
    assert_eq!(json_to_graphql(&json!(-17)), "-17");
    assert_eq!(json_to_graphql(&json!(3.15)), "3.15");
}

#[test]
fn test_json_to_graphql_simple_string() {
    assert_eq!(json_to_graphql(&json!("hello")), "\"hello\"");
    assert_eq!(json_to_graphql(&json!("")), "\"\"");
}

#[test]
fn test_json_to_graphql_string_with_quotes() {
    assert_eq!(
        json_to_graphql(&json!("Hello \"World\"")),
        "\"Hello \\\"World\\\"\""
    );
}

#[test]
fn test_json_to_graphql_string_with_backslashes() {
    assert_eq!(
        json_to_graphql(&json!("path\\to\\file")),
        "\"path\\\\to\\\\file\""
    );
}

#[test]
fn test_json_to_graphql_string_with_newlines() {
    assert_eq!(json_to_graphql(&json!("line1\nline2")), "\"line1\\nline2\"");
}

#[test]
fn test_json_to_graphql_string_with_carriage_return() {
    assert_eq!(json_to_graphql(&json!("line1\rline2")), "\"line1\\rline2\"");
}

#[test]
fn test_json_to_graphql_string_with_tabs() {
    assert_eq!(json_to_graphql(&json!("col1\tcol2")), "\"col1\\tcol2\"");
}

#[test]
fn test_json_to_graphql_string_with_mixed_escapes() {
    assert_eq!(
        json_to_graphql(&json!("line1\nline2\t\"quoted\"\r\\end")),
        "\"line1\\nline2\\t\\\"quoted\\\"\\r\\\\end\""
    );
}

#[test]
fn test_json_to_graphql_array_empty() {
    assert_eq!(json_to_graphql(&json!([])), "[]");
}

#[test]
fn test_json_to_graphql_array_simple() {
    assert_eq!(json_to_graphql(&json!([1, 2, 3])), "[1, 2, 3]");
}

#[test]
fn test_json_to_graphql_array_mixed() {
    assert_eq!(
        json_to_graphql(&json!(["hello", 42, true, null])),
        "[\"hello\", 42, true, null]"
    );
}

#[test]
fn test_json_to_graphql_array_nested() {
    assert_eq!(
        json_to_graphql(&json!([[1, 2], [3, 4]])),
        "[[1, 2], [3, 4]]"
    );
}

#[test]
fn test_json_to_graphql_object_simple() {
    let result = json_to_graphql(&json!({"name": "Alice", "age": 30}));
    assert!(
        result == "{name: \"Alice\", age: 30}" || result == "{age: 30, name: \"Alice\"}",
        "Unexpected result: {}",
        result
    );
}

#[test]
fn test_json_to_graphql_object_nested() {
    let result = json_to_graphql(&json!({"user": {"name": "Bob"}}));
    assert_eq!(result, "{user: {name: \"Bob\"}}");
}

#[test]
fn test_json_to_graphql_object_with_array() {
    let result = json_to_graphql(&json!({"tags": ["a", "b"]}));
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
    let result = json_to_graphql(&value);
    assert!(result.contains("name: \"Alice\\nSmith\""));
    assert!(result.contains("tags: [\"admin\", \"user\"]"));
    assert!(result.contains("active: true"));
}

#[test]
fn test_json_to_graphql_unicode() {
    assert_eq!(json_to_graphql(&json!("héllo 世界")), "\"héllo 世界\"");
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
