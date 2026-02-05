use super::*;

#[test]
fn test_parse_subscription_basic() {
    let query = r#"
        subscription {
            User {
                _docID
                name
            }
        }
    "#;

    let result = parse_request(query).unwrap();
    match result {
        ParsedOperation::Subscription { select } => {
            assert_eq!(select.collection_name, "User");
            assert_eq!(select.fields.len(), 2);
        }
        _ => panic!("Expected subscription"),
    }
}

#[test]
fn test_parse_subscription_with_filter() {
    let query = r#"
        subscription {
            User(filter: {active: {_eq: true}}) {
                _docID
                name
                email
            }
        }
    "#;

    let result = parse_request(query).unwrap();
    match result {
        ParsedOperation::Subscription { select } => {
            assert_eq!(select.collection_name, "User");
            assert!(select.filter.is_some());
        }
        _ => panic!("Expected subscription"),
    }
}

#[test]
fn test_parse_subscription_with_variables() {
    let query = r#"
        subscription($active: Boolean!) {
            User(filter: {active: {_eq: $active}}) {
                _docID
                name
            }
        }
    "#;

    let variables = HashMap::from([("active".to_string(), serde_json::json!(true))]);
    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Subscription { select } => {
            assert_eq!(select.collection_name, "User");
            let filter = select.filter.as_ref().unwrap();
            let conditions = filter.conditions();
            assert_eq!(
                conditions.get("active").unwrap().get("_eq"),
                Some(&serde_json::json!(true))
            );
        }
        _ => panic!("Expected subscription"),
    }
}

#[test]
fn test_parse_subscription_multiple_root_fields_error() {
    let query = r#"
        subscription {
            User {
                name
            }
            Post {
                title
            }
        }
    "#;

    let result = parse_request(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("exactly one root field"));
}

#[test]
fn test_parse_subscription_with_nested_fields() {
    let query = r#"
        subscription {
            User {
                _docID
                name
                posts {
                    _docID
                    title
                }
            }
        }
    "#;

    let result = parse_request(query).unwrap();
    match result {
        ParsedOperation::Subscription { select } => {
            assert_eq!(select.collection_name, "User");
            // Should have 3 fields: _docID, name, and posts (nested)
            assert_eq!(select.fields.len(), 3);
        }
        _ => panic!("Expected subscription"),
    }
}

#[test]
fn test_cannot_mix_subscription_and_query() {
    // GraphQL doesn't allow mixing operation types in a single document
    // But we can test that our parser handles it correctly
    let query = r#"
        subscription {
            User { name }
        }
    "#;

    let query_op = r#"
        query {
            User { name }
        }
    "#;

    // Each should parse independently
    assert!(matches!(
        parse_request(query).unwrap(),
        ParsedOperation::Subscription { .. }
    ));
    assert!(matches!(
        parse_request(query_op).unwrap(),
        ParsedOperation::Query { .. }
    ));
}

#[test]
fn test_subscription_with_doc_id() {
    let query = r#"
        subscription {
            User(docID: "bae-123") {
                _docID
                name
            }
        }
    "#;

    let result = parse_request(query).unwrap();
    match result {
        ParsedOperation::Subscription { select } => {
            assert_eq!(select.doc_ids, Some(vec!["bae-123".to_string()]));
        }
        _ => panic!("Expected subscription"),
    }
}

#[test]
fn test_subscription_with_default_variable() {
    let query = r#"
        subscription($limit: Int = 10) {
            User(limit: $limit) {
                _docID
                name
            }
        }
    "#;

    // Don't provide the variable - should use default
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Subscription { select } => {
            assert_eq!(select.limit.as_ref().unwrap().limit, Some(10));
        }
        _ => panic!("Expected subscription"),
    }
}
