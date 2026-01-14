//! Integration tests for the GraphQL query parser.

use query::parse_query;

#[test]
fn test_parse_simple_query() {
    let query = "{ Users { _docID name } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");
    assert_eq!(selects[0].fields.len(), 2);
}

#[test]
fn test_parse_query_with_filter() {
    let query = r#"{ Users(filter: {name: {_eq: "Alice"}}) { _docID name } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert!(selects[0].filter.is_some());
}

#[test]
fn test_parse_query_with_limit_offset() {
    let query = "{ Users(limit: 10, offset: 5) { _docID name } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    let limit = selects[0].limit.as_ref().unwrap();
    assert_eq!(limit.limit, Some(10));
    assert_eq!(limit.offset, 5);
}

#[test]
fn test_parse_query_with_order() {
    let query = "{ Users(order: {name: ASC, age: DESC}) { _docID name age } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    let order = selects[0].order_by.as_ref().unwrap();
    assert_eq!(order.conditions.len(), 2);
}

#[test]
fn test_parse_query_with_doc_ids() {
    let query = r#"{ Users(docIDs: ["doc1", "doc2"]) { _docID name } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    let doc_ids = selects[0].doc_ids.as_ref().unwrap();
    assert_eq!(doc_ids.len(), 2);
    assert_eq!(doc_ids[0], "doc1");
}

#[test]
fn test_parse_query_with_alias() {
    let query = "{ allUsers: Users { _docID name } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");
    assert_eq!(selects[0].field.output_name(), "allUsers");
}

#[test]
fn test_parse_multiple_collections() {
    let query = "{ Users { name } Posts { title } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 2);
    assert_eq!(selects[0].collection_name, "Users");
    assert_eq!(selects[1].collection_name, "Posts");
}

#[test]
fn test_parse_nested_selection() {
    use query::mapper::Requestable;

    let query = "{ Users { name posts { title } } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].fields.len(), 2);

    // Second field should be a nested Select
    match &selects[0].fields[1] {
        Requestable::Select(nested) => {
            assert_eq!(nested.collection_name, "posts");
        }
        _ => panic!("expected nested select"),
    }
}

#[test]
fn test_parse_empty_query_fails() {
    let query = "";
    let result = parse_query(query);
    assert!(result.is_err());
}

#[test]
fn test_parse_invalid_query_fails() {
    let query = "{ Users { name }";
    let result = parse_query(query);
    assert!(result.is_err());
}

#[test]
fn test_parse_show_deleted() {
    let query = "{ Users(showDeleted: true) { _docID name } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert!(selects[0].show_deleted);
}

#[test]
fn test_parse_query_with_named_operation() {
    let query = "query GetUsers { Users { _docID name } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");
}

#[test]
fn test_document_mapping_created() {
    let query = "{ Users { _docID name age } }";
    let selects = parse_query(query).unwrap();

    let mapping = &selects[0].document_mapping;
    assert!(mapping.has_field("_docID"));
    assert!(mapping.has_field("name"));
    assert!(mapping.has_field("age"));
}

#[test]
fn test_filter_with_multiple_operators() {
    let query = r#"{ Users(filter: {age: {_gte: 18, _lt: 65}}) { name } }"#;
    let selects = parse_query(query).unwrap();

    assert!(selects[0].filter.is_some());
}

#[test]
fn test_filter_with_nested_and() {
    let query = r#"{ Users(filter: {_and: [{name: {_eq: "Alice"}}, {age: {_gt: 20}}]}) { name } }"#;
    let selects = parse_query(query).unwrap();

    assert!(selects[0].filter.is_some());
}

// Error path tests

#[test]
fn test_parse_mutation_returns_error() {
    let query = r#"mutation { createUser(input: {name: "Alice"}) { _docID } }"#;
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("mutations not yet supported"));
}

#[test]
fn test_parse_subscription_returns_error() {
    let query = "subscription { Users { name } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("subscriptions not supported"));
}

#[test]
fn test_parse_fragment_definition_returns_error() {
    let query = "fragment UserFields on User { name } query { Users { ...UserFields } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("fragments not yet supported"));
}

#[test]
fn test_parse_inline_fragment_returns_error() {
    let query = "{ Users { ... on User { name } } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("inline fragments not yet supported"));
}

#[test]
fn test_parse_negative_limit_returns_error() {
    let query = "{ Users(limit: -1) { name } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("limit must be non-negative"));
}

#[test]
fn test_parse_negative_offset_returns_error() {
    let query = "{ Users(offset: -5) { name } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("offset must be non-negative"));
}

#[test]
fn test_parse_unknown_argument_returns_error() {
    let query = "{ Users(unknownArg: 123) { name } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("unknown argument 'unknownArg'"));
}

#[test]
fn test_parse_cid_wrong_type_returns_error() {
    let query = "{ Users(cid: 123) { name } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("cid argument must be a string"));
}

#[test]
fn test_parse_show_deleted_wrong_type_returns_error() {
    let query = r#"{ Users(showDeleted: "true") { name } }"#;
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("showDeleted argument must be a boolean"));
}

#[test]
fn test_parse_invalid_order_direction_returns_error() {
    let query = "{ Users(order: {name: INVALID}) { name } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid order direction"));
}

#[test]
fn test_parse_filter_non_object_returns_error() {
    let query = r#"{ Users(filter: "not an object") { name } }"#;
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("filter must be an object"));
}
