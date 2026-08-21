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
    // Go DefraDB only allows one field per order object
    let query = "{ Users(order: {name: ASC}) { _docID name age } }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    let order = selects[0].order_by.as_ref().unwrap();
    assert_eq!(order.conditions.len(), 1);
    assert_eq!(order.conditions[0].fields[0], "name");
}

#[test]
fn test_parse_query_with_multiple_order_fields_returns_error() {
    // Go DefraDB requires each order argument to only define one field
    let query = "{ Users(order: {name: ASC, age: DESC}) { _docID name age } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("each order argument can only define one field"));
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
fn test_parse_query_with_cid_scalar() {
    let query = r#"{ Users(cid: "bafy-one") { _docID name } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(selects[0].cid, Some(vec!["bafy-one".to_string()]));
}

#[test]
fn test_parse_query_with_cid_list_of_one() {
    let query = r#"{ Users(cid: ["bafy-one"]) { _docID name } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(selects[0].cid, Some(vec!["bafy-one".to_string()]));
}

#[test]
fn test_parse_query_with_multiple_cids() {
    let query = r#"{ Users(cid: ["bafy-one", "bafy-two"]) { _docID name } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(
        selects[0].cid,
        Some(vec!["bafy-one".to_string(), "bafy-two".to_string()])
    );
}

#[test]
fn test_parse_commits_with_multiple_cids() {
    let query = r#"{ _commits(cid: ["bafy-one", "bafy-two"]) { cid } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(
        selects[0].cid,
        Some(vec!["bafy-one".to_string(), "bafy-two".to_string()])
    );
}

#[test]
fn test_parse_commits_with_doc_id_scalar() {
    let query = r#"{ _commits(docID: "bae-one") { cid } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(selects[0].doc_ids, Some(vec!["bae-one".to_string()]));
}

#[test]
fn test_parse_commits_with_doc_id_list_of_one() {
    let query = r#"{ _commits(docID: ["bae-one"]) { cid } }"#;
    let selects = parse_query(query).unwrap();

    assert_eq!(selects[0].doc_ids, Some(vec!["bae-one".to_string()]));
}

#[test]
fn test_parse_commits_with_multiple_doc_ids_returns_error() {
    let query = r#"{ _commits(docID: ["bae-one", "bae-two"]) { cid } }"#;
    let result = parse_query(query);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("querying by multiple docIDs is not yet supported"));
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
