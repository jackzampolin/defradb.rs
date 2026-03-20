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
fn test_parse_query_rejects_mutation() {
    // parse_query should reject mutations (use parse_mutations instead)
    let query = r#"mutation { create_Users(input: [{name: "Alice"}]) { _docID } }"#;
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Expected query but got mutation"));
}

#[test]
fn test_parse_mutations_works() {
    use query::parse_mutations;

    let query = r#"mutation { create_Users(input: [{name: "Alice"}]) { _docID } }"#;
    let result = parse_mutations(query);
    assert!(result.is_ok());
    let mutations = result.unwrap();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].collection_name, "Users");
}

#[test]
fn test_parse_query_rejects_subscription() {
    // parse_query() specifically expects queries, not subscriptions
    // Use parse_request() to parse subscriptions
    let query = "subscription { Users { name } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("subscription"),
        "Error should mention that subscriptions are not expected by parse_query()"
    );
}

#[test]
fn test_parse_fragment_definition_works() {
    use query::mapper::Requestable;

    let query = "fragment UserFields on User { name age } query { Users { _docID ...UserFields } }";
    let result = parse_query(query);
    assert!(result.is_ok(), "Fragment parsing should succeed");

    let selects = result.unwrap();
    assert_eq!(selects.len(), 1);
    // Should have _docID + name + age from fragment
    assert_eq!(selects[0].fields.len(), 3);

    // Verify fields
    let field_names: Vec<&str> = selects[0]
        .fields
        .iter()
        .filter_map(|f| match f {
            Requestable::Field(f) => Some(f.name.as_str()),
            _ => None,
        })
        .collect();
    assert!(field_names.contains(&"_docID"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"age"));
}

#[test]
fn test_parse_inline_fragment_works() {
    use query::mapper::Requestable;

    let query = "{ Users { _docID ... on User { name age } } }";
    let result = parse_query(query);
    assert!(result.is_ok(), "Inline fragment parsing should succeed");

    let selects = result.unwrap();
    assert_eq!(selects.len(), 1);
    // Should have _docID + name + age from inline fragment
    assert_eq!(selects[0].fields.len(), 3);

    // Verify fields
    let field_names: Vec<&str> = selects[0]
        .fields
        .iter()
        .filter_map(|f| match f {
            Requestable::Field(f) => Some(f.name.as_str()),
            _ => None,
        })
        .collect();
    assert!(field_names.contains(&"_docID"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"age"));
}

#[test]
fn test_parse_undefined_fragment_returns_error() {
    let query = "query { Users { ...UndefinedFragment } }";
    let result = parse_query(query);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("undefined fragment"),
        "Expected error about undefined fragment"
    );
}

#[test]
fn test_parse_nested_fragment_works() {
    let query = r#"
        fragment NameField on User { name }
        fragment UserInfo on User { ...NameField age }
        query { Users { _docID ...UserInfo } }
    "#;
    let result = parse_query(query);
    assert!(result.is_ok(), "Nested fragment parsing should succeed");

    let selects = result.unwrap();
    assert_eq!(selects.len(), 1);
    // Should have _docID + name (from nested) + age
    assert_eq!(selects[0].fields.len(), 3);
}

#[test]
fn test_parse_circular_fragment_returns_error() {
    // Fragment A references B, B references A
    let query = r#"
        fragment A on User { name ...B }
        fragment B on User { age ...A }
        query { Users { ...A } }
    "#;
    let result = parse_query(query);
    assert!(result.is_err(), "Circular fragment should error");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("circular fragment reference"),
        "Expected error about circular fragment"
    );
}

#[test]
fn test_parse_self_referential_fragment_returns_error() {
    // Fragment that references itself
    let query = r#"
        fragment A on User { name ...A }
        query { Users { ...A } }
    "#;
    let result = parse_query(query);
    assert!(result.is_err(), "Self-referential fragment should error");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("circular fragment reference"),
        "Expected error about circular fragment"
    );
}

#[test]
fn test_parse_deeply_nested_fragments_succeeds() {
    use query::mapper::Requestable;

    // Create a chain of 10 fragments (non-circular but deep)
    let query = r#"
        fragment F10 on User { name }
        fragment F9 on User { ...F10 }
        fragment F8 on User { ...F9 }
        fragment F7 on User { ...F8 }
        fragment F6 on User { ...F7 }
        fragment F5 on User { ...F6 }
        fragment F4 on User { ...F5 }
        fragment F3 on User { ...F4 }
        fragment F2 on User { ...F3 }
        fragment F1 on User { ...F2 }
        query { Users { _docID ...F1 } }
    "#;
    let result = parse_query(query);
    assert!(result.is_ok(), "Deep fragment nesting should work");

    let selects = result.unwrap();
    assert_eq!(selects.len(), 1);
    // Should have _docID + name (from deeply nested fragment)
    assert_eq!(selects[0].fields.len(), 2);

    // Verify both fields are present
    let field_names: Vec<&str> = selects[0]
        .fields
        .iter()
        .filter_map(|f| match f {
            Requestable::Field(f) => Some(f.name.as_str()),
            _ => None,
        })
        .collect();
    assert!(field_names.contains(&"_docID"));
    assert!(field_names.contains(&"name"));
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
        .contains("cid must be a string or list"));
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
    // Error format matches Go DefraDB
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Argument \"order\" has invalid value"));
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

// EXPLAIN tests

#[test]
fn test_parse_explain_directive() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query { selects, explain } => {
            assert!(
                explain.is_some(),
                "Expected explain=Some for @explain directive"
            );
            assert_eq!(explain, Some(ExplainType::Simple));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_explain_directive_with_type_simple() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain(type: simple) { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query { selects, explain } => {
            assert_eq!(explain, Some(ExplainType::Simple));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_explain_directive_with_type_execute() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain(type: execute) { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query { selects, explain } => {
            assert_eq!(explain, Some(ExplainType::Execute));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_explain_directive_with_type_debug() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain(type: debug) { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query { selects, explain } => {
            assert_eq!(explain, Some(ExplainType::Debug));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_query_without_explain() {
    use query::query_parse::{parse_request, ParsedOperation};

    let query = "query { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query { selects, explain } => {
            assert!(
                explain.is_none(),
                "Expected explain=None without @explain directive"
            );
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_bare_query_without_explain() {
    use query::query_parse::{parse_request, ParsedOperation};

    // Bare selection set (no 'query' keyword)
    let query = "{ Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query { selects, explain } => {
            assert!(
                explain.is_none(),
                "Expected explain=None for bare selection set"
            );
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_top_level_aggregate() {
    use query::mapper::Requestable;

    let query = "{ AVG(Users: {field: Age}) }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(
        selects[0].collection_name, "Users",
        "Collection name should be extracted from aggregate target"
    );
    assert_eq!(selects[0].fields.len(), 1);

    // The field should be an aggregate
    match &selects[0].fields[0] {
        Requestable::Aggregate(agg) => {
            assert_eq!(agg.aggregate_type, query::mapper::AggregateType::Average);
            assert_eq!(agg.targets.len(), 1);
            assert_eq!(agg.targets[0].host_name, "Users");
            assert_eq!(agg.targets[0].field_name, Some("Age".to_string()));
        }
        _ => panic!("Expected aggregate"),
    }
}

#[test]
fn test_parse_top_level_count() {
    use query::mapper::Requestable;

    let query = "{ COUNT(Users: {}) }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");

    match &selects[0].fields[0] {
        Requestable::Aggregate(agg) => {
            assert_eq!(agg.aggregate_type, query::mapper::AggregateType::Count);
            assert_eq!(agg.targets.len(), 1);
            assert_eq!(agg.targets[0].host_name, "Users");
        }
        _ => panic!("Expected aggregate"),
    }
}

#[test]
fn test_parse_top_level_aggregate_with_alias() {
    use query::mapper::Requestable;

    let query = "{ average: AVG(Users: {field: Age}) }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");

    match &selects[0].fields[0] {
        Requestable::Aggregate(agg) => {
            assert_eq!(agg.alias, Some("average".to_string()));
        }
        _ => panic!("Expected aggregate"),
    }
}
