//! Integration tests for the GraphQL query parser: error paths and fragments.

use query::parse_query;

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
        result.unwrap_err().to_string().contains("Unknown fragment"),
        "Expected error about unknown fragment"
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
        .contains("has invalid value"));
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
