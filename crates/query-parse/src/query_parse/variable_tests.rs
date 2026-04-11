use super::*;
use serde_json::json;

#[test]
fn test_variable_in_filter() {
    let query = r#"
        query($name: String!) {
            Users(filter: {name: {_eq: $name}}) {
                _docID
                name
            }
        }
    "#;

    let variables = HashMap::from([("name".to_string(), json!("Alice"))]);

    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert_eq!(selects.len(), 1);
            let filter = selects[0].filter.as_ref().unwrap();
            let conditions = filter.conditions();
            let name_cond = conditions.get("name").unwrap();
            assert_eq!(name_cond.get("_eq"), Some(&json!("Alice")));
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_in_limit() {
    let query = r#"
        query($lim: Int!) {
            Users(limit: $lim) {
                _docID
            }
        }
    "#;

    let variables = HashMap::from([("lim".to_string(), json!(10))]);

    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(10));
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_in_doc_ids() {
    let query = r#"
        query($ids: [String!]!) {
            Users(docIDs: $ids) {
                _docID
                name
            }
        }
    "#;

    let variables = HashMap::from([("ids".to_string(), json!(["bae-123", "bae-456"]))]);

    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert_eq!(
                selects[0].doc_ids,
                Some(vec!["bae-123".to_string(), "bae-456".to_string()])
            );
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_in_mutation_input() {
    let query = r#"
        mutation($userName: String!, $userAge: Int!) {
            create_Users(input: [{name: $userName, age: $userAge}]) {
                _docID
            }
        }
    "#;

    let variables = HashMap::from([
        ("userName".to_string(), json!("Bob")),
        ("userAge".to_string(), json!(25)),
    ]);

    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Mutation { mutations, .. } => {
            assert_eq!(mutations.len(), 1);
            let input = &mutations[0].create_input[0];
            assert_eq!(input.get("name"), Some(&json!("Bob")));
            assert_eq!(input.get("age"), Some(&json!(25)));
        }
        _ => panic!("Expected mutation"),
    }
}

#[test]
fn test_variable_in_mutation_doc_ids() {
    let query = r#"
        mutation($id: String!) {
            delete_Users(docIDs: [$id]) {
                _docID
            }
        }
    "#;

    let variables = HashMap::from([("id".to_string(), json!("bae-999"))]);

    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Mutation { mutations, .. } => {
            assert_eq!(mutations[0].doc_ids, Some(vec!["bae-999".to_string()]));
        }
        _ => panic!("Expected mutation"),
    }
}

#[test]
fn test_undefined_variable_error() {
    let query = r#"
        query {
            Users(filter: {name: {_eq: $undefined}}) {
                _docID
            }
        }
    "#;

    let variables = HashMap::new();
    let result = parse_request_with_variables(query, Some(&variables), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("was not provided"));
}

#[test]
fn test_no_variables_provided_error() {
    let query = r#"
        query {
            Users(filter: {name: {_eq: $name}}) {
                _docID
            }
        }
    "#;

    let result = parse_request_with_variables(query, None, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("was not provided"));
}

#[test]
fn test_query_without_variables_still_works() {
    let query = r#"
        {
            Users(filter: {name: {_eq: "Alice"}}) {
                _docID
                name
            }
        }
    "#;

    // No variables provided
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert_eq!(selects.len(), 1);
            let filter = selects[0].filter.as_ref().unwrap();
            let conditions = filter.conditions();
            let name_cond = conditions.get("name").unwrap();
            assert_eq!(name_cond.get("_eq"), Some(&json!("Alice")));
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_type_mismatch_int() {
    let query = r#"
        query($lim: Int!) {
            Users(limit: $lim) {
                _docID
            }
        }
    "#;

    // Provide string instead of int
    let variables = HashMap::from([("lim".to_string(), json!("not an int"))]);
    let result = parse_request_with_variables(query, Some(&variables), None);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("must be of type Int"));
}

#[test]
fn test_multiple_variables() {
    let query = r#"
        query($name: String!, $minAge: Int!, $lim: Int!) {
            Users(filter: {name: {_eq: $name}, age: {_gte: $minAge}}, limit: $lim) {
                _docID
                name
                age
            }
        }
    "#;

    let variables = HashMap::from([
        ("name".to_string(), json!("Alice")),
        ("minAge".to_string(), json!(18)),
        ("lim".to_string(), json!(5)),
    ]);

    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(5));
            let filter = selects[0].filter.as_ref().unwrap();
            let conditions = filter.conditions();
            assert_eq!(
                conditions.get("name").unwrap().get("_eq"),
                Some(&json!("Alice"))
            );
            assert_eq!(conditions.get("age").unwrap().get("_gte"), Some(&json!(18)));
        }
        _ => panic!("Expected query"),
    }
}

// =========================================================================
// Variable type mismatch tests
// =========================================================================

#[test]
fn test_variable_type_mismatch_bool() {
    let query = r#"
        query($deleted: Boolean!) {
            Users(showDeleted: $deleted) {
                _docID
            }
        }
    "#;

    // Provide string instead of bool
    let variables = HashMap::from([("deleted".to_string(), json!("true"))]);
    let result = parse_request_with_variables(query, Some(&variables), None);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("must be of type Boolean"));
}

#[test]
fn test_variable_type_mismatch_string() {
    let query = r#"
        query($c: String!) {
            Users(cid: $c) {
                _docID
            }
        }
    "#;

    // Provide integer instead of string
    let variables = HashMap::from([("c".to_string(), json!(12345))]);
    let result = parse_request_with_variables(query, Some(&variables), None);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("must be of type String"));
}

#[test]
fn test_variable_in_order_direction() {
    let query = r#"
        query($dir: String!) {
            Users(order: {name: $dir}) {
                _docID
                name
            }
        }
    "#;

    let variables = HashMap::from([("dir".to_string(), json!("DESC"))]);
    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            let order = selects[0].order_by.as_ref().unwrap();
            assert_eq!(
                order.conditions[0].direction,
                query_types::mapper::OrderDirection::Desc
            );
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_invalid_order_direction() {
    let query = r#"
        query($dir: String!) {
            Users(order: {name: $dir}) {
                _docID
            }
        }
    "#;

    let variables = HashMap::from([("dir".to_string(), json!("INVALID"))]);
    let result = parse_request_with_variables(query, Some(&variables), None);
    assert!(result.is_err());
    // Error format matches Go DefraDB
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("has invalid value"));
}

// =========================================================================
// Variable default value tests
// =========================================================================

#[test]
fn test_variable_default_value_used_when_not_provided() {
    let query = r#"
        query($name: String = "DefaultName") {
            Users(filter: {name: {_eq: $name}}) {
                _docID
                name
            }
        }
    "#;

    // Don't provide the variable - should use default
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            let filter = selects[0].filter.as_ref().unwrap();
            let conditions = filter.conditions();
            assert_eq!(
                conditions.get("name").unwrap().get("_eq"),
                Some(&json!("DefaultName"))
            );
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_provided_value_overrides_default() {
    let query = r#"
        query($name: String = "DefaultName") {
            Users(filter: {name: {_eq: $name}}) {
                _docID
                name
            }
        }
    "#;

    // Provide a value - should override default
    let variables = HashMap::from([("name".to_string(), json!("ProvidedName"))]);
    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            let filter = selects[0].filter.as_ref().unwrap();
            let conditions = filter.conditions();
            assert_eq!(
                conditions.get("name").unwrap().get("_eq"),
                Some(&json!("ProvidedName"))
            );
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_default_int_value() {
    let query = r#"
        query($lim: Int = 50) {
            Users(limit: $lim) {
                _docID
            }
        }
    "#;

    // Don't provide the variable - should use default 50
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(50));
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_default_boolean_value() {
    let query = r#"
        query($deleted: Boolean = true) {
            Users(showDeleted: $deleted) {
                _docID
            }
        }
    "#;

    // Don't provide the variable - should use default true
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert!(selects[0].show_deleted);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_multiple_variables_with_some_defaults() {
    let query = r#"
        query($name: String!, $minAge: Int = 18, $lim: Int = 10) {
            Users(filter: {name: {_eq: $name}, age: {_gte: $minAge}}, limit: $lim) {
                _docID
                name
            }
        }
    "#;

    // Only provide $name, use defaults for $minAge and $lim
    let variables = HashMap::from([("name".to_string(), json!("Alice"))]);
    let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(10));
            let filter = selects[0].filter.as_ref().unwrap();
            let conditions = filter.conditions();
            assert_eq!(
                conditions.get("name").unwrap().get("_eq"),
                Some(&json!("Alice"))
            );
            assert_eq!(conditions.get("age").unwrap().get("_gte"), Some(&json!(18)));
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_mutation_variable_default_value() {
    let query = r#"
        mutation($name: String = "DefaultUser") {
            create_Users(input: [{name: $name}]) {
                _docID
            }
        }
    "#;

    // Don't provide the variable - should use default
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Mutation { mutations, .. } => {
            let input = &mutations[0].create_input;
            assert_eq!(input[0].get("name"), Some(&json!("DefaultUser")));
        }
        _ => panic!("Expected mutation"),
    }
}

#[test]
fn test_variable_default_array_value() {
    let query = r#"
        query($ids: [String!] = ["id1", "id2"]) {
            Users(docIDs: $ids) {
                _docID
            }
        }
    "#;

    // Don't provide the variable - should use default array
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            let doc_ids = selects[0].doc_ids.as_ref().unwrap();
            assert_eq!(doc_ids.len(), 2);
            assert_eq!(doc_ids[0], "id1");
            assert_eq!(doc_ids[1], "id2");
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_default_null_value() {
    let query = r#"
        query($name: String = null) {
            Users(filter: {name: {_eq: $name}}) {
                _docID
            }
        }
    "#;

    // Don't provide the variable - should use default null
    let result = parse_request_with_variables(query, None, None).unwrap();
    match result {
        ParsedOperation::Query { selects, .. } => {
            let filter = selects[0].filter.as_ref().unwrap();
            let conditions = filter.conditions();
            assert_eq!(
                conditions.get("name").unwrap().get("_eq"),
                Some(&JsonValue::Null)
            );
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_variable_default_cannot_reference_other_variable() {
    // Note: GraphQL spec doesn't allow variable references in default values
    // graphql-parser rejects this at parse time with a parse error
    let query = r#"
        query($a: String = $b, $b: String = "test") {
            Users(filter: {name: {_eq: $a}}) {
                _docID
            }
        }
    "#;

    let result = parse_request_with_variables(query, None, None);
    // graphql-parser rejects this at the parse level since variable references
    // aren't allowed in default value position per the GraphQL spec
    assert!(
        result.is_err(),
        "Expected error for variable reference in default value, but got: {:?}",
        result
    );
}
