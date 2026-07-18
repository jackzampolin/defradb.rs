use super::*;
use query_types::mapper::MutationType;

#[test]
fn test_parse_create_mutation() {
    let query = r#"
        mutation {
            create_Users(input: [{name: "Alice", age: 30}]) {
                _docID
                name
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    assert_eq!(mutations.len(), 1);

    let m = &mutations[0];
    assert_eq!(m.mutation_type, MutationType::Create);
    assert_eq!(m.collection_name, "Users");
    assert_eq!(m.create_input.len(), 1);
    assert_eq!(
        m.create_input[0].get("name"),
        Some(&JsonValue::String("Alice".to_string()))
    );
}

#[test]
fn test_parse_create_multiple_documents() {
    let query = r#"
        mutation {
            create_Users(input: [
                {name: "Alice", age: 30},
                {name: "Bob", age: 25}
            ]) {
                _docID
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    assert_eq!(mutations[0].create_input.len(), 2);
}

#[test]
fn test_parse_update_mutation() {
    let query = r#"
        mutation {
            update_Users(docIDs: ["bae-123"], input: {email: "new@example.com"}) {
                _docID
                email
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    assert_eq!(mutations.len(), 1);

    let m = &mutations[0];
    assert_eq!(m.mutation_type, MutationType::Update);
    assert_eq!(m.collection_name, "Users");
    assert_eq!(m.doc_ids, Some(vec!["bae-123".to_string()]));
    assert_eq!(
        m.update_input.get("email"),
        Some(&JsonValue::String("new@example.com".to_string()))
    );
}

#[test]
fn test_parse_update_with_filter() {
    let query = r#"
        mutation {
            update_Users(filter: {name: {_eq: "Alice"}}, input: {active: false}) {
                _docID
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    let m = &mutations[0];
    assert!(m.filter.is_some());
    assert!(m.doc_ids.is_none());
}

#[test]
fn test_parse_delete_mutation() {
    let query = r#"
        mutation {
            delete_Users(docIDs: ["bae-123", "bae-456"]) {
                _docID
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    assert_eq!(mutations.len(), 1);

    let m = &mutations[0];
    assert_eq!(m.mutation_type, MutationType::Delete);
    assert_eq!(m.collection_name, "Users");
    assert_eq!(
        m.doc_ids,
        Some(vec!["bae-123".to_string(), "bae-456".to_string()])
    );
}

#[test]
fn test_parse_delete_with_filter() {
    let query = r#"
        mutation {
            delete_Users(filter: {active: {_eq: false}}) {
                _docID
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    let m = &mutations[0];
    assert!(m.filter.is_some());
}

#[test]
fn test_parse_multiple_mutations() {
    let query = r#"
        mutation {
            create_Users(input: [{name: "Alice"}]) {
                _docID
            }
            delete_Posts(docIDs: ["bae-999"]) {
                _docID
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    assert_eq!(mutations.len(), 2);
    assert_eq!(mutations[0].mutation_type, MutationType::Create);
    assert_eq!(mutations[1].mutation_type, MutationType::Delete);
}

#[test]
fn test_parse_mutation_fragment_spread_in_request_order() {
    let query = r#"
        mutation {
            ...AddFirst
            second: add_User(input: {name: "Second"}) { name }
        }

        fragment AddFirst on Mutation {
            first: add_User(input: {name: "First"}) { name }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    let output_names: Vec<_> = mutations.iter().map(Mutation::output_name).collect();

    assert_eq!(output_names, ["first", "second"]);
}

#[test]
fn test_parse_mutation_inline_fragment_in_request_order() {
    let query = r#"
        mutation {
            first: add_User(input: {name: "First"}) { name }
            ... on Mutation {
                second: add_User(input: {name: "Second"}) { name }
            }
            third: add_User(input: {name: "Third"}) { name }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    let output_names: Vec<_> = mutations.iter().map(Mutation::output_name).collect();

    assert_eq!(output_names, ["first", "second", "third"]);
}

#[test]
fn test_create_missing_input_error() {
    let query = r#"
        mutation {
            create_Users {
                _docID
            }
        }
    "#;

    let result = parse_mutations(query);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("requires 'input'"));
}

#[test]
fn test_update_without_target_succeeds() {
    // Go DefraDB allows update without filter or docIDs (meaning update all)
    let query = r#"
        mutation {
            update_Users(input: {name: "Bob"}) {
                _docID
            }
        }
    "#;

    let result = parse_mutations(query);
    assert!(
        result.is_ok(),
        "update without target should succeed: {:?}",
        result
    );
    let mutations = result.unwrap();
    assert_eq!(mutations.len(), 1);
    assert!(mutations[0].doc_ids.is_none());
    assert!(mutations[0].filter.is_none());
}

#[test]
fn test_delete_without_target_succeeds() {
    // Go DefraDB allows delete without filter or docIDs (meaning delete all)
    let query = r#"
        mutation {
            delete_Users {
                _docID
            }
        }
    "#;

    let result = parse_mutations(query);
    assert!(
        result.is_ok(),
        "delete without target should succeed: {:?}",
        result
    );
    let mutations = result.unwrap();
    assert_eq!(mutations.len(), 1);
    assert!(mutations[0].doc_ids.is_none());
    assert!(mutations[0].filter.is_none());
}

#[test]
fn test_invalid_mutation_name_error() {
    let query = r#"
        mutation {
            Users(input: [{name: "Alice"}]) {
                _docID
            }
        }
    "#;

    let result = parse_mutations(query);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid mutation name"));
}

#[test]
fn test_query_still_works() {
    let query = r#"
        {
            Users {
                _docID
                name
            }
        }
    "#;

    let selects = parse_query(query).unwrap();
    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");
}

#[test]
fn test_cannot_mix_query_and_mutation() {
    // Note: GraphQL parser won't actually allow this syntax,
    // but we handle it anyway
    let query = r#"
        mutation {
            create_Users(input: [{name: "Alice"}]) { _docID }
        }
    "#;

    // This should work as pure mutation
    let result = parse_mutations(query);
    assert!(result.is_ok());

    // parse_query should fail on mutation
    let result = parse_query(query);
    assert!(result.is_err());
}

#[test]
fn test_parse_upsert_mutation_go_style() {
    // Go DefraDB upsert syntax: filter, add, update (all required)
    let query = r#"
        mutation {
            upsert_Users(
                filter: {name: {_eq: "Bob"}},
                add: {name: "Bob", age: 40},
                update: {age: 40}
            ) {
                _docID
                name
                age
            }
        }
    "#;

    let mutations = parse_mutations(query).unwrap();
    assert_eq!(mutations.len(), 1);

    let m = &mutations[0];
    assert_eq!(m.mutation_type, MutationType::Upsert);
    assert_eq!(m.collection_name, "Users");
    assert!(m.filter.is_some());
    // create_input is stored as a single-element vec
    assert_eq!(m.create_input.len(), 1);
    assert_eq!(
        m.create_input[0].get("name"),
        Some(&JsonValue::String("Bob".to_string()))
    );
    // update_input is the fields to update
    assert_eq!(
        m.update_input.get("age"),
        Some(&JsonValue::Number(40.into()))
    );
}

#[test]
fn test_upsert_missing_filter_error() {
    // Go style requires filter
    let query = r#"
        mutation {
            upsert_Users(
                add: {name: "Bob", age: 40},
                update: {age: 40}
            ) {
                _docID
            }
        }
    "#;

    let result = parse_mutations(query);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("filter"));
}

#[test]
fn test_upsert_missing_add_error() {
    // Go style requires add
    let query = r#"
        mutation {
            upsert_Users(
                filter: {name: {_eq: "Bob"}},
                update: {age: 40}
            ) {
                _docID
            }
        }
    "#;

    let result = parse_mutations(query);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("add"));
}

#[test]
fn test_upsert_missing_update_error() {
    // Go style requires update
    let query = r#"
        mutation {
            upsert_Users(
                filter: {name: {_eq: "Bob"}},
                add: {name: "Bob", age: 40}
            ) {
                _docID
            }
        }
    "#;

    let result = parse_mutations(query);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("update"));
}
