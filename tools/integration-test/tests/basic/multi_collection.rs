use integration_test::{for_each_runtime, TestCluster};

/// Validates that creating 11+ collections works correctly.
/// Regression test for Go PR #4457 where field short ID retrieval broke
/// with 10+ collections due to prefix-overlap in decimal-encoded IDs.
async fn many_collections_field_ids_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // Create 12 collections — enough to exceed single-digit short IDs
    let schemas = vec![
        "type Alpha { name: String  value: Int }",
        "type Bravo { title: String  count: Int }",
        "type Charlie { label: String  score: Float }",
        "type Delta { tag: String  amount: Int }",
        "type Echo { desc: String  priority: Int }",
        "type Foxtrot { code: String  level: Int }",
        "type Golf { memo: String  rank: Int }",
        "type Hotel { note: String  weight: Float }",
        "type India { body: String  height: Int }",
        "type Juliet { text: String  width: Int }",
        "type Kilo { summary: String  depth: Int }",
        "type Lima { content: String  length: Int }",
    ];

    for sdl in &schemas {
        client.schema_add(sdl).expect("failed to add schema");
    }

    // Insert a document into each collection and query it back
    let collections = [
        (
            "Alpha",
            r#"mutation { add_Alpha(input: {name: "a", value: 1}) { _docID name value } }"#,
            "name",
            "a",
        ),
        (
            "Bravo",
            r#"mutation { add_Bravo(input: {title: "b", count: 2}) { _docID title count } }"#,
            "title",
            "b",
        ),
        (
            "Charlie",
            r#"mutation { add_Charlie(input: {label: "c", score: 3.0}) { _docID label } }"#,
            "label",
            "c",
        ),
        (
            "Delta",
            r#"mutation { add_Delta(input: {tag: "d", amount: 4}) { _docID tag amount } }"#,
            "tag",
            "d",
        ),
        (
            "Echo",
            r#"mutation { add_Echo(input: {desc: "e", priority: 5}) { _docID desc priority } }"#,
            "desc",
            "e",
        ),
        (
            "Foxtrot",
            r#"mutation { add_Foxtrot(input: {code: "f", level: 6}) { _docID code level } }"#,
            "code",
            "f",
        ),
        (
            "Golf",
            r#"mutation { add_Golf(input: {memo: "g", rank: 7}) { _docID memo rank } }"#,
            "memo",
            "g",
        ),
        (
            "Hotel",
            r#"mutation { add_Hotel(input: {note: "h", weight: 8.0}) { _docID note } }"#,
            "note",
            "h",
        ),
        (
            "India",
            r#"mutation { add_India(input: {body: "i", height: 9}) { _docID body height } }"#,
            "body",
            "i",
        ),
        (
            "Juliet",
            r#"mutation { add_Juliet(input: {text: "j", width: 10}) { _docID text width } }"#,
            "text",
            "j",
        ),
        (
            "Kilo",
            r#"mutation { add_Kilo(input: {summary: "k", depth: 11}) { _docID summary depth } }"#,
            "summary",
            "k",
        ),
        (
            "Lima",
            r#"mutation { add_Lima(input: {content: "l", length: 12}) { _docID content length } }"#,
            "content",
            "l",
        ),
    ];

    for (col_name, mutation, _, _) in &collections {
        let data = client
            .query(mutation)
            .unwrap_or_else(|e| panic!("failed to create doc in {}: {}", col_name, e));

        let create_key = format!("add_{}", col_name);
        assert!(
            data[&create_key][0]["_docID"].is_string(),
            "missing _docID for {}",
            col_name
        );
    }

    // Query each collection back to verify field values are correct
    for (col_name, _, field_name, expected_value) in &collections {
        let query = format!("query {{ {} {{ {} }} }}", col_name, field_name);
        let data = client
            .query(&query)
            .unwrap_or_else(|e| panic!("failed to query {}: {}", col_name, e));

        let results = data[*col_name]
            .as_array()
            .unwrap_or_else(|| panic!("expected array for {}", col_name));

        assert_eq!(results.len(), 1, "{} should have 1 document", col_name);
        assert_eq!(
            results[0][*field_name].as_str().unwrap(),
            *expected_value,
            "wrong value for {}.{}",
            col_name,
            field_name
        );
    }
}

for_each_runtime!(many_collections_field_ids, many_collections_field_ids_test);
