use std::process::Command;

use integration_test::{DefraClient, TestCluster};
use serde_json::{json, Value};

struct ScalarCase {
    collection: &'static str,
    kind: &'static str,
    required: &'static str,
    other_required: &'static str,
    optional: &'static str,
    expected_required: Value,
    expected_optional: Value,
    indexed: bool,
}

fn cases() -> Vec<ScalarCase> {
    vec![
        ScalarCase {
            collection: "BoolLensCase",
            kind: "Boolean",
            required: "true",
            other_required: "false",
            optional: "false",
            expected_required: json!(true),
            expected_optional: json!(false),
            indexed: false,
        },
        ScalarCase {
            collection: "IntLensCase",
            kind: "Int",
            required: "42",
            other_required: "8",
            optional: "-7",
            expected_required: json!(42),
            expected_optional: json!(-7),
            indexed: false,
        },
        ScalarCase {
            collection: "FloatLensCase",
            kind: "Float",
            required: "12.5",
            other_required: "1.5",
            optional: "-0.25",
            expected_required: json!(12.5),
            expected_optional: json!(-0.25),
            indexed: false,
        },
        ScalarCase {
            collection: "Float32LensCase",
            kind: "Float32",
            required: "3.5",
            other_required: "2.5",
            optional: "-1.5",
            expected_required: json!(3.5),
            expected_optional: json!(-1.5),
            indexed: false,
        },
        ScalarCase {
            collection: "DateTimeLensCase",
            kind: "DateTime",
            required: r#""2026-08-01T12:34:56Z""#,
            other_required: r#""2026-08-02T12:34:56Z""#,
            optional: r#""1999-12-31T23:59:59Z""#,
            expected_required: json!("2026-08-01T12:34:56Z"),
            expected_optional: json!("1999-12-31T23:59:59Z"),
            indexed: true,
        },
        ScalarCase {
            collection: "StringLensCase",
            kind: "String",
            required: r#""required""#,
            other_required: r#""nulls""#,
            optional: r#""optional""#,
            expected_required: json!("required"),
            expected_optional: json!("optional"),
            indexed: false,
        },
        ScalarCase {
            collection: "BlobLensCase",
            kind: "Blob",
            required: r#""00ff10""#,
            other_required: r#""112233""#,
            optional: r#""abcdef""#,
            expected_required: json!("00ff10"),
            expected_optional: json!("abcdef"),
            indexed: false,
        },
        ScalarCase {
            collection: "JsonLensCase",
            kind: "JSON",
            required: "{present: true}",
            other_required: "{other: false}",
            optional: r#"["value", true, false]"#,
            expected_required: json!({"present": true}),
            expected_optional: json!(["value", true, false]),
            indexed: false,
        },
    ]
}

fn query_both(rust: &DefraClient, go: &DefraClient, query: &str) -> (Value, Value) {
    (
        rust.query(query)
            .unwrap_or_else(|error| panic!("Rust query failed: {error}\n{query}")),
        go.query(query)
            .unwrap_or_else(|error| panic!("Go query failed: {error}\n{query}")),
    )
}

fn create_document(
    rust: &DefraClient,
    go: &DefraClient,
    collection: &str,
    required: &str,
    optional: Option<&str>,
) -> String {
    let optional = optional
        .map(|value| format!(", optional: {value}"))
        .unwrap_or_default();
    let mutation = format!(
        "mutation {{ add_{collection}(input: {{required: {required}{optional}}}) {{ _docID }} }}"
    );
    let (rust_result, go_result) = query_both(rust, go, &mutation);
    let rust_id = rust_result[format!("add_{collection}")][0]["_docID"]
        .as_str()
        .unwrap_or_else(|| panic!("Rust _docID missing from {rust_result}"));
    let go_id = go_result[format!("add_{collection}")][0]["_docID"]
        .as_str()
        .unwrap_or_else(|| panic!("Go _docID missing from {go_result}"));
    assert_eq!(rust_id, go_id, "{collection} document IDs differ");
    rust_id.to_string()
}

fn commits(client: &DefraClient, doc_id: &str) -> Vec<(String, i64, String)> {
    let response = client
        .query(&format!(
            r#"query {{
                _commits(docID: ["{doc_id}"]) {{
                    cid
                    fieldName
                    height
                }}
            }}"#
        ))
        .expect("query document commits");
    let mut commits = response["_commits"]
        .as_array()
        .unwrap_or_else(|| panic!("_commits response is not an array: {response}"))
        .iter()
        .map(|commit| {
            (
                commit["fieldName"].as_str().expect("fieldName").to_string(),
                commit["height"].as_i64().expect("height"),
                commit["cid"].as_str().expect("cid").to_string(),
            )
        })
        .collect::<Vec<_>>();
    commits.sort();
    commits
}

fn assert_cids_match(rust: &DefraClient, go: &DefraClient, doc_ids: &[String]) {
    for doc_id in doc_ids {
        assert_eq!(
            commits(rust, doc_id),
            commits(go, doc_id),
            "block CIDs differ for {doc_id}"
        );
    }
}

fn migration_config() -> String {
    let lens = integration_test::wasm_lens::wasm_lens_defra();
    lens.build().expect("build set_default lens");
    json!({
        "Lenses": [{
            "Path": lens.module_path(),
            "Arguments": {"dst": "migrated", "value": true}
        }]
    })
    .to_string()
}

fn patch_with_migration(client: &DefraClient, url: &str, patch: &str, migration: &str) {
    let output = Command::new(client.binary_path())
        .arg("--url")
        .arg(url.strip_prefix("http://").unwrap_or(url))
        .args(["client", "collection", "patch", patch, migration])
        .output()
        .expect("run collection patch with migration");
    assert!(
        output.status.success(),
        "collection patch with migration failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn rows(client: &DefraClient, collection: &str, query: &str) -> Vec<Value> {
    let mut rows = client.query(query).expect("query migrated documents")[collection]
        .as_array()
        .unwrap_or_else(|| panic!("{collection} rows missing"))
        .clone();
    rows.sort_by(|left, right| left["_docID"].as_str().cmp(&right["_docID"].as_str()));
    rows
}

#[tokio::test]
async fn go_lens_migration_http_parity() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_development()
        .build()
        .await
        .unwrap();
    let rust = cluster.client(0);
    let go = cluster.client(1);
    let migration = migration_config();

    for case in cases() {
        let index = if case.indexed { " @index" } else { "" };
        let schema = format!(
            "type {} {{ required: {}!{} optional: {} }}",
            case.collection, case.kind, index, case.kind
        );
        rust.schema_add(&schema)
            .unwrap_or_else(|error| panic!("add Rust {} schema: {error}", case.collection));
        go.schema_add(&schema)
            .unwrap_or_else(|error| panic!("add Go {} schema: {error}", case.collection));

        let filled_id = create_document(
            &rust,
            &go,
            case.collection,
            case.required,
            Some(case.optional),
        );
        let null_id = create_document(&rust, &go, case.collection, case.other_required, None);
        let doc_ids = vec![filled_id.clone(), null_id.clone()];
        assert_cids_match(&rust, &go, &doc_ids);

        let patch = format!(
            r#"[{{"op":"add","path":"/{}/Fields/-","value":{{"Name":"migrated","Kind":"Boolean"}}}}]"#,
            case.collection
        );
        patch_with_migration(&rust, cluster.api_url(0), &patch, &migration);
        patch_with_migration(&go, cluster.api_url(1), &patch, &migration);
        assert_cids_match(&rust, &go, &doc_ids);

        let query = format!(
            "query {{ {} {{ _docID required optional migrated }} }}",
            case.collection
        );
        let rust_rows = rows(&rust, case.collection, &query);
        let go_rows = rows(&go, case.collection, &query);
        assert_eq!(rust_rows, go_rows, "{} migrated values", case.collection);
        let filled = rust_rows
            .iter()
            .find(|row| row["_docID"] == filled_id)
            .expect("filled document");
        assert_eq!(filled["required"], case.expected_required);
        assert_eq!(filled["optional"], case.expected_optional);
        assert_eq!(filled["migrated"], json!(true));
        let null = rust_rows
            .iter()
            .find(|row| row["_docID"] == null_id)
            .expect("null document");
        assert!(null["optional"].is_null());
        assert_eq!(null["migrated"], json!(true));

        if case.indexed {
            let indexed_query = format!(
                "query {{ {}(filter: {{required: {{_eq: {}}}}}) {{ _docID required migrated }} }}",
                case.collection, case.required
            );
            let rust_indexed = rows(&rust, case.collection, &indexed_query);
            let go_indexed = rows(&go, case.collection, &indexed_query);
            assert_eq!(rust_indexed, go_indexed, "reindexed DateTime query");
            assert_eq!(rust_indexed.len(), 1);
            assert_eq!(rust_indexed[0]["_docID"], filled_id);
        }
    }
}
