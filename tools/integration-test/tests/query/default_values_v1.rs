use integration_test::{for_each_runtime, TestCluster};
use serde_json::{json, Value};

const SCHEMA: &str = r#"
    type Defaults {
        active: Boolean @default(value: true)
        created: DateTime @default(value: "2000-07-23T03:00:00-00:00")
        name: String @default(value: "Bob")
        age: Int @default(value: 40)
        points: Float @default(value: 10)
        points32: Float32 @default(value: 11)
        points64: Float64 @default(value: 12)
        metadata: JSON @default(value: "{\"one\":1}")
        image: Blob @default(value: "ff0099")
    }
"#;

fn row(result: &Value) -> &Value {
    result["Defaults"]
        .as_array()
        .and_then(|rows| rows.first())
        .unwrap_or_else(|| panic!("Defaults row missing from response: {result}"))
}

async fn default_directive_value_v1_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add Defaults schema");
    node.query(r#"mutation { add_Defaults(input: {}) { _docID } }"#)
        .expect("create document with omitted fields");

    let result = node
        .query(
            r#"query {
                Defaults {
                    active
                    created
                    name
                    age
                    points
                    points32
                    points64
                    metadata
                    image
                }
            }"#,
        )
        .expect("query materialized defaults");
    let defaults = row(&result);

    assert_eq!(defaults["active"], json!(true));
    assert_eq!(defaults["created"], json!("2000-07-23T03:00:00Z"));
    assert_eq!(defaults["name"], json!("Bob"));
    assert_eq!(defaults["age"], json!(40));
    assert_eq!(defaults["points"].as_f64(), Some(10.0));
    assert_eq!(defaults["points32"].as_f64(), Some(11.0));
    assert_eq!(defaults["points64"].as_f64(), Some(12.0));
    assert_eq!(defaults["metadata"], json!("{\"one\":1}"));
    assert_eq!(defaults["image"], json!("ff0099"));
}

for_each_runtime!(default_directive_value_v1, default_directive_value_v1_test);
