//! #72 regression: a DateTime field must stay visible to the cursor across a
//! reindex.
//!
//! Documents loaded from CBOR storage carry DateTime fields as `String` (storage
//! is schema-blind: `Time` is written as an untagged text string and read back as
//! `String`). When a reindex rebuilds the secondary index from stored documents,
//! the index builder must re-coerce them to `Time` — otherwise the entries are
//! `encode_string_*` instead of `encode_time_*` (a disjoint byte range) and the
//! rows silently vanish from `order:[{created_at: …}]` cursor queries. This is the
//! production "#72" bug (3,505 of 4,540 CodingSessions hidden after a reindex).
//!
//! The reindex is triggered the same way prod hit it: registering a lens
//! migration (`maybe_reindex_after_migration` → `reindex.rs`).

use integration_test::{DefraClient, TestCluster};
use serde_json::Value;

const SCHEMA: &str = "type Item { name: String  created_at: DateTime }";
const PATCH_V1_TO_V2: &str =
    r#"[{"op":"add","path":"/Item/Fields/-","value":{"Name":"placeholder","Kind":"String"}}]"#;

fn version_id(client: &DefraClient) -> String {
    client
        .collection_describe_version("Item")
        .expect("describe Item")["VersionID"]
        .as_str()
        .expect("VersionID")
        .to_string()
}

#[tokio::test]
async fn datetime_index_survives_reindex_and_stays_cursor_visible() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_development()
        .build()
        .await
        .unwrap();
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema add");
    let v1 = version_id(&node);

    let stamps = [
        "2026-01-05T18:05:40Z",
        "2026-02-10T09:00:00Z",
        "2026-03-15T12:30:00Z",
        "2026-04-20T08:15:00Z",
        "2026-05-29T13:06:28Z",
    ];
    for (i, ts) in stamps.iter().enumerate() {
        node.query(&format!(
            r#"mutation {{ add_Item(input: {{ name: "e{i}", created_at: "{ts}" }}) {{ _docID }} }}"#
        ))
        .expect("seed item");
    }

    node.index_create("Item", &["created_at"], Some("idx_created_at"), false)
        .expect("create created_at index");

    // Trigger a reindex by registering a lens migration (v1 -> v2). The reindex
    // reads each document back from storage (DateTime decodes as String) and
    // rebuilds the index — the path the production "maintenance reindex" hit.
    node.collection_patch(PATCH_V1_TO_V2).expect("patch to v2");
    let v2 = version_id(&node);
    let lens = integration_test::wasm_lens::wasm_lens_defra();
    lens.build().expect("build set_default lens");
    let cfg = serde_json::json!({
        "Lenses": [{
            "Path": lens.module_path(),
            "Arguments": {"dst": "placeholder", "value": "x"}
        }]
    })
    .to_string();
    node.lens_set(&v1, &v2, &cfg).expect("lens_set v1->v2");

    // All five rows must remain reachable through the DateTime cursor.
    let result: Value = node
        .query(
            r#"{ _cursor { Item(first: 100, order: [{created_at: ASC}]) { _docID created_at } } }"#,
        )
        .expect("cursor query");
    let rows = result["_cursor"]["Item"].as_array().expect("Item array");
    assert_eq!(
        rows.len(),
        stamps.len(),
        "all DateTime rows must remain visible after a reindex; got {} of {} \
         (String-typed entries are excluded from the DateTime index)",
        rows.len(),
        stamps.len()
    );
}
