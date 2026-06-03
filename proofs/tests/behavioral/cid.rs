//! CID family — the content-addressed `_docID` the binary computes is
//! deterministic across independent instances and distinguishes content.
//! Model: `proofs/lean/Cid` (`cid_injective_mod_hash`).

use crate::support;
use defra_harness::TestCluster;

async fn doc_id_for(input: &str) -> String {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build single-node cluster against the release binary");
    let client = cluster.client(0);
    client
        .schema_add("type User { name: String  age: Int }")
        .expect("schema add");
    let data = client
        .query(&format!(
            "mutation {{ add_User(input: {{ {input} }}) {{ _docID }} }}"
        ))
        .expect("create document");
    data["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID present in mutation result")
        .to_string()
}

#[tokio::test]
async fn cid_determinism_same_content_same_docid() {
    let a = doc_id_for(r#"name: "Alice", age: 30"#).await;
    let b = doc_id_for(r#"name: "Alice", age: 30"#).await;
    assert_eq!(
        a, b,
        "same content must yield the same content-addressed _docID"
    );

    let c = doc_id_for(r#"name: "Bob", age: 30"#).await;
    assert_ne!(a, c, "different content must yield a different _docID");
}
