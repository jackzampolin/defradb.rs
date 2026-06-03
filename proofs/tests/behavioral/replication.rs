//! B3 / DAG convergence — a document created on node0 reaches node1 under a
//! configured replicator (eventual convergence).
//! Model: `MC_S4_ModelB` / `MC_Conv_Eventual` (proofs/tla).

use crate::support;
use defra_harness::TestCluster;
use std::time::{Duration, Instant};

#[tokio::test]
async fn two_node_replication_converges() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let schema = "type User { name: String  age: Int }";
    node0.schema_add(schema).expect("schema node0");
    node1.schema_add(schema).expect("schema node1");

    let info1 = node1.p2p_info().expect("p2p info node1");
    let addr1 = info1
        .as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .expect("node1 p2p address");
    node0.p2p_connect(&[addr1]).expect("connect node0 -> node1");
    node0
        .p2p_collection_add(&["User"])
        .expect("subscribe node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("subscribe node1");
    node0
        .p2p_replicator_set(&["User"], addr1)
        .expect("set replicator node0 -> node1");

    let created = node0
        .query(r#"mutation { add_User(input: {name: "Repl", age: 7}) { _docID } }"#)
        .expect("create on node0");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // Anti-tautology: it genuinely exists on the source node.
    let src = node0
        .query("query { User { _docID } }")
        .expect("source query");
    assert_eq!(
        src["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "document must exist on node0 before checking replication"
    );

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let r = node1
            .query("query { User { _docID name age } }")
            .expect("node1 query");
        if let Some(u) = r["User"].as_array().and_then(|a| a.first()) {
            assert_eq!(
                u["_docID"].as_str().unwrap(),
                doc_id,
                "replicated _docID must match the source"
            );
            assert_eq!(u["name"], "Repl", "replicated field value must match");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "document did not replicate to node1 within 20s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
