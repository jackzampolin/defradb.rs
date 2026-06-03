//! Replicator lifecycle (no-loss / resume) — a document that ALREADY EXISTS on
//! node0 *before* the replicator is configured is still delivered to node1.
//! The replicator must enumerate pre-existing collection state on setup (the
//! backfill / existing-doc-replay path), not only forward writes that arrive
//! after wiring.
//! Model: `MC_S4_ModelB` / `MC_Conv_Eventual` (proofs/tla) — convergence holds
//! regardless of whether the write precedes or follows replicator configuration.
//!
//! Distinct from `replication::two_node_replication_converges`, which creates
//! the document AFTER all P2P wiring (the live-forward path). Here the document
//! exists first, so the only way it reaches node1 is via backfill enumeration.
//!
//! Anti-tautology: the document's existence on node0 is asserted *before* any
//! wiring or any check against node1, so a node1 hit cannot pass vacuously from
//! a failed source write.

use crate::support;
use defra_harness::TestCluster;
use std::time::{Duration, Instant};

#[tokio::test]
async fn replicator_backfill_no_loss() {
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

    // 1. Create the document on node0 FIRST — before any peer connection,
    //    collection subscription, or replicator is configured.
    let created = node0
        .query(r#"mutation { add_User(input: {name: "Preexisting", age: 11}) { _docID } }"#)
        .expect("create on node0 prior to wiring");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // Anti-tautology (positive asserted before any negative): the document
    // genuinely exists on the source node, so a later node1 hit is meaningful
    // and a later node1 miss is a real no-loss violation rather than a no-op.
    let src = node0
        .query("query { User { _docID name age } }")
        .expect("source query");
    let src_rows = src["User"].as_array().expect("source User array");
    assert_eq!(
        src_rows.len(),
        1,
        "pre-existing document must exist on node0 before the replicator is configured"
    );
    assert_eq!(
        src_rows[0]["_docID"].as_str().unwrap(),
        doc_id,
        "source document _docID must match the created one"
    );

    // 2. NOW wire P2P: connect, subscribe both nodes, then set the replicator.
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
        .expect("set replicator node0 -> node1 (must backfill pre-existing docs)");

    // 3. Poll node1 until the PRE-EXISTING document is delivered. No new write
    //    happens after wiring, so success proves the replicator enumerated and
    //    replayed already-existing state (no loss of pre-existing data).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let r = node1
            .query("query { User { _docID name age } }")
            .expect("node1 query");
        if let Some(u) = r["User"].as_array().and_then(|a| {
            a.iter()
                .find(|row| row["_docID"].as_str() == Some(doc_id.as_str()))
        }) {
            assert_eq!(
                u["name"], "Preexisting",
                "backfilled field value must match the source"
            );
            assert_eq!(u["age"], 11, "backfilled field value must match the source");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "pre-existing document was lost: it did not backfill to node1 within 20s \
             (replicator did not enumerate existing docs)"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
