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
use defra_harness::{DefraClient, TestCluster};
use std::time::{Duration, Instant};

fn create_user(node: &DefraClient, name: &str, age: i64) -> String {
    let d = node
        .query(&format!(
            r#"mutation {{ add_User(input: {{name: "{name}", age: {age}}}) {{ _docID }} }}"#
        ))
        .expect("create user");
    d["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string()
}

/// Poll node1 until it has `doc_id`; `Ok(())` if it arrives within `timeout`.
async fn poll_node1_has(node: &DefraClient, doc_id: &str, timeout: Duration) -> Result<(), ()> {
    let deadline = Instant::now() + timeout;
    loop {
        let r = node
            .query("query { User { _docID } }")
            .expect("node1 query");
        let present = r["User"]
            .as_array()
            .map(|a| a.iter().any(|x| x["_docID"].as_str() == Some(doc_id)))
            .unwrap_or(false);
        if present {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

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

/// Replicator resume across an actual disconnect: node1 is restarted (severing
/// the P2P connection — config-level replicator add/delete does NOT gate live
/// sync between connected peers, only a real disconnect does). A write made on
/// node0 after the disruption must still arrive once node1 returns and the
/// replicator is re-established, and node1 must retain its prior state across
/// the restart. Model: `MC_Replicator_Resumable_Green` vs `MC_Replicator_Naive_Red`.
#[tokio::test]
async fn replicator_resume_across_restart() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        // A disk store so node1's state (schema + documents) survives the restart;
        // the default in-memory store is wiped when the process is respawned.
        .with_store("redb")
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type User { name: String  age: Int }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");

    let addr1 = {
        let info = cluster.client(1).p2p_info().expect("p2p info node1");
        info.as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .expect("node1 p2p address")
            .to_string()
    };
    cluster
        .client(0)
        .p2p_connect(&[addr1.as_str()])
        .expect("connect");
    cluster
        .client(0)
        .p2p_collection_add(&["User"])
        .expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&["User"])
        .expect("subscribe node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &addr1)
        .expect("set replicator node0 -> node1");

    // Baseline: a write replicates forward while both nodes are up.
    let id1 = create_user(&cluster.client(0), "Baseline", 1);
    poll_node1_has(&cluster.client(1), &id1, Duration::from_secs(20))
        .await
        .expect("baseline write must replicate live before the disconnect");

    // Disconnect: restart node1 (preserves its data dir, severs the connection).
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    // State survived the restart: node1 still has the baseline document.
    poll_node1_has(&cluster.client(1), &id1, Duration::from_secs(10))
        .await
        .expect("baseline write must persist across node1 restart");

    // A write made on node0 after the disruption.
    let id2 = create_user(&cluster.client(0), "PostRestart", 2);

    // Resume: reconnect and re-establish the replicator; the post-disconnect
    // write must be delivered — no loss across the disconnect/reconnect.
    let addr1b = {
        let info = cluster
            .client(1)
            .p2p_info()
            .expect("p2p info node1 after restart");
        info.as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .expect("node1 p2p address after restart")
            .to_string()
    };
    cluster.client(0).p2p_connect(&[addr1b.as_str()]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &addr1b)
        .expect("re-establish replicator after restart");
    poll_node1_has(&cluster.client(1), &id2, Duration::from_secs(30))
        .await
        .expect("post-disconnect write must arrive after reconnect (no loss / resume)");
}
