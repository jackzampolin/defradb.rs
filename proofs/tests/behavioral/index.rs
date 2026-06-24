//! Index-maintenance — after create/update/delete, an indexed-field lookup
//! returns exactly the live docs: no stale entry, none missing.
//! Model: `proofs/lean/IndexMaintenance` (`onDocumentUpdate_correct`).

use crate::support;
use defra_harness::{DefraClient, TestCluster};
use serde_json::Value;
use std::time::{Duration, Instant};

fn count(v: &Value) -> usize {
    v["User"].as_array().map(|a| a.len()).unwrap_or(0)
}

fn node_addr(cluster: &TestCluster, index: usize) -> String {
    let info = cluster.client(index).p2p_info().expect("p2p info");
    info.as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .expect("p2p address")
        .to_string()
}

fn wire_user(cluster: &TestCluster) {
    let (a0, a1) = (node_addr(cluster, 0), node_addr(cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0)
        .expect("replicator 1->0");
}

fn rewire_user(cluster: &TestCluster) {
    let (a0, a1) = (node_addr(cluster, 0), node_addr(cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster
        .client(0)
        .p2p_replicator_delete(&["User"], Some(&a1))
        .ok();
    cluster
        .client(1)
        .p2p_replicator_delete(&["User"], Some(&a0))
        .ok();
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &a1)
        .expect("rewire replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0)
        .expect("rewire replicator 1->0");
}

fn materialized_age(node: &DefraClient) -> i64 {
    node.query("query { User { age } }").unwrap_or_default()["User"]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|doc| doc["age"].as_i64())
        .unwrap_or(-1)
}

fn count_by_age(node: &DefraClient, age: i64) -> usize {
    support::count_by_index(node, age)
}

async fn poll_age(node: &DefraClient, age: i64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if materialized_age(node) == age {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn indexed_lww_state(node: &DefraClient) -> (i64, usize, usize, usize) {
    (
        materialized_age(node),
        count_by_age(node, 99),
        count_by_age(node, 20),
        count_by_age(node, 10),
    )
}

fn index_filter_uses_index(node: &DefraClient) -> bool {
    node.query("query @explain(type: simple) { User(filter: {age: {_eq: 99}}) { name } }")
        .map(|v| v.to_string().to_lowercase().contains("index"))
        .unwrap_or(false)
}

async fn poll_index_matches_winner(node: &DefraClient, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if indexed_lww_state(node) == (99, 1, 0, 0) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn index_no_stale_no_missing() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build single-node cluster");
    let node = cluster.client(0);
    node.schema_add("type User { name: String @index  age: Int }")
        .expect("schema with @index");

    let created = node
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    let by = |n: &str| {
        format!(r#"query {{ User(filter: {{name: {{_eq: "{n}"}}}}) {{ _docID name }} }}"#)
    };

    // Present after create.
    assert_eq!(
        count(&node.query(&by("Alice")).expect("lookup Alice")),
        1,
        "indexed lookup must find the document after create"
    );

    // Update the indexed field: old key must have NO stale entry; new key present.
    node.query(&format!(
        r#"mutation {{ update_User(docID: "{doc_id}", input: {{name: "Alicia"}}) {{ _docID }} }}"#
    ))
    .expect("update");
    assert_eq!(
        count(&node.query(&by("Alice")).expect("lookup old key")),
        0,
        "old indexed key must have no stale entry after update"
    );
    assert_eq!(
        count(&node.query(&by("Alicia")).expect("lookup new key")),
        1,
        "new indexed key must be present after update (none missing)"
    );

    // Delete: the index entry must be gone.
    node.query(&format!(
        r#"mutation {{ delete_User(docID: "{doc_id}") {{ _docID }} }}"#
    ))
    .expect("delete");
    assert_eq!(
        count(&node.query(&by("Alicia")).expect("lookup after delete")),
        0,
        "deleted document must leave no index entry"
    );
}

/// CRDT merge index reconciliation. The indexed field is an LWW value; after a
/// restart partition, node0 writes the losing value `20` and node1 writes the
/// winning value `99`. When the commit DAG converges, indexed queries must match
/// the materialized winner exactly: `99` finds the doc, while both stale values
/// (`20` and the seed `10`) find nothing.
#[tokio::test]
async fn index_reconciles_lww_merge_after_restart() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type User { name: String  age: Int @index }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    wire_user(&cluster);

    let created = cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice", age: 10}) { _docID } }"#)
        .expect("create");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        poll_age(&cluster.client(1), 10, Duration::from_secs(20)).await,
        "seed document must replicate to node1 before the indexed LWW partition"
    );
    assert!(
        index_filter_uses_index(&cluster.client(0)),
        "age filter must exercise the secondary index, not just a full scan"
    );

    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: 20}}) {{ _docID }} }}"#
        ))
        .expect("node0 writes losing indexed value");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: 99}}) {{ _docID }} }}"#
        ))
        .expect("node1 writes winning indexed value");

    rewire_user(&cluster);
    assert!(
        support::poll_dags_converged(
            &cluster.client(0),
            &cluster.client(1),
            &doc_id,
            Duration::from_secs(40)
        )
        .await,
        "indexed LWW DAGs did not converge; node0={:?} node1={:?}",
        support::commit_cids(&cluster.client(0), &doc_id),
        support::commit_cids(&cluster.client(1), &doc_id)
    );

    for n in 0..2 {
        if !poll_index_matches_winner(&cluster.client(n), Duration::from_secs(30)).await {
            panic!(
                "node{n} index does not match materialized LWW winner; state={:?}",
                indexed_lww_state(&cluster.client(n))
            );
        }
    }
}
