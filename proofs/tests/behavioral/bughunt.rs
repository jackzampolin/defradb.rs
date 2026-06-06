//! BUG-HUNT (ignored, reporting) — convergence/concurrency probes against the
//! real binary, on the surface where the LWW priority-reconcile bug lived. These
//! report each node's converged state (not assert) so divergences are visible.
//!
//! Run: cargo test -p conformance --test tla_conformance bughunt:: \
//!        -- --ignored --test-threads=1 --nocapture

use crate::support;
use defra_harness::{DefraClient, TestCluster};
use std::time::{Duration, Instant};

fn node_addr(cluster: &TestCluster, i: usize) -> String {
    let info = cluster.client(i).p2p_info().expect("p2p info");
    info.as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .expect("p2p address")
        .to_string()
}

fn hits(node: &DefraClient) -> i64 {
    node.query("query { Tally { hits } }").expect("query Tally")["Tally"][0]["hits"]
        .as_i64()
        .unwrap_or(-1)
}

async fn wire(cluster: &TestCluster) {
    let (a0, a1) = (node_addr(cluster, 0), node_addr(cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["Tally"]).ok();
    cluster.client(1).p2p_collection_add(&["Tally"]).ok();
    cluster.client(0).p2p_replicator_set(&["Tally"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["Tally"], &a0).ok();
}

/// Concurrent PCounter increments (node0 +45, node1 +45) — optionally across a
/// restart-partition — must converge to the SUM (90) on both nodes. Counters are
/// non-idempotent (applied-CID dedup), so a re-merge that drops or double-counts
/// an increment is a serious, visible bug.
async fn run_counter(label: &str, restart: Option<usize>) {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        // Stable peer-id across restart (so a peer-id change can't confound).
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    wire(&cluster).await;

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await; // converge seed

    if let Some(idx) = restart {
        cluster
            .restart_node(idx, Duration::from_secs(30))
            .await
            .expect("restart node");
    }

    let inc = |node: &DefraClient| {
        node.query(&format!(
            r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 45}}) {{ _docID }} }}"#
        ))
        .expect("increment");
    };
    inc(&cluster.client(0));
    inc(&cluster.client(1));

    if restart.is_some() {
        let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
        cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
        cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
        cluster.client(0).p2p_collection_add(&["Tally"]).ok();
        cluster.client(1).p2p_collection_add(&["Tally"]).ok();
        cluster
            .client(0)
            .p2p_replicator_delete(&["Tally"], Some(&a1b))
            .ok();
        cluster
            .client(1)
            .p2p_replicator_delete(&["Tally"], Some(&a0b))
            .ok();
        cluster.client(0).p2p_replicator_set(&["Tally"], &a1b).ok();
        cluster.client(1).p2p_replicator_set(&["Tally"], &a0b).ok();
    }

    // Poll for convergence up to a deadline (counters merge by accumulating
    // unique deltas, so give it time).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (h0, h1) = (hits(&cluster.client(0)), hits(&cluster.client(1)));
        if (h0 == h1 && h0 == 90) || Instant::now() >= deadline {
            let converged = h0 == h1 && h0 == 90;
            eprintln!(
                "BUGHUNT[{label}] node0.hits={h0} node1.hits={h1} | expected 90 | CONVERGED={converged}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_counter_live() {
    run_counter("counter_live", None).await;
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_counter_restart() {
    run_counter("counter_restart(node1)", Some(1)).await;
}

/// MULTI-ROUND counter robustness for the reconcile fix: each node runs `rounds`
/// sequential `+10` local increments, interleaved with live replication, so the
/// store<->blob reconcile is exercised repeatedly. Expected total = seed(0) +
/// 2*rounds*10. A reconcile that double-counts under repeated local+remote
/// interleaving would overshoot; one that drops would undershoot.
async fn run_counter_multiround(rounds: usize) {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    wire(&cluster).await;

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await;

    for _ in 0..rounds {
        for n in [0usize, 1] {
            cluster
                .client(n)
                .query(&format!(
                    r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 10}}) {{ _docID }} }}"#
                ))
                .expect("increment");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let want = (2 * rounds * 10) as i64;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (h0, h1) = (hits(&cluster.client(0)), hits(&cluster.client(1)));
        if (h0 == h1 && h0 == want) || Instant::now() >= deadline {
            let converged = h0 == h1 && h0 == want;
            eprintln!(
                "BUGHUNT[counter_multiround({rounds})] node0.hits={h0} node1.hits={h1} | expected {want} | CONVERGED={converged}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_counter_multiround() {
    run_counter_multiround(5).await;
}

/// DELETE vs concurrent UPDATE race. node0 deletes the document while node1
/// updates a field, concurrently, then both heal. Go gates counter/LWW merges on
/// a `DeletedObjectMarker`; this reports each node's view (doc present? field
/// value?) so a divergent delete/update resolution is visible.
#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_delete_update_race() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node cluster");

    let schema = "type User { name: String  age: Int }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster.client(0).p2p_replicator_set(&["User"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["User"], &a0).ok();

    let created = cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create");
    let id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Concurrent: node0 deletes, node1 updates age.
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ delete_User(docID: "{id}") {{ _docID }} }}"#
        ))
        .ok();
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 99}}) {{ _docID }} }}"#
        ))
        .ok();

    tokio::time::sleep(Duration::from_secs(10)).await;

    let view = |n: usize| -> String {
        let r = cluster
            .client(n)
            .query("query { User { _docID age } }")
            .unwrap_or_default();
        match r["User"].as_array().and_then(|a| a.first()) {
            Some(d) => format!("present(age={})", d["age"]),
            None => "deleted".to_string(),
        }
    };
    let (v0, v1) = (view(0), view(1));
    eprintln!(
        "BUGHUNT[delete_update_race] node0={v0} node1={v1} | CONVERGED={}",
        v0 == v1
    );
}

/// 3-NODE TRANSITIVE propagation: a linear chain node0 -> node1 -> node2 (no
/// direct node0 -> node2 replicator). A write on node0 must reach node2 via
/// node1's onward replication. Reports whether node2 receives it.
#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_3node_transitive() {
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("3-node cluster");

    let schema = "type User { name: String }";
    for n in 0..3 {
        cluster.client(n).schema_add(schema).expect("schema");
        cluster.client(n).p2p_collection_add(&["User"]).ok();
    }
    let addr: Vec<String> = (0..3).map(|n| node_addr(&cluster, n)).collect();
    // Chain: 0 -> 1 -> 2.
    cluster.client(0).p2p_connect(&[addr[1].as_str()]).ok();
    cluster.client(1).p2p_connect(&[addr[2].as_str()]).ok();
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &addr[1])
        .ok();
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &addr[2])
        .ok();

    cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Relayed"}) { _docID } }"#)
        .expect("create");

    let names = |n: usize| -> Vec<String> {
        cluster
            .client(n)
            .query("query { User { name } }")
            .unwrap_or_default()["User"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|u| u["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let got = names(2).iter().any(|s| s == "Relayed");
        if got || Instant::now() >= deadline {
            eprintln!(
                "BUGHUNT[3node_transitive] node1={:?} node2={:?} | node2_received={got}",
                names(1),
                names(2)
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// INDEXED-FIELD convergence: a secondary index is a THIRD materialized view (on
/// top of the document blob and the CRDT store). Two nodes concurrently set the
/// same indexed LWW field to different values; LWW picks the higher value (99).
/// After convergence each node must agree on the value AND its index must be
/// consistent: a filter query for the WINNER returns the doc, and a filter query
/// for the LOSER returns nothing — on BOTH nodes. A stale index entry (the loser
/// value still pointing at the doc, or the winner missing) is the bug this hunts.
///
/// `restart` optionally restarts a node between the seed and the concurrent
/// writes — the exact trigger of the LWW two-store bug (a re-walk re-materializes
/// the doc; does the index follow?).
async fn run_indexed_lww(label: &str, restart: Option<usize>) {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node cluster");

    let schema = "type User { name: String  age: Int @index }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster.client(0).p2p_replicator_set(&["User"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["User"], &a0).ok();

    let created = cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice", age: 10}) { _docID } }"#)
        .expect("create");
    let id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await;

    if let Some(idx) = restart {
        cluster
            .restart_node(idx, Duration::from_secs(30))
            .await
            .expect("restart node");
    }

    // Concurrent same-field LWW: node0 -> 20, node1 -> 99. Higher value wins (99).
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 20}}) {{ _docID }} }}"#
        ))
        .ok();
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 99}}) {{ _docID }} }}"#
        ))
        .ok();

    if restart.is_some() {
        let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
        cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
        cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
        cluster.client(0).p2p_collection_add(&["User"]).ok();
        cluster.client(1).p2p_collection_add(&["User"]).ok();
        cluster
            .client(0)
            .p2p_replicator_delete(&["User"], Some(&a1b))
            .ok();
        cluster
            .client(1)
            .p2p_replicator_delete(&["User"], Some(&a0b))
            .ok();
        cluster.client(0).p2p_replicator_set(&["User"], &a1b).ok();
        cluster.client(1).p2p_replicator_set(&["User"], &a0b).ok();
    }

    tokio::time::sleep(Duration::from_secs(10)).await;

    // Index-filtered counts: how many docs each node returns for a given age.
    let by_index = |n: usize, age: i64| -> usize {
        cluster
            .client(n)
            .query(&format!(
                "query {{ User(filter: {{age: {{_eq: {age}}}}}) {{ name }} }}"
            ))
            .unwrap_or_default()["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    };
    let plain_age = |n: usize| -> i64 {
        cluster
            .client(n)
            .query("query { User { age } }")
            .unwrap_or_default()["User"][0]["age"]
            .as_i64()
            .unwrap_or(-1)
    };
    let (p0, p1) = (plain_age(0), plain_age(1));
    // Winner must be findable by index; loser (20) and the seed (10) must not be.
    let idx_ok = |n: usize| by_index(n, 99) == 1 && by_index(n, 20) == 0 && by_index(n, 10) == 0;
    let converged = p0 == p1 && p0 == 99 && idx_ok(0) && idx_ok(1);

    // Honesty check: confirm the filter query actually planned an index scan (not
    // a full collection scan that would make the idx counts trivial).
    let index_used = cluster
        .client(0)
        .query("query @explain(type: simple) { User(filter: {age: {_eq: 99}}) { name } }")
        .map(|v| v.to_string().to_lowercase().contains("index"))
        .unwrap_or(false);

    eprintln!(
        "BUGHUNT[{label}] node0(age={p0},idx99={},idx20={},idx10={}) node1(age={p1},idx99={},idx20={},idx10={}) | index_used={index_used} | CONVERGED={converged}",
        by_index(0, 99), by_index(0, 20), by_index(0, 10),
        by_index(1, 99), by_index(1, 20), by_index(1, 10),
    );
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_indexed_lww_convergence() {
    run_indexed_lww("indexed_lww_live", None).await;
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_indexed_lww_restart() {
    run_indexed_lww("indexed_lww_restart(node1)", Some(1)).await;
}

/// ENCRYPTED-FIELD convergence: an encrypted field routes its delta through the
/// KMS write path (random per-write key, key distributed over the encryption
/// gossip topic) rather than the plain block path. Two nodes concurrently update
/// the same encrypted LWW field; the LWW winner's delta must be decryptable on
/// both nodes (its key delivered) and both must materialize the same plaintext.
/// A key that doesn't reach the peer, or a two-store reconcile that mishandles
/// the ciphertext, shows up as divergence here.
async fn run_encrypted_lww(label: &str, restart: Option<usize>) {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_encryption()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node encrypted cluster");

    let schema = "type Vault { name: String  secret: String }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["Vault"]).ok();
    cluster.client(1).p2p_collection_add(&["Vault"]).ok();
    cluster.client(0).p2p_replicator_set(&["Vault"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["Vault"], &a0).ok();

    let created = cluster
        .client(0)
        .query(
            r#"mutation { add_Vault(input: {name: "v", secret: "s0"}, encryptFields: [secret]) { _docID } }"#,
        )
        .expect("create encrypted");
    let id = created["add_Vault"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(4)).await;

    if let Some(idx) = restart {
        cluster
            .restart_node(idx, Duration::from_secs(30))
            .await
            .expect("restart node");
    }

    // Concurrent updates to the encrypted field. LWW tie-break (equal priority) ->
    // lexicographically greater value wins; "zzz" > "aaa".
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Vault(docID: "{id}", input: {{secret: "aaa"}}) {{ _docID }} }}"#
        ))
        .ok();
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_Vault(docID: "{id}", input: {{secret: "zzz"}}) {{ _docID }} }}"#
        ))
        .ok();

    if restart.is_some() {
        let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
        cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
        cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
        cluster.client(0).p2p_collection_add(&["Vault"]).ok();
        cluster.client(1).p2p_collection_add(&["Vault"]).ok();
        cluster
            .client(0)
            .p2p_replicator_delete(&["Vault"], Some(&a1b))
            .ok();
        cluster
            .client(1)
            .p2p_replicator_delete(&["Vault"], Some(&a0b))
            .ok();
        cluster.client(0).p2p_replicator_set(&["Vault"], &a1b).ok();
        cluster.client(1).p2p_replicator_set(&["Vault"], &a0b).ok();
    }
    tokio::time::sleep(Duration::from_secs(12)).await;

    let secret = |n: usize| -> String {
        cluster
            .client(n)
            .query("query { Vault { secret } }")
            .unwrap_or_default()["Vault"][0]["secret"]
            .as_str()
            .unwrap_or("<none>")
            .to_string()
    };
    let (s0, s1) = (secret(0), secret(1));
    let converged = s0 == s1 && s0 == "zzz";
    eprintln!(
        "BUGHUNT[{label}] node0.secret={s0:?} node1.secret={s1:?} | expected \"zzz\" | CONVERGED={converged}"
    );
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_encrypted_lww_convergence() {
    run_encrypted_lww("encrypted_lww", None).await;
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_encrypted_lww_restart() {
    run_encrypted_lww("encrypted_lww_restart(node1)", Some(1)).await;
}

/// 3-NODE counter accumulation: each of three fully-meshed nodes increments the
/// same counter by 10, concurrently. Every node's delta must reach the other two
/// and accumulate — all three converge to 30. With only two nodes the counter
/// reconcile is symmetric; a third node exposes whether a delta that arrives via
/// a different peer still accumulates (no dedup collision, no lost cross-peer
/// delta).
#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_counter_3node() {
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("3-node cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
    let addr: Vec<String> = (0..3).map(|n| node_addr(&cluster, n)).collect();
    for n in 0..3 {
        cluster.client(n).schema_add(schema).expect("schema");
        cluster.client(n).p2p_collection_add(&["Tally"]).ok();
    }
    // Full mesh: every node replicates to the other two.
    for i in 0..3 {
        for (j, peer_addr) in addr.iter().enumerate() {
            if i != j {
                cluster.client(i).p2p_connect(&[peer_addr.as_str()]).ok();
                cluster
                    .client(i)
                    .p2p_replicator_set(&["Tally"], peer_addr)
                    .ok();
            }
        }
    }

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // BARRIER: wait until ALL THREE nodes have the seed doc (hits==0) before any
    // node increments — so a node can't build its increment on a pre-seed base.
    // This isolates a genuine merge/dedup race from seed-propagation timing.
    let seed_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if (0..3).all(|n| hits(&cluster.client(n)) == 0) {
            break;
        }
        assert!(
            Instant::now() < seed_deadline,
            "seed did not reach all 3 nodes"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    for n in 0..3 {
        cluster
            .client(n)
            .query(&format!(
                r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 10}}) {{ _docID }} }}"#
            ))
            .expect("increment");
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let h: Vec<i64> = (0..3).map(|n| hits(&cluster.client(n))).collect();
        let ok = h.iter().all(|&x| x == 30);
        if ok || Instant::now() >= deadline {
            eprintln!(
                "BUGHUNT[counter_3node] node0={} node1={} node2={} | expected 30 each | CONVERGED={ok}",
                h[0], h[1], h[2]
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// PNCounter with DECREMENT — the counter reconcile fix (#1014) was validated for
/// PCounter (increment-only). PNCounter allows negative deltas, so a node's local
/// state can be BELOW the seed. node0 +50, node1 -30, concurrently (optionally
/// across a restart-partition); both must converge to seed(0) + 50 - 30 = 20. A
/// reconcile that mishandles a decrement (e.g. clamps at 0, drops the negative
/// local delta, or materializes the signed value wrong) shows up here.
async fn run_pncounter(label: &str, restart: Option<usize>) {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pncounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    wire(&cluster).await;

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await;

    if let Some(idx) = restart {
        cluster
            .restart_node(idx, Duration::from_secs(30))
            .await
            .expect("restart node");
    }

    // node0 increments +50; node1 decrements -30 (PNCounter allows negatives).
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 50}}) {{ _docID }} }}"#
        ))
        .expect("inc node0");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: -30}}) {{ _docID }} }}"#
        ))
        .expect("dec node1");

    if restart.is_some() {
        let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
        cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
        cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
        cluster.client(0).p2p_collection_add(&["Tally"]).ok();
        cluster.client(1).p2p_collection_add(&["Tally"]).ok();
        cluster
            .client(0)
            .p2p_replicator_delete(&["Tally"], Some(&a1b))
            .ok();
        cluster
            .client(1)
            .p2p_replicator_delete(&["Tally"], Some(&a0b))
            .ok();
        cluster.client(0).p2p_replicator_set(&["Tally"], &a1b).ok();
        cluster.client(1).p2p_replicator_set(&["Tally"], &a0b).ok();
    }

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (h0, h1) = (hits(&cluster.client(0)), hits(&cluster.client(1)));
        if (h0 == h1 && h0 == 20) || Instant::now() >= deadline {
            let converged = h0 == h1 && h0 == 20;
            eprintln!(
                "BUGHUNT[{label}] node0.hits={h0} node1.hits={h1} | expected 20 | CONVERGED={converged}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_pncounter_live() {
    run_pncounter("pncounter_live", None).await;
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_pncounter_restart() {
    run_pncounter("pncounter_restart(node1)", Some(1)).await;
}

/// MIXED-FIELD doc: an LWW field (`name`) AND a counter field (`views`) in the
/// same document. node0 updates the LWW field; node1 increments the counter,
/// concurrently (optionally across a restart). Both stores must reconcile
/// INDEPENDENTLY: converge to name="alice" (only node0 set it) AND views=10 (only
/// node1 incremented). The hazard: merging one field type re-materializes the
/// document blob and could clobber the other field's pending local write if the
/// composite re-materialization doesn't reconcile both stores.
async fn run_mixed_fields(label: &str, restart: Option<usize>) {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node cluster");

    let schema = "type Mixed { name: String  views: Int @crdt(type: pcounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["Mixed"]).ok();
    cluster.client(1).p2p_collection_add(&["Mixed"]).ok();
    cluster.client(0).p2p_replicator_set(&["Mixed"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["Mixed"], &a0).ok();

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Mixed(input: {name: "seed", views: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Mixed"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await;

    if let Some(idx) = restart {
        cluster
            .restart_node(idx, Duration::from_secs(30))
            .await
            .expect("restart node");
    }

    // node0 sets the LWW field; node1 increments the counter — different fields.
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Mixed(docID: "{id}", input: {{name: "alice"}}) {{ _docID }} }}"#
        ))
        .expect("node0 name");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_Mixed(docID: "{id}", input: {{views: 10}}) {{ _docID }} }}"#
        ))
        .expect("node1 views");

    if restart.is_some() {
        let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
        cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
        cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
        cluster.client(0).p2p_collection_add(&["Mixed"]).ok();
        cluster.client(1).p2p_collection_add(&["Mixed"]).ok();
        cluster
            .client(0)
            .p2p_replicator_delete(&["Mixed"], Some(&a1b))
            .ok();
        cluster
            .client(1)
            .p2p_replicator_delete(&["Mixed"], Some(&a0b))
            .ok();
        cluster.client(0).p2p_replicator_set(&["Mixed"], &a1b).ok();
        cluster.client(1).p2p_replicator_set(&["Mixed"], &a0b).ok();
    }

    let read = |n: usize| -> (String, i64) {
        let r = cluster
            .client(n)
            .query("query { Mixed { name views } }")
            .unwrap_or_default();
        let d = &r["Mixed"][0];
        (
            d["name"].as_str().unwrap_or("<none>").to_string(),
            d["views"].as_i64().unwrap_or(-1),
        )
    };
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let (n0, v0) = read(0);
        let (n1, v1) = read(1);
        let ok = n0 == "alice" && n1 == "alice" && v0 == 10 && v1 == 10;
        if ok || Instant::now() >= deadline {
            eprintln!(
                "BUGHUNT[{label}] node0=(name={n0},views={v0}) node1=(name={n1},views={v1}) | expected name=alice views=10 | CONVERGED={ok}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_mixed_fields_live() {
    run_mixed_fields("mixed_fields_live", None).await;
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_mixed_fields_restart() {
    run_mixed_fields("mixed_fields_restart(node1)", Some(1)).await;
}
