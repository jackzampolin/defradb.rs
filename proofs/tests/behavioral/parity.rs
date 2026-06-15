//! PARITY INVESTIGATION (ignored) — does the same-doc concurrent-merge
//! convergence bug (see `partition.rs`) reproduce in Go DefraDB, and do Go and
//! Rust converge across a mixed cluster?
//!
//! Requires the harness-compatible Go `defradb` on PATH (the binary the
//! integration tests use — its CLI has `client collection add`, unlike some
//! branch builds which use `client schema add`):
//!   PATH=<go-repo>/build:$PATH   (i.e. .../sourcenetwork/defradb/build/defradb)
//! Run:
//!   PATH=<go-repo>/build:$PATH cargo test -p conformance --test tla_conformance \
//!     parity:: -- --ignored --test-threads=1 --nocapture
//!
//! The `parity_samedoc_*` / `parity_delete_update_*` probes REPORT (not assert) —
//! they print each node's converged state so we can compare Go-only, Rust-only,
//! and mixed at a semantic-divergence point. The `parity_counter_3node_*` and
//! `parity_indexed_lww_*` tests ASSERT: Rust must converge to the SAME value Go
//! does (Go is the parity target). They stay `#[ignore]` (the default no-Go
//! conformance run skips them) and are exercised by the go-compat CI step, which
//! selects ONLY the asserting tests by name (`-- --ignored parity_counter_3node
//! parity_indexed_lww`) with the Go binary on PATH.

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

fn city_age(node: &DefraClient) -> (String, i64) {
    let r = node
        .query("query { User { age city } }")
        .expect("query User");
    let d = &r["User"][0];
    (
        d["city"].as_str().unwrap_or("<none>").to_string(),
        d["age"].as_i64().unwrap_or(-1),
    )
}

/// Same-doc concurrent-edit convergence: seed one doc, optionally restart
/// `restart` to sever the link (the bug trigger), then node0 updates `age` and
/// node1 updates `city`, heal, and report whether the two replicas agree.
/// `restart = None` runs the LIVE case (no partition) — concurrent edits over a
/// live connection, which isolates pure cross-impl merge interop.
async fn run_samedoc(mut cluster: TestCluster, label: &str, restart: Option<usize>) {
    let schema = "type User { name: String  age: Int  city: String }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");

    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster.client(0).p2p_replicator_set(&["User"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["User"], &a0).ok();

    cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30, city: "NYC"}) { _docID } }"#)
        .expect("create");
    // let the seed converge to node1
    tokio::time::sleep(Duration::from_secs(3)).await;
    let id = cluster
        .client(0)
        .query("query { User { _docID } }")
        .expect("q")["User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    if let Some(idx) = restart {
        cluster
            .restart_node(idx, Duration::from_secs(30))
            .await
            .expect("restart node");
    }

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 31}}) {{ _docID }} }}"#
        ))
        .expect("node0 age");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{city: "LA"}}) {{ _docID }} }}"#
        ))
        .expect("node1 city");

    if restart.is_some() {
        // Heal: reconnect and re-establish replication both ways.
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

    let (c0, ag0) = city_age(&cluster.client(0));
    let (c1, ag1) = city_age(&cluster.client(1));
    let converged = c0 == c1 && ag0 == ag1;
    let restart_desc = match restart {
        Some(i) => format!("node{i}"),
        None => "none(live)".to_string(),
    };
    eprintln!(
        "PARITY[{label}] restart={restart_desc} | node0=(city={c0},age={ag0}) node1=(city={c1},age={ag1}) | CONVERGED={converged} | expected city=LA age=31"
    );
}

/// Mixed Go<->Rust, LIVE (no restart): node0=Rust, node1=Go, concurrent edits to
/// the same document over a live connection. Isolates pure cross-implementation
/// merge interop.
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_samedoc_mixed_live() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed cluster");
    // node0 = Rust, node1 = Go (harness assigns Rust indices first).
    run_samedoc(cluster, "mixed_live(rust0,go1)", None).await;
}

/// Build a mixed Rust(node0)/Go(node1) cluster with per-node native disk stores
/// (`with_node_store`: Rust=redb, Go=badger) so EACH node persists across a
/// restart. This is what makes the strongest cross-impl test — mixed + restart —
/// possible (a cluster-wide store can't satisfy both impls).
async fn mixed_disk_cluster() -> TestCluster {
    TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        // A persistent keyring so each node keeps a STABLE libp2p peer-id across a
        // restart (production behavior). Under `--no-keyring` BOTH impls generate
        // an ephemeral peer-id, so a restart changes it and a peer's replicator
        // can't re-target the new id — not a product bug, a test-mode artifact.
        .with_keyring()
        .with_node_store(0, "redb") // node0 = Rust
        .with_node_store(1, "badger") // node1 = Go
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed disk cluster")
}

/// Mixed + restart with the GO node (node1) restarted — Go restarted, peer Rust.
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_samedoc_mixed_restart_go() {
    run_samedoc(
        mixed_disk_cluster().await,
        "mixed_restart_go(rust0,go1)",
        Some(1),
    )
    .await;
}

/// Mixed + restart with the RUST node (node0) restarted, peer Go.
///
/// Converges (via `mixed_disk_cluster`'s persistent keyring). NOTE on the
/// investigation: under `--no-keyring` this DIVERGED — but it was NOT a product
/// bug. Both Go and Rust generate an EPHEMERAL libp2p peer-id under
/// `--no-keyring`, so a restart changes it; a peer's replicator then can't
/// re-target the new id (Go's delete-by-address 404s, leaving a stale replicator
/// pushing to the dead peer-id) and the restarted node never re-receives the
/// concurrent write. A real node uses a persistent keyring -> stable peer-id ->
/// the connection simply reconnects, which is what this test now exercises.
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_samedoc_mixed_restart_rust() {
    run_samedoc(
        mixed_disk_cluster().await,
        "mixed_restart_rust(rust0,go1)",
        Some(0),
    )
    .await;
}

/// Control: Rust<->Rust (redb) — should reproduce the divergence.
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_samedoc_rust_rust() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("rust-rust cluster");
    run_samedoc(cluster, "rust_rust", Some(1)).await;
}

/// Concurrent PCounter increments (node0 +45, node1 +45) over a cluster — must
/// converge to the SUM (90) on both nodes. The Rust-only case exposed a
/// two-store divergence (local increments updated only the document blob, not
/// the CRDT accumulation store) where the node that received the doc by
/// replication first silently dropped its own increment. This reports the
/// Go-only and mixed cases to confirm Go converges (the parity target) and that
/// Rust<->Go interop accumulates correctly.
async fn run_counter_parity(cluster: TestCluster, label: &str) {
    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");

    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["Tally"]).ok();
    cluster.client(1).p2p_collection_add(&["Tally"]).ok();
    cluster.client(0).p2p_replicator_set(&["Tally"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["Tally"], &a0).ok();

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await;

    for n in [0usize, 1] {
        cluster
            .client(n)
            .query(&format!(
                r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 45}}) {{ _docID }} }}"#
            ))
            .expect("increment");
    }

    tokio::time::sleep(Duration::from_secs(10)).await;

    let hits = |n: usize| -> i64 {
        cluster
            .client(n)
            .query("query { Tally { hits } }")
            .expect("q Tally")["Tally"][0]["hits"]
            .as_i64()
            .unwrap_or(-1)
    };
    let (h0, h1) = (hits(0), hits(1));
    let converged = h0 == h1 && h0 == 90;
    eprintln!(
        "PARITY[{label}] node0.hits={h0} node1.hits={h1} | expected 90 | CONVERGED={converged}"
    );
}

/// Concurrent DELETE vs UPDATE on the same document. node0 deletes, node1
/// updates a field, concurrently, over a live link. Reports each node's final
/// view so the delete-vs-update resolution can be compared across impls — a
/// classic semantic divergence point (does delete win, or does the concurrent
/// update revive the doc?). The property that matters is that BOTH agree AND that
/// Go and Rust agree with EACH OTHER.
async fn run_delete_update_parity(cluster: TestCluster, label: &str) {
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
        "PARITY[{label}] node0={v0} node1={v1} | CONVERGED={}",
        v0 == v1
    );
}

/// Go<->Go delete-vs-update resolution (badger).
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_delete_update_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_store("badger")
        .with_development()
        .build()
        .await
        .expect("go-go cluster");
    run_delete_update_parity(cluster, "delete_update_go_go").await;
}

/// Mixed Rust(node0, deletes)<->Go(node1, updates) delete-vs-update resolution.
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_delete_update_mixed() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed cluster");
    run_delete_update_parity(cluster, "delete_update_mixed(rust0_del,go1_upd)").await;
}

/// Go<->Go counter convergence (badger) — the parity target.
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_counter_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_store("badger")
        .with_development()
        .build()
        .await
        .expect("go-go cluster");
    run_counter_parity(cluster, "counter_go_go").await;
}

/// Mixed Rust(node0)<->Go(node1) counter convergence, live.
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_counter_mixed_live() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed cluster");
    run_counter_parity(cluster, "counter_mixed_live(rust0,go1)").await;
}

/// The key question: Go<->Go (badger) — does Go diverge too?
#[ignore = "parity investigation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_samedoc_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_store("badger")
        // Go under the harness default `--no-keyring` requires `--development`.
        .with_development()
        .build()
        .await
        .expect("go-go cluster");
    run_samedoc(cluster, "go_go", Some(1)).await;
}

fn tally_hits(node: &DefraClient) -> i64 {
    node.query("query { Tally { hits } }").unwrap_or_default()["Tally"][0]["hits"]
        .as_i64()
        .unwrap_or(-1)
}

async fn poll_all_tally_hits(
    cluster: &TestCluster,
    nodes: usize,
    want: i64,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if (0..nodes).all(|n| tally_hits(&cluster.client(n)) == want) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// THREE-node PCounter parity (ASSERTING): three fully-meshed nodes each `+10`
/// must converge to `30` on every node. Go is the parity target — `go_go` must
/// converge to 30, and a mixed Rust/Go mesh must agree. This is the cross-impl
/// twin of `partition::convergence_concurrent_counter_3node_full_mesh_sum`: it
/// confirms Rust's two-store counter reconcile produces the same accumulation Go
/// does even when each delta arrives via two distinct peers of the OTHER impl.
async fn run_counter_3node_parity(cluster: TestCluster, label: &str) {
    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
    let addr: Vec<String> = (0..3).map(|n| node_addr(&cluster, n)).collect();
    for n in 0..3 {
        cluster.client(n).schema_add(schema).expect("schema");
        cluster
            .client(n)
            .p2p_collection_add(&["Tally"])
            .expect("subscribe");
    }
    // Fail fast on a wiring error rather than degrade into a converge-deadline
    // timeout (a swallowed setup failure would look like non-convergence).
    for i in 0..3 {
        for (j, peer) in addr.iter().enumerate() {
            if i != j {
                cluster
                    .client(i)
                    .p2p_connect(&[peer.as_str()])
                    .expect("connect");
                cluster
                    .client(i)
                    .p2p_replicator_set(&["Tally"], peer)
                    .expect("replicator");
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

    // Barrier: every node holds the seed before any increment.
    assert!(
        poll_all_tally_hits(&cluster, 3, 0, Duration::from_secs(30)).await,
        "[{label}] seed (hits=0) did not reach all three nodes"
    );

    for n in 0..3 {
        cluster
            .client(n)
            .query(&format!(
                r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 10}}) {{ _docID }} }}"#
            ))
            .expect("increment");
    }

    let converged = poll_all_tally_hits(&cluster, 3, 30, Duration::from_secs(40)).await;
    assert!(
        converged,
        "[{label}] did not converge to 30 on all nodes; hits = [{}, {}, {}]",
        tally_hits(&cluster.client(0)),
        tally_hits(&cluster.client(1)),
        tally_hits(&cluster.client(2)),
    );
}

/// Go<->Go<->Go 3-node counter (badger) — the parity target.
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_counter_3node_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(3)
        .with_p2p()
        .with_store("badger")
        .with_development()
        .build()
        .await
        .expect("go-go-go cluster");
    run_counter_3node_parity(cluster, "counter_3node_go_go").await;
}

/// Mixed Rust(node0)<->Go(node1,node2) 3-node counter — a Rust creator with two
/// Go peers in a full mesh; all must agree with Go's accumulation.
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_counter_3node_mixed() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(2)
        .with_p2p()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed 3-node cluster");
    run_counter_3node_parity(cluster, "counter_3node_mixed(rust0,go1,go2)").await;
}

async fn poll_index_resolved(node: &DefraClient, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if support::indexed_age(node) == 99
            && support::count_by_index(node, 99) == 1
            && support::count_by_index(node, 20) == 0
            && support::count_by_index(node, 10) == 0
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// INDEXED-LWW parity (ASSERTING): an `@index`'d LWW field updated concurrently
/// (node0 -> 20, node1 -> 99) must, on both impls, materialize 99 AND resolve
/// ONLY 99 through the index — no stale entry for the seed (10) or the loser
/// (20). Cross-impl twin of `partition::convergence_indexed_lww_restart_merge`:
/// it confirms Rust's index maintenance follows the reconciled merge exactly as
/// Go's does.
///
/// `rust_explain_node` names the Rust node (when the cluster has one) on which to
/// assert the filter actually plans an index scan — otherwise a broken index that
/// silently fell back to a full collection scan would still return the right
/// counts and the assertions would prove nothing. We only check the Rust node
/// (the regression target); Go is the trusted reference, and Go/Rust print
/// different explain shapes so a single substring check can't span both.
async fn run_indexed_lww_parity(
    cluster: TestCluster,
    label: &str,
    rust_explain_node: Option<usize>,
) {
    let schema = "type User { name: String  age: Int @index }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");

    // Fail fast on a wiring error rather than degrade into a converge-deadline
    // timeout (a swallowed setup failure would look like non-convergence).
    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster
        .client(0)
        .p2p_connect(&[a1.as_str()])
        .expect("connect 0->1");
    cluster
        .client(1)
        .p2p_connect(&[a0.as_str()])
        .expect("connect 1->0");
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
        .p2p_replicator_set(&["User"], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0)
        .expect("replicator 1->0");

    let created = cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice", age: 10}) { _docID } }"#)
        .expect("create");
    let id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // Barrier: node1 has the seed (resolvable by index) before the concurrent edits.
    let seed_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if support::indexed_age(&cluster.client(1)) == 10
            && support::count_by_index(&cluster.client(1), 10) == 1
        {
            break;
        }
        assert!(
            Instant::now() < seed_deadline,
            "[{label}] seed (age=10) did not reach node1 via index"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Concurrent same-field LWW: node0 -> 20, node1 -> 99. Higher value wins (99).
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 20}}) {{ _docID }} }}"#
        ))
        .expect("node0 age=20");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 99}}) {{ _docID }} }}"#
        ))
        .expect("node1 age=99");

    // MERGE PROOF: node1 locally wrote the winner (99), so its index-resolved
    // check is satisfied by its own write; require the identical commit DAG on
    // both impls first, so node1's leg only passes once it has actually MERGED
    // node0's delta (and node0 received node1's) — not from a local write alone.
    assert!(
        support::poll_dags_converged(
            &cluster.client(0),
            &cluster.client(1),
            &id,
            Duration::from_secs(40)
        )
        .await,
        "[{label}] indexed-LWW DAGs did not converge across impls: a replica never merged the other's delta"
    );

    for n in [0usize, 1] {
        assert!(
            poll_index_resolved(&cluster.client(n), Duration::from_secs(40)).await,
            "[{label}] node{n} index did not reconcile to 99-only; age={} idx99={} idx20={} idx10={}",
            support::indexed_age(&cluster.client(n)),
            support::count_by_index(&cluster.client(n), 99),
            support::count_by_index(&cluster.client(n), 20),
            support::count_by_index(&cluster.client(n), 10),
        );
    }

    // Honesty: confirm the Rust node actually plans an index scan, so the counts
    // above exercise index maintenance rather than a full collection scan.
    if let Some(n) = rust_explain_node {
        let index_used = cluster
            .client(n)
            .query("query @explain(type: simple) { User(filter: {age: {_eq: 99}}) { name } }")
            .map(|v| v.to_string().to_lowercase().contains("index"))
            .unwrap_or(false);
        assert!(
            index_used,
            "[{label}] node{n} (Rust) must plan an index scan, else the index counts prove nothing"
        );
    }
}

/// Go<->Go indexed-LWW (badger) — the parity target.
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_indexed_lww_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_store("badger")
        .with_development()
        .build()
        .await
        .expect("go-go cluster");
    run_indexed_lww_parity(cluster, "indexed_lww_go_go", None).await;
}

/// Mixed Rust(node0)<->Go(node1) indexed-LWW, live — Rust seeds/loses, Go wins;
/// both impls must resolve the winner through the index.
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_indexed_lww_mixed() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed cluster");
    run_indexed_lww_parity(cluster, "indexed_lww_mixed(rust0,go1)", Some(0)).await;
}
