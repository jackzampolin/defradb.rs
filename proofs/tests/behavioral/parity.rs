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
//! and mixed at a semantic-divergence point. The `parity_counter_3node_*`,
//! `parity_mixed_fields_3node_*`, `parity_lww_tie_partition_*`, and
//! `parity_indexed_lww_*` tests ASSERT: Rust must converge to the SAME value Go
//! does (Go is the parity target). They stay `#[ignore]` (the default no-Go
//! conformance run skips them) and are exercised manually unless the go-compat CI
//! step opts into them.
//!
//! `parity_unique_twins_*` (#1134) is a KNOWN-DIVERGENCE pin, not a
//! convergence contract: `parity_unique_twins_rust_rust` asserts #1126's
//! canonical-pick semantics (both twins persist, smallest docID owns the
//! unique slot). `parity_unique_twins_go_go` asserts current upstream Go
//! behavior — a unique-index twin merge is rejected atomically inside the
//! merge transaction (`internal/db/index.go` `saveUniqueKey` /
//! `internal/db/merge.go`), and the sender silently treats the rejection as
//! success because `message.Send` checks the request's `GetErrMessage()`
//! instead of the response's (`internal/db/p2p/message/message.go`), so the
//! two replicas disagree permanently on scan membership and indexed
//! ownership. The go_go probe is an intentionally asserting
//! known-Go-divergence test: it must FAIL the moment upstream Go starts
//! converging, forcing this pin to be updated/removed rather than letting
//! the compatibility contract drift silently. A mixed Rust<->Go topology
//! probe is deferred in #1134: the observed mixed behavior does NOT match
//! the atomic-rejection model (Rust's block-by-block PushLog replay lands
//! field-delta blocks on the Go peer before the composite is rejected,
//! leaving a scan-visible unindexed partial document) and is pending
//! adjudication there. See defradb.rs#1134 (upstream Go tracking issue not
//! filed yet — link here once it is).

use crate::support;
use defra_harness::{DefraClient, NodeKind, TestCluster};
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

fn created_user_doc_id<'a>(created: &'a serde_json::Value, create_field: &str) -> Option<&'a str> {
    created[create_field]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|doc| doc["_docID"].as_str())
        .or_else(|| created[create_field]["_docID"].as_str())
}

fn create_user_seed(node: &DefraClient, label: &str) -> String {
    let create_fields = match node.kind() {
        NodeKind::Rust => ["add_User", "create_User"],
        NodeKind::Go => ["create_User", "add_User"],
    };
    let mut attempts = Vec::new();
    for create_field in create_fields {
        match node.query(&format!(
            r#"mutation {{ {create_field}(input: {{name: "seed"}}) {{ _docID }} }}"#
        )) {
            Ok(created) => {
                if let Some(id) = created_user_doc_id(&created, create_field) {
                    return id.to_string();
                }
                attempts.push(format!("{create_field}: {created}"));
            }
            Err(err) => attempts.push(format!("{create_field}: {err:#}")),
        }
    }
    panic!(
        "[{label}] no User create mutation returned _docID in expected shape; attempts: {}",
        attempts.join(" | ")
    );
}

fn user_name(node: &DefraClient) -> String {
    node.query("query { User { name } }").unwrap_or_default()["User"]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|doc| doc["name"].as_str())
        .unwrap_or("<missing>")
        .to_string()
}

async fn poll_all_user_name(
    cluster: &TestCluster,
    nodes: usize,
    want: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if (0..nodes).all(|n| user_name(&cluster.client(n)) == want) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn poll_user_names_agree(
    cluster: &TestCluster,
    nodes: usize,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let states: Vec<_> = (0..nodes).map(|n| user_name(&cluster.client(n))).collect();
        if states
            .first()
            .is_some_and(|first| first != "<missing>" && states.iter().all(|state| state == first))
        {
            return states.into_iter().next();
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

fn user_name_commits(node: &DefraClient, doc_id: &str) -> serde_json::Value {
    node.query(&format!(
        r#"query {{
            _commits(
                docID: "{doc_id}",
                filter: {{fieldName: {{_eq: "name"}}}},
                order: {{height: ASC}}
            ) {{
                cid
                height
                fieldName
                delta
                links {{ cid }}
                heads {{ cid }}
            }}
        }}"#
    ))
    .unwrap_or_default()
}

fn wire_user_bidirectional(cluster: &TestCluster) {
    let (a0, a1) = (node_addr(cluster, 0), node_addr(cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
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
        .expect("replicator node0");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0)
        .expect("replicator node1");
}

/// Controlled equal-priority LWW probe: the nodes are intentionally not wired
/// until after they independently create the same seed document and write
/// height-2 sibling values. This avoids the live-mesh artifact where one writer
/// can observe the other first and produce a higher-priority, non-tie update.
async fn run_lww_tie_partition_probe(cluster: TestCluster, label: &str, expected_name: &str) {
    let schema = "type User { name: String }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");

    let id = create_user_seed(&cluster.client(0), label);
    let id1 = create_user_seed(&cluster.client(1), label);
    assert_eq!(
        id, id1,
        "[{label}] independently-created seed docs must share a content-addressed docID"
    );
    assert!(
        poll_all_user_name(&cluster, 2, "seed", Duration::from_secs(30)).await,
        "[{label}] seed was not visible on both isolated nodes"
    );

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{name: "alice"}}) {{ _docID }} }}"#
        ))
        .expect("node0 name=alice");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{name: "zoe"}}) {{ _docID }} }}"#
        ))
        .expect("node1 name=zoe");

    eprintln!(
        "LWW_TIE[{label}] before connect: node0={} commits0={} | node1={} commits1={}",
        user_name(&cluster.client(0)),
        user_name_commits(&cluster.client(0), &id),
        user_name(&cluster.client(1)),
        user_name_commits(&cluster.client(1), &id),
    );

    wire_user_bidirectional(&cluster);
    assert!(
        support::poll_dags_converged(
            &cluster.client(0),
            &cluster.client(1),
            &id,
            Duration::from_secs(45),
        )
        .await,
        "[{label}] DAGs did not converge after heal"
    );

    let agreed = poll_user_names_agree(&cluster, 2, Duration::from_secs(30))
        .await
        .unwrap_or_else(|| {
            panic!(
                "[{label}] nodes did not agree after DAG convergence: node0={} node1={}",
                user_name(&cluster.client(0)),
                user_name(&cluster.client(1)),
            )
        });
    eprintln!(
        "LWW_TIE[{label}] after heal: agreed={agreed} commits0={} commits1={}",
        user_name_commits(&cluster.client(0), &id),
        user_name_commits(&cluster.client(1), &id),
    );
    assert_eq!(agreed, expected_name, "[{label}] LWW tie winner");
}

#[ignore = "parity instrumentation; run with --ignored"]
#[tokio::test]
async fn parity_lww_tie_partition_rust_rust() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("rust-rust cluster");
    run_lww_tie_partition_probe(cluster, "lww_tie_partition_rust_rust", "alice").await;
}

#[ignore = "parity instrumentation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_lww_tie_partition_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_store("badger")
        .with_keyring()
        .with_development()
        .build()
        .await
        .expect("go-go cluster");
    run_lww_tie_partition_probe(cluster, "lww_tie_partition_go_go", "alice").await;
}

#[ignore = "parity instrumentation; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_lww_tie_partition_mixed() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_keyring()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed cluster");
    run_lww_tie_partition_probe(cluster, "lww_tie_partition_mixed(rust0,go1)", "alice").await;
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

fn mixed_fields_state(node: &DefraClient) -> (String, i64) {
    let r = node
        .query("query { Mixed { name views } }")
        .expect("query Mixed");
    r["Mixed"]
        .as_array()
        .and_then(|rows| rows.first())
        .map(|doc| {
            (
                doc["name"].as_str().unwrap_or("<none>").to_string(),
                doc["views"].as_i64().unwrap_or(-1),
            )
        })
        .unwrap_or_else(|| ("<missing>".to_string(), -1))
}

fn created_mixed_doc_id<'a>(created: &'a serde_json::Value, create_field: &str) -> Option<&'a str> {
    created[create_field]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|doc| doc["_docID"].as_str())
        .or_else(|| created[create_field]["_docID"].as_str())
}

fn create_mixed_seed(node: &DefraClient, label: &str) -> String {
    let create_fields = match node.kind() {
        NodeKind::Rust => ["add_Mixed", "create_Mixed"],
        NodeKind::Go => ["create_Mixed", "add_Mixed"],
    };
    let mut attempts = Vec::new();
    for create_field in create_fields {
        match node.query(&format!(
            r#"mutation {{ {create_field}(input: {{name: "seed", views: 0}}) {{ _docID }} }}"#
        )) {
            Ok(created) => {
                if let Some(id) = created_mixed_doc_id(&created, create_field) {
                    return id.to_string();
                }
                attempts.push(format!("{create_field}: {created}"));
            }
            Err(err) => attempts.push(format!("{create_field}: {err:#}")),
        }
    }
    panic!(
        "[{label}] no Mixed create mutation returned _docID in expected shape; attempts: {}",
        attempts.join(" | ")
    );
}

async fn poll_all_mixed_fields_state(
    cluster: &TestCluster,
    nodes: usize,
    want: (&str, i64),
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if (0..nodes)
            .all(|n| mixed_fields_state(&cluster.client(n)) == (want.0.to_string(), want.1))
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn poll_mixed_fields_dags_converged(
    cluster: &TestCluster,
    nodes: usize,
    doc_id: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let commits: Vec<_> = (0..nodes)
            .map(|n| support::commit_cids(&cluster.client(n), doc_id))
            .collect();
        if !commits.iter().any(|c| c.is_empty()) && commits.windows(2).all(|w| w[0] == w[1]) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn poll_mixed_fields_agreed_state(
    cluster: &TestCluster,
    nodes: usize,
    timeout: Duration,
) -> Option<(String, i64)> {
    let deadline = Instant::now() + timeout;
    loop {
        let states: Vec<_> = (0..nodes)
            .map(|n| mixed_fields_state(&cluster.client(n)))
            .collect();
        if states.first().is_some_and(|first| {
            first.0 != "<missing>" && states.iter().all(|state| state == first)
        }) {
            return states.into_iter().next();
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// THREE-node mixed-field probe (ASSERTING): the same document receives one LWW
/// update and two counter increments.
///
/// This is the mixed Counter/LWW counterpart to the partition tests for #1048:
/// every runtime combination must materialize the same LWW value (`alice`) while
/// still accumulating the independent counter deltas to 17. Same-field LWW
/// tie-breaking is pinned separately by `parity_lww_tie_partition_*`.
async fn run_mixed_fields_3node_probe(
    cluster: TestCluster,
    label: &str,
    expected_name: &str,
    expected_views: i64,
) {
    let schema = "type Mixed { name: String  views: Int @crdt(type: pcounter) }";
    let addr: Vec<String> = (0..3).map(|n| node_addr(&cluster, n)).collect();
    for n in 0..3 {
        cluster.client(n).schema_add(schema).expect("schema");
        cluster
            .client(n)
            .p2p_collection_add(&["Mixed"])
            .expect("subscribe");
    }
    for i in 0..3 {
        for (j, peer) in addr.iter().enumerate() {
            if i != j {
                cluster
                    .client(i)
                    .p2p_connect(&[peer.as_str()])
                    .expect("connect");
                cluster
                    .client(i)
                    .p2p_replicator_set(&["Mixed"], peer)
                    .expect("replicator");
            }
        }
    }

    let id = create_mixed_seed(&cluster.client(0), label);

    assert!(
        poll_all_mixed_fields_state(&cluster, 3, ("seed", 0), Duration::from_secs(30)).await,
        "[{label}] seed (name=seed, views=0) did not reach all three nodes"
    );
    assert!(
        poll_mixed_fields_dags_converged(&cluster, 3, &id, Duration::from_secs(30)).await,
        "[{label}] seed DAG did not converge before the mixed-field updates"
    );

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Mixed(docID: "{id}", input: {{name: "alice"}}) {{ _docID }} }}"#
        ))
        .expect("node0 name=alice");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_Mixed(docID: "{id}", input: {{views: 10}}) {{ _docID }} }}"#
        ))
        .expect("node1 views=10");
    cluster
        .client(2)
        .query(&format!(
            r#"mutation {{ update_Mixed(docID: "{id}", input: {{views: 7}}) {{ _docID }} }}"#
        ))
        .expect("node2 views=7");

    assert!(
        poll_mixed_fields_dags_converged(&cluster, 3, &id, Duration::from_secs(45)).await,
        "[{label}] mixed-field DAGs did not converge, so final-state parity would be inert"
    );

    let agreed = poll_mixed_fields_agreed_state(&cluster, 3, Duration::from_secs(45))
        .await
        .unwrap_or_else(|| {
            panic!(
                "[{label}] did not materialize one agreed mixed-field state after DAG convergence; states = [{:?}, {:?}, {:?}]",
                mixed_fields_state(&cluster.client(0)),
                mixed_fields_state(&cluster.client(1)),
                mixed_fields_state(&cluster.client(2)),
            )
        });
    assert!(
        agreed.0 == expected_name && agreed.1 == expected_views,
        "[{label}] mixed-field state diverged from Go-compatible semantics; got {agreed:?}, expected name={expected_name} and views={expected_views}",
    );
}

/// Rust<->Rust<->Rust mixed-field control for the asserting parity probe.
#[ignore = "parity (asserting); run with --ignored"]
#[tokio::test]
async fn parity_mixed_fields_3node_rust_rust() {
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("rust-rust-rust cluster");
    run_mixed_fields_3node_probe(cluster, "mixed_fields_3node_rust_rust", "alice", 17).await;
}

/// Go<->Go<->Go mixed-field control. Go is the parity target; Rust and mixed
/// clusters must match this materialized state exactly.
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_mixed_fields_3node_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(3)
        .with_p2p()
        .with_store("badger")
        .with_development()
        .build()
        .await
        .expect("go-go-go cluster");
    run_mixed_fields_3node_probe(cluster, "mixed_fields_3node_go_go", "alice", 17).await;
}

/// Mixed Rust(node0)<->Go(node1,node2) mixed-field control. The cross-impl mesh
/// must agree with Go's materialized LWW value and exact counter sum.
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_mixed_fields_3node_mixed() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(2)
        .with_p2p()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed 3-node cluster");
    run_mixed_fields_3node_probe(
        cluster,
        "mixed_fields_3node_mixed(rust0,go1,go2)",
        "alice",
        17,
    )
    .await;
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
/// (20). Cross-impl twin of `index::index_reconciles_lww_merge_after_restart`:
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

/// Go<->Go<->Go same-doc counter STORM — the parity target. Confirms the upstream
/// Go binary converges to the exact sum under the identical concurrent-burst storm
/// that exposed the Rust #1021 under-count (it does; Go's single value key + merge
/// queue serialize it). Uses the cluster-agnostic `support::run_counter_storm`.
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_counter_storm_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(3)
        .with_p2p()
        .with_store("badger")
        .with_development()
        .build()
        .await
        .expect("go-go-go cluster");
    support::run_counter_storm(&cluster, "pcounter", "Int", &[1.0, 1.0, 1.0], 3, 4).await;
}

/// Mixed Rust(node0)<->Go(node1,node2) same-doc counter STORM — every node (Rust AND
/// the two Go peers) must converge to the exact accumulation under concurrent
/// same-doc bursts across a mixed mesh (the cross-impl twin of
/// `partition::convergence_concurrent_same_doc_merge_storm`).
///
/// KNOWN-FAILING DIAGNOSTIC (intermittent), kept asserting but OUT of the blocking
/// go-compat CI leg. Investigation (link-level DAG dumps + instrumented Go binary)
/// established: all three nodes hold a byte-identical commit DAG, Rust materializes
/// the EXACT sum, and the two Go peers intermittently materialize +k too high. The
/// miscount is a timing-sensitive DOUBLE-APPLY on the Go reference side — Go's
/// `coreblock.ProcessBlock` runs `incrementValue` (RMW) unconditionally with no
/// per-block `IsMerged` guard (dedup is purely structural via `loadComposites`),
/// whereas Rust's counter merge guards on `is_merged(cid)`. It never reproduces in
/// pure go<->go (`parity_counter_storm_go_go`), is triggered by the Rust node's
/// delivery timing (`RUST_LOG=info`), and is suppressed by Go-side instrumentation.
/// Tracked in #1043; promote back into the blocking leg once resolved.
#[ignore = "known-failing diagnostic (Go-side double-apply, #1043); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_counter_storm_mixed() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(2)
        .with_p2p()
        .with_development()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("mixed 3-node cluster");
    support::run_counter_storm(&cluster, "pcounter", "Int", &[1.0, 1.0, 1.0], 3, 4).await;
}

// ---- #1134: unique-index twin merge divergence pin ----
//
// Fixed schema + fixed fixture `seed` values, chosen so the resulting
// content-addressed docIDs sort deterministically: node0 always seeds
// TWIN_SEED_SMALL, node1 always seeds TWIN_SEED_LARGE, and
// id(TWIN_SEED_SMALL) < id(TWIN_SEED_LARGE) is re-verified at runtime by an
// ordering fence in `setup_unique_twins` rather than assumed blindly. This
// keeps the fixture assignment (node0 = smaller docID) deterministic across
// runs instead of branching on whichever docID happens to sort first — see
// #1134.
const UNIQUE_TWIN_SCHEMA: &str = "type Account { handle: String @index(unique: true)  seed: Int }";
const TWIN_SEED_SMALL: i64 = 5;
const TWIN_SEED_LARGE: i64 = 6;

fn account_create_fields(node: &DefraClient) -> [&'static str; 2] {
    match node.kind() {
        NodeKind::Rust => ["add_Account", "create_Account"],
        NodeKind::Go => ["create_Account", "add_Account"],
    }
}

fn create_account_twin(node: &DefraClient, label: &str, seed: i64) -> String {
    let mut attempts = Vec::new();
    for create_field in account_create_fields(node) {
        match node.query(&format!(
            r#"mutation {{ {create_field}(input: {{handle: "twin", seed: {seed}}}) {{ _docID }} }}"#
        )) {
            Ok(created) => {
                if let Some(id) = created_user_doc_id(&created, create_field) {
                    return id.to_string();
                }
                attempts.push(format!("{create_field}: {created}"));
            }
            Err(err) => attempts.push(format!("{create_field}: {err:#}")),
        }
    }
    panic!(
        "[{label}] no Account create mutation returned _docID in expected shape; attempts: {}",
        attempts.join(" | ")
    );
}

/// Full collection scan (docID-level presence — bypasses the unique index
/// entirely, so it proves whether a twin persisted at all, independent of
/// which one the index resolved to).
fn account_scan_ids(node: &DefraClient) -> Vec<String> {
    node.query("query { Account { _docID } }")
        .unwrap_or_default()["Account"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|d| d["_docID"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Unique-indexed lookup on the shared `handle` value — the resolved
/// owner(s) of the unique slot, as opposed to everything physically present
/// (`account_scan_ids`).
fn account_indexed_owner(node: &DefraClient) -> Vec<String> {
    node.query(r#"query { Account(filter: {handle: {_eq: "twin"}}) { _docID } }"#)
        .unwrap_or_default()["Account"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|d| d["_docID"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

async fn poll_account_scan_count(node: &DefraClient, want: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if account_scan_ids(node).len() == want {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Honesty check mirroring `run_indexed_lww_parity`'s `rust_explain_node`:
/// confirm the Rust node actually plans an index scan on the unique field, so
/// a full-scan fallback can't make the indexed-owner assertions vacuous.
fn assert_rust_explain_uses_index(node: &DefraClient, label: &str) {
    let index_used = node
        .query(
            r#"query @explain(type: simple) { Account(filter: {handle: {_eq: "twin"}}) { seed } }"#,
        )
        .map(|v| v.to_string().to_lowercase().contains("index"))
        .unwrap_or(false);
    assert!(
        index_used,
        "[{label}] Rust node must plan an index scan on the unique field, else the index assertions prove nothing"
    );
}

fn wire_account_bidirectional(cluster: &TestCluster) {
    let (a0, a1) = (node_addr(cluster, 0), node_addr(cluster, 1));
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
        .p2p_collection_add(&["Account"])
        .expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Account"])
        .expect("subscribe node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["Account"], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["Account"], &a0)
        .expect("replicator 1->0");
}

/// #1134 steps 1-3, shared by both topologies: independent schema +
/// unique index on isolated nodes, distinct twins holding the identical
/// unique value created BEFORE any P2P wiring (fixed fixtures, ordering
/// fenced at runtime), then bidirectional collection subscriptions +
/// replicators. Returns (node0's docID, node1's docID).
async fn setup_unique_twins(cluster: &TestCluster, label: &str) -> (String, String) {
    cluster
        .client(0)
        .schema_add(UNIQUE_TWIN_SCHEMA)
        .expect("schema node0");
    cluster
        .client(1)
        .schema_add(UNIQUE_TWIN_SCHEMA)
        .expect("schema node1");

    let id0 = create_account_twin(&cluster.client(0), label, TWIN_SEED_SMALL);
    let id1 = create_account_twin(&cluster.client(1), label, TWIN_SEED_LARGE);
    assert_ne!(
        id0, id1,
        "[{label}] distinct docIDs required for a real twin conflict"
    );
    assert!(
        id0 < id1,
        "[{label}] ordering fence: node0's fixture (seed={TWIN_SEED_SMALL}) must stay \
         the lexicographically smaller docID — content-addressing appears to have \
         changed (got node0={id0} node1={id1}); recompute the fixed TWIN_SEED_* \
         fixtures rather than adjusting downstream assertions"
    );

    wire_account_bidirectional(cluster);
    (id0, id1)
}

/// Rust<->Rust control (#1134): pins #1126's canonical-pick semantics for
/// this exact scenario shape — both independently-created twins persist, and
/// the unique slot converges to the lexicographically smallest docID
/// identically on both replicas. This is the "Rust is internally consistent"
/// anchor the `_go_go` divergence pin is measured against. (A mixed
/// Rust<->Go topology probe is deferred in #1134 pending the partial-replay
/// finding — see the module header.)
#[ignore = "parity (asserting); run with --ignored"]
#[tokio::test]
async fn parity_unique_twins_rust_rust() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("rust-rust cluster");
    let label = "unique_twins_rust_rust";
    let (id0, _id1) = setup_unique_twins(&cluster, label).await;

    for n in [0usize, 1] {
        assert!(
            poll_account_scan_count(&cluster.client(n), 2, Duration::from_secs(40)).await,
            "[{label}] node{n} did not converge to both twins scan-visible; ids={:?}",
            account_scan_ids(&cluster.client(n))
        );
    }

    for n in [0usize, 1] {
        assert_eq!(
            account_indexed_owner(&cluster.client(n)),
            vec![id0.clone()],
            "[{label}] node{n} unique index must resolve to the smallest docID (#1126 canonical pick)"
        );
    }

    assert_rust_explain_uses_index(&cluster.client(0), label);
}

/// Go<->Go KNOWN-DIVERGENCE pin (#1134): current upstream Go behavior for
/// this exact scenario shape. `saveUniqueKey` performs a bare existence
/// check inside the merge transaction (`internal/db/index.go`), so the
/// incoming twin's merge is rejected and the whole merge transaction
/// (including `MarkAsMerged` and the head update) is discarded
/// (`internal/db/merge.go`). The push sender then treats the rejection as
/// success because `message.Send` checks the request's error field instead
/// of the response's (`internal/db/p2p/message/message.go`), deletes its
/// retry record, and reports the replicator `Active`. Net effect: each
/// replica permanently retains ONLY its own local twin, and reconnection /
/// ordinary replicator retry never repairs it (there is nothing left in
/// either retry queue to re-drive).
///
/// This test MUST start failing the moment upstream Go changes this
/// behavior — that failure is the signal to update or remove this pin, not
/// to patch the assertions blind. (A mixed Rust<->Go topology probe is
/// deferred in #1134 pending the partial-replay finding — see the module
/// header.)
#[ignore = "parity (asserting); needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn parity_unique_twins_go_go() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_store("badger")
        .with_development()
        .build()
        .await
        .expect("go-go cluster");
    let label = "unique_twins_go_go";
    let (id0, id1) = setup_unique_twins(&cluster, label).await;

    // Not a convergence poll: ordinary replicator retry does not repair this
    // divergence (the sender believes the push already succeeded and drained
    // its retry queue). This is a generous stabilization wait before
    // asserting the permanent, non-converged end state.
    tokio::time::sleep(Duration::from_secs(20)).await;

    assert_eq!(
        account_scan_ids(&cluster.client(0)),
        vec![id0.clone()],
        "[{label}] node0 must retain ONLY its own local twin — Go silently drops the peer's twin"
    );
    assert_eq!(
        account_scan_ids(&cluster.client(1)),
        vec![id1.clone()],
        "[{label}] node1 must retain ONLY its own local twin — Go silently drops the peer's twin"
    );
    assert_eq!(
        account_indexed_owner(&cluster.client(0)),
        vec![id0],
        "[{label}] node0's unique index must resolve to its own local twin"
    );
    assert_eq!(
        account_indexed_owner(&cluster.client(1)),
        vec![id1],
        "[{label}] node1's unique index must resolve to its own local twin"
    );
}
