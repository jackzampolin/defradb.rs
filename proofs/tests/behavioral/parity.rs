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
//! These report (not assert) — they print each node's converged state so we can
//! compare Go-only, Rust-only, and mixed.

use crate::support;
use defra_harness::{DefraClient, TestCluster};
use std::time::Duration;

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
/// merge interop. (A mixed + restart variant — which would surface the
/// Rust-side bug — is blocked: the restarted node needs a disk store its impl
/// supports, but the harness sets one store for the whole cluster and Go/Rust
/// share no disk store. The Rust-vs-Go split is already localized by the
/// rust_rust vs go_go results.)
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
