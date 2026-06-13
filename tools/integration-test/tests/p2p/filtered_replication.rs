//! End-to-end coverage for filtered replication (PR #1033).
//!
//! Filtered replication is a Rust-only extension: a replicator may carry a
//! per-collection `{Field, Value}` predicate so a source node only pushes
//! documents whose field matches. The feature spans live push, backfill, SE
//! artifacts, and `@immutable` enforcement. These tests exercise that behavior
//! across two running nodes via the real CLI/HTTP surface — the layer where the
//! emergent "non-matching document must NOT arrive" property is observable.
//!
//! Because the predicate has no Go equivalent, these are explicit `rust_nodes`
//! tests rather than `for_each_p2p_topology!` (which would generate Go variants).

use std::process::Command;
use std::time::Duration;

use integration_test::{
    extract_doc_id, extract_p2p_addr, poll_until, TestCluster, P2P_POLL_INTERVAL, P2P_TIMEOUT,
};

const AGENT_SCHEMA: &str = "type AgentDoc { agent_did: String @immutable  body: String }";
const ALICE: &str = "did:key:alice";
const BOB: &str = "did:key:bob";

/// Grace window used to confirm a non-matching document stays absent *after*
/// a matching document has already replicated. Once the matching doc arrives we
/// know the push pipeline ran, so continued absence of the non-matching doc is
/// meaningful rather than merely "not yet replicated".
const ABSENCE_GRACE: Duration = Duration::from_secs(3);

/// The CLI `--url` flag expects a bare `host:port`, while `api_url` carries the
/// `http://` scheme. The harness's own client uses the node's bare address.
fn socket_addr(cluster: &TestCluster, node: usize) -> String {
    let url = cluster.api_url(node);
    url.strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url)
        .to_string()
}

/// Add a filtered replicator from `node` to `addr` over the CLI, exercising the
/// `--filter-field` / `--filter-value` flags the PR introduced. The CLI wraps
/// `--filter-value` as a JSON string, matching string scalar fields.
fn run_replicator_add(
    cluster: &TestCluster,
    node: usize,
    collections: &[&str],
    addr: &str,
    field: &str,
    value: &str,
) -> std::process::Output {
    let client = cluster.client(node);
    let cols = collections.join(",");
    Command::new(client.binary_path())
        .arg("--url")
        .arg(socket_addr(cluster, node))
        .args([
            "client",
            "p2p",
            "replicator",
            "add",
            "-c",
            &cols,
            "--filter-field",
            field,
            "--filter-value",
            value,
            addr,
        ])
        .output()
        .expect("exec filtered replicator add")
}

fn add_filtered_replicator(
    cluster: &TestCluster,
    node: usize,
    collections: &[&str],
    addr: &str,
    field: &str,
    value: &str,
) {
    let output = run_replicator_add(cluster, node, collections, addr, field, value);
    assert!(
        output.status.success(),
        "filtered replicator add failed: status={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Run a GraphQL query/mutation over the CLI and return the raw process output,
/// so a test can observe error payloads the typed `query()` helper discards.
fn run_query(cluster: &TestCluster, node: usize, gql: &str) -> std::process::Output {
    let client = cluster.client(node);
    Command::new(client.binary_path())
        .arg("--url")
        .arg(socket_addr(cluster, node))
        .args(["client", "query", gql])
        .output()
        .expect("exec query")
}

fn agent_did_values(cluster: &TestCluster, node: usize) -> Vec<String> {
    let result = cluster
        .client(node)
        .query("query { AgentDoc { _docID agent_did body } }")
        .expect("query AgentDoc");
    result["AgentDoc"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r["agent_did"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

async fn wait_for_log_ready(cluster: &TestCluster, count: usize) {
    for i in 0..count {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{i} P2P listener did not start"));
    }
}

/// Core of tests #1 (negative match) and #2 (positive match + full-DAG
/// completeness): a matching document replicates with ALL fields populated,
/// while a non-matching document is excluded. Parameterized over transport.
async fn filtered_excludes_nonmatching(cluster: TestCluster) {
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(AGENT_SCHEMA).expect("schema node0");
    node1.schema_add(AGENT_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");
    // The receiver does NOT subscribe to the collection: it must receive
    // documents only via the filtered replicator push. Subscribing it would
    // pull all docs over collection pubsub and bypass the filter.
    add_filtered_replicator(&cluster, 0, &["AgentDoc"], &addr1, "agent_did", ALICE);

    // Confirm the predicate landed in the persisted replicator record, read via
    // the HTTP API (wire-format coverage for the Rust-only `Filters` extension).
    // NOTE: the CLI `replicator list` deserializer (`P2pReplicatorInfo`) does not
    // carry a `filters` field, so this must be asserted over HTTP, not the CLI.
    let listed: serde_json::Value =
        reqwest::get(format!("{}/api/v0/p2p/replicators", cluster.api_url(0)))
            .await
            .expect("GET replicators")
            .json()
            .await
            .expect("parse replicators json");
    let listed_str = serde_json::to_string(&listed).unwrap();
    assert!(
        listed_str.contains("Filters") && listed_str.contains("agent_did"),
        "HTTP replicator list should expose the Filters predicate: {listed_str}"
    );

    let matching = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "selected"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc");
    let matching_id = extract_doc_id(&matching, "add_AgentDoc");

    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "excluded"}}) {{ _docID }} }}"#
        ))
        .expect("create non-matching doc");

    // A SECOND matching doc, created AFTER the non-matching one. Replicator pushes
    // are ordered, so once this arrives we know the non-matching doc's push slot was
    // already processed-and-skipped — making its absence conclusive without a sleep.
    let matching2 = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "selected-2"}}) {{ _docID }} }}"#
        ))
        .expect("create second matching doc");
    let matching2_id = extract_doc_id(&matching2, "add_AgentDoc");

    // Both matching docs must arrive; the first fully materialized (body present)
    // proves the full document DAG was delivered (filtered peers bypass Bitswap).
    let node1_for_poll = cluster.client(1);
    let matching_id_poll = matching_id.clone();
    let matching2_id_poll = matching2_id.clone();
    poll_until(
        || {
            let result = node1_for_poll
                .query("query { AgentDoc { _docID agent_did body } }")
                .unwrap_or_default();
            let Some(rows) = result["AgentDoc"].as_array() else {
                return false;
            };
            let first_full = rows.iter().any(|r| {
                r["_docID"].as_str() == Some(matching_id_poll.as_str())
                    && r["agent_did"].as_str() == Some(ALICE)
                    && r["body"].as_str() == Some("selected")
            });
            let second = rows
                .iter()
                .any(|r| r["_docID"].as_str() == Some(matching2_id_poll.as_str()));
            first_full && second
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "both matching documents did not replicate to filtered peer",
    )
    .await;

    // The non-matching doc must be absent: the peer holds only the two matching docs.
    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids.len(),
        2,
        "filtered peer must hold exactly the two matching docs, found: {dids:?}"
    );
    assert!(
        dids.iter().all(|d| d == ALICE),
        "filtered peer must hold only matching documents, found: {dids:?}"
    );
}

/// #1 + #2 over libp2p.
#[tokio::test]
async fn rust_filtered_replication_excludes_nonmatching() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    filtered_excludes_nonmatching(cluster).await;
}

/// #7: identical guarantee must hold over the iroh transport (defra-agent's
/// production transport).
#[tokio::test]
async fn rust_filtered_replication_excludes_nonmatching_iroh() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();
    filtered_excludes_nonmatching(cluster).await;
}

/// #5: a replicator added AFTER documents already exist must backfill only the
/// documents matching its predicate.
#[tokio::test]
async fn rust_filtered_replication_backfill_respects_filter() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(AGENT_SCHEMA).expect("schema node0");
    node1.schema_add(AGENT_SCHEMA).expect("schema node1");

    // Create documents BEFORE wiring replication so delivery comes from
    // backfill, not live push.
    let matching = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "backfilled"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc");
    let matching_id = extract_doc_id(&matching, "add_AgentDoc");
    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "excluded"}}) {{ _docID }} }}"#
        ))
        .expect("create non-matching doc");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");
    // Receiver relies on the filtered replicator push only (see note above).
    add_filtered_replicator(&cluster, 0, &["AgentDoc"], &addr1, "agent_did", ALICE);

    let node1_for_poll = cluster.client(1);
    let matching_id_poll = matching_id.clone();
    poll_until(
        || {
            let result = node1_for_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(matching_id_poll.as_str()))
            })
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "matching document did not backfill to filtered peer",
    )
    .await;

    tokio::time::sleep(ABSENCE_GRACE).await;
    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids,
        vec![ALICE.to_string()],
        "backfill must respect the filter, found: {dids:?}"
    );
}

/// #4: `@immutable` enforcement on the LOCAL write path. Updating the immutable
/// field must be rejected; updating other fields must succeed.
#[tokio::test]
async fn rust_immutable_field_rejects_local_update() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(AGENT_SCHEMA).expect("schema");

    let created = node
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "v1"}}) {{ _docID }} }}"#
        ))
        .expect("create doc");
    let doc_id = extract_doc_id(&created, "add_AgentDoc");

    // Allowed: mutate a non-immutable field.
    node.query(&format!(
        r#"mutation {{ update_AgentDoc(docID: "{doc_id}", input: {{body: "v2"}}) {{ _docID body }} }}"#
    ))
    .expect("update body should succeed");

    // Rejected: mutate the immutable field. Observe the rejection directly from
    // the raw CLI output so a regression into silently DROPPING the field (a
    // no-op that also leaves the value unchanged) cannot pass this test.
    let rejected = run_query(
        &cluster,
        0,
        &format!(
            r#"mutation {{ update_AgentDoc(docID: "{doc_id}", input: {{agent_did: "{BOB}"}}) {{ _docID }} }}"#
        ),
    );
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert!(
        output.contains("immutable"),
        "immutable update must surface an error mentioning the immutable field, got: {output}"
    );

    let after = node
        .query("query { AgentDoc { agent_did body } }")
        .expect("query after update");
    let row = &after["AgentDoc"][0];
    assert_eq!(
        row["agent_did"].as_str(),
        Some(ALICE),
        "immutable field must not change"
    );
    assert_eq!(
        row["body"].as_str(),
        Some("v2"),
        "non-immutable field update should have persisted"
    );
}

/// #6: the CLI contract — `--filter-value` without `--filter-field` is rejected
/// before any network call (clap `requires`).
#[tokio::test]
async fn rust_filtered_replicator_cli_requires_filter_field() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    let client = cluster.client(0);
    let output = Command::new(client.binary_path())
        .arg("--url")
        .arg(socket_addr(&cluster, 0))
        .args([
            "client",
            "p2p",
            "replicator",
            "add",
            "-c",
            "AgentDoc",
            "--filter-value",
            ALICE,
            "/ip4/127.0.0.1/tcp/1/p2p/12D3KooWGjMkcMy5PM9iSbgWWgUnH5dQhvzhNu7w3Gk4kHZBsxnJ",
        ])
        .output()
        .expect("exec replicator add");
    assert!(
        !output.status.success(),
        "--filter-value without --filter-field must be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("filter-field"),
        "error should mention the missing --filter-field, got: {stderr}"
    );
}

/// #8: the filter must gate the encrypted (SE-artifact) push path too. A
/// non-matching encrypted document must not reach the filtered peer; a matching
/// one must (its `_docID` is observable even without the decryption key).
#[tokio::test]
async fn rust_filtered_replication_encrypted_respects_filter() {
    const SECRET_SCHEMA: &str = "type SecretDoc { agent_did: String @immutable  secret: String }";
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(SECRET_SCHEMA).expect("schema node0");
    node1.schema_add(SECRET_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["SecretDoc"])
        .expect("subscribe 0");
    // Receiver relies on the filtered replicator push only (see note above).
    add_filtered_replicator(&cluster, 0, &["SecretDoc"], &addr1, "agent_did", ALICE);

    let matching = node0
        .query(&format!(
            r#"mutation {{ add_SecretDoc(input: {{agent_did: "{ALICE}", secret: "s"}}, encryptFields: [secret]) {{ _docID }} }}"#
        ))
        .expect("create matching encrypted doc");
    let matching_id = extract_doc_id(&matching, "add_SecretDoc");
    node0
        .query(&format!(
            r#"mutation {{ add_SecretDoc(input: {{agent_did: "{BOB}", secret: "s"}}, encryptFields: [secret]) {{ _docID }} }}"#
        ))
        .expect("create non-matching encrypted doc");

    // Second matching doc created AFTER the non-matching one anchors the
    // exclusion to ordered delivery rather than a wall-clock window.
    let matching2 = node0
        .query(&format!(
            r#"mutation {{ add_SecretDoc(input: {{agent_did: "{ALICE}", secret: "s2"}}, encryptFields: [secret]) {{ _docID }} }}"#
        ))
        .expect("create second matching encrypted doc");
    let matching2_id = extract_doc_id(&matching2, "add_SecretDoc");

    let node1_for_poll = cluster.client(1);
    let matching_id_poll = matching_id.clone();
    let matching2_id_poll = matching2_id.clone();
    poll_until(
        || {
            let result = node1_for_poll
                .query("query { SecretDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["SecretDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&matching_id_poll.as_str()) && ids.contains(&matching2_id_poll.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "matching encrypted documents did not replicate to filtered peer",
    )
    .await;

    let result = node1
        .query("query { SecretDoc { _docID } }")
        .expect("query SecretDoc on node1");
    let count = result["SecretDoc"].as_array().map(Vec::len).unwrap_or(0);
    assert_eq!(
        count, 2,
        "filtered peer must hold only the two matching encrypted documents"
    );
}

const DUMMY_PEER_ADDR: &str =
    "/ip4/127.0.0.1/tcp/1/p2p/12D3KooWGjMkcMy5PM9iSbgWWgUnH5dQhvzhNu7w3Gk4kHZBsxnJ";

/// G8: a filter on a field absent from the collection schema is rejected at
/// add time (prevents a typo silently producing zero replication). Validation
/// runs before any dial, so an unreachable dummy peer address is fine.
#[tokio::test]
async fn rust_filtered_replicator_rejects_unknown_field() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 1).await;
    cluster.client(0).schema_add(AGENT_SCHEMA).expect("schema");

    let output = run_replicator_add(
        &cluster,
        0,
        &["AgentDoc"],
        DUMMY_PEER_ADDR,
        "nonexistent_field",
        ALICE,
    );
    assert!(
        !output.status.success(),
        "filtering on a non-existent field must be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "error should explain the field is not in the schema, got: {stderr}"
    );
}

/// G8: a filter on an existing-but-mutable field is rejected — the filter key
/// must be `@immutable` (the B3 split-ownership guard).
#[tokio::test]
async fn rust_filtered_replicator_rejects_non_immutable_field() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 1).await;
    cluster.client(0).schema_add(AGENT_SCHEMA).expect("schema");

    // `body` exists but is not @immutable.
    let output = run_replicator_add(&cluster, 0, &["AgentDoc"], DUMMY_PEER_ADDR, "body", "x");
    assert!(
        !output.status.success(),
        "filtering on a mutable field must be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("immutable"),
        "error should require the filter field be @immutable, got: {stderr}"
    );
}

/// G8/F1: filtering on a non-String scalar field with a value whose type cannot
/// match it is rejected at add time. The CLI wraps `--filter-value` as a JSON
/// string, so a filter on an `@immutable` Int field would otherwise pass
/// validation yet silently match zero documents.
#[tokio::test]
async fn rust_filtered_replicator_rejects_type_mismatch() {
    const INT_SCHEMA: &str = "type IntDoc { count: Int @immutable  body: String }";
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 1).await;
    cluster.client(0).schema_add(INT_SCHEMA).expect("schema");

    // `count` is a scalar @immutable Int, but the CLI sends "30" as a string.
    let output = run_replicator_add(&cluster, 0, &["IntDoc"], DUMMY_PEER_ADDR, "count", "30");
    assert!(
        !output.status.success(),
        "string filter value against an Int field must be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("does not match the field type"),
        "error should explain the value/field type mismatch, got: {stderr}"
    );
}

// #3 (remote-merge enforcement of `@immutable`) is intentionally NOT an
// integration-suite test. The guard in `composite_persist.rs` is defense-in-depth
// against a malicious or divergent peer and is unreachable through two honest
// nodes: local validation blocks the originating update, and content-addressed
// doc IDs prevent two honest nodes from forging the same docID with differing
// immutable values. It is covered directly at the merge layer by
// `db-merge`'s `remote_composite_merge_rejects_immutable_field_change`.
