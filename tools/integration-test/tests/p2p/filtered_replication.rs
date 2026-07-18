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
    extract_doc_id, extract_p2p_addr, generate_identity, poll_until, TestCluster,
    P2P_POLL_INTERVAL, P2P_TIMEOUT, USER_ACP_POLICY,
};

const AGENT_SCHEMA: &str = "type AgentDoc { agent_did: String @immutable  body: String }";
const ALICE: &str = "did:key:alice";
const BOB: &str = "did:key:bob";
const CAROL: &str = "did:key:carol";

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

/// Add a filtered replicator authenticated as `identity_hex` (for Controlled/ACP mode).
fn add_filtered_replicator_with_identity(
    cluster: &TestCluster,
    node: usize,
    collections: &[&str],
    addr: &str,
    field: &str,
    value: &str,
    identity_hex: &str,
) {
    let client = cluster.client(node);
    let cols = collections.join(",");
    let output = Command::new(client.binary_path())
        .arg("--url")
        .arg(socket_addr(cluster, node))
        .args([
            "client",
            "-i",
            identity_hex,
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
        .expect("exec filtered replicator add (with identity)");
    assert!(
        output.status.success(),
        "filtered replicator add (identity) failed: status={} stderr={}",
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
    // `P2pReplicatorInfo` now also carries the `Filters` field, so the CLI
    // `replicator list` path is asserted separately in
    // `rust_filtered_replication_cli_list_renders_filter`.
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

    // The non-matching doc is filtered at the SENDER and never pushed, so its
    // absence is guaranteed by the filter — not by timing. Create a second
    // matching doc and wait for it so the assertion runs only after the push
    // pipeline has demonstrably delivered, replacing the earlier wall-clock sleep.
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

    // The non-matching doc is excluded by the sender-side backfill filter, not by
    // this window. Backfill is unordered (no "after" anchor like the live-push
    // tests), so a short settle window remains before asserting absence.
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
/// non-matching encrypted document must not reach the filtered peer; matching
/// documents must arrive with decrypted secrets, proving their field blocks
/// triggered DEK resolution.
#[tokio::test]
async fn rust_filtered_replication_encrypted_respects_filter() {
    const SECRET_SCHEMA: &str = "type SecretDoc { agent_did: String @immutable  secret: String }";
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_encryption()
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

    // The non-matching doc is filtered at the sender and never pushed; the
    // second matching doc just ensures delivery happened before asserting absence.
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
                .query("query { SecretDoc { _docID secret } }")
                .unwrap_or_default();
            let Some(rows) = result["SecretDoc"].as_array() else {
                return false;
            };
            let has_decrypted = |id: &str, secret: &str| {
                rows.iter().any(|row| {
                    row["_docID"].as_str() == Some(id) && row["secret"].as_str() == Some(secret)
                })
            };
            has_decrypted(&matching_id_poll, "s") && has_decrypted(&matching2_id_poll, "s2")
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "matching encrypted documents did not replicate and decrypt on filtered peer",
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

/// #1038 Gap 3: the filter must gate the encrypted (SE-artifact) push path with a
/// RICH (`_in`) predicate too. Two of three encrypted docs match the set
/// (alice + carol); the third (bob) must not reach the filtered peer. As with the
/// `_eq` encrypted test, the payload is encrypted so visibility is asserted on
/// `_docID`. CAROL is the ordering anchor, created after the non-matching BOB doc:
/// once she arrives the push pipeline has provably run, so BOB's absence is the
/// filter rather than a not-yet-delivered race.
#[tokio::test]
async fn rust_filtered_replication_encrypted_rich_filter() {
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

    let filter_json = format!(r#"{{"agent_did":{{"_in":["{ALICE}","{CAROL}"]}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["SecretDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "IN-set encrypted replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let mk = |did: &str, secret: &str| {
        format!(
            r#"mutation {{ add_SecretDoc(input: {{agent_did: "{did}", secret: "{secret}"}}, encryptFields: [secret]) {{ _docID }} }}"#
        )
    };

    let alice_doc = node0
        .query(&mk(ALICE, "s-alice"))
        .expect("create matching encrypted alice doc");
    let alice_id = extract_doc_id(&alice_doc, "add_SecretDoc");

    node0
        .query(&mk(BOB, "s-bob"))
        .expect("create non-matching encrypted bob doc");

    let carol_doc = node0
        .query(&mk(CAROL, "s-carol"))
        .expect("create matching encrypted carol doc (ordering anchor)");
    let carol_id = extract_doc_id(&carol_doc, "add_SecretDoc");

    let node1_poll = cluster.client(1);
    let a_id = alice_id.clone();
    let c_id = carol_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { SecretDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["SecretDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&a_id.as_str()) && ids.contains(&c_id.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "matching encrypted docs did not replicate to IN-set filtered peer",
    )
    .await;

    let result = node1
        .query("query { SecretDoc { _docID } }")
        .expect("query SecretDoc on node1");
    let ids: Vec<String> = result["SecretDoc"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r["_docID"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        ids.len(),
        2,
        "IN-set filtered peer must hold exactly the two matching encrypted docs, found: {ids:?}"
    );
    assert!(
        ids.contains(&alice_id) && ids.contains(&carol_id),
        "IN-set filtered peer must hold the alice and carol encrypted docs, found: {ids:?}"
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

/// G8/F6: the "must be a scalar LWW field" branch of filter validation is
/// belt-and-suspenders — the schema layer already forbids `@immutable` on any
/// non-LWW field, so a filter field that passes the `@immutable` check is
/// necessarily a scalar LWW field. There is therefore no way to construct an
/// `@immutable` non-LWW field to reach that branch; verify the underlying schema
/// invariant directly instead.
#[tokio::test]
async fn rust_schema_rejects_immutable_non_lww_field() {
    const COUNTER_SCHEMA: &str =
        r#"type CounterDoc { hits: Int @crdt(type: "pncounter") @immutable  body: String }"#;
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();

    let err = cluster
        .client(0)
        .schema_add(COUNTER_SCHEMA)
        .expect_err("@immutable on a non-LWW (counter) field must be rejected by the schema");
    let msg = err.to_string();
    assert!(
        msg.contains("only LWW register fields can be immutable"),
        "schema rejection should explain @immutable requires an LWW field, got: {msg}"
    );
}

/// #1-test / F7: filtered replication under ACP (Controlled access mode). Every
/// other filtered test runs in Open mode, where `bitswap/filter.rs` short-circuits
/// (`mode.is_open() -> true`) before the filtered-replicator gating. Running under
/// `with_acp_local()` keeps `check_access` in Controlled mode so its
/// filtered-replicator logic is active. (The deny branch itself fires only on a
/// Bitswap WANT for a known non-matching CID, which the harness cannot issue; that
/// branch stays covered by the in-process unit test
/// `controlled_mode_denies_filtered_replicator_data_block_requests`.)
#[tokio::test]
async fn rust_filtered_replication_excludes_nonmatching_controlled_mode() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let alice = generate_identity(node0.binary_path()).expect("generate identity");

    let policy = node0
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("policy node0");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("policy id")
        .to_string();
    node1
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("policy node1");

    let schema = format!(
        r#"type User @policy(id: "{policy_id}", resource: "users") {{ agent_did: String @immutable  name: String }}"#
    );
    node0
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("schema node0");
    node1
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["User"]).expect("subscribe 0");
    add_filtered_replicator_with_identity(
        &cluster,
        0,
        &["User"],
        &addr1,
        "agent_did",
        ALICE,
        &alice.private_key_hex,
    );

    let mk = |did: &str, name: &str| {
        format!(
            r#"mutation {{ add_User(input: {{agent_did: "{did}", name: "{name}"}}) {{ _docID }} }}"#
        )
    };
    let matching = node0
        .query_with_identity(&mk(ALICE, "a"), &alice.private_key_hex)
        .expect("create matching");
    let matching_id = extract_doc_id(&matching, "add_User");
    node0
        .query_with_identity(&mk(BOB, "b"), &alice.private_key_hex)
        .expect("create non-matching");
    // BOB is filtered at the sender and never pushed; the second matching doc
    // just ensures the push pipeline has delivered before we assert absence.
    let matching2 = node0
        .query_with_identity(&mk(ALICE, "a2"), &alice.private_key_hex)
        .expect("create second matching");
    let matching2_id = extract_doc_id(&matching2, "add_User");

    // Replicated docs are unregistered on node1, so they are public there.
    let node1_for_poll = cluster.client(1);
    let m1 = matching_id.clone();
    let m2 = matching2_id.clone();
    poll_until(
        || {
            let result = node1_for_poll
                .query("query { User { _docID agent_did } }")
                .unwrap_or_default();
            let Some(rows) = result["User"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "matching docs did not replicate to filtered peer under ACP",
    )
    .await;

    let result = node1
        .query("query { User { agent_did } }")
        .expect("query node1");
    let rows = result["User"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "filtered peer must hold only the two matching docs under ACP, found: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r["agent_did"].as_str() == Some(ALICE)),
        "filtered peer must hold only matching documents under ACP, found: {rows:?}"
    );
}

/// A typed Float filter value (2.0) — set via HTTP, since the CLI only sends
/// string values — matches a whole-number Float field that materializes as an
/// integer JSON number, exercising numeric-aware filter matching end to end.
#[tokio::test]
async fn rust_filtered_replication_float_filter_matches_numerically() {
    const SCORE_SCHEMA: &str = "type ScoreDoc { score: Float @immutable  body: String }";
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    node0.schema_add(SCORE_SCHEMA).expect("schema node0");
    node1.schema_add(SCORE_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["ScoreDoc"])
        .expect("subscribe 0");

    // Typed Float filter value (2.0) — must go via HTTP; the CLI wraps as a string.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v0/p2p/replicators", cluster.api_url(0)))
        .json(&serde_json::json!({
            "Collections": ["ScoreDoc"],
            "Addresses": [addr1],
            "Filters": {"ScoreDoc": {"Field": "score", "Value": 2.0}}
        }))
        .send()
        .await
        .expect("add filtered replicator");
    assert!(
        resp.status().is_success(),
        "HTTP filtered replicator add failed: {}",
        resp.status()
    );

    let matching = node0
        .query(r#"mutation { add_ScoreDoc(input: {score: 2.0, body: "a"}) { _docID } }"#)
        .expect("matching");
    let matching_id = extract_doc_id(&matching, "add_ScoreDoc");
    node0
        .query(r#"mutation { add_ScoreDoc(input: {score: 3.0, body: "b"}) { _docID } }"#)
        .expect("non-matching");
    let matching2 = node0
        .query(r#"mutation { add_ScoreDoc(input: {score: 2.0, body: "a2"}) { _docID } }"#)
        .expect("matching2");
    let matching2_id = extract_doc_id(&matching2, "add_ScoreDoc");

    let node1_poll = cluster.client(1);
    let m1 = matching_id.clone();
    let m2 = matching2_id.clone();
    poll_until(
        || {
            let r = node1_poll
                .query("query { ScoreDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = r["ScoreDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|x| x["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "Float-matching docs did not replicate to filtered peer",
    )
    .await;

    let result = node1
        .query("query { ScoreDoc { _docID } }")
        .expect("query node1");
    let count = result["ScoreDoc"].as_array().map(Vec::len).unwrap_or(0);
    assert_eq!(
        count, 2,
        "filtered peer must hold only the two score=2.0 docs, not score=3.0"
    );
}

// #3 (remote-merge enforcement of `@immutable`) is intentionally NOT an
// integration-suite test. The guard in `composite_persist.rs` is defense-in-depth
// against a malicious or divergent peer and is unreachable through two honest
// nodes: local validation blocks the originating update, and content-addressed
// doc IDs prevent two honest nodes from forging the same docID with differing
// immutable values. It is covered directly at the merge layer by
// `db-merge`'s `remote_composite_merge_rejects_immutable_field_change`.

/// Add a replicator with a raw query-filter conditions JSON via the CLI --filter flag.
fn run_replicator_add_filter(
    cluster: &TestCluster,
    node: usize,
    collections: &[&str],
    addr: &str,
    filter_json: &str,
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
            "--filter",
            filter_json,
            addr,
        ])
        .output()
        .expect("exec replicator add --filter")
}

/// Add a replicator with a raw query-filter conditions JSON authenticated as
/// `identity_hex` (Controlled/ACP mode). Combines `--filter <json>` with `-i`.
fn add_filtered_replicator_json_with_identity(
    cluster: &TestCluster,
    node: usize,
    collections: &[&str],
    addr: &str,
    filter_json: &str,
    identity_hex: &str,
) {
    let client = cluster.client(node);
    let cols = collections.join(",");
    let output = Command::new(client.binary_path())
        .arg("--url")
        .arg(socket_addr(cluster, node))
        .args([
            "client",
            "-i",
            identity_hex,
            "p2p",
            "replicator",
            "add",
            "-c",
            &cols,
            "--filter",
            filter_json,
            addr,
        ])
        .output()
        .expect("exec replicator add --filter (with identity)");
    assert!(
        output.status.success(),
        "rich filtered replicator add (identity) failed: status={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

/// IN-set predicate: only docs whose `agent_did` is in `[alice, carol]` replicate.
///
/// The ordering-anchor is the carol doc created after the non-matching bob doc.
/// Once carol arrives on node1 the push pipeline has provably run; bob's absence
/// is then attributable to the filter, not to a not-yet-delivered race.
#[tokio::test]
async fn rust_filtered_replication_in_set() {
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

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");

    let filter_json = format!(r#"{{"agent_did":{{"_in":["{ALICE}","{CAROL}"]}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AgentDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "IN-set replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let alice_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "alice-1"}}) {{ _docID }} }}"#
        ))
        .expect("create alice doc");
    let alice_id = extract_doc_id(&alice_doc, "add_AgentDoc");

    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "bob-1"}}) {{ _docID }} }}"#
        ))
        .expect("create bob doc");

    let carol_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{CAROL}", body: "carol-1"}}) {{ _docID }} }}"#
        ))
        .expect("create carol doc (ordering anchor)");
    let carol_id = extract_doc_id(&carol_doc, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let alice_id_poll = alice_id.clone();
    let carol_id_poll = carol_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID agent_did } }")
                .unwrap_or_default();
            let Some(rows) = result["AgentDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&alice_id_poll.as_str()) && ids.contains(&carol_id_poll.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "alice and carol docs did not replicate to IN-set filtered peer",
    )
    .await;

    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids.len(),
        2,
        "IN-set filtered peer must hold exactly 2 docs (alice + carol), found: {dids:?}"
    );
    assert!(
        dids.iter().all(|d| d == ALICE || d == CAROL),
        "IN-set filtered peer must hold only alice and carol, found: {dids:?}"
    );
    assert!(
        !dids.iter().any(|d| d == BOB),
        "bob must be excluded by IN-set filter, found: {dids:?}"
    );
}

/// Composite AND predicate: only docs where `agent_did = alice` AND `kind = keep` replicate.
#[tokio::test]
async fn rust_filtered_replication_composite_and() {
    const AND_SCHEMA: &str =
        "type AndDoc { agent_did: String @immutable  kind: String @immutable  seq: Int }";

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(AND_SCHEMA).expect("schema node0");
    node1.schema_add(AND_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["AndDoc"]).expect("subscribe 0");

    let filter_json = format!(r#"{{"agent_did":{{"_eq":"{ALICE}"}},"kind":{{"_eq":"keep"}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AndDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "composite AND replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let mk = |did: &str, kind: &str, seq: i32| {
        format!(
            r#"mutation {{ add_AndDoc(input: {{agent_did: "{did}", kind: "{kind}", seq: {seq}}}) {{ _docID }} }}"#
        )
    };

    let match1 = node0
        .query(&mk(ALICE, "keep", 1))
        .expect("create (alice, keep, 1) doc");
    let match1_id = extract_doc_id(&match1, "add_AndDoc");

    node0
        .query(&mk(ALICE, "drop", 2))
        .expect("create (alice, drop) doc");
    node0
        .query(&mk(BOB, "keep", 3))
        .expect("create (bob, keep) doc");

    let match2 = node0
        .query(&mk(ALICE, "keep", 4))
        .expect("create second (alice, keep, 4) anchor");
    let match2_id = extract_doc_id(&match2, "add_AndDoc");

    let node1_poll = cluster.client(1);
    let m1 = match1_id.clone();
    let m2 = match2_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AndDoc { _docID agent_did kind } }")
                .unwrap_or_default();
            let Some(rows) = result["AndDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "composite AND matching docs did not replicate",
    )
    .await;

    let result = node1
        .query("query { AndDoc { agent_did kind } }")
        .expect("query AndDoc on node1");
    let rows = result["AndDoc"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "composite AND filtered peer must hold exactly 2 (alice,keep) docs, found: {rows:?}"
    );
    assert!(
        rows.iter()
            .all(|r| r["agent_did"].as_str() == Some(ALICE) && r["kind"].as_str() == Some("keep")),
        "composite AND filtered peer must hold only (alice,keep) docs, found: {rows:?}"
    );
}

/// A predicate on a mutable field (`body`) must be rejected at add time.
#[tokio::test]
async fn rust_filtered_replicator_rejects_predicate_non_immutable_field() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 1).await;
    cluster.client(0).schema_add(AGENT_SCHEMA).expect("schema");

    let out = run_replicator_add_filter(
        &cluster,
        0,
        &["AgentDoc"],
        DUMMY_PEER_ADDR,
        r#"{"body":{"_eq":"x"}}"#,
    );
    assert!(
        !out.status.success(),
        "filtering on a mutable field via --filter must be rejected"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("immutable"),
        "error should require the filter field be @immutable, got: {stderr}"
    );
}

/// A filter with an empty `Field` and no `Conditions` is structurally invalid
/// over HTTP. The adapter rejects it with a 4xx before any P2P dial is attempted.
/// (The internal `Acp` variant of `p2p::ReplicationFilter` cannot be expressed
/// over the HTTP wire format at all; see the `replication-filter` unit tests for
/// that coverage.)
#[tokio::test]
async fn rust_filtered_replicator_rejects_empty_field_over_http() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 1).await;
    cluster.client(0).schema_add(AGENT_SCHEMA).expect("schema");

    let resp = reqwest::Client::new()
        .post(format!("{}/api/v0/p2p/replicators", cluster.api_url(0)))
        .json(&serde_json::json!({
            "Collections": ["AgentDoc"],
            "Addresses": [DUMMY_PEER_ADDR],
            "Filters": {"AgentDoc": {"Field": "", "Value": null}}
        }))
        .send()
        .await
        .expect("POST replicators");

    assert!(
        resp.status().is_client_error(),
        "empty-field filter must be rejected with a 4xx, got: {}",
        resp.status()
    );
}

/// The legacy `{Field, Value}` HTTP shape still routes correctly through the new
/// filter resolution path — back-compat for clients that have not migrated to
/// the `Conditions` shape.
#[tokio::test]
async fn rust_filtered_replication_legacy_format_back_compat() {
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

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");

    let resp = reqwest::Client::new()
        .post(format!("{}/api/v0/p2p/replicators", cluster.api_url(0)))
        .json(&serde_json::json!({
            "Collections": ["AgentDoc"],
            "Addresses": [addr1],
            "Filters": {"AgentDoc": {"Field": "agent_did", "Value": ALICE}}
        }))
        .send()
        .await
        .expect("POST replicators legacy");
    assert!(
        resp.status().is_success(),
        "legacy {{Field, Value}} replicator add failed: {}",
        resp.status()
    );

    let alice1 = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "a1"}}) {{ _docID }} }}"#
        ))
        .expect("alice1");
    let alice1_id = extract_doc_id(&alice1, "add_AgentDoc");

    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "b1"}}) {{ _docID }} }}"#
        ))
        .expect("bob1");

    let alice2 = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "a2"}}) {{ _docID }} }}"#
        ))
        .expect("alice2 anchor");
    let alice2_id = extract_doc_id(&alice2, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let a1 = alice1_id.clone();
    let a2 = alice2_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["AgentDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&a1.as_str()) && ids.contains(&a2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "legacy-format alice docs did not replicate",
    )
    .await;

    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids.len(),
        2,
        "legacy back-compat: only alice docs must replicate, found: {dids:?}"
    );
    assert!(
        dids.iter().all(|d| d == ALICE),
        "legacy back-compat: bob must be excluded, found: {dids:?}"
    );
}

/// A replicator's filter can be updated in-place (upsert) by re-adding with the
/// same peer+collection but a new predicate. Documents that match the updated
/// filter but not the original must begin replicating after the upsert without
/// requiring a remove/re-add cycle.
#[tokio::test]
async fn rust_filtered_replication_mutable_filter_upsert() {
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

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");

    let narrow_filter = format!(r#"{{"agent_did":{{"_in":["{ALICE}"]}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AgentDoc"], &addr1, &narrow_filter);
    assert!(
        out.status.success(),
        "initial narrow filter add failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let alice1 = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "alice-before-upsert"}}) {{ _docID }} }}"#
        ))
        .expect("alice1");
    let alice1_id = extract_doc_id(&alice1, "add_AgentDoc");

    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{CAROL}", body: "carol-before-upsert"}}) {{ _docID }} }}"#
        ))
        .expect("carol before upsert");

    let node1_poll = cluster.client(1);
    let a1 = alice1_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(a1.as_str()))
            })
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "alice1 did not replicate before upsert",
    )
    .await;

    // Carol arrived on node0 first but must still be absent on node1 at this
    // point (the narrow filter excludes her). A brief settle window confirms
    // absence before the upsert changes the filter.
    tokio::time::sleep(ABSENCE_GRACE).await;
    let before_upsert = agent_did_values(&cluster, 1);
    assert!(
        !before_upsert.iter().any(|d| d == CAROL),
        "carol must not be present before filter upsert, found: {before_upsert:?}"
    );

    // Upsert the replicator with a wider filter that now includes carol.
    let wide_filter = format!(r#"{{"agent_did":{{"_in":["{ALICE}","{CAROL}"]}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AgentDoc"], &addr1, &wide_filter);
    assert!(
        out.status.success(),
        "filter upsert failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Create new docs after the upsert so delivery comes from live push under
    // the updated filter (not backfill, which is separate behavior).
    let carol2 = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{CAROL}", body: "carol-after-upsert"}}) {{ _docID }} }}"#
        ))
        .expect("carol2 after upsert");
    let carol2_id = extract_doc_id(&carol2, "add_AgentDoc");

    let alice2 = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "alice-anchor"}}) {{ _docID }} }}"#
        ))
        .expect("alice2 ordering anchor");
    let alice2_id = extract_doc_id(&alice2, "add_AgentDoc");

    let node1_poll2 = cluster.client(1);
    let c2 = carol2_id.clone();
    let a2 = alice2_id.clone();
    poll_until(
        || {
            let result = node1_poll2
                .query("query { AgentDoc { _docID agent_did } }")
                .unwrap_or_default();
            let Some(rows) = result["AgentDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&c2.as_str()) && ids.contains(&a2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "carol2 or alice2 did not replicate after filter upsert",
    )
    .await;

    let dids_after = agent_did_values(&cluster, 1);
    assert!(
        dids_after.iter().any(|d| d == CAROL),
        "carol must replicate after filter upsert, found: {dids_after:?}"
    );
    assert!(
        !dids_after.iter().any(|d| d == BOB),
        "bob must remain excluded after filter upsert, found: {dids_after:?}"
    );
}

/// Backfill with `_in` set filter: docs created BEFORE replicator is added must
/// be backfilled only if they appear in the IN set.
///
/// This test would fail if the backfill path used `EqOnlyFilterMatcher` because
/// `_in` has no `_eq` key — EqOnly would match nothing and silently drop ALL docs.
#[tokio::test]
async fn rust_filtered_replication_in_set_backfill() {
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

    // Create all docs BEFORE wiring replication so delivery comes from backfill.
    let alice_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "alice-backfill"}}) {{ _docID }} }}"#
        ))
        .expect("create alice doc");
    let alice_id = extract_doc_id(&alice_doc, "add_AgentDoc");

    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "bob-backfill"}}) {{ _docID }} }}"#
        ))
        .expect("create bob doc");

    let carol_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{CAROL}", body: "carol-backfill"}}) {{ _docID }} }}"#
        ))
        .expect("create carol doc");
    let carol_id = extract_doc_id(&carol_doc, "add_AgentDoc");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");

    let filter_json = format!(r#"{{"agent_did":{{"_in":["{ALICE}","{CAROL}"]}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AgentDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "IN-set backfill replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    // Both alice and carol must arrive via backfill.
    let node1_poll = cluster.client(1);
    let alice_id_poll = alice_id.clone();
    let carol_id_poll = carol_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID agent_did } }")
                .unwrap_or_default();
            let Some(rows) = result["AgentDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&alice_id_poll.as_str()) && ids.contains(&carol_id_poll.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "alice and carol did not backfill to IN-set filtered peer",
    )
    .await;

    // Bob must be absent. After both matching docs arrived the push pipeline has
    // provably run, so bob's absence is attributable to the filter.
    tokio::time::sleep(ABSENCE_GRACE).await;
    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids.len(),
        2,
        "IN-set backfill filtered peer must hold exactly 2 docs (alice + carol), found: {dids:?}"
    );
    assert!(
        dids.iter().all(|d| d == ALICE || d == CAROL),
        "IN-set backfill filtered peer must hold only alice and carol, found: {dids:?}"
    );
    assert!(
        !dids.iter().any(|d| d == BOB),
        "bob must be excluded by IN-set backfill filter, found: {dids:?}"
    );
}

/// OR predicate: docs where `agent_did = alice` OR `kind = keep` replicate;
/// (bob, drop) satisfies neither arm and must be excluded.
#[tokio::test]
async fn rust_filtered_replication_or() {
    const OR_SCHEMA: &str = "type OrDoc { agent_did: String @immutable  kind: String @immutable }";

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(OR_SCHEMA).expect("schema node0");
    node1.schema_add(OR_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["OrDoc"]).expect("subscribe 0");

    let filter_json =
        format!(r#"{{"_or":[{{"agent_did":{{"_eq":"{ALICE}"}}}},{{"kind":{{"_eq":"keep"}}}}]}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["OrDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "OR replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let mk = |did: &str, kind: &str| {
        format!(
            r#"mutation {{ add_OrDoc(input: {{agent_did: "{did}", kind: "{kind}"}}) {{ _docID }} }}"#
        )
    };

    let match_alice = node0
        .query(&mk(ALICE, "x"))
        .expect("create (alice, x) — matches via agent_did arm");
    let match_alice_id = extract_doc_id(&match_alice, "add_OrDoc");

    node0
        .query(&mk(BOB, "drop"))
        .expect("create (bob, drop) — matches neither arm");

    let match_keep = node0
        .query(&mk(BOB, "keep"))
        .expect("create (bob, keep) — matches via kind arm (ordering anchor)");
    let match_keep_id = extract_doc_id(&match_keep, "add_OrDoc");

    let node1_poll = cluster.client(1);
    let m_alice = match_alice_id.clone();
    let m_keep = match_keep_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { OrDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["OrDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m_alice.as_str()) && ids.contains(&m_keep.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "OR-matching docs did not replicate to filtered peer",
    )
    .await;

    let result = node1
        .query("query { OrDoc { agent_did kind } }")
        .expect("query OrDoc on node1");
    let rows = result["OrDoc"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "OR filtered peer must hold exactly 2 matching docs, found: {rows:?}"
    );
    let has_drop = rows.iter().any(|r| r["kind"].as_str() == Some("drop"));
    assert!(
        !has_drop,
        "(bob, drop) must be excluded by OR filter, found: {rows:?}"
    );
}

/// NE predicate: docs where `agent_did != bob` replicate; bob is excluded.
#[tokio::test]
async fn rust_filtered_replication_ne_excludes() {
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

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");

    let filter_json = format!(r#"{{"agent_did":{{"_ne":"{BOB}"}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AgentDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "NE replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let alice_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "alice-ne"}}) {{ _docID }} }}"#
        ))
        .expect("alice doc");
    let alice_id = extract_doc_id(&alice_doc, "add_AgentDoc");

    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "bob-ne"}}) {{ _docID }} }}"#
        ))
        .expect("bob doc");

    let carol_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{CAROL}", body: "carol-ne"}}) {{ _docID }} }}"#
        ))
        .expect("carol doc (ordering anchor)");
    let carol_id = extract_doc_id(&carol_doc, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let a_id = alice_id.clone();
    let c_id = carol_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["AgentDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&a_id.as_str()) && ids.contains(&c_id.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "alice and carol did not replicate under NE filter",
    )
    .await;

    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids.len(),
        2,
        "NE filter must admit exactly alice + carol (not bob), found: {dids:?}"
    );
    assert!(
        !dids.iter().any(|d| d == BOB),
        "bob must be excluded by NE filter, found: {dids:?}"
    );
    assert!(
        dids.iter().any(|d| d == ALICE) && dids.iter().any(|d| d == CAROL),
        "alice and carol must be present, found: {dids:?}"
    );
}

/// Range predicate (GTE): docs with `tier >= 2` replicate; tier=1 is excluded.
#[tokio::test]
async fn rust_filtered_replication_range() {
    const TIER_SCHEMA: &str = "type TierDoc { tier: Int @immutable  body: String }";

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(TIER_SCHEMA).expect("schema node0");
    node1.schema_add(TIER_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["TierDoc"]).expect("subscribe 0");

    let filter_json = r#"{"tier":{"_gte":2}}"#;
    let out = run_replicator_add_filter(&cluster, 0, &["TierDoc"], &addr1, filter_json);
    assert!(
        out.status.success(),
        "range (GTE) replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let mk = |tier: i32, body: &str| {
        format!(
            r#"mutation {{ add_TierDoc(input: {{tier: {tier}, body: "{body}"}}) {{ _docID }} }}"#
        )
    };

    node0.query(&mk(1, "tier1")).expect("tier=1 doc");

    let tier2 = node0.query(&mk(2, "tier2")).expect("tier=2 doc");
    let tier2_id = extract_doc_id(&tier2, "add_TierDoc");

    let tier3 = node0
        .query(&mk(3, "tier3"))
        .expect("tier=3 doc (ordering anchor)");
    let tier3_id = extract_doc_id(&tier3, "add_TierDoc");

    let node1_poll = cluster.client(1);
    let t2 = tier2_id.clone();
    let t3 = tier3_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { TierDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["TierDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&t2.as_str()) && ids.contains(&t3.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "tier>=2 docs did not replicate to range-filtered peer",
    )
    .await;

    let result = node1
        .query("query { TierDoc { tier } }")
        .expect("query TierDoc on node1");
    let rows = result["TierDoc"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "range filter must admit exactly tier=2 and tier=3, found: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r["tier"].as_i64().unwrap_or(0) >= 2),
        "all replicated docs must have tier >= 2, found: {rows:?}"
    );
    let has_tier1 = rows.iter().any(|r| r["tier"].as_i64() == Some(1));
    assert!(
        !has_tier1,
        "tier=1 doc must be excluded by range filter, found: {rows:?}"
    );
}

/// Typed Int equality filter: only docs with `code = 7` replicate.
/// Proves Int materialization + matching e2e (the silent-zero-match guard for Int).
#[tokio::test]
async fn rust_filtered_replication_int_match() {
    const INT_SCHEMA: &str = "type IntDoc { code: Int @immutable  body: String }";

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(INT_SCHEMA).expect("schema node0");
    node1.schema_add(INT_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["IntDoc"]).expect("subscribe 0");

    let filter_json = r#"{"code":{"_eq":7}}"#;
    let out = run_replicator_add_filter(&cluster, 0, &["IntDoc"], &addr1, filter_json);
    assert!(
        out.status.success(),
        "Int _eq replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let match1 = node0
        .query(r#"mutation { add_IntDoc(input: {code: 7, body: "seven-1"}) { _docID } }"#)
        .expect("code=7 doc");
    let match1_id = extract_doc_id(&match1, "add_IntDoc");

    node0
        .query(r#"mutation { add_IntDoc(input: {code: 9, body: "nine"}) { _docID } }"#)
        .expect("code=9 doc");

    let match2 = node0
        .query(r#"mutation { add_IntDoc(input: {code: 7, body: "seven-2"}) { _docID } }"#)
        .expect("code=7 anchor");
    let match2_id = extract_doc_id(&match2, "add_IntDoc");

    let node1_poll = cluster.client(1);
    let m1 = match1_id.clone();
    let m2 = match2_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { IntDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["IntDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "Int code=7 docs did not replicate to Int-filtered peer",
    )
    .await;

    let result = node1
        .query("query { IntDoc { code } }")
        .expect("query IntDoc on node1");
    let rows = result["IntDoc"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "Int filter must admit exactly the two code=7 docs, found: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r["code"].as_i64() == Some(7)),
        "all replicated IntDoc must have code=7, found: {rows:?}"
    );
}

/// Bool equality filter: only docs with `active = true` replicate.
/// Proves Bool materialization + matching e2e.
#[tokio::test]
async fn rust_filtered_replication_bool_match() {
    const FLAG_SCHEMA: &str = "type FlagDoc { active: Boolean @immutable  body: String }";

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(FLAG_SCHEMA).expect("schema node0");
    node1.schema_add(FLAG_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["FlagDoc"]).expect("subscribe 0");

    let filter_json = r#"{"active":{"_eq":true}}"#;
    let out = run_replicator_add_filter(&cluster, 0, &["FlagDoc"], &addr1, filter_json);
    assert!(
        out.status.success(),
        "Bool _eq replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let match1 = node0
        .query(r#"mutation { add_FlagDoc(input: {active: true, body: "on-1"}) { _docID } }"#)
        .expect("active=true doc");
    let match1_id = extract_doc_id(&match1, "add_FlagDoc");

    node0
        .query(r#"mutation { add_FlagDoc(input: {active: false, body: "off"}) { _docID } }"#)
        .expect("active=false doc");

    let match2 = node0
        .query(r#"mutation { add_FlagDoc(input: {active: true, body: "on-2"}) { _docID } }"#)
        .expect("active=true anchor");
    let match2_id = extract_doc_id(&match2, "add_FlagDoc");

    let node1_poll = cluster.client(1);
    let m1 = match1_id.clone();
    let m2 = match2_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { FlagDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["FlagDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "Bool active=true docs did not replicate to Bool-filtered peer",
    )
    .await;

    let result = node1
        .query("query { FlagDoc { active } }")
        .expect("query FlagDoc on node1");
    let rows = result["FlagDoc"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "Bool filter must admit exactly the two active=true docs, found: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r["active"].as_bool() == Some(true)),
        "all replicated FlagDoc must have active=true, found: {rows:?}"
    );
}

/// DateTime range filter: docs with `at >= 2026-01-01T00:00:00Z` replicate;
/// earlier dates are excluded. Proves DateTime materialization + comparison e2e.
#[tokio::test]
async fn rust_filtered_replication_datetime() {
    const EVENT_SCHEMA: &str = "type EventDoc { at: DateTime @immutable  body: String }";

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(EVENT_SCHEMA).expect("schema node0");
    node1.schema_add(EVENT_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["EventDoc"])
        .expect("subscribe 0");

    let filter_json = r#"{"at":{"_gte":"2026-01-01T00:00:00Z"}}"#;
    let out = run_replicator_add_filter(&cluster, 0, &["EventDoc"], &addr1, filter_json);
    assert!(
        out.status.success(),
        "DateTime GTE replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    node0
        .query(
            r#"mutation { add_EventDoc(input: {at: "2025-06-01T00:00:00Z", body: "old"}) { _docID } }"#,
        )
        .expect("old datetime doc");

    let match1 = node0
        .query(
            r#"mutation { add_EventDoc(input: {at: "2026-01-01T00:00:00Z", body: "new-1"}) { _docID } }"#,
        )
        .expect("matching datetime doc");
    let match1_id = extract_doc_id(&match1, "add_EventDoc");

    let match2 = node0
        .query(
            r#"mutation { add_EventDoc(input: {at: "2026-06-01T00:00:00Z", body: "new-2"}) { _docID } }"#,
        )
        .expect("matching datetime anchor");
    let match2_id = extract_doc_id(&match2, "add_EventDoc");

    let node1_poll = cluster.client(1);
    let m1 = match1_id.clone();
    let m2 = match2_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { EventDoc { _docID } }")
                .unwrap_or_default();
            let Some(rows) = result["EventDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "DateTime >= 2026 docs did not replicate to DateTime-filtered peer",
    )
    .await;

    let result = node1
        .query("query { EventDoc { body } }")
        .expect("query EventDoc on node1");
    let rows = result["EventDoc"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "DateTime filter must admit exactly the two >= 2026 docs, found: {rows:?}"
    );
    let has_old = rows.iter().any(|r| r["body"].as_str() == Some("old"));
    assert!(
        !has_old,
        "old (2025) EventDoc must be excluded by DateTime filter, found: {rows:?}"
    );
}

/// Iroh transport variant of `rust_filtered_replication_in_set`: verifies the
/// rich `_in` filter works over Iroh (previously broken in the iroh adapter).
#[tokio::test]
async fn rust_filtered_replication_in_set_iroh() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();
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

    let filter_json = format!(r#"{{"agent_did":{{"_in":["{ALICE}","{CAROL}"]}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AgentDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "IN-set iroh replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let alice_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "alice-iroh"}}) {{ _docID }} }}"#
        ))
        .expect("alice doc");
    let alice_id = extract_doc_id(&alice_doc, "add_AgentDoc");

    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "bob-iroh"}}) {{ _docID }} }}"#
        ))
        .expect("bob doc");

    let carol_doc = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{CAROL}", body: "carol-iroh"}}) {{ _docID }} }}"#
        ))
        .expect("carol doc (ordering anchor)");
    let carol_id = extract_doc_id(&carol_doc, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let a_id = alice_id.clone();
    let c_id = carol_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID agent_did } }")
                .unwrap_or_default();
            let Some(rows) = result["AgentDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&a_id.as_str()) && ids.contains(&c_id.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "alice and carol did not replicate over iroh IN-set filter",
    )
    .await;

    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids.len(),
        2,
        "iroh IN-set filtered peer must hold exactly 2 docs (alice + carol), found: {dids:?}"
    );
    assert!(
        dids.iter().all(|d| d == ALICE || d == CAROL),
        "iroh IN-set filtered peer must hold only alice and carol, found: {dids:?}"
    );
    assert!(
        !dids.iter().any(|d| d == BOB),
        "bob must be excluded by iroh IN-set filter, found: {dids:?}"
    );
}

/// Gap 2: deleting a filtered replicator must stop further filtered pushes. A
/// matching doc created BEFORE the delete replicates; a matching doc created
/// AFTER the delete must not, even though it satisfies the (now-removed) filter.
#[tokio::test]
async fn rust_filtered_replication_delete_stops_push() {
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

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");
    add_filtered_replicator(&cluster, 0, &["AgentDoc"], &addr1, "agent_did", ALICE);

    // A matching doc created while the replicator is live must replicate.
    let before = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "before-delete"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc before delete");
    let before_id = extract_doc_id(&before, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let before_id_poll = before_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(before_id_poll.as_str()))
            })
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "matching doc did not replicate before replicator delete",
    )
    .await;

    // Delete the replicator (try full multiaddr first, then bare peer ID).
    let peer_id = addr1.rsplit("/p2p/").next().unwrap_or(&addr1);
    node0
        .p2p_replicator_delete(&["AgentDoc"], Some(&addr1))
        .or_else(|_| node0.p2p_replicator_delete(&["AgentDoc"], Some(peer_id)))
        .expect("p2p_replicator_delete");

    // A NEW matching doc created after the delete must NOT arrive. With no
    // replicator left there is no ordering anchor, so absence is confirmed over
    // the fixed ABSENCE_GRACE window (the push pipeline already proved live above).
    let after = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "after-delete"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc after delete");
    let after_id = extract_doc_id(&after, "add_AgentDoc");

    tokio::time::sleep(ABSENCE_GRACE).await;
    let result = node1
        .query("query { AgentDoc { _docID } }")
        .expect("query node1 after delete");
    let ids: Vec<&str> = result["AgentDoc"]
        .as_array()
        .map(|rows| rows.iter().filter_map(|r| r["_docID"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        ids.contains(&before_id.as_str()),
        "the pre-delete doc must remain on node1, found: {ids:?}"
    );
    assert!(
        !ids.contains(&after_id.as_str()),
        "a matching doc created after replicator delete must NOT be pushed, found: {ids:?}"
    );
}

/// Iroh transport variant of `rust_filtered_replication_delete_stops_push`:
/// deleting a filtered replicator over Iroh (defra-agent's production transport)
/// must stop further filtered pushes. Exercises the iroh adapter's delete path
/// end to end — name->CID resolution, coordinator/registry removal, and
/// peerstore handling. A matching doc created BEFORE the delete replicates; a
/// matching doc created AFTER the delete must not.
#[tokio::test]
async fn rust_filtered_replication_delete_stops_push_iroh() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();
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
    add_filtered_replicator(&cluster, 0, &["AgentDoc"], &addr1, "agent_did", ALICE);

    // A matching doc created while the replicator is live must replicate.
    let before = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "before-delete"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc before delete");
    let before_id = extract_doc_id(&before, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let before_id_poll = before_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(before_id_poll.as_str()))
            })
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "matching doc did not replicate before replicator delete over iroh",
    )
    .await;

    // Delete the replicator (try full multiaddr first, then bare peer ID).
    let peer_id = addr1.rsplit("/p2p/").next().unwrap_or(&addr1);
    node0
        .p2p_replicator_delete(&["AgentDoc"], Some(&addr1))
        .or_else(|_| node0.p2p_replicator_delete(&["AgentDoc"], Some(peer_id)))
        .expect("p2p_replicator_delete");

    // A NEW matching doc created after the delete must NOT arrive. With no
    // replicator left there is no ordering anchor, so absence is confirmed over
    // the fixed ABSENCE_GRACE window (the push pipeline already proved live above).
    let after = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "after-delete"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc after delete");
    let after_id = extract_doc_id(&after, "add_AgentDoc");

    tokio::time::sleep(ABSENCE_GRACE).await;
    let result = node1
        .query("query { AgentDoc { _docID } }")
        .expect("query node1 after delete");
    let ids: Vec<&str> = result["AgentDoc"]
        .as_array()
        .map(|rows| rows.iter().filter_map(|r| r["_docID"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        ids.contains(&before_id.as_str()),
        "the pre-delete doc must remain on node1 over iroh, found: {ids:?}"
    );
    assert!(
        !ids.contains(&after_id.as_str()),
        "a matching doc created after replicator delete must NOT be pushed over iroh, found: {ids:?}"
    );
}

/// Gap 5/6: a rich `Conditions` predicate created over the direct HTTP
/// `POST /api/v0/p2p/replicators` endpoint must round-trip structurally through
/// the `GET` list response (no P2P replication exercised — pure wire-format
/// coverage for the create -> list rich-predicate seam).
#[tokio::test]
async fn rust_filtered_replication_http_conditions_roundtrip() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 1).await;
    cluster.client(0).schema_add(AGENT_SCHEMA).expect("schema");

    let conditions = serde_json::json!({"agent_did": {"_in": [ALICE, CAROL]}});
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v0/p2p/replicators", cluster.api_url(0)))
        .json(&serde_json::json!({
            "Collections": ["AgentDoc"],
            "Addresses": [DUMMY_PEER_ADDR],
            "Filters": {"AgentDoc": {"Conditions": conditions}}
        }))
        .send()
        .await
        .expect("POST replicators with rich Conditions");
    assert!(
        resp.status().is_success(),
        "rich-Conditions replicator add failed: {}",
        resp.status()
    );

    let listed: serde_json::Value =
        reqwest::get(format!("{}/api/v0/p2p/replicators", cluster.api_url(0)))
            .await
            .expect("GET replicators")
            .json()
            .await
            .expect("parse replicators json");

    let entry = listed
        .as_array()
        .and_then(|arr| arr.first())
        .expect("at least one replicator listed");
    let filters = entry["Filters"]
        .as_object()
        .expect("replicator must carry a Filters object");
    // The Filters key may be the resolved collection-id rather than the name.
    let (_collection, filter) = filters
        .iter()
        .next()
        .expect("Filters object must have exactly one entry");
    assert_eq!(
        filter["Conditions"], conditions,
        "rich predicate must round-trip through the HTTP list intact, got: {listed}"
    );
}

/// Gap 7: the CLI `replicator list` must render a rich `Conditions` predicate
/// structurally (not just as an opaque blob). Proves `P2pReplicatorInfo` carries
/// and re-serializes the `Filters` field.
#[tokio::test]
async fn rust_filtered_replication_cli_list_renders_filter() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 1).await;
    cluster.client(0).schema_add(AGENT_SCHEMA).expect("schema");

    let conditions = serde_json::json!({"agent_did": {"_in": [ALICE, BOB]}});
    let filter_json = serde_json::to_string(&conditions).unwrap();
    let out = run_replicator_add_filter(&cluster, 0, &["AgentDoc"], DUMMY_PEER_ADDR, &filter_json);
    assert!(
        out.status.success(),
        "CLI rich-filter replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let listed = cluster
        .client(0)
        .p2p_replicator_list()
        .expect("p2p_replicator_list");

    let entry = listed
        .as_array()
        .and_then(|arr| arr.first())
        .expect("CLI must list at least one replicator");
    let filters = entry["Filters"]
        .as_object()
        .expect("CLI replicator list must render a Filters object");
    let (_collection, filter) = filters
        .iter()
        .next()
        .expect("Filters object must have exactly one entry");
    assert_eq!(
        filter["Conditions"], conditions,
        "CLI replicator list must render the rich predicate structurally, got: {listed}"
    );
}

/// Iroh transport variant of `rust_filtered_replication_composite_and`: a
/// composite `agent_did = alice AND kind = keep` predicate must gate the live
/// push path over Iroh (defra-agent's production transport). A doc matching both
/// arms replicates; a doc matching only one arm must be excluded.
#[tokio::test]
async fn rust_filtered_replication_composite_and_iroh() {
    const AND_SCHEMA: &str =
        "type AndDoc { agent_did: String @immutable  kind: String @immutable  seq: Int }";

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(AND_SCHEMA).expect("schema node0");
    node1.schema_add(AND_SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["AndDoc"]).expect("subscribe 0");

    let filter_json = format!(r#"{{"agent_did":{{"_eq":"{ALICE}"}},"kind":{{"_eq":"keep"}}}}"#);
    let out = run_replicator_add_filter(&cluster, 0, &["AndDoc"], &addr1, &filter_json);
    assert!(
        out.status.success(),
        "composite AND iroh replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let mk = |did: &str, kind: &str, seq: i32| {
        format!(
            r#"mutation {{ add_AndDoc(input: {{agent_did: "{did}", kind: "{kind}", seq: {seq}}}) {{ _docID }} }}"#
        )
    };

    let match1 = node0
        .query(&mk(ALICE, "keep", 1))
        .expect("create (alice, keep, 1) doc");
    let match1_id = extract_doc_id(&match1, "add_AndDoc");

    node0
        .query(&mk(ALICE, "drop", 2))
        .expect("create (alice, drop) doc — matches only one arm");
    node0
        .query(&mk(BOB, "keep", 3))
        .expect("create (bob, keep) doc — matches only one arm");

    let match2 = node0
        .query(&mk(ALICE, "keep", 4))
        .expect("create second (alice, keep, 4) anchor");
    let match2_id = extract_doc_id(&match2, "add_AndDoc");

    let node1_poll = cluster.client(1);
    let m1 = match1_id.clone();
    let m2 = match2_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AndDoc { _docID agent_did kind } }")
                .unwrap_or_default();
            let Some(rows) = result["AndDoc"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "composite AND matching docs did not replicate over iroh",
    )
    .await;

    let result = node1
        .query("query { AndDoc { agent_did kind } }")
        .expect("query AndDoc on node1");
    let rows = result["AndDoc"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "iroh composite AND filtered peer must hold exactly 2 (alice,keep) docs, found: {rows:?}"
    );
    assert!(
        rows.iter()
            .all(|r| r["agent_did"].as_str() == Some(ALICE) && r["kind"].as_str() == Some("keep")),
        "iroh composite AND filtered peer must hold only (alice,keep) docs, found: {rows:?}"
    );
}

/// Iroh transport variant of `rust_filtered_replication_backfill_respects_filter`:
/// documents created BEFORE the replicator is added must be backfilled over Iroh,
/// respecting the filter. This exercises the iroh backfill path
/// (`push_existing_docs_via_transport`) with a predicate — the matching doc must
/// arrive while the non-matching doc stays absent.
#[tokio::test]
async fn rust_filtered_replication_backfill_iroh() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
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
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "backfilled-iroh"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc");
    let matching_id = extract_doc_id(&matching, "add_AgentDoc");
    node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "excluded-iroh"}}) {{ _docID }} }}"#
        ))
        .expect("create non-matching doc");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0");
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
        "matching document did not backfill to filtered peer over iroh",
    )
    .await;

    // Backfill is unordered (no "after" anchor like the live-push tests), so a
    // short settle window remains before asserting the non-matching doc's absence.
    tokio::time::sleep(ABSENCE_GRACE).await;
    let dids = agent_did_values(&cluster, 1);
    assert_eq!(
        dids,
        vec![ALICE.to_string()],
        "iroh backfill must respect the filter, found: {dids:?}"
    );
}

/// #1038 Gap 4: a rich `_in` predicate composed with the Controlled-mode ACP
/// gate. The push path runs TWO independent gates in sequence — the ACP access
/// check + creator-DID resolution, AND the replication filter. This proves an
/// `_in` set filter and the ACP gate compose: alice+carol (in the set) replicate,
/// bob (out of set) is excluded, all under Controlled access.
#[tokio::test]
async fn rust_filtered_replication_acp_in_set_controlled_mode() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let alice = generate_identity(node0.binary_path()).expect("generate identity");

    let policy = node0
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("policy node0");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("policy id")
        .to_string();
    node1
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("policy node1");

    let schema = format!(
        r#"type User @policy(id: "{policy_id}", resource: "users") {{ agent_did: String @immutable  name: String }}"#
    );
    node0
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("schema node0");
    node1
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["User"]).expect("subscribe 0");

    let filter_json = format!(r#"{{"agent_did":{{"_in":["{ALICE}","{CAROL}"]}}}}"#);
    add_filtered_replicator_json_with_identity(
        &cluster,
        0,
        &["User"],
        &addr1,
        &filter_json,
        &alice.private_key_hex,
    );

    let mk = |did: &str, name: &str| {
        format!(
            r#"mutation {{ add_User(input: {{agent_did: "{did}", name: "{name}"}}) {{ _docID }} }}"#
        )
    };

    let alice_doc = node0
        .query_with_identity(&mk(ALICE, "a"), &alice.private_key_hex)
        .expect("create alice doc");
    let alice_id = extract_doc_id(&alice_doc, "add_User");

    node0
        .query_with_identity(&mk(BOB, "b"), &alice.private_key_hex)
        .expect("create bob doc");

    // CAROL is the ordering anchor: created after BOB, so once she arrives on
    // node1 the push pipeline has provably run and BOB's absence is the filter.
    let carol_doc = node0
        .query_with_identity(&mk(CAROL, "c"), &alice.private_key_hex)
        .expect("create carol doc (ordering anchor)");
    let carol_id = extract_doc_id(&carol_doc, "add_User");

    // Replicated docs are unregistered on node1, so they are public there.
    let node1_poll = cluster.client(1);
    let a_id = alice_id.clone();
    let c_id = carol_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { User { _docID agent_did } }")
                .unwrap_or_default();
            let Some(rows) = result["User"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&a_id.as_str()) && ids.contains(&c_id.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "alice and carol did not replicate to ACP IN-set filtered peer",
    )
    .await;

    let result = node1
        .query("query { User { agent_did } }")
        .expect("query node1");
    let dids: Vec<String> = result["User"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r["agent_did"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        dids.len(),
        2,
        "ACP IN-set filtered peer must hold exactly 2 docs (alice + carol), found: {dids:?}"
    );
    assert!(
        dids.iter().all(|d| d == ALICE || d == CAROL),
        "ACP IN-set filtered peer must hold only alice and carol, found: {dids:?}"
    );
    assert!(
        !dids.iter().any(|d| d == BOB),
        "bob must be excluded by ACP IN-set filter, found: {dids:?}"
    );
}

/// #1038 Gap 4: a composite AND predicate composed with the Controlled-mode ACP
/// gate. Two filter clauses (`agent_did = alice` AND `kind = keep`) × ACP gating
/// × creator-DID resolution — the composition most likely to hide a seam. A doc
/// matching BOTH clauses replicates; a doc matching only one is excluded, under
/// Controlled access.
#[tokio::test]
async fn rust_filtered_replication_acp_composite_controlled_mode() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let alice = generate_identity(node0.binary_path()).expect("generate identity");

    let policy = node0
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("policy node0");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("policy id")
        .to_string();
    node1
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("policy node1");

    let schema = format!(
        r#"type User @policy(id: "{policy_id}", resource: "users") {{ agent_did: String @immutable  kind: String @immutable  name: String }}"#
    );
    node0
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("schema node0");
    node1
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0.p2p_collection_add(&["User"]).expect("subscribe 0");

    let filter_json = format!(r#"{{"agent_did":{{"_eq":"{ALICE}"}},"kind":{{"_eq":"keep"}}}}"#);
    add_filtered_replicator_json_with_identity(
        &cluster,
        0,
        &["User"],
        &addr1,
        &filter_json,
        &alice.private_key_hex,
    );

    let mk = |did: &str, kind: &str, name: &str| {
        format!(
            r#"mutation {{ add_User(input: {{agent_did: "{did}", kind: "{kind}", name: "{name}"}}) {{ _docID }} }}"#
        )
    };

    let match1 = node0
        .query_with_identity(&mk(ALICE, "keep", "m1"), &alice.private_key_hex)
        .expect("create (alice, keep) doc");
    let match1_id = extract_doc_id(&match1, "add_User");

    node0
        .query_with_identity(&mk(ALICE, "drop", "x1"), &alice.private_key_hex)
        .expect("create (alice, drop) doc — matches only one clause");
    node0
        .query_with_identity(&mk(BOB, "keep", "x2"), &alice.private_key_hex)
        .expect("create (bob, keep) doc — matches only one clause");

    let match2 = node0
        .query_with_identity(&mk(ALICE, "keep", "m2"), &alice.private_key_hex)
        .expect("create second (alice, keep) anchor");
    let match2_id = extract_doc_id(&match2, "add_User");

    // Replicated docs are unregistered on node1, so they are public there.
    let node1_poll = cluster.client(1);
    let m1 = match1_id.clone();
    let m2 = match2_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { User { _docID agent_did kind } }")
                .unwrap_or_default();
            let Some(rows) = result["User"].as_array() else {
                return false;
            };
            let ids: Vec<&str> = rows.iter().filter_map(|r| r["_docID"].as_str()).collect();
            ids.contains(&m1.as_str()) && ids.contains(&m2.as_str())
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "composite (alice,keep) docs did not replicate to ACP filtered peer",
    )
    .await;

    let result = node1
        .query("query { User { agent_did kind } }")
        .expect("query node1");
    let rows = result["User"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        rows.len(),
        2,
        "ACP composite filtered peer must hold exactly 2 (alice,keep) docs, found: {rows:?}"
    );
    assert!(
        rows.iter()
            .all(|r| r["agent_did"].as_str() == Some(ALICE) && r["kind"].as_str() == Some("keep")),
        "ACP composite filtered peer must hold only (alice,keep) docs, found: {rows:?}"
    );
}

/// #1038 Gap 1: the filter must hold on the push-RETRY/recovery path, not only on
/// the live push. A push to a down replicator fails and is recorded in the
/// peerstore retry queue; the server's retry-drain loop later re-pushes via
/// `db_merge::retry_doc`, which re-loads the document and re-applies
/// `document_matches_filter` (so a non-matching queued doc is skipped at retry
/// time). This drives that path by killing node1 while node0 enqueues both a
/// matching and a non-matching doc, then restarts node1 and asserts recovery
/// delivered the matching doc while the filter still excluded the non-matching one.
#[tokio::test]
async fn rust_filtered_replication_retry_respects_filter() {
    // A keyring is required so each node's libp2p peer identity is persisted and
    // survives restart (without it the node uses an ephemeral peer key and comes
    // back with a new peer ID, which the source node's replicator can never reach).
    // A persistent store likewise keeps node1's prior data across the restart.
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_keyring()
        .with_store("redb")
        .build()
        .await
        .unwrap();
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
    add_filtered_replicator(&cluster, 0, &["AgentDoc"], &addr1, "agent_did", ALICE);

    // A matching doc delivered while both nodes are up proves the live path works
    // before we exercise recovery.
    let live = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "live-match"}}) {{ _docID }} }}"#
        ))
        .expect("create live matching doc");
    let live_id = extract_doc_id(&live, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let live_poll = live_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(live_poll.as_str()))
            })
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "live matching doc did not replicate before takedown",
    )
    .await;

    // Take node1 down so node0's pushes fail and enter the retry queue. The push
    // failure is what populates the retry path the filter must continue to gate.
    cluster.nodes[1].process.kill();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // While node1 is DOWN, create both a matching and a non-matching doc on node0.
    // Both pushes fail (peer unreachable) and are queued for retry; the filter is
    // applied at retry time, not enqueue time.
    let retry_match = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "retry-match"}}) {{ _docID }} }}"#
        ))
        .expect("create matching doc during downtime");
    let retry_match_id = extract_doc_id(&retry_match, "add_AgentDoc");

    let retry_skip = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "retry-skip"}}) {{ _docID }} }}"#
        ))
        .expect("create non-matching doc during downtime");
    let retry_skip_id = extract_doc_id(&retry_skip, "add_AgentDoc");

    // Bring node1 back. It reuses its rootdir and ports, so node0's persisted
    // replicator can reconnect and the retry-drain loop can deliver.
    cluster
        .restart_node(1, Duration::from_secs(60))
        .await
        .expect("restart node1");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P after restart");

    // Ephemeral transport state is lost on restart; re-establish the connection so
    // the replicator/retry-drain has a live channel to push over.
    let addr1_after = extract_p2p_addr(&cluster, 1);
    cluster
        .client(0)
        .p2p_connect(&[&addr1_after])
        .expect("reconnect 0->1 after restart");

    // The matching doc must arrive on the recovery path. A generous deadline
    // accommodates reconnect latency plus the retry-drain interval.
    let recovery_deadline = P2P_TIMEOUT + Duration::from_secs(30);
    let node1_after = cluster.client(1);
    let match_poll = retry_match_id.clone();
    poll_until(
        || {
            let result = node1_after
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(match_poll.as_str()))
            })
        },
        recovery_deadline,
        P2P_POLL_INTERVAL,
        "retry-match doc did not arrive on the recovery path after restart",
    )
    .await;

    // The matching doc arriving is the sync barrier: the recovery path has run.
    // A short grace confirms the non-matching doc stays absent — the filter held
    // on the retry/recovery path.
    tokio::time::sleep(ABSENCE_GRACE).await;
    let result = node1_after
        .query("query { AgentDoc { _docID } }")
        .expect("query node1 after recovery");
    let ids: Vec<&str> = result["AgentDoc"]
        .as_array()
        .map(|rows| rows.iter().filter_map(|r| r["_docID"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        ids.contains(&live_id.as_str()),
        "the live pre-takedown doc must remain on node1, found: {ids:?}"
    );
    assert!(
        ids.contains(&retry_match_id.as_str()),
        "the matching doc must be delivered on the recovery path, found: {ids:?}"
    );
    assert!(
        !ids.contains(&retry_skip_id.as_str()),
        "the non-matching doc must NOT be delivered on the retry/recovery path, found: {ids:?}"
    );
}

/// #1038 Gap 2: the filter must survive a SOURCE-node restart. On startup the
/// replicator-restore loop reloads persisted `ReplicatorInfo` into the in-memory
/// swarm registry; if it reloads only the collections (dropping `.filters`), the
/// restored replicator becomes push-everything and leaks non-matching documents.
/// This kills and restarts the SOURCE (node0), then proves the restored
/// replicator both still pushes matching docs AND still excludes non-matching ones.
#[tokio::test]
async fn rust_filtered_replication_source_restart_preserves_filter() {
    // Keyring => stable peer identity across restart; redb => persistent
    // peerstore/data so the persisted replicator (and its filter) survive.
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_keyring()
        .with_store("redb")
        .build()
        .await
        .unwrap();
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
    add_filtered_replicator(&cluster, 0, &["AgentDoc"], &addr1, "agent_did", ALICE);

    // A matching doc delivered while both nodes are up proves live filtered
    // replication works before the restart.
    let live = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "live-match"}}) {{ _docID }} }}"#
        ))
        .expect("create live matching doc");
    let live_id = extract_doc_id(&live, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let live_poll = live_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(live_poll.as_str()))
            })
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "live matching doc did not replicate before source restart",
    )
    .await;

    // Restart the SOURCE node. On startup node0 reloads its persisted replicator
    // (the path under test) into the swarm registry.
    cluster.nodes[0].process.kill();
    cluster
        .restart_node(0, Duration::from_secs(60))
        .await
        .expect("restart node0");
    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P after restart");

    // Ephemeral transport state is lost on restart; re-establish the connection so
    // the restored replicator has a live channel to push over.
    let addr1_after = extract_p2p_addr(&cluster, 1);
    cluster
        .client(0)
        .p2p_connect(&[&addr1_after])
        .expect("reconnect 0->1 after source restart");

    // After restart create a NON-MATCHING (BOB) doc, then a MATCHING (ALICE) doc as
    // a delivery/ordering barrier. With the bug the restored replicator has no
    // filter, so BOB leaks; with the fix BOB is excluded.
    let node0_after = cluster.client(0);
    let skip = node0_after
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{BOB}", body: "post-restart-skip"}}) {{ _docID }} }}"#
        ))
        .expect("create non-matching doc after restart");
    let skip_id = extract_doc_id(&skip, "add_AgentDoc");

    let after_match = node0_after
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "post-restart-match"}}) {{ _docID }} }}"#
        ))
        .expect("create matching anchor doc after restart");
    let after_match_id = extract_doc_id(&after_match, "add_AgentDoc");

    // The post-restart matching doc arriving proves the restored replicator is
    // pushing again, making it a meaningful sync barrier for the absence check.
    let recovery_deadline = P2P_TIMEOUT + Duration::from_secs(30);
    let node1_after = cluster.client(1);
    let match_poll = after_match_id.clone();
    poll_until(
        || {
            let result = node1_after
                .query("query { AgentDoc { _docID } }")
                .unwrap_or_default();
            result["AgentDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(match_poll.as_str()))
            })
        },
        recovery_deadline,
        P2P_POLL_INTERVAL,
        "post-restart matching doc did not replicate (restored replicator not pushing)",
    )
    .await;

    // The restored replicator demonstrably pushed; a short grace confirms the
    // non-matching doc stays absent — the filter survived the source restart.
    tokio::time::sleep(ABSENCE_GRACE).await;
    let result = node1_after
        .query("query { AgentDoc { _docID } }")
        .expect("query node1 after source restart");
    let ids: Vec<&str> = result["AgentDoc"]
        .as_array()
        .map(|rows| rows.iter().filter_map(|r| r["_docID"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        ids.contains(&live_id.as_str()),
        "the live pre-restart doc must remain on node1, found: {ids:?}"
    );
    assert!(
        ids.contains(&after_match_id.as_str()),
        "the post-restart matching doc must be delivered, found: {ids:?}"
    );
    assert!(
        !ids.contains(&skip_id.as_str()),
        "the non-matching doc must NOT be delivered after source restart, found: {ids:?}"
    );
}

/// Add a (non-filtered) replicator covering multiple collections via the CLI.
fn run_replicator_add_plain(
    cluster: &TestCluster,
    node: usize,
    collections: &[&str],
    addr: &str,
) -> std::process::Output {
    let client = cluster.client(node);
    let cols = collections.join(",");
    Command::new(client.binary_path())
        .arg("--url")
        .arg(socket_addr(cluster, node))
        .args(["client", "p2p", "replicator", "add", "-c", &cols, addr])
        .output()
        .expect("exec plain replicator add")
}

async fn list_replicators_http(cluster: &TestCluster, node: usize) -> serde_json::Value {
    reqwest::get(format!("{}/api/v0/p2p/replicators", cluster.api_url(node)))
        .await
        .expect("GET replicators")
        .json()
        .await
        .expect("parse replicators json")
}

/// #1038 two-store divergence: a PARTIAL `replicator delete -c <one>` on a
/// multi-collection replicator removes only that collection from the in-memory
/// push registry, but the CLI adapter must ALSO re-persist the remaining
/// collections to the peerstore — not wipe the whole peer entry. On the buggy
/// code the peerstore is deleted unconditionally, so `replicator list` (read from
/// the peerstore) shows the peer GONE while push still serves the survivor, and
/// the survivor silently stops after a restart. The fix mirrors the embedded
/// adapter: delete the peerstore entry only on FULL removal, else re-persist the
/// remaining collections.
#[tokio::test]
async fn rust_filtered_replication_partial_delete_keeps_remaining() {
    const OTHER_SCHEMA: &str = "type OtherDoc { body: String }";

    // Keyring => stable peer identity across restart; redb => persistent peerstore
    // so the survivor's re-persisted row must actually be reloaded after restart.
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_keyring()
        .with_store("redb")
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0
        .schema_add(AGENT_SCHEMA)
        .expect("AgentDoc schema node0");
    node1
        .schema_add(AGENT_SCHEMA)
        .expect("AgentDoc schema node1");
    node0
        .schema_add(OTHER_SCHEMA)
        .expect("OtherDoc schema node0");
    node1
        .schema_add(OTHER_SCHEMA)
        .expect("OtherDoc schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc", "OtherDoc"])
        .expect("subscribe 0 to both");

    // One replicator covering BOTH collections (no filter — the bug is about the
    // peerstore re-persist branch, not predicate matching).
    let out = run_replicator_add_plain(&cluster, 0, &["AgentDoc", "OtherDoc"], &addr1);
    assert!(
        out.status.success(),
        "two-collection replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    // Sanity: the peer is listed with both collections.
    let before = list_replicators_http(&cluster, 0).await;
    let before_entry = before
        .as_array()
        .and_then(|arr| arr.first())
        .expect("replicator listed before delete");
    let before_cols = before_entry["CollectionIDs"]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0);
    assert_eq!(
        before_cols, 2,
        "replicator must list both collections before partial delete, got: {before}"
    );

    // PARTIAL delete: remove only AgentDoc, leaving OtherDoc.
    let peer_id = addr1.rsplit("/p2p/").next().unwrap_or(&addr1);
    node0
        .p2p_replicator_delete(&["AgentDoc"], Some(&addr1))
        .or_else(|_| node0.p2p_replicator_delete(&["AgentDoc"], Some(peer_id)))
        .expect("partial p2p_replicator_delete");

    // CORE ASSERTION: the peer must STILL be present, now with exactly one
    // remaining collection. On the buggy code the peerstore entry is wiped, so
    // the list is empty and this fails.
    let after = list_replicators_http(&cluster, 0).await;
    let after_arr = after.as_array().cloned().unwrap_or_default();
    assert_eq!(
        after_arr.len(),
        1,
        "peer must remain after PARTIAL delete (not be wiped from peerstore), got: {after}"
    );
    let remaining_cols = after_arr[0]["CollectionIDs"]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0);
    assert_eq!(
        remaining_cols, 1,
        "exactly the survivor (OtherDoc) must remain listed, got: {after}"
    );

    // The survivor still replicates; the removed collection no longer does.
    let other = node0
        .query(r#"mutation { add_OtherDoc(input: {body: "survivor"}) { _docID } }"#)
        .expect("create OtherDoc");
    let other_id = extract_doc_id(&other, "add_OtherDoc");
    let removed = node0
        .query(&format!(
            r#"mutation {{ add_AgentDoc(input: {{agent_did: "{ALICE}", body: "removed"}}) {{ _docID }} }}"#
        ))
        .expect("create AgentDoc after partial delete");
    let removed_id = extract_doc_id(&removed, "add_AgentDoc");

    let node1_poll = cluster.client(1);
    let other_id_poll = other_id.clone();
    poll_until(
        || {
            let result = node1_poll
                .query("query { OtherDoc { _docID } }")
                .unwrap_or_default();
            result["OtherDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(other_id_poll.as_str()))
            })
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        "survivor (OtherDoc) did not replicate after partial delete",
    )
    .await;

    // The OtherDoc arrived (push pipeline proved live); the removed-collection
    // doc must stay absent over the grace window.
    tokio::time::sleep(ABSENCE_GRACE).await;
    let result = node1
        .query("query { AgentDoc { _docID } }")
        .expect("query node1 AgentDoc after partial delete");
    let agent_ids: Vec<&str> = result["AgentDoc"]
        .as_array()
        .map(|rows| rows.iter().filter_map(|r| r["_docID"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        !agent_ids.contains(&removed_id.as_str()),
        "a doc in the removed collection must NOT replicate after partial delete, found: {agent_ids:?}"
    );

    // #1077 regression guard. `getall` now reports the LIVE in-memory registry, so
    // a partial-delete that wiped the whole peerstore row would be MASKED here (the
    // survivor stays in the registry until the process exits). Restart node0 so the
    // registry is rebuilt from the peerstore: if the row had been clobbered, the
    // survivor now vanishes from `getall` and stops replicating.
    cluster.nodes[0].process.kill();
    cluster
        .restart_node(0, Duration::from_secs(60))
        .await
        .expect("restart node0 after partial delete");
    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P after restart");

    let after_restart = list_replicators_http(&cluster, 0).await;
    let after_restart_arr = after_restart.as_array().cloned().unwrap_or_default();
    assert_eq!(
        after_restart_arr.len(),
        1,
        "survivor must persist as a replicator across restart (peerstore not wiped), got: {after_restart}"
    );
    assert_eq!(
        after_restart_arr[0]["CollectionIDs"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0),
        1,
        "exactly the survivor collection must persist across restart, got: {after_restart}"
    );

    // Ephemeral transport state is lost on restart; reconnect so the reloaded
    // replicator has a live channel, then prove the survivor still replicates.
    let addr1_after = extract_p2p_addr(&cluster, 1);
    cluster
        .client(0)
        .p2p_connect(&[&addr1_after])
        .expect("reconnect 0->1 after restart");

    let node0_after = cluster.client(0);
    let survivor_after = node0_after
        .query(r#"mutation { add_OtherDoc(input: {body: "survivor-after-restart"}) { _docID } }"#)
        .expect("create OtherDoc after restart");
    let survivor_after_id = extract_doc_id(&survivor_after, "add_OtherDoc");

    let node1_after = cluster.client(1);
    let survivor_after_poll = survivor_after_id.clone();
    poll_until(
        || {
            let result = node1_after
                .query("query { OtherDoc { _docID } }")
                .unwrap_or_default();
            result["OtherDoc"].as_array().is_some_and(|rows| {
                rows.iter()
                    .any(|r| r["_docID"].as_str() == Some(survivor_after_poll.as_str()))
            })
        },
        P2P_TIMEOUT + Duration::from_secs(30),
        P2P_POLL_INTERVAL,
        "survivor (OtherDoc) did not replicate after source restart",
    )
    .await;
}

/// HTTP lifecycle/wiring smoke test for `getall` (`GET /api/v0/p2p/replicators`):
/// adding a replicator surfaces its peer id, collections, `Addresses`, and
/// `Status` over the real adapter+HTTP path, and fully deleting it removes the
/// peer from the listing.
///
/// Note on scope: `add`/`delete` mutate the live registry and the peerstore
/// together, so this does NOT distinguish live-authoritative reporting from a
/// peerstore read — those divergence semantics (live wins, persisted-only peers
/// dropped, metadata overlay) are locked by the unit test
/// `live_replicator_state_is_authoritative_over_persisted_collections`. This test
/// only locks that the `get_replicators` call sites are wired and the wire shape
/// is correct end to end.
#[tokio::test]
async fn rust_replicator_getall_reports_replicator_lifecycle() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    wait_for_log_ready(&cluster, 2).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0
        .schema_add(AGENT_SCHEMA)
        .expect("AgentDoc schema node0");
    node1
        .schema_add(AGENT_SCHEMA)
        .expect("AgentDoc schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    let peer_id = addr1.rsplit("/p2p/").next().unwrap_or(&addr1).to_string();
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node0
        .p2p_collection_add(&["AgentDoc"])
        .expect("subscribe 0 to AgentDoc");

    let out = run_replicator_add_plain(&cluster, 0, &["AgentDoc"], &addr1);
    assert!(
        out.status.success(),
        "replicator add failed: status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    let listed = list_replicators_http(&cluster, 0).await;
    let entry = listed
        .as_array()
        .and_then(|arr| arr.first())
        .cloned()
        .unwrap_or_else(|| panic!("expected exactly one replicator listed, got: {listed}"));

    // Identity + collection membership are reported.
    assert_eq!(
        entry["ID"].as_str(),
        Some(peer_id.as_str()),
        "getall must report the peer id, got: {entry}"
    );
    let cols: Vec<&str> = entry["CollectionIDs"]
        .as_array()
        .map(|a| a.iter().filter_map(|c| c.as_str()).collect())
        .unwrap_or_default();
    assert_eq!(
        cols.len(),
        1,
        "getall must report exactly the replicator's collection membership, got: {entry}"
    );

    // The address and a numeric Status (0=Active / 1=Inactive) are present in the
    // wire shape. (Both come from the peerstore record written by `add`, so this
    // checks the HTTP shape, not the live-vs-persisted overlay.)
    assert!(
        entry["Addresses"]
            .as_array()
            .is_some_and(|addrs| !addrs.is_empty()),
        "getall must surface the persisted replicator address, got: {entry}"
    );
    assert!(
        entry["Status"].is_u64(),
        "getall must surface the persisted status field, got: {entry}"
    );

    // Lifecycle: fully removing the replicator's only collection deletes the peer
    // from both the live registry and the peerstore, so getall must no longer list
    // it. (This exercises the delete->report path; it does not isolate live from
    // persisted, since the delete clears both.)
    node0
        .p2p_replicator_delete(&["AgentDoc"], Some(&addr1))
        .or_else(|_| node0.p2p_replicator_delete(&["AgentDoc"], Some(&peer_id)))
        .expect("full p2p_replicator_delete");

    let after_delete = list_replicators_http(&cluster, 0).await;
    let still_listed = after_delete
        .as_array()
        .map(|arr| {
            arr.iter()
                .any(|r| r["ID"].as_str() == Some(peer_id.as_str()))
        })
        .unwrap_or(false);
    assert!(
        !still_listed,
        "getall must reflect the deleted replicator's removal, got: {after_delete}"
    );
}
