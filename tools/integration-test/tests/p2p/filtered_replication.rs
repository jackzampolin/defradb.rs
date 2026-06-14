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

/// The HTTP wire format cannot express the internal `Acp` variant of
/// `p2p::ReplicationFilter` — `ReplicationFilter` carries only `Field`, `Value`,
/// and `Conditions`. Sending an unknown key produces a filter where `Field` is
/// empty and `Conditions` is None, which the adapter rejects with 4xx.
#[tokio::test]
async fn rust_filtered_replicator_rejects_acp_variant() {
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
