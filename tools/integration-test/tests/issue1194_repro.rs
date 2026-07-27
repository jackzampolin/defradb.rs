//! Issue #1194 regression: concurrent `update_X` mutations against DISTINCT
//! documents through the HTTP GraphQL API must not fail with
//! `transaction conflict. Please retry`.
//!
//! Root cause: field-delta blocks are content-addressed (`b` + CID) and carry
//! no document identity. Two documents whose updates produce byte-identical
//! deltas (e.g. both rewrite `status: "streaming"` at the same chain depth)
//! write the SAME blockstore key, and the conflict tracker treated that blind
//! write-write overlap as a transaction conflict. Identical-content block
//! writes are idempotent and must not conflict; true same-document conflicts
//! must survive (see the control below).

use std::sync::Arc;

use integration_test::TestCluster;
use serde_json::{json, Value};

struct RoundStats {
    ok: usize,
    conflict: usize,
    other: Vec<String>,
}

async fn gql(http: &reqwest::Client, url: &str, query: &str) -> Result<Value, String> {
    let resp = http
        .post(format!("{url}/api/v0/graphql"))
        .json(&json!({ "query": query }))
        .send()
        .await
        .map_err(|e| format!("http error: {e}"))?;
    let body: Value = resp.json().await.map_err(|e| format!("bad json: {e}"))?;
    if let Some(errors) = body.get("errors").and_then(|e| e.as_array()) {
        if !errors.is_empty() {
            return Err(errors
                .iter()
                .map(|e| e["message"].as_str().unwrap_or("?").to_string())
                .collect::<Vec<_>>()
                .join("; "));
        }
    }
    Ok(body["data"].clone())
}

async fn run_workload(
    api_url: &str,
    collection: &str,
    input_extra: &str,
    doc_ids: &[String],
    concurrency: usize,
    rounds: usize,
) -> RoundStats {
    let http = reqwest::Client::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));
    let mut handles = Vec::new();
    for (worker, doc_id) in doc_ids.iter().take(concurrency).enumerate() {
        let http = http.clone();
        let url = api_url.to_string();
        let doc_id = doc_id.clone();
        let collection = collection.to_string();
        let input_extra = input_extra.to_string();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let mut stats = RoundStats {
                ok: 0,
                conflict: 0,
                other: Vec::new(),
            };
            for round in 0..rounds {
                barrier.wait().await;
                let mutation = format!(
                    r#"mutation {{ update_{collection}(filter: {{_docID: {{_eq: "{doc_id}"}}, status: {{_eq: "streaming"}}}}, input: {{content: "w{worker}-r{round}"{input_extra}}}) {{ _docID }} }}"#
                );
                match gql(&http, &url, &mutation).await {
                    Ok(data) => {
                        let n = data[format!("update_{collection}")]
                            .as_array()
                            .map(|a| a.len())
                            .unwrap_or(0);
                        if n == 1 {
                            stats.ok += 1;
                        } else {
                            stats.other.push(format!("updated {n} docs"));
                        }
                    }
                    Err(msg) if msg.contains("transaction conflict") => {
                        stats.conflict += 1;
                    }
                    Err(msg) => stats.other.push(msg),
                }
            }
            stats
        }));
    }
    let mut total = RoundStats {
        ok: 0,
        conflict: 0,
        other: Vec::new(),
    };
    for h in handles {
        let s = h.await.expect("worker panicked");
        total.ok += s.ok;
        total.conflict += s.conflict;
        total.other.extend(s.other);
    }
    total
}

/// Eight docs, barrier-synchronized concurrent updates at concurrency 2/4/8.
/// Every attempt must succeed on the first try: distinct documents may not
/// leak transaction conflicts through the HTTP API.
async fn run_scenario(schema: &str, collection: &str, input_extra: &str) {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_store("redb")
        .build()
        .await
        .expect("build cluster");
    let node = cluster.client(0);
    let api_url = cluster.api_url(0).to_string();

    node.schema_add(schema).expect("schema add");

    let http = reqwest::Client::new();
    let mut doc_ids = Vec::new();
    for i in 0..8 {
        let data = gql(
            &http,
            &api_url,
            &format!(
                r#"mutation {{ add_{collection}(input: {{status: "streaming", content: "init-{i}"}}) {{ _docID }} }}"#
            ),
        )
        .await
        .expect("create doc");
        doc_ids.push(
            data[format!("add_{collection}")][0]["_docID"]
                .as_str()
                .expect("docID")
                .to_string(),
        );
    }

    let rounds = 10;
    for concurrency in [2usize, 4, 8] {
        let stats = run_workload(
            &api_url,
            collection,
            input_extra,
            &doc_ids,
            concurrency,
            rounds,
        )
        .await;
        let attempts = concurrency * rounds;
        println!(
            "[{collection}] concurrency={concurrency}: attempts={attempts} ok={} conflict={} other={:?}",
            stats.ok, stats.conflict, stats.other
        );
        assert_eq!(
            stats.conflict, 0,
            "[{collection}] concurrency={concurrency}: transaction conflicts leaked for \
             updates to distinct documents"
        );
        assert!(
            stats.other.is_empty(),
            "[{collection}] concurrency={concurrency}: unexpected errors: {:?}",
            stats.other
        );
        assert_eq!(stats.ok, attempts);
    }

    // No writes lost, duplicated, or applied to the wrong document: each doc
    // holds its own worker's final-round content.
    for (worker, doc_id) in doc_ids.iter().enumerate() {
        let data = gql(
            &http,
            &api_url,
            &format!(r#"query {{ {collection}(docID: "{doc_id}") {{ content status }} }}"#),
        )
        .await
        .expect("final read");
        let last = rounds - 1;
        assert_eq!(
            data[collection][0]["content"],
            json!(format!("w{worker}-r{last}")),
            "doc {worker} final content"
        );
        assert_eq!(data[collection][0]["status"], json!("streaming"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn issue1194_unindexed_status() {
    run_scenario(
        "type AgentResponse { status: String  content: String }",
        "AgentResponse",
        "",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn issue1194_indexed_status() {
    run_scenario(
        "type AgentResponseIdx { status: String @index  content: String }",
        "AgentResponseIdx",
        "",
    )
    .await;
}

/// The reported shape: the mutation input rewrites `status: "streaming"`
/// unchanged on every flush, producing byte-identical field-delta blocks
/// across documents. This was the failing variant.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn issue1194_indexed_status_rewrite() {
    run_scenario(
        "type AgentResponseIdxRw { status: String @index  content: String }",
        "AgentResponseIdxRw",
        r#", status: "streaming""#,
    )
    .await;
}

/// Same as above without the index: proves the collision is on the shared
/// content-addressed delta block, not on secondary-index keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn issue1194_unindexed_status_rewrite() {
    run_scenario(
        "type AgentResponseRw { status: String  content: String }",
        "AgentResponseRw",
        r#", status: "streaming""#,
    )
    .await;
}

/// Deterministic core of #1194: two interactive transactions with overlapping
/// snapshots update two DIFFERENT documents with a byte-identical field delta
/// (same `status` value at the same CRDT chain depth => same block CID). Both
/// commits must succeed. Before the fix the second commit always failed with
/// a write-write conflict on the shared block key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn issue1194_identical_delta_txn_pair_distinct_docs() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_store("redb")
        .build()
        .await
        .expect("build cluster");
    let node = cluster.client(0);

    node.schema_add("type Snap { status: String  content: String }")
        .expect("schema add");

    let mut ids = Vec::new();
    for i in 0..2 {
        let data = node
            .query(&format!(
                r#"mutation {{ add_Snap(input: {{status: "streaming", content: "init-{i}"}}) {{ _docID }} }}"#
            ))
            .expect("create doc");
        ids.push(data["add_Snap"][0]["_docID"].as_str().unwrap().to_string());
    }

    let tx1 = node.tx_create().expect("tx1 create");
    let tx2 = node.tx_create().expect("tx2 create");

    node.query_with_tx(
        &format!(
            r#"mutation {{ update_Snap(docID: "{}", input: {{status: "streaming"}}) {{ _docID }} }}"#,
            ids[0]
        ),
        &tx1,
    )
    .expect("tx1 update doc0");
    node.query_with_tx(
        &format!(
            r#"mutation {{ update_Snap(docID: "{}", input: {{status: "streaming"}}) {{ _docID }} }}"#,
            ids[1]
        ),
        &tx2,
    )
    .expect("tx2 update doc1");

    node.tx_commit(&tx1).expect("tx1 commit");
    node.tx_commit(&tx2).expect(
        "tx2 commit: identical-content block writes to distinct documents must not conflict",
    );
}

/// Control: two interactive transactions updating the SAME document remain a
/// genuine conflict — the second commit must abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn issue1194_same_doc_txn_pair_still_conflicts() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_store("redb")
        .build()
        .await
        .expect("build cluster");
    let node = cluster.client(0);

    node.schema_add("type Snap { status: String  content: String }")
        .expect("schema add");

    let data = node
        .query(r#"mutation { add_Snap(input: {status: "streaming", content: "init"}) { _docID } }"#)
        .expect("create doc");
    let id = data["add_Snap"][0]["_docID"].as_str().unwrap().to_string();

    let tx1 = node.tx_create().expect("tx1 create");
    let tx2 = node.tx_create().expect("tx2 create");

    node.query_with_tx(
        &format!(
            r#"mutation {{ update_Snap(docID: "{id}", input: {{content: "a"}}) {{ _docID }} }}"#
        ),
        &tx1,
    )
    .expect("tx1 update");
    node.query_with_tx(
        &format!(
            r#"mutation {{ update_Snap(docID: "{id}", input: {{content: "b"}}) {{ _docID }} }}"#
        ),
        &tx2,
    )
    .expect("tx2 update");

    node.tx_commit(&tx1).expect("tx1 commit");
    let err = node
        .tx_commit(&tx2)
        .expect_err("tx2 commit must conflict: same document, overlapping snapshots");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("transaction conflict"),
        "expected a transaction conflict, got: {msg}"
    );
}
