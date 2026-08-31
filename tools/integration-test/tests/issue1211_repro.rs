//! Issue #1211 regression: concurrent updates to distinct documents in a
//! branchable collection must not conflict through the collection head set.

use std::sync::Arc;

use integration_test::TestCluster;
use serde_json::{json, Value};

async fn gql(http: &reqwest::Client, url: &str, query: &str) -> Result<Value, String> {
    let response = http
        .post(format!("{url}/api/v0/graphql"))
        .json(&json!({ "query": query }))
        .send()
        .await
        .map_err(|error| format!("http error: {error}"))?;
    let body: Value = response
        .json()
        .await
        .map_err(|error| format!("bad json: {error}"))?;
    if let Some(errors) = body.get("errors").and_then(Value::as_array) {
        if !errors.is_empty() {
            return Err(errors
                .iter()
                .map(|error| error["message"].as_str().unwrap_or("?").to_string())
                .collect::<Vec<_>>()
                .join("; "));
        }
    }
    Ok(body["data"].clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn concurrent_distinct_branchable_doc_updates_do_not_conflict() {
    const WRITERS: usize = 16;
    const ROUNDS: usize = 20;

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_store("regolith")
        .build()
        .await
        .expect("build cluster");
    let node = cluster.client(0);
    let api_url = cluster.api_url(0).to_string();

    node.schema_add(
        r#"
        type AgentResponse @branchable {
            response_key: String @index(unique: true)
            status: String @index
            content: String
            reasoning: String
            token_count: Int
            reasoning_progress_seq: Int
        }
        "#,
    )
    .expect("schema add");

    let http = reqwest::Client::new();
    let mut doc_ids = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let data = gql(
            &http,
            &api_url,
            &format!(
                r#"mutation {{
                    add_AgentResponse(input: {{
                        response_key: "response-{writer}",
                        status: "streaming",
                        content: "initial",
                        reasoning: "initial",
                        token_count: 0,
                        reasoning_progress_seq: 0
                    }}) {{ _docID }}
                }}"#
            ),
        )
        .await
        .expect("create document");
        doc_ids.push(
            data["add_AgentResponse"][0]["_docID"]
                .as_str()
                .expect("document ID")
                .to_string(),
        );
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));
    let mut tasks = Vec::with_capacity(WRITERS);
    for (writer, doc_id) in doc_ids.iter().cloned().enumerate() {
        let http = http.clone();
        let api_url = api_url.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            for round in 0..ROUNDS {
                barrier.wait().await;
                gql(
                    &http,
                    &api_url,
                    &format!(
                        r#"mutation {{
                            update_AgentResponse(
                                filter: {{
                                    _docID: {{_eq: "{doc_id}"}},
                                    status: {{_eq: "streaming"}}
                                }},
                                input: {{
                                    content: "writer-{writer}-round-{round}",
                                    reasoning: "reasoning-{writer}-{round}",
                                    token_count: {round},
                                    reasoning_progress_seq: {round}
                                }}
                            ) {{ _docID }}
                        }}"#
                    ),
                )
                .await
                .map_err(|error| format!("round {round}: {error}"))?;
            }
            Ok::<_, String>(())
        }));
    }

    for (writer, task) in tasks.into_iter().enumerate() {
        task.await
            .expect("writer task panicked")
            .unwrap_or_else(|error| panic!("writer {writer} failed: {error}"));
    }

    for (writer, doc_id) in doc_ids.iter().enumerate() {
        let data = gql(
            &http,
            &api_url,
            &format!(
                r#"query {{
                    AgentResponse(docID: "{doc_id}") {{
                        content
                        reasoning
                        token_count
                        reasoning_progress_seq
                    }}
                }}"#
            ),
        )
        .await
        .expect("read final document");
        let expected_round = ROUNDS - 1;
        let document = &data["AgentResponse"][0];
        assert_eq!(
            document["content"],
            json!(format!("writer-{writer}-round-{expected_round}"))
        );
        assert_eq!(
            document["reasoning"],
            json!(format!("reasoning-{writer}-{expected_round}"))
        );
        assert_eq!(document["token_count"], json!(expected_round));
        assert_eq!(document["reasoning_progress_seq"], json!(expected_round));
    }
}
