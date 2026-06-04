use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use integration_test::TestCluster;
use serde_json::{json, Value};

async fn graphql_query(
    client: &reqwest::Client,
    api_url: &str,
    query: &str,
) -> Result<Value, String> {
    let response = client
        .post(format!("{api_url}/api/v0/graphql"))
        .json(&json!({ "query": query }))
        .send()
        .await
        .map_err(|error| format!("graphql request failed: {error}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("graphql response body failed: {error}"))?;
    if !status.is_success() {
        return Err(format!("graphql status={status} body={body}"));
    }

    let json: Value =
        serde_json::from_str(&body).map_err(|error| format!("graphql JSON failed: {error}"))?;
    let errors = json["errors"].as_array().cloned().unwrap_or_default();
    if !errors.is_empty() {
        return Err(format!("graphql returned errors: {body}"));
    }

    Ok(json["data"].clone())
}

fn crash_reopen_ack_target() -> usize {
    std::env::var("LARK_CRASH_REOPEN_ACK_TARGET")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(50)
}

async fn wait_for_acked_writes(acked: &Arc<Mutex<Vec<i64>>>, target: usize) {
    let timeout_secs = 10_u64.max((target as u64).saturating_div(10));
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if acked.lock().unwrap().len() >= target {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {target} acknowledged writes"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn rust_lark_survives_sigkill_and_reopen_after_acknowledged_writes() {
    let _root = integration_test::workspace_root();
    let mut cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_store("lark")
        .health_timeout(Duration::from_secs(60))
        .build()
        .await
        .expect("build lark cluster");

    let node = cluster.client(0);
    node.schema_add(
        "type CrashDoc {
            seq: Int @index(unique: true)
            label: String @index
            body: String
        }",
    )
    .expect("add CrashDoc schema");

    let api_url = cluster.api_url(0).to_string();
    let acked = Arc::new(Mutex::new(Vec::new()));
    let writer_acked = Arc::clone(&acked);
    let target_ack_count = crash_reopen_ack_target();
    let max_attempts = (target_ack_count.max(50) * 4) as i64;

    let writer = tokio::spawn(async move {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("http client");

        for seq in 0..max_attempts {
            let body = format!("payload-{seq}-{}", "x".repeat(256));
            let gql = format!(
                r#"mutation {{
                    add_CrashDoc(input: {{seq: {seq}, label: "doc-{seq}", body: "{body}"}}) {{
                        seq
                    }}
                }}"#
            );

            match graphql_query(&http, &api_url, &gql).await {
                Ok(_) => writer_acked.lock().unwrap().push(seq),
                Err(_) => break,
            }

            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    wait_for_acked_writes(&acked, target_ack_count).await;
    cluster.nodes[0].process.kill();
    writer.await.expect("writer task should not panic");

    let acknowledged = acked.lock().unwrap().clone();
    assert!(
        !acknowledged.is_empty(),
        "test must observe at least one acknowledged write"
    );

    cluster
        .restart_node(0, Duration::from_secs(60))
        .await
        .expect("restart lark node after kill");

    let restarted = cluster.client(0);
    let data = restarted
        .query("query { CrashDoc { seq label } }")
        .expect("query CrashDoc after restart");
    let docs = data["CrashDoc"]
        .as_array()
        .expect("CrashDoc result should be an array");
    let found: HashSet<i64> = docs.iter().filter_map(|doc| doc["seq"].as_i64()).collect();

    for seq in &acknowledged {
        assert!(
            found.contains(seq),
            "acknowledged write seq={seq} was missing after kill/restart"
        );
    }
}
