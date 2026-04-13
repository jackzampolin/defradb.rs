use std::sync::{Arc, Mutex};
use std::time::Duration;

use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};
use serde_json::Value;
use tokio::task::JoinHandle;

fn auth_token(identity_hex: &str, audience: &str) -> String {
    let key_bytes = hex::decode(identity_hex).expect("identity hex must decode");
    let key_type = match key_bytes.len() {
        32 => crypto::KeyType::Secp256k1,
        64 => crypto::KeyType::Ed25519,
        len => panic!("unsupported identity length: {len}"),
    };
    let identity =
        identity::RawIdentity::from_bytes(key_type, &key_bytes).expect("raw identity from bytes");
    let audience_host = audience
        .strip_prefix("https://")
        .or_else(|| audience.strip_prefix("http://"))
        .unwrap_or(audience);
    let token = identity::new_token(
        &identity,
        Duration::from_secs(15 * 60),
        Some(audience_host.to_string()),
        None,
    )
    .expect("generate auth token");
    String::from_utf8(token).expect("token must be utf-8")
}

async fn open_events_sse_with_auth(
    api_url: &str,
    event_filter: &str,
    identity_hex: &str,
) -> (JoinHandle<()>, Arc<Mutex<Vec<Value>>>) {
    let url = format!("{api_url}/api/v0/events?event={event_filter}");
    let events: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);
    let token = auth_token(identity_hex, api_url);
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let resp = match client.get(&url).bearer_auth(token).send().await {
            Ok(resp) => {
                assert!(
                    resp.status().is_success(),
                    "authenticated SSE request failed: status={}",
                    resp.status()
                );
                let _ = connected_tx.send(());
                resp
            }
            Err(err) => {
                let _ = connected_tx.send(());
                panic!("SSE request failed: {err}");
            }
        };

        let mut buf = String::new();
        let mut stream = resp.bytes_stream();
        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("SSE chunk error");
            buf.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buf.find("\n\n") {
                let block = buf[..pos].to_string();
                buf = buf[pos + 2..].to_string();

                let mut event_type = String::new();
                let mut data = String::new();
                for line in block.lines() {
                    if let Some(rest) = line.strip_prefix("event:") {
                        event_type = rest.trim().to_string();
                    } else if let Some(rest) = line.strip_prefix("data:") {
                        data = rest.trim().to_string();
                    }
                }

                if event_type == "next" {
                    if let Ok(value) = serde_json::from_str::<Value>(&data) {
                        events_clone.lock().unwrap().push(value);
                    }
                }
            }
        }
    });

    let _ = connected_rx.await;

    (handle, events)
}

fn branchable_users_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type User @branchable @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    )
}

async fn acp_events_sse_filters_unauthorized_subscribers_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary_path = node.binary_path().to_path_buf();
    let api_url = cluster.api_url(0).to_string();

    let alice = generate_identity(&binary_path).expect("generate Alice identity");
    let bob = generate_identity(&binary_path).expect("generate Bob identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema with ACP policy");

    let created = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create protected doc");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    let (bob_handle, bob_events) =
        open_events_sse_with_auth(&api_url, "update", &bob.private_key_hex).await;
    let (alice_handle, alice_events) =
        open_events_sse_with_auth(&api_url, "update", &alice.private_key_hex).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    node.query_with_identity(
        r#"mutation { update_User(filter: {name: {_eq: "Secret"}}, input: {age: 43}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("update protected doc");

    tokio::time::sleep(Duration::from_secs(2)).await;

    bob_handle.abort();
    alice_handle.abort();

    let bob_events = bob_events.lock().unwrap().clone();
    assert!(
        bob_events.is_empty(),
        "unauthorized subscriber should receive no update events, got: {:?}",
        bob_events
    );

    let alice_events = alice_events.lock().unwrap().clone();
    assert!(
        !alice_events.is_empty(),
        "authorized subscriber should receive at least one update event"
    );
    assert!(
        alice_events.iter().any(|event| {
            event.pointer("/data/doc_id").and_then(Value::as_str) == Some(doc_id.as_str())
        }),
        "authorized subscriber should receive the protected doc event, got: {:?}",
        alice_events
    );
}

#[tokio::test]
async fn rust_acp_events_sse_filters_unauthorized_subscribers() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_events_sse_filters_unauthorized_subscribers_test(cluster).await;
}

#[tokio::test]
async fn rust_acp_events_sse_preserves_authorized_branchable_collection_updates() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();

    let node = cluster.client(0);
    let binary_path = node.binary_path().to_path_buf();
    let api_url = cluster.api_url(0).to_string();

    let alice = generate_identity(&binary_path).expect("generate Alice identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");

    let schema = branchable_users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add branchable schema with ACP policy");

    let created = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create protected branchable doc");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    let (alice_handle, alice_events) =
        open_events_sse_with_auth(&api_url, "update", &alice.private_key_hex).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    node.query_with_identity(
        r#"mutation { update_User(filter: {name: {_eq: "Secret"}}, input: {age: 43}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("update protected branchable doc");

    tokio::time::sleep(Duration::from_secs(2)).await;

    alice_handle.abort();

    let alice_events = alice_events.lock().unwrap().clone();
    assert!(
        alice_events.iter().any(|event| {
            event.pointer("/data/doc_id").and_then(Value::as_str) == Some(doc_id.as_str())
        }),
        "authorized subscriber should still receive the document-scoped update, got: {:?}",
        alice_events
    );
    assert!(
        alice_events
            .iter()
            .any(|event| event.pointer("/data/doc_id").and_then(Value::as_str) == Some("")),
        "authorized subscriber should still receive the branchable collection-level update, got: {:?}",
        alice_events
    );
}
