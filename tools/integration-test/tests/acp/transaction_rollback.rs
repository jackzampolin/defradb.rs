use std::time::Duration;

use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};
use serde_json::Value;

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

async fn tx_create(client: &reqwest::Client, api_url: &str) -> String {
    let response = client
        .post(format!("{api_url}/api/v0/tx"))
        .send()
        .await
        .expect("create transaction request");
    let status = response.status();
    let body = response.text().await.expect("transaction create body");
    assert!(
        status.is_success(),
        "tx create failed: status={} body={}",
        status,
        body
    );
    let json: Value = serde_json::from_str(&body).expect("tx create must return JSON");
    json["id"]
        .as_u64()
        .expect("transaction create response must include numeric id")
        .to_string()
}

async fn tx_discard(client: &reqwest::Client, api_url: &str, tx_id: &str) {
    let response = client
        .delete(format!("{api_url}/api/v0/tx/{tx_id}"))
        .send()
        .await
        .expect("discard transaction request");
    let status = response.status();
    let body = response.text().await.expect("transaction discard body");
    assert!(
        status.is_success(),
        "tx discard failed: status={} body={}",
        status,
        body
    );
}

async fn graphql_query(
    client: &reqwest::Client,
    api_url: &str,
    identity_hex: &str,
    query: &str,
    tx_id: Option<&str>,
) -> Value {
    let mut body = serde_json::json!({ "query": query });
    if let Some(tx_id) = tx_id {
        body["txn_id"] = serde_json::json!(tx_id);
    }
    let response = client
        .post(format!("{api_url}/api/v0/graphql"))
        .bearer_auth(auth_token(identity_hex, api_url))
        .json(&body)
        .send()
        .await
        .expect("graphql request");
    let status = response.status();
    let response_body = response.text().await.expect("graphql response body");
    assert!(
        status.is_success(),
        "graphql request failed: status={} body={}",
        status,
        response_body
    );
    let json: Value = serde_json::from_str(&response_body).expect("graphql response JSON");
    let errors = json["errors"].as_array().cloned().unwrap_or_default();
    assert!(
        errors.is_empty(),
        "graphql returned errors: {}",
        response_body
    );
    json["data"].clone()
}

async fn acp_transaction_rollback_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();
    let api_url = cluster.api_url(0).to_string();
    let http = reqwest::Client::new();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing policy id");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    let created = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "RollbackDoc", age: 7}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create document");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("created document should have _docID")
        .to_string();

    node.acp_relationship_add("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant reader relationship");

    let read_query = "query { User { _docID name age } }";
    let bob_before = node
        .query_with_identity(read_query, &bob.private_key_hex)
        .expect("Bob query before transaction");
    let bob_before_docs = bob_before["User"].as_array().expect("Bob User array");
    assert_eq!(bob_before_docs.len(), 1, "Bob should see the shared document");
    assert_eq!(bob_before_docs[0]["_docID"], doc_id);

    let tx_id = tx_create(&http, &api_url).await;
    let delete_query = format!(r#"mutation {{ delete_User(docID: "{}") {{ _docID }} }}"#, doc_id);
    let delete_result = graphql_query(
        &http,
        &api_url,
        &alice.private_key_hex,
        &delete_query,
        Some(&tx_id),
    )
    .await;
    let deleted_docs = delete_result["delete_User"]
        .as_array()
        .expect("delete_User should be an array");
    assert_eq!(deleted_docs.len(), 1, "delete in txn should affect one document");
    assert_eq!(deleted_docs[0]["_docID"], doc_id);

    let inside_tx = graphql_query(
        &http,
        &api_url,
        &alice.private_key_hex,
        read_query,
        Some(&tx_id),
    )
    .await;
    let inside_tx_docs = inside_tx["User"].as_array().expect("inside-tx User array");
    assert_eq!(
        inside_tx_docs.len(),
        0,
        "document should be absent inside the uncommitted delete transaction"
    );

    tx_discard(&http, &api_url, &tx_id).await;

    let alice_after = node
        .query_with_identity(read_query, &alice.private_key_hex)
        .expect("Alice query after rollback");
    let alice_after_docs = alice_after["User"].as_array().expect("Alice User array");
    assert_eq!(alice_after_docs.len(), 1, "Alice should still see the document");
    assert_eq!(alice_after_docs[0]["_docID"], doc_id);

    let bob_after = node
        .query_with_identity(read_query, &bob.private_key_hex)
        .expect("Bob query after rollback");
    let bob_after_docs = bob_after["User"].as_array().expect("Bob User array");
    assert_eq!(bob_after_docs.len(), 1, "Bob should still see the document");
    assert_eq!(bob_after_docs[0]["_docID"], doc_id);
}

#[tokio::test]
async fn rust_acp_transaction_rollback() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .expect("build cluster");
    acp_transaction_rollback_test(cluster).await;
}
