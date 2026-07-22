use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::Once;
use std::time::{Duration, Instant};

use query::QueryResponse;
use serde_json::Value as JsonValue;

use super::{EmbeddedNode, P2PConfig};

fn init_tracing() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into())
            .add_directive(
                "iroh_quinn_proto::connection=error"
                    .parse()
                    .expect("valid tracing directive"),
            )
            .add_directive(
                "noq_proto::connection=error"
                    .parse()
                    .expect("valid tracing directive"),
            );
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
    });
}

fn test_p2p_config() -> P2PConfig {
    P2PConfig {
        port: 0,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
        discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
        max_concurrent_multipath_paths: None,
        secret_key_path: None,
        load_persisted_collections: false,
        max_concurrent_dag_fetches: p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
        max_concurrent_push_tasks: p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
        max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
        rate_limit_burst: p2p::sync::DEFAULT_RATE_LIMIT_BURST,
        rate_limit_rate: p2p::sync::DEFAULT_RATE_LIMIT_RATE,
        max_pending_dags: p2p::sync::DEFAULT_MAX_PENDING_DAGS,
    }
}

fn desktop_like_streaming_p2p_config() -> P2PConfig {
    let mut config = test_p2p_config();
    config.max_concurrent_push_tasks = 32;
    config.rate_limit_burst = 5_000;
    config.rate_limit_rate = 500.0;
    config
}

fn persistent_p2p_config(secret_key_path: PathBuf) -> P2PConfig {
    let mut config = test_p2p_config();
    config.secret_key_path = Some(secret_key_path);
    config.load_persisted_collections = true;
    config
}

fn unique_data_path(test_name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "defra-node-{test_name}-{}-{nanos}",
        std::process::id()
    ))
}

async fn build_persistent_p2p_node(data_path: PathBuf, secret_key_path: PathBuf) -> EmbeddedNode {
    EmbeddedNode::builder()
        .data_path(data_path)
        .with_p2p(persistent_p2p_config(secret_key_path))
        .build()
        .await
        .expect("build persistent P2P node")
}

async fn wait_for_listen_addr(node: &EmbeddedNode) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = node
            .p2p()
            .expect("P2P should be enabled")
            .listen_addresses()
            .await
            .expect("listen_addresses should succeed");
        if let Some(addr) = addrs.first() {
            return addr.clone();
        }
        assert!(
            Instant::now() < deadline,
            "node never exposed a P2P listen address"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_connected_peer(node: &EmbeddedNode) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let peers = node
            .p2p()
            .expect("P2P should be enabled")
            .connected_peers()
            .await
            .expect("connected_peers should succeed");
        if !peers.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node never reported a connected peer"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn collection_len(data: &JsonValue, collection: &str) -> usize {
    data.get(collection)
        .and_then(|v| v.as_array())
        .map(|docs| docs.len())
        .unwrap_or(0)
}

async fn wait_for_collection_len(node: &EmbeddedNode, collection: &str, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = node
            .execute(&format!("query {{ {collection} {{ _docID name age }} }}"))
            .await;
        assert!(
            response.errors.is_empty(),
            "query returned errors: {:?}",
            response.errors
        );

        let len = response
            .data
            .as_ref()
            .map(|data| collection_len(data, collection))
            .unwrap_or(0);
        if len >= expected {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "collection {collection} never reached {expected} docs; last response: {:?}",
            response.data
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_user_age(node: &EmbeddedNode, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = node.execute("query { User { _docID age } }").await;
        assert!(
            response.errors.is_empty(),
            "query returned errors: {:?}",
            response.errors
        );
        let replicated = response
            .data
            .as_ref()
            .and_then(|data| data.get("User"))
            .and_then(|value| value.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("age"))
            .and_then(|value| value.as_i64())
            == Some(expected);
        if replicated {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "User age never reached {expected}; last response: {:?}",
            response.data
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

const SESSION_TURN_SDL: &str = r#"
    type Conversation @branchable {
        sessionID: String @index(unique: true)
        latestRequestID: String @index
        status: String @index
        updatedAt: String
    }

    type Request @branchable {
        requestID: String @index(unique: true)
        sessionID: String @index
        content: String
        status: String @index
        lifecycleState: String @index
        createdAt: String
    }

    type Response @branchable {
        responseKey: String @index(unique: true)
        requestID: String @index
        sessionID: String @index
        content: String
        reasoning: String
        status: String @index
        progressSeq: Int
        materializedMessageSequence: Int
        completedAt: String
    }

    type Message @branchable {
        messageKey: String @index(unique: true)
        sessionID: String @index
        sequence: Int @index
        role: String
        content: String
    }

    type ToolCall @branchable {
        toolCallKey: String @index(unique: true)
        sessionID: String @index
        requestID: String @index
        messageSequence: Int
        toolName: String @index
        status: String
        result: String
        startedAt: String
        completedAt: String
    }

    type ToolResult @branchable {
        toolResultKey: String @index(unique: true)
        sessionID: String @index
        requestID: String @index
        toolName: String @index
        outputText: String
        createdAt: String
    }
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TurnSnapshot {
    request_status: Option<String>,
    request_lifecycle_state: Option<String>,
    response_status: Option<String>,
    response_progress_seq: Option<i64>,
    materialized_message_sequence: Option<i64>,
    response_content_len: usize,
    response_reasoning_len: usize,
    message_count: usize,
    tool_call_count: usize,
    completed_tool_call_count: usize,
    tool_result_count: usize,
    latest_request_id: Option<String>,
    conversation_status: Option<String>,
}

fn json_string_literal(value: &str) -> String {
    serde_json::to_string(value).expect("serialize GraphQL string literal")
}

fn extract_created_doc_id(response: &QueryResponse, field_name: &str) -> String {
    response
        .data
        .as_ref()
        .and_then(|data| data.get(field_name))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|value| value.as_str())
        .expect("mutation response missing _docID")
        .to_string()
}

fn build_turn_chunk(chunk_index: usize) -> String {
    format!(
        "chunk-{chunk_index:03}: {}\n",
        "The desktop mirrors the remote agent over replicated branchable documents and the follow-up turn keeps rewriting the same response doc while tool state accumulates. "
            .repeat(2 + (chunk_index % 3))
    )
}

fn build_tool_result_payload(tool_index: usize) -> String {
    format!(
        "tool-output-{tool_index:03}: {}\n{}\n{}",
        "The document model stores tool outputs in replicated rows so the subscriber can reconstruct state without an HTTP fallback."
            .repeat(4),
        "This payload is intentionally chunky to stress pushlog delivery while the response document is still receiving cumulative content rewrites."
            .repeat(4),
        "The follow-up turn intentionally creates more tool traffic than the first turn so the receiver has to absorb branchable rewrites and sideband documents together."
            .repeat(4),
    )
}

fn synthetic_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch");
    format!(
        "{}.{:09}Z",
        since_epoch.as_secs(),
        since_epoch.subsec_nanos()
    )
}

async fn install_one_way_replicator(
    sender: &EmbeddedNode,
    receiver: &EmbeddedNode,
    collections: &[&str],
) {
    let sender_addr = wait_for_listen_addr(sender).await;
    let receiver_addr = wait_for_listen_addr(receiver).await;
    let sender_p2p = sender.p2p().expect("sender p2p");
    let receiver_p2p = receiver.p2p().expect("receiver p2p");

    sender_p2p
        .connect_peer(&receiver_addr)
        .await
        .expect("connect sender -> receiver");
    wait_for_connected_peer(sender).await;
    wait_for_connected_peer(receiver).await;

    let collection_names = collections
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    sender_p2p
        .add_collections(collection_names.clone())
        .await
        .expect("add collections to sender p2p");
    receiver_p2p
        .add_collections(collection_names.clone())
        .await
        .expect("add collections to receiver p2p");
    receiver_p2p
        .add_replicator(
            collection_names.clone(),
            Some(&sender_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("authorize sender as receiver-side replicator");
    sender_p2p
        .add_replicator(
            collection_names,
            Some(&receiver_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("set sender -> receiver replicator");
}

const AGENT_SCHEMA: &str = "type AgentDoc { agent_did: String @immutable  body: String }";

async fn install_filtered_one_way_replicator(
    sender: &EmbeddedNode,
    receiver: &EmbeddedNode,
    collections: &[&str],
    filters: defra_http::router::ReplicationFilters,
) {
    let sender_addr = wait_for_listen_addr(sender).await;
    let receiver_addr = wait_for_listen_addr(receiver).await;
    let sender_p2p = sender.p2p().expect("sender p2p");
    let receiver_p2p = receiver.p2p().expect("receiver p2p");

    sender_p2p
        .connect_peer(&receiver_addr)
        .await
        .expect("connect sender -> receiver");
    wait_for_connected_peer(sender).await;
    wait_for_connected_peer(receiver).await;

    let collection_names = collections
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    sender_p2p
        .add_collections(collection_names.clone())
        .await
        .expect("add collections to sender p2p");
    receiver_p2p
        .add_collections(collection_names.clone())
        .await
        .expect("add collections to receiver p2p");
    receiver_p2p
        .add_replicator(
            collection_names.clone(),
            Some(&sender_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("authorize sender as receiver-side replicator");
    sender_p2p
        .add_replicator(
            collection_names,
            Some(&receiver_addr),
            filters,
            Vec::new(),
            None,
        )
        .await
        .expect("set sender -> receiver filtered replicator");
}

async fn query_agent_doc_dids(node: &EmbeddedNode) -> Vec<String> {
    let response = node.execute("query { AgentDoc { agent_did } }").await;
    assert!(
        response.errors.is_empty(),
        "AgentDoc query returned errors: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentDoc"))
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    row.get("agent_did")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn wait_for_agent_doc_dids(node: &EmbeddedNode, expected_dids: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let present = query_agent_doc_dids(node).await;
        if expected_dids
            .iter()
            .all(|did| present.contains(&did.to_string()))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "receiver never got expected dids {:?}; last seen: {:?}",
            expected_dids,
            present
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn agent_did_in_filter(dids: &[&str]) -> defra_http::router::ReplicationFilters {
    let conds = serde_json::json!({"agent_did": {"_in": dids}});
    let conditions = conds
        .as_object()
        .expect("conditions must be an object")
        .clone();
    let mut filters = defra_http::router::ReplicationFilters::new();
    filters.insert(
        "AgentDoc".to_string(),
        defra_http::router::ReplicationFilter::predicate(conditions),
    );
    filters
}

async fn fetch_turn_snapshot(
    node: &EmbeddedNode,
    session_id: &str,
    request_id: &str,
) -> Option<TurnSnapshot> {
    let response = node
        .execute(&format!(
            r#"query {{
                Conversation(
                    filter: {{ sessionID: {{ _eq: {session_id} }} }},
                    limit: 1
                ) {{
                    latestRequestID
                    status
                }}
                Request(
                    filter: {{ requestID: {{ _eq: {request_id} }} }},
                    limit: 1
                ) {{
                    status
                    lifecycleState
                }}
                Response(
                    filter: {{ requestID: {{ _eq: {request_id} }} }},
                    limit: 1
                ) {{
                    status
                    progressSeq
                    materializedMessageSequence
                    content
                    reasoning
                }}
                Message(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    _docID
                }}
                ToolCall(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    status
                    completedAt
                }}
                ToolResult(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    _docID
                }}
            }}"#,
            session_id = json_string_literal(session_id),
            request_id = json_string_literal(request_id),
        ))
        .await;
    assert!(
        response.errors.is_empty(),
        "turn snapshot query returned errors: {:?}",
        response.errors
    );

    let data = response.data.as_ref()?;
    let conversation = data
        .get("Conversation")
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())?;
    let request = data
        .get("Request")
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())?;
    let response_row = data
        .get("Response")
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())?;
    let messages = data
        .get("Message")
        .and_then(|rows| rows.as_array())
        .cloned()
        .unwrap_or_default();
    let tool_calls = data
        .get("ToolCall")
        .and_then(|rows| rows.as_array())
        .cloned()
        .unwrap_or_default();
    let tool_results = data
        .get("ToolResult")
        .and_then(|rows| rows.as_array())
        .cloned()
        .unwrap_or_default();

    let completed_tool_call_count = tool_calls
        .iter()
        .filter(|row| {
            row.get("completedAt")
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.trim().is_empty())
                || row
                    .get("status")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| value == "completed")
        })
        .count();

    Some(TurnSnapshot {
        request_status: request
            .get("status")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        request_lifecycle_state: request
            .get("lifecycleState")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        response_status: response_row
            .get("status")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        response_progress_seq: response_row
            .get("progressSeq")
            .and_then(|value| value.as_i64()),
        materialized_message_sequence: response_row
            .get("materializedMessageSequence")
            .and_then(|value| value.as_i64()),
        response_content_len: response_row
            .get("content")
            .and_then(|value| value.as_str())
            .map(str::len)
            .unwrap_or(0),
        response_reasoning_len: response_row
            .get("reasoning")
            .and_then(|value| value.as_str())
            .map(str::len)
            .unwrap_or(0),
        message_count: messages.len(),
        tool_call_count: tool_calls.len(),
        completed_tool_call_count,
        tool_result_count: tool_results.len(),
        latest_request_id: conversation
            .get("latestRequestID")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        conversation_status: conversation
            .get("status")
            .and_then(|value| value.as_str())
            .map(str::to_string),
    })
}

async fn wait_for_turn_snapshot(
    node: &EmbeddedNode,
    session_id: &str,
    request_id: &str,
    expected: &TurnSnapshot,
    timeout: Duration,
) -> Option<TurnSnapshot> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(current) = fetch_turn_snapshot(node, session_id, request_id).await {
            if current == *expected {
                return Some(current);
            }
            if Instant::now() >= deadline {
                return Some(current);
            }
        } else if Instant::now() >= deadline {
            return None;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn fetch_turn_diagnostics(
    node: &EmbeddedNode,
    session_id: &str,
    request_id: &str,
) -> serde_json::Value {
    let response = node
        .execute(&format!(
            r#"query {{
                Conversation(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    _docID
                    sessionID
                    latestRequestID
                    status
                }}
                Request {{
                    _docID
                    requestID
                    sessionID
                    status
                    lifecycleState
                }}
                RequestBySession: Request(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    _docID
                    requestID
                    sessionID
                    status
                    lifecycleState
                }}
                RequestByID: Request(filter: {{ requestID: {{ _eq: {request_id} }} }}) {{
                    _docID
                    requestID
                    sessionID
                    status
                    lifecycleState
                }}
                Response {{
                    _docID
                    responseKey
                    requestID
                    sessionID
                    status
                    progressSeq
                    materializedMessageSequence
                    content
                    reasoning
                }}
                ResponseByRequest: Response(filter: {{ requestID: {{ _eq: {request_id} }} }}) {{
                    _docID
                    responseKey
                    requestID
                    sessionID
                    status
                    progressSeq
                    materializedMessageSequence
                    content
                    reasoning
                }}
                Message {{
                    _docID
                    messageKey
                    sessionID
                    sequence
                    role
                    content
                }}
                MessageBySession: Message(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    _docID
                    messageKey
                    sessionID
                    sequence
                    role
                    content
                }}
                ToolCall {{
                    _docID
                    toolCallKey
                    sessionID
                    requestID
                    messageSequence
                    toolName
                    status
                    completedAt
                }}
                ToolCallBySession: ToolCall(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    _docID
                    toolCallKey
                    sessionID
                    requestID
                    messageSequence
                    toolName
                    status
                    completedAt
                }}
                ToolResult {{
                    _docID
                    toolResultKey
                    sessionID
                    requestID
                    toolName
                }}
                ToolResultBySession: ToolResult(filter: {{ sessionID: {{ _eq: {session_id} }} }}) {{
                    _docID
                    toolResultKey
                    sessionID
                    requestID
                    toolName
                }}
            }}"#,
            session_id = json_string_literal(session_id),
            request_id = json_string_literal(request_id),
        ))
        .await;
    assert!(
        response.errors.is_empty(),
        "turn diagnostics query returned errors: {:?}",
        response.errors
    );

    fn summarize_rows(
        data: &serde_json::Map<String, serde_json::Value>,
        field: &str,
        id_field: &str,
        extra_fields: &[&str],
    ) -> serde_json::Value {
        let rows = data
            .get(field)
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        serde_json::json!({
            "count": rows.len(),
            "rows": rows.into_iter().map(|row| {
                let mut summary = serde_json::Map::new();
                if let Some(value) = row.get("_docID") {
                    summary.insert("_docID".to_string(), value.clone());
                }
                if let Some(value) = row.get(id_field) {
                    summary.insert(id_field.to_string(), value.clone());
                }
                for field_name in extra_fields {
                    if let Some(value) = row.get(*field_name) {
                        if (*field_name == "content" || *field_name == "reasoning")
                            && value.is_string()
                        {
                            summary.insert(
                                format!("{field_name}Len"),
                                serde_json::json!(value.as_str().map(str::len).unwrap_or(0)),
                            );
                        } else {
                            summary.insert((*field_name).to_string(), value.clone());
                        }
                    }
                }
                serde_json::Value::Object(summary)
            }).collect::<Vec<_>>()
        })
    }

    let data = response
        .data
        .as_ref()
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();

    serde_json::json!({
        "ConversationBySession": summarize_rows(&data, "Conversation", "sessionID", &["latestRequestID", "status"]),
        "RequestAll": summarize_rows(&data, "Request", "requestID", &["sessionID", "status", "lifecycleState"]),
        "RequestBySession": summarize_rows(&data, "RequestBySession", "requestID", &["sessionID", "status", "lifecycleState"]),
        "RequestByID": summarize_rows(&data, "RequestByID", "requestID", &["sessionID", "status", "lifecycleState"]),
        "ResponseAll": summarize_rows(&data, "Response", "responseKey", &["requestID", "sessionID", "status", "progressSeq", "materializedMessageSequence", "content", "reasoning"]),
        "ResponseByRequest": summarize_rows(&data, "ResponseByRequest", "responseKey", &["requestID", "sessionID", "status", "progressSeq", "materializedMessageSequence", "content", "reasoning"]),
        "MessageAll": summarize_rows(&data, "Message", "messageKey", &["sessionID", "sequence", "role", "content"]),
        "MessageBySession": summarize_rows(&data, "MessageBySession", "messageKey", &["sessionID", "sequence", "role", "content"]),
        "ToolCallAll": summarize_rows(&data, "ToolCall", "toolCallKey", &["sessionID", "requestID", "messageSequence", "toolName", "status", "completedAt"]),
        "ToolCallBySession": summarize_rows(&data, "ToolCallBySession", "toolCallKey", &["sessionID", "requestID", "messageSequence", "toolName", "status", "completedAt"]),
        "ToolResultAll": summarize_rows(&data, "ToolResult", "toolResultKey", &["sessionID", "requestID", "toolName"]),
        "ToolResultBySession": summarize_rows(&data, "ToolResultBySession", "toolResultKey", &["sessionID", "requestID", "toolName"]),
    })
}

struct TurnSpec<'a> {
    session_id: &'a str,
    request_id: &'a str,
    prompt: &'a str,
    user_sequence: usize,
    assistant_sequence: usize,
    chunk_count: usize,
    tool_call_every: usize,
}

async fn create_turn(node: &EmbeddedNode, spec: TurnSpec<'_>) {
    let TurnSpec {
        session_id,
        request_id,
        prompt,
        user_sequence,
        assistant_sequence,
        chunk_count,
        tool_call_every,
    } = spec;

    let now = synthetic_timestamp();
    let upsert_conversation = node
        .execute(&format!(
            r#"mutation {{
                upsert_Conversation(
                    filter: {{ sessionID: {{ _eq: {session_id} }} }},
                    add: {{
                        sessionID: {session_id},
                        latestRequestID: {request_id},
                        status: "active",
                        updatedAt: {updated_at}
                    }},
                    update: {{
                        latestRequestID: {request_id},
                        status: "active",
                        updatedAt: {updated_at}
                    }}
                ) {{ _docID }}
            }}"#,
            session_id = json_string_literal(session_id),
            request_id = json_string_literal(request_id),
            updated_at = json_string_literal(&now),
        ))
        .await;
    assert!(
        upsert_conversation.errors.is_empty(),
        "conversation upsert returned errors: {:?}",
        upsert_conversation.errors
    );

    let request_response = node
        .execute(&format!(
            r#"mutation {{
                add_Request(input: {{
                    requestID: {request_id},
                    sessionID: {session_id},
                    content: {content},
                    status: "processing",
                    lifecycleState: "processing",
                    createdAt: {created_at}
                }}) {{ _docID }}
            }}"#,
            request_id = json_string_literal(request_id),
            session_id = json_string_literal(session_id),
            content = json_string_literal(prompt),
            created_at = json_string_literal(&now),
        ))
        .await;
    assert!(
        request_response.errors.is_empty(),
        "request add returned errors: {:?}",
        request_response.errors
    );

    let user_message = node
        .execute(&format!(
            r#"mutation {{
                add_Message(input: {{
                    messageKey: {message_key},
                    sessionID: {session_id},
                    sequence: {sequence},
                    role: "user",
                    content: {content}
                }}) {{ _docID }}
            }}"#,
            message_key = json_string_literal(&format!("{session_id}:{user_sequence}")),
            session_id = json_string_literal(session_id),
            sequence = user_sequence,
            content = json_string_literal(prompt),
        ))
        .await;
    assert!(
        user_message.errors.is_empty(),
        "user message add returned errors: {:?}",
        user_message.errors
    );

    let response_doc = node
        .execute(&format!(
            r#"mutation {{
                add_Response(input: {{
                    responseKey: {response_key},
                    requestID: {request_id},
                    sessionID: {session_id},
                    content: "",
                    reasoning: "",
                    status: "streaming",
                    progressSeq: 0,
                    materializedMessageSequence: null,
                    completedAt: ""
                }}) {{ _docID }}
            }}"#,
            response_key = json_string_literal(request_id),
            request_id = json_string_literal(request_id),
            session_id = json_string_literal(session_id),
        ))
        .await;
    assert!(
        response_doc.errors.is_empty(),
        "response add returned errors: {:?}",
        response_doc.errors
    );
    let response_doc_id = extract_created_doc_id(&response_doc, "add_Response");

    let mut content = String::new();
    let mut reasoning = String::new();
    let mut progress_seq = 0;

    for chunk_index in 0..chunk_count {
        content.push_str(&build_turn_chunk(chunk_index));
        let content_update = node
            .execute(&format!(
                r#"mutation {{
                    update_Response(
                        filter: {{ _docID: {{ _eq: {doc_id} }} }},
                        input: {{
                            content: {content}
                        }}
                    ) {{ _docID }}
                }}"#,
                doc_id = json_string_literal(&response_doc_id),
                content = json_string_literal(&content),
            ))
            .await;
        assert!(
            content_update.errors.is_empty(),
            "content update at chunk {chunk_index} returned errors: {:?}",
            content_update.errors
        );

        if chunk_index % 8 == 0 {
            reasoning.push_str(&format!(
                "reason-{chunk_index:03}: {}\n",
                "following the branchable response head and transcript materialization.".repeat(2)
            ));
            let reasoning_update = node
                .execute(&format!(
                    r#"mutation {{
                        update_Response(
                            filter: {{ _docID: {{ _eq: {doc_id} }} }},
                            input: {{
                                reasoning: {reasoning}
                            }}
                        ) {{ _docID }}
                    }}"#,
                    doc_id = json_string_literal(&response_doc_id),
                    reasoning = json_string_literal(&reasoning),
                ))
                .await;
            assert!(
                reasoning_update.errors.is_empty(),
                "reasoning update at chunk {chunk_index} returned errors: {:?}",
                reasoning_update.errors
            );

            progress_seq += 1;
            let progress_update = node
                .execute(&format!(
                    r#"mutation {{
                        update_Response(
                            filter: {{ _docID: {{ _eq: {doc_id} }} }},
                            input: {{
                                progressSeq: {progress_seq}
                            }}
                        ) {{ _docID }}
                    }}"#,
                    doc_id = json_string_literal(&response_doc_id),
                    progress_seq = progress_seq,
                ))
                .await;
            assert!(
                progress_update.errors.is_empty(),
                "progress update at chunk {chunk_index} returned errors: {:?}",
                progress_update.errors
            );
        }

        if chunk_index % tool_call_every == 0 {
            let tool_index = chunk_index / tool_call_every;
            let tool_call_key = format!("{request_id}:tool-call-{tool_index:03}");
            let tool_call_upsert = node
                .execute(&format!(
                    r#"mutation {{
                        upsert_ToolCall(
                            filter: {{ toolCallKey: {{ _eq: {tool_call_key} }} }},
                            add: {{
                                toolCallKey: {tool_call_key},
                                sessionID: {session_id},
                                requestID: {request_id},
                                messageSequence: {assistant_sequence},
                                toolName: "read_file",
                                status: "called",
                                result: "",
                                startedAt: {started_at},
                                completedAt: ""
                            }},
                            update: {{
                                status: "called"
                            }}
                        ) {{ _docID }}
                    }}"#,
                    tool_call_key = json_string_literal(&tool_call_key),
                    session_id = json_string_literal(session_id),
                    request_id = json_string_literal(request_id),
                    assistant_sequence = assistant_sequence,
                    started_at = json_string_literal(&synthetic_timestamp()),
                ))
                .await;
            assert!(
                tool_call_upsert.errors.is_empty(),
                "tool call upsert at chunk {chunk_index} returned errors: {:?}",
                tool_call_upsert.errors
            );

            let tool_call_complete = node
                .execute(&format!(
                    r#"mutation {{
                        update_ToolCall(
                            filter: {{ toolCallKey: {{ _eq: {tool_call_key} }} }},
                            input: {{
                                result: {result},
                                status: "completed",
                                completedAt: {completed_at}
                            }}
                        ) {{ _docID }}
                    }}"#,
                    tool_call_key = json_string_literal(&tool_call_key),
                    result = json_string_literal(&format!(
                        "tool-result-{tool_index:03}: replicated over the same session follow-up turn"
                    )),
                    completed_at = json_string_literal(&synthetic_timestamp()),
                ))
                .await;
            assert!(
                tool_call_complete.errors.is_empty(),
                "tool call completion at chunk {chunk_index} returned errors: {:?}",
                tool_call_complete.errors
            );

            let tool_result_add = node
                .execute(&format!(
                    r#"mutation {{
                        add_ToolResult(input: {{
                            toolResultKey: {tool_result_key},
                            sessionID: {session_id},
                            requestID: {request_id},
                            toolName: "read_file",
                            outputText: {output_text},
                            createdAt: {created_at}
                        }}) {{ _docID }}
                    }}"#,
                    tool_result_key =
                        json_string_literal(&format!("{request_id}:tool-result-{tool_index:03}")),
                    session_id = json_string_literal(session_id),
                    request_id = json_string_literal(request_id),
                    output_text = json_string_literal(&build_tool_result_payload(tool_index)),
                    created_at = json_string_literal(&synthetic_timestamp()),
                ))
                .await;
            assert!(
                tool_result_add.errors.is_empty(),
                "tool result add at chunk {chunk_index} returned errors: {:?}",
                tool_result_add.errors
            );
        }

        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let finalize_response = node
        .execute(&format!(
            r#"mutation {{
                update_Response(
                    filter: {{ _docID: {{ _eq: {doc_id} }} }},
                    input: {{
                        content: {content},
                        reasoning: {reasoning},
                        status: "complete",
                        completedAt: {completed_at}
                    }}
                ) {{ _docID }}
            }}"#,
            doc_id = json_string_literal(&response_doc_id),
            content = json_string_literal(&content),
            reasoning = json_string_literal(&reasoning),
            completed_at = json_string_literal(&synthetic_timestamp()),
        ))
        .await;
    assert!(
        finalize_response.errors.is_empty(),
        "response finalize returned errors: {:?}",
        finalize_response.errors
    );

    let finalize_request = node
        .execute(&format!(
            r#"mutation {{
                update_Request(
                    filter: {{ requestID: {{ _eq: {request_id} }} }},
                    input: {{
                        status: "completed",
                        lifecycleState: "completed"
                    }}
                ) {{ _docID }}
            }}"#,
            request_id = json_string_literal(request_id),
        ))
        .await;
    assert!(
        finalize_request.errors.is_empty(),
        "request finalize returned errors: {:?}",
        finalize_request.errors
    );

    let assistant_message = node
        .execute(&format!(
            r#"mutation {{
                add_Message(input: {{
                    messageKey: {message_key},
                    sessionID: {session_id},
                    sequence: {sequence},
                    role: "assistant",
                    content: {content}
                }}) {{ _docID }}
            }}"#,
            message_key = json_string_literal(&format!("{session_id}:{assistant_sequence}")),
            session_id = json_string_literal(session_id),
            sequence = assistant_sequence,
            content = json_string_literal(&content),
        ))
        .await;
    assert!(
        assistant_message.errors.is_empty(),
        "assistant message add returned errors: {:?}",
        assistant_message.errors
    );

    let materialize_response = node
        .execute(&format!(
            r#"mutation {{
                update_Response(
                    filter: {{ _docID: {{ _eq: {doc_id} }} }},
                    input: {{
                        materializedMessageSequence: {assistant_sequence}
                    }}
                ) {{ _docID }}
            }}"#,
            doc_id = json_string_literal(&response_doc_id),
            assistant_sequence = assistant_sequence,
        ))
        .await;
    assert!(
        materialize_response.errors.is_empty(),
        "response materialization update returned errors: {:?}",
        materialize_response.errors
    );

    let complete_conversation = node
        .execute(&format!(
            r#"mutation {{
                update_Conversation(
                    filter: {{ sessionID: {{ _eq: {session_id} }} }},
                    input: {{
                        latestRequestID: {request_id},
                        status: "completed",
                        updatedAt: {updated_at}
                    }}
                ) {{ _docID }}
            }}"#,
            session_id = json_string_literal(session_id),
            request_id = json_string_literal(request_id),
            updated_at = json_string_literal(&synthetic_timestamp()),
        ))
        .await;
    assert!(
        complete_conversation.errors.is_empty(),
        "conversation completion returned errors: {:?}",
        complete_conversation.errors
    );
}

#[tokio::test]
async fn live_replicator_pushes_post_config_writes() {
    init_tracing();

    let node0 = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build node0");
    let node1 = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build node1");

    node0
        .add_schema("type User { name: String age: Int }")
        .await
        .expect("schema on node0");
    node1
        .add_schema("type User { name: String age: Int }")
        .await
        .expect("schema on node1");

    let addr0 = wait_for_listen_addr(&node0).await;
    let addr1 = wait_for_listen_addr(&node1).await;

    let p2p0 = node0.p2p().expect("node0 p2p");
    let p2p1 = node1.p2p().expect("node1 p2p");

    p2p0.connect_peer(&addr1)
        .await
        .expect("connect node0 -> node1");
    wait_for_connected_peer(&node0).await;
    wait_for_connected_peer(&node1).await;

    p2p0.add_collections(vec!["User".to_string()])
        .await
        .expect("subscribe node0 User");
    p2p1.add_collections(vec!["User".to_string()])
        .await
        .expect("subscribe node1 User");

    p2p1.add_replicator(
        vec!["User".to_string()],
        Some(&addr0),
        Default::default(),
        Vec::new(),
        None,
    )
    .await
    .expect("authorize node0 on node1");
    p2p0.add_replicator(
        vec!["User".to_string()],
        Some(&addr1),
        Default::default(),
        Vec::new(),
        None,
    )
    .await
    .expect("set replicator node0 -> node1");

    let response = node0
        .execute(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
        .await;
    assert!(
        response.errors.is_empty(),
        "mutation returned errors: {:?}",
        response.errors
    );

    wait_for_collection_len(&node1, "User", 1).await;

    node0.shutdown().await;
    node1.shutdown().await;
}

#[tokio::test]
async fn p2p_document_subscriptions_survive_restart_and_delete() {
    init_tracing();

    let data_path = unique_data_path("p2p-doc-subscriptions");
    let secret_key_path = data_path.join("p2p.key");

    let doc_id = {
        let node = build_persistent_p2p_node(data_path.clone(), secret_key_path.clone()).await;

        node.add_schema("type Note { text: String }")
            .await
            .expect("add schema");
        let response = node
            .execute(r#"mutation { add_Note(input: {text: "persist me"}) { _docID } }"#)
            .await;
        assert!(
            response.errors.is_empty(),
            "mutation returned errors: {:?}",
            response.errors
        );
        let doc_id = extract_created_doc_id(&response, "add_Note");

        node.p2p()
            .expect("p2p")
            .add_documents(vec![defra_http::router::P2pDocumentRequest {
                collection: String::new(),
                doc_id: doc_id.clone(),
            }])
            .await
            .expect("add document subscription");
        let docs = node
            .p2p()
            .expect("p2p")
            .get_documents()
            .await
            .expect("list document subscriptions");
        assert!(docs.iter().any(|doc| doc.doc_id == doc_id));

        node.shutdown().await;
        doc_id
    };

    {
        let node = build_persistent_p2p_node(data_path.clone(), secret_key_path.clone()).await;

        let docs = node
            .p2p()
            .expect("p2p")
            .get_documents()
            .await
            .expect("list restored document subscriptions");
        assert!(
            docs.iter().any(|doc| doc.doc_id == doc_id),
            "persisted document subscription was not restored"
        );

        node.p2p()
            .expect("p2p")
            .remove_documents(vec![defra_http::router::P2pDocumentRequest {
                collection: String::new(),
                doc_id: doc_id.clone(),
            }])
            .await
            .expect("remove document subscription");
        let docs = node
            .p2p()
            .expect("p2p")
            .get_documents()
            .await
            .expect("list document subscriptions after remove");
        assert!(!docs.iter().any(|doc| doc.doc_id == doc_id));

        node.shutdown().await;
    }

    {
        let node = build_persistent_p2p_node(data_path.clone(), secret_key_path).await;

        let docs = node
            .p2p()
            .expect("p2p")
            .get_documents()
            .await
            .expect("list document subscriptions after restart");
        assert!(
            !docs.iter().any(|doc| doc.doc_id == doc_id),
            "removed document subscription was restored after restart"
        );

        node.shutdown().await;
    }

    let _ = tokio::fs::remove_dir_all(data_path).await;
}

#[tokio::test]
async fn p2p_replicator_survives_embedded_restart() {
    init_tracing();

    let data_path0 = unique_data_path("p2p-replicator-node0");
    let data_path1 = unique_data_path("p2p-replicator-node1");
    let secret_key_path0 = data_path0.join("p2p.key");
    let secret_key_path1 = data_path1.join("p2p.key");

    let doc_id = {
        let node0 = build_persistent_p2p_node(data_path0.clone(), secret_key_path0.clone()).await;
        let node1 = build_persistent_p2p_node(data_path1.clone(), secret_key_path1.clone()).await;

        node0
            .add_schema("type User { name: String age: Int }")
            .await
            .expect("schema on node0");
        node1
            .add_schema("type User { name: String age: Int }")
            .await
            .expect("schema on node1");

        install_one_way_replicator(&node0, &node1, &["User"]).await;

        let response = node0
            .execute(
                r#"mutation { add_User(input: {name: "Persisted", age: 30}) { _docID name age } }"#,
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "mutation returned errors: {:?}",
            response.errors
        );
        let doc_id = extract_created_doc_id(&response, "add_User");

        wait_for_collection_len(&node1, "User", 1).await;

        node0.shutdown().await;
        node1.shutdown().await;
        doc_id
    };

    {
        let node0 = build_persistent_p2p_node(data_path0.clone(), secret_key_path0).await;
        let node1 = build_persistent_p2p_node(data_path1.clone(), secret_key_path1).await;

        let addr1 = wait_for_listen_addr(&node1).await;
        node0
            .p2p()
            .expect("node0 p2p")
            .connect_peer(&addr1)
            .await
            .expect("reconnect node0 -> node1");
        wait_for_connected_peer(&node0).await;
        wait_for_connected_peer(&node1).await;

        let response = node0
            .execute(&format!(
                r#"mutation {{ update_User(docID: "{}", input: {{age: 42}}) {{ _docID age }} }}"#,
                doc_id
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "update returned errors: {:?}",
            response.errors
        );

        wait_for_user_age(&node1, 42).await;

        node0.shutdown().await;
        node1.shutdown().await;
    }

    let _ = tokio::fs::remove_dir_all(data_path0).await;
    let _ = tokio::fs::remove_dir_all(data_path1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress test for same-session follow-up replication under desktop-like load"]
async fn live_replicator_same_session_followup_turn_converges() {
    init_tracing();

    let node0 = EmbeddedNode::builder()
        .with_p2p(desktop_like_streaming_p2p_config())
        .build()
        .await
        .expect("build node0");
    let node1 = EmbeddedNode::builder()
        .with_p2p(desktop_like_streaming_p2p_config())
        .build()
        .await
        .expect("build node1");

    node0
        .add_schema(SESSION_TURN_SDL)
        .await
        .expect("schema on node0");
    node1
        .add_schema(SESSION_TURN_SDL)
        .await
        .expect("schema on node1");

    install_one_way_replicator(
        &node0,
        &node1,
        &[
            "Conversation",
            "Request",
            "Response",
            "Message",
            "ToolCall",
            "ToolResult",
        ],
    )
    .await;

    let session_id = "sess-followup";
    create_turn(
        &node0,
        TurnSpec {
            session_id,
            request_id: "req-1",
            prompt: "Hey amy can you tell me about the p2p communcation between the agent and the desktop in this app and the docuemnt based request model?",
            user_sequence: 1,
            assistant_sequence: 2,
            chunk_count: 64,
            tool_call_every: 3,
        },
    )
    .await;

    let first_expected = fetch_turn_snapshot(&node0, session_id, "req-1")
        .await
        .expect("sender snapshot for first turn should exist");
    let first_receiver = wait_for_turn_snapshot(
        &node1,
        session_id,
        "req-1",
        &first_expected,
        Duration::from_secs(60),
    )
    .await;
    if first_receiver != Some(first_expected.clone()) {
        let sender_diag = fetch_turn_diagnostics(&node0, session_id, "req-1").await;
        let receiver_diag = fetch_turn_diagnostics(&node1, session_id, "req-1").await;
        node0.shutdown().await;
        node1.shutdown().await;
        panic!(
            "first turn never converged before follow-up: receiver={first_receiver:?} expected={first_expected:?} sender_diag={sender_diag:?} receiver_diag={receiver_diag:?}",
        );
    }

    create_turn(
        &node0,
        TurnSpec {
            session_id,
            request_id: "req-2",
            prompt: "Awesome breakdown, can you please tell me what you like about the architecture and point to files?",
            user_sequence: 3,
            assistant_sequence: 4,
            chunk_count: 128,
            tool_call_every: 2,
        },
    )
    .await;

    let second_expected = fetch_turn_snapshot(&node0, session_id, "req-2")
        .await
        .expect("sender snapshot for follow-up should exist");
    let receiver = wait_for_turn_snapshot(
        &node1,
        session_id,
        "req-2",
        &second_expected,
        Duration::from_secs(180),
    )
    .await;

    let expected_receiver = Some(second_expected.clone());
    let sender_diag = if receiver != expected_receiver {
        Some(fetch_turn_diagnostics(&node0, session_id, "req-2").await)
    } else {
        None
    };
    let receiver_diag = if receiver != expected_receiver {
        Some(fetch_turn_diagnostics(&node1, session_id, "req-2").await)
    } else {
        None
    };

    node0.shutdown().await;
    node1.shutdown().await;

    assert_eq!(
        receiver,
        expected_receiver,
        "same-session followup turn failed to converge without explicit sync nudge; receiver={receiver:?} expected={second_expected:?} sender_diag={sender_diag:?} receiver_diag={receiver_diag:?}",
    );
}

#[tokio::test]
async fn live_replicator_filtered_push_respects_predicate() {
    init_tracing();

    let sender = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build sender");
    let receiver = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build receiver");

    sender
        .add_schema(AGENT_SCHEMA)
        .await
        .expect("schema on sender");
    receiver
        .add_schema(AGENT_SCHEMA)
        .await
        .expect("schema on receiver");

    install_filtered_one_way_replicator(
        &sender,
        &receiver,
        &["AgentDoc"],
        agent_did_in_filter(&["did:key:alice", "did:key:carol"]),
    )
    .await;

    for (agent_did, body) in [
        ("did:key:alice", "alice body"),
        ("did:key:bob", "bob body"),
        ("did:key:carol", "carol body"),
    ] {
        let response = sender
            .execute(&format!(
                r#"mutation {{ add_AgentDoc(input: {{agent_did: "{agent_did}", body: "{body}"}}) {{ _docID }} }}"#
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "add_AgentDoc({agent_did}) returned errors: {:?}",
            response.errors
        );
    }

    wait_for_agent_doc_dids(&receiver, &["did:key:alice", "did:key:carol"]).await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let present = query_agent_doc_dids(&receiver).await;
    assert!(
        !present.contains(&"did:key:bob".to_string()),
        "bob's doc must NOT have replicated; receiver has: {present:?}"
    );
    assert_eq!(
        present.len(),
        2,
        "receiver must have exactly 2 docs; got: {present:?}"
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn filtered_backfill_via_transport_pusher_respects_predicate() {
    init_tracing();

    let sender = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build sender");
    let receiver = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build receiver");

    sender
        .add_schema(AGENT_SCHEMA)
        .await
        .expect("schema on sender");
    receiver
        .add_schema(AGENT_SCHEMA)
        .await
        .expect("schema on receiver");

    for (agent_did, body) in [
        ("did:key:alice", "alice body"),
        ("did:key:bob", "bob body"),
        ("did:key:carol", "carol body"),
    ] {
        let response = sender
            .execute(&format!(
                r#"mutation {{ add_AgentDoc(input: {{agent_did: "{agent_did}", body: "{body}"}}) {{ _docID }} }}"#
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "pre-replicator add_AgentDoc({agent_did}) returned errors: {:?}",
            response.errors
        );
    }

    install_filtered_one_way_replicator(
        &sender,
        &receiver,
        &["AgentDoc"],
        agent_did_in_filter(&["did:key:alice", "did:key:carol"]),
    )
    .await;

    wait_for_agent_doc_dids(&receiver, &["did:key:alice", "did:key:carol"]).await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let present = query_agent_doc_dids(&receiver).await;
    assert!(
        !present.contains(&"did:key:bob".to_string()),
        "bob's doc must NOT have backfilled; receiver has: {present:?}"
    );
    assert_eq!(
        present.len(),
        2,
        "receiver must have exactly 2 docs after backfill; got: {present:?}"
    );

    sender.shutdown().await;
    receiver.shutdown().await;
}
