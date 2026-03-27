//! Data generation helpers for benchmark fixtures.

use std::fmt::Write as _;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value as JsonValue;

use crate::benchmark_queries::escape_graphql;
use crate::benchmark_support::{CodingSessionFixtureConfig, FixtureSession, SessionKind};
use crate::EmbeddedNode;

const INSERT_BATCH_SIZE: usize = 25;

pub(crate) async fn create_session(
    node: &EmbeddedNode,
    session: &FixtureSession,
) -> Result<String> {
    let user_message_count = (session.message_count / 3).max(1);
    let query = format!(
        r#"mutation {{
  add_CodingSession(input: [{{
    session_id: "{session_id}"
    message_count: {message_count}
    user_message_count: {user_message_count}
  }}]) {{
    _docID
  }}
}}"#,
        session_id = escape_graphql(&session.session_id),
        message_count = session.message_count,
        user_message_count = user_message_count,
    );

    let data = ensure_success(node.execute(&query).await, &session.session_id)?;
    data.get("add_CodingSession")
        .and_then(extract_doc_id_value)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| {
            format!(
                "missing _docID for {} response={}",
                session.session_id, data
            )
        })
}

pub(crate) async fn create_messages(
    node: &EmbeddedNode,
    config: &CodingSessionFixtureConfig,
    session: &FixtureSession,
    session_doc_id: &str,
) -> Result<()> {
    let docs = (0..session.message_count)
        .map(|index| {
            let sequence = index + 1;
            let role = if index % 3 == 0 { "user" } else { "assistant" };
            let created_at = timestamp_for(index);
            let message_id = format!("{}-msg-{sequence:06}", session.session_id);
            let content = message_content(config, session.kind, role, index);

            format!(
                r#"{{
  message_id: "{message_id}"
  _sessionID: "{session_doc_id}"
  sequence: {sequence}
  role: "{role}"
  created_at: "{created_at}"
  content: "{content}"
}}"#,
                message_id = escape_graphql(&message_id),
                session_doc_id = escape_graphql(session_doc_id),
                sequence = sequence,
                role = role,
                created_at = created_at,
                content = escape_graphql(&content),
            )
        })
        .collect::<Vec<_>>();

    execute_batched_add(node, &session.session_id, "CodingMessage", &docs).await
}

pub(crate) async fn create_actions(
    node: &EmbeddedNode,
    config: &CodingSessionFixtureConfig,
    session: &FixtureSession,
    session_doc_id: &str,
) -> Result<()> {
    let docs = (0..session.action_count)
        .map(|index| {
            let created_at = timestamp_for(index + session.message_count);
            let action_type = action_type(index);
            let target = action_target(session.kind, index);
            let command = action_command(config, session.kind, index);

            format!(
                r#"{{
  _sessionID: "{session_doc_id}"
  action_type: "{action_type}"
  target: "{target}"
  created_at: "{created_at}"
  command: "{command}"
}}"#,
                session_doc_id = escape_graphql(session_doc_id),
                action_type = escape_graphql(action_type),
                target = escape_graphql(&target),
                created_at = created_at,
                command = escape_graphql(&command),
            )
        })
        .collect::<Vec<_>>();

    execute_batched_add(node, &session.session_id, "CodingAction", &docs).await
}

async fn execute_batched_add(
    node: &EmbeddedNode,
    context: &str,
    collection_name: &str,
    docs: &[String],
) -> Result<()> {
    for chunk in docs.chunks(INSERT_BATCH_SIZE) {
        let mut query = String::from("mutation {\n");
        writeln!(&mut query, "  add_{collection_name}(input: [").unwrap();
        for doc in chunk {
            writeln!(&mut query, "    {doc}", doc = doc).unwrap();
        }
        writeln!(&mut query, "  ]) {{ _docID }}").unwrap();
        query.push('}');
        let response = node.execute(&query).await;
        ensure_success(response, context)?;
    }

    Ok(())
}

pub(crate) fn ensure_success(response: crate::QueryResponse, context: &str) -> Result<JsonValue> {
    if response.has_errors() {
        let messages = response
            .errors
            .into_iter()
            .map(|error| error.message)
            .collect::<Vec<_>>()
            .join("; ");
        bail!("{context}: {messages}");
    }

    response
        .data
        .ok_or_else(|| anyhow!("missing response data for {context}"))
}

fn extract_doc_id_value(value: &JsonValue) -> Option<&JsonValue> {
    value
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("_docID"))
        .or_else(|| value.get("_docID"))
}

fn timestamp_for(index: usize) -> String {
    let day = (index / 86_400) % 27 + 1;
    let hour = (index / 3_600) % 24;
    let minute = (index / 60) % 60;
    let second = index % 60;
    format!("2026-01-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn message_content(
    config: &CodingSessionFixtureConfig,
    kind: SessionKind,
    role: &str,
    index: usize,
) -> String {
    let target_bytes = config.message_target_bytes(kind, role);
    let focus = primary_focus(index);
    let rare_terms = rare_terms(index);
    let mut body = if role == "user" {
        format!(
            "Need help debugging nested bm25 search in a coding session.\n\
\n\
Context:\n\
- workload: {}\n\
- issue: session-local search for {} is slower than expected\n\
- suspected bottleneck: relation narrowing before bm25 scoring\n\
- files: crates/query/src/runner/query/nested.rs, crates/query/src/planner/joins/mod.rs\n\
\n\
Observed behavior:\n\
- query: CodingSession -> messages(order: score DESC, limit: 10)\n\
- terms: cargo query planner rocksdb {}\n\
- session message index: {}\n\
\n\
Please inspect the query plan, explain output, and relation join path.\n",
            session_label(kind),
            focus,
            rare_terms,
            index
        )
    } else {
        format!(
            "I profiled the nested coding-session query and the hot path is still dominated by join work before scoring.\n\
\n\
Plan:\n\
1. Narrow one-to-many children with the foreign-key index.\n\
2. Re-run explain(type: execute) and compare bm25Node iterations.\n\
3. Measure warm/cold p50 and p95 on rocksdb-backed fixtures.\n\
\n\
Relevant terms: cargo query planner index bm25 rocksdb {} {}\n\
\n\
```graphql\n\
query {{\n  CodingSession(filter: {{ session_id: {{ _eq: \"fixture-hot-session\" }} }}, limit: 1) {{\n    messages(order: {{ _alias: {{ score: DESC }} }}, limit: 10) {{\n      score: BM25(query: \"{}\", fields: [\"content\"])\n      content\n    }}\n  }}\n}}\n\
```\n",
            focus,
            rare_terms,
            focus
        )
    };

    pad_message_payload(&mut body, kind, role, index, target_bytes);
    body
}

fn action_type(index: usize) -> &'static str {
    match index % 4 {
        0 => "shell",
        1 => "search",
        2 => "test",
        _ => "bench",
    }
}

fn action_target(kind: SessionKind, index: usize) -> String {
    let area = match kind {
        SessionKind::Hot => "crates/query/src/runner/query/nested.rs",
        SessionKind::Medium => "crates/query/src/planner/joins/mod.rs",
        SessionKind::Background => "crates/query/src/plan/type_join/type_join_many/children.rs",
    };

    format!("{area}#L{}", 100 + (index % 200))
}

fn action_command(config: &CodingSessionFixtureConfig, kind: SessionKind, index: usize) -> String {
    let target_bytes = config.action_target_bytes(kind);
    let base = match index % 6 {
        0 => "cargo test -p query nested:: -- --nocapture",
        1 => "cargo bench -p defra-node --features rocksdb --bin coding-session-bench",
        2 => "rg bm25 crates/query/src/runner/query/nested.rs",
        3 => "cargo clippy --all -- -D warnings",
        4 => "rg pushdown crates/query/src/planner/joins/mod.rs",
        _ => "cargo test -p query type_join_many:: -- --nocapture",
    };
    let suffix = match kind {
        SessionKind::Hot if index.is_multiple_of(7) => " -- candidate pushdown cargo",
        SessionKind::Medium if index.is_multiple_of(5) => " -- bench rocksdb",
        SessionKind::Background if index.is_multiple_of(11) => " -- rg noise",
        _ => "",
    };
    let mut command = format!("bash -lc '{}{}'", base, suffix);
    pad_action_command(&mut command, kind, index, target_bytes);
    command
}

fn session_label(kind: SessionKind) -> &'static str {
    match kind {
        SessionKind::Hot => "hot",
        SessionKind::Medium => "medium",
        SessionKind::Background => "background",
    }
}

fn primary_focus(index: usize) -> &'static str {
    match index % 6 {
        0 => "cargo",
        1 => "query",
        2 => "planner",
        3 => "rocksdb",
        4 => "bm25",
        _ => "index",
    }
}

fn rare_terms(index: usize) -> String {
    [
        (29, "wand"),
        (31, "pushdown"),
        (37, "candidate"),
        (43, "turbo"),
        (47, "bm25"),
    ]
    .into_iter()
    .filter_map(|(divisor, term)| index.is_multiple_of(divisor).then_some(term))
    .collect::<Vec<_>>()
    .join(" ")
}

fn pad_message_payload(
    body: &mut String,
    kind: SessionKind,
    role: &str,
    index: usize,
    target_bytes: usize,
) {
    let session_label = session_label(kind);
    let focus = primary_focus(index);
    let rare_terms = rare_terms(index);
    let mut block = 0usize;

    while body.len() < target_bytes {
        let snippet = match (role, block % 4) {
            ("user", 0) => format!(
                "\n```text\nerror[E0277]: relation narrowing failed for session scope {index}\nhelp: inspect typeJoinMany cache, _sessionID filter, and rocksdb index path\n```\n"
            ),
            ("user", 1) => "Paths:\n- crates/query/src/plan/type_join/type_join_many/children.rs\n- crates/query/src/plan/type_join/type_join_many/plan_node.rs\n- crates/query/src/runner/query/nested.rs\n".to_string(),
            ("user", 2) => format!(
                "Expected outcome: fewer bm25 iterations, better top-k behavior, stable relevance for {} {}.\n",
                focus, rare_terms
            ),
            ("user", _) => format!(
                "Notes: session={} role=user offset={} limit=10 search terms=cargo query planner rocksdb {}\n",
                session_label, block, rare_terms
            ),
            (_, 0) => format!(
                "\n```rust\nlet explain = runner.execute(\"query @explain(type: execute) {{ CodingSession {{ messages {{ score: BM25(query: \\\"{}\\\", fields: [\\\"content\\\"]) }} }} }}\").await?;\nassert!(explain[\"explain\"].is_object());\n```\n",
                focus
            ),
            (_, 1) => format!(
                "Profiling notes: hot session={} block={} shows join narrowing, bm25 scoring, result shaping, and top-k ordering.\n",
                session_label, block
            ),
            (_, 2) => format!(
                "Representative coding data: rust compiler output, cargo test failures, rg matches, graphql explain JSON, file references, and planner traces. Terms={} {}.\n",
                focus, rare_terms
            ),
            _ => format!(
                "```json\n{{\"session\":\"{}\",\"focus\":\"{}\",\"candidate\":\"{}\",\"engine\":\"rocksdb\",\"bm25\":\"nested\"}}\n```\n",
                session_label,
                focus,
                if rare_terms.is_empty() { "none" } else { &rare_terms }
            ),
        };
        body.push_str(&snippet);
        block += 1;
    }

    body.truncate(target_bytes);
}

fn pad_action_command(command: &mut String, kind: SessionKind, index: usize, target_bytes: usize) {
    let focus = primary_focus(index);
    let session_label = session_label(kind);
    let paths = [
        "crates/query/src/runner/query/nested.rs",
        "crates/query/src/planner/joins/mod.rs",
        "crates/query/src/plan/type_join/type_join_many/children.rs",
        "crates/query/src/plan/type_join/type_join_many/plan_node.rs",
    ];
    let mut cursor = 0usize;

    while command.len() < target_bytes {
        let path = paths[cursor % paths.len()];
        let segment = match cursor % 3 {
            0 => format!(" ; rg {focus} {path}"),
            1 => format!(" ; cargo test -p query nested:: -- --nocapture --exact smoke_{session_label}_{cursor}"),
            _ => format!(" ; cargo bench -p defra-node --features rocksdb --bin coding-session-bench -- --case hot_messages_cargo --limit {}",
                5 + (cursor % 5)),
        };
        command.push_str(&segment);
        cursor += 1;
    }

    command.truncate(target_bytes);
}

pub(crate) fn scale_bytes(base: usize, scale_percent: usize) -> usize {
    base.saturating_mul(scale_percent) / 100
}
