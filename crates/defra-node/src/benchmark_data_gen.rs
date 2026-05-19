//! Data generation helpers for benchmark fixtures.

use std::fmt::Write as _;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value as JsonValue;

use crate::benchmark_queries::escape_graphql;
use crate::benchmark_support::{CodingSessionFixtureConfig, FixtureSession, SessionKind};
use crate::search_chunks::{derive_search_chunks, SearchChunkingConfig};
use crate::EmbeddedNode;

const INSERT_BATCH_SIZE: usize = 25;
const DEFAULT_CHUNK_CONFIG: SearchChunkingConfig = SearchChunkingConfig {
    max_chars: 640,
    overlap_chars: 96,
};

#[derive(Debug, Clone)]
pub(crate) struct InsertedMessageDoc {
    pub doc_id: String,
    pub message_id: String,
    pub role: String,
    pub created_at: String,
    pub content: String,
    body: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InsertedActionDoc {
    pub doc_id: String,
    pub parent_message_id: String,
    pub action_type: String,
    pub target: String,
    pub created_at: String,
    pub command: String,
    body: String,
}

pub(crate) async fn create_project(node: &EmbeddedNode, project_path: &str) -> Result<String> {
    let (repo_owner, repo_name) = repo_info(project_path);
    let owner_field = repo_owner
        .as_deref()
        .map(|value| format!(r#"repo_owner: "{}""#, escape_graphql(value)))
        .unwrap_or_default();
    let name_field = repo_name
        .as_deref()
        .map(|value| format!(r#"repo_name: "{}""#, escape_graphql(value)))
        .unwrap_or_default();
    let mut create_fields = vec![format!(r#"path: "{}""#, escape_graphql(project_path))];
    if !name_field.is_empty() {
        create_fields.push(name_field);
    }
    if !owner_field.is_empty() {
        create_fields.push(owner_field);
    }
    let query = format!(
        r#"mutation {{
  add_CodingProject(input: [{{
    {fields}
  }}]) {{
    _docID
  }}
}}"#,
        fields = create_fields.join("\n    "),
    );

    let data = ensure_success(node.execute(&query).await, project_path)?;
    data.get("add_CodingProject")
        .and_then(extract_doc_id_value)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| {
            format!(
                "missing CodingProject _docID for {} response={}",
                project_path, data
            )
        })
}

pub(crate) async fn create_session(
    node: &EmbeddedNode,
    session: &FixtureSession,
    project_doc_id: &str,
) -> Result<String> {
    let user_message_count = (session.message_count / 3).max(1);
    let created_at = timestamp_for(0);
    let finished_at = timestamp_for(session.message_count + session.action_count);
    let input_tokens = session.message_count.saturating_mul(180);
    let output_tokens = session.message_count.saturating_mul(340);
    let tools_used = serde_json::to_string(&["Read", "Bash", "Search"]).unwrap();
    let query = format!(
        r#"mutation {{
  add_CodingSession(input: [{{
    session_id: "{session_id}"
    _projectID: "{project_doc_id}"
    git_branch: "{git_branch}"
    source: "{source}"
    model_primary: "{model_primary}"
    claude_version: "{cli_version}"
    title: "{title}"
    archived: false
    git_sha: "{git_sha}"
    git_origin_url: "{git_origin_url}"
    agent_role: "{agent_role}"
    reasoning_effort: "{reasoning_effort}"
    created_at: "{created_at}"
    finished_at: "{finished_at}"
    message_count: {message_count}
    user_message_count: {user_message_count}
    input_tokens: {input_tokens}
    output_tokens: {output_tokens}
    tools_used: {tools_used}
    first_prompt: "{first_prompt}"
    summary: "{summary}"
  }}]) {{
    _docID
  }}
}}"#,
        session_id = escape_graphql(&session.session_id),
        project_doc_id = escape_graphql(project_doc_id),
        git_branch = escape_graphql(session.git_branch()),
        source = escape_graphql(session.source_label()),
        model_primary = escape_graphql(session.model_primary()),
        cli_version = escape_graphql(cli_version_for(session.source_label())),
        title = escape_graphql(&session_title(session)),
        git_sha = escape_graphql(&fake_git_sha(session)),
        git_origin_url = escape_graphql(&git_origin_url(&session.project_path)),
        agent_role = escape_graphql(agent_role(session.kind)),
        reasoning_effort = escape_graphql(reasoning_effort(session.kind)),
        created_at = created_at,
        finished_at = finished_at,
        message_count = session.message_count,
        user_message_count = user_message_count,
        input_tokens = input_tokens,
        output_tokens = output_tokens,
        tools_used = tools_used,
        first_prompt = escape_graphql(&first_prompt(session)),
        summary = escape_graphql(&session_summary(session)),
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
) -> Result<Vec<InsertedMessageDoc>> {
    let docs = (0..session.message_count)
        .map(|index| {
            let sequence = index + 1;
            let role = if index % 3 == 0 { "user" } else { "assistant" };
            let created_at = timestamp_for(index);
            let message_id = format!("{}-msg-{sequence:06}", session.session_id);
            let content = message_content(config, session.kind, role, index);
            let tool_uses = if role == "assistant" {
                serde_json::to_string(&assistant_tool_uses(index))
                    .unwrap_or_else(|_| "[]".to_string())
            } else {
                "[]".to_string()
            };
            let files_referenced = serde_json::to_string(&files_referenced(session.kind, index))
                .unwrap_or_else(|_| "[]".to_string());
            let input_tokens = content.len().saturating_div(8).max(24);
            let output_tokens = content.len().saturating_div(6).max(32);
            let body = format!(
                r#"{{
  message_id: "{message_id}"
  _sessionID: "{session_doc_id}"
  sequence: {sequence}
  role: "{role}"
  model: "{model}"
  created_at: "{created_at}"
  content: "{content}"
  tool_uses: {tool_uses}
  files_referenced: {files_referenced}
  input_tokens: {input_tokens}
  output_tokens: {output_tokens}
}}"#,
                message_id = escape_graphql(&message_id),
                session_doc_id = escape_graphql(session_doc_id),
                sequence = sequence,
                role = role,
                model = escape_graphql(session.model_primary()),
                created_at = created_at.as_str(),
                content = escape_graphql(&content),
                tool_uses = tool_uses,
                files_referenced = files_referenced,
                input_tokens = input_tokens,
                output_tokens = output_tokens,
            );

            InsertedMessageDoc {
                doc_id: String::new(),
                message_id,
                role: role.to_string(),
                created_at,
                content,
                body,
            }
        })
        .collect::<Vec<_>>();
    let bodies = docs.iter().map(|doc| doc.body.clone()).collect::<Vec<_>>();
    let doc_ids =
        execute_batched_add_collect_doc_ids(node, &session.session_id, "CodingMessage", &bodies)
            .await?;
    Ok(docs
        .into_iter()
        .zip(doc_ids)
        .map(|(mut doc, doc_id)| {
            doc.doc_id = doc_id;
            doc
        })
        .collect())
}

pub(crate) async fn create_actions(
    node: &EmbeddedNode,
    config: &CodingSessionFixtureConfig,
    session: &FixtureSession,
    session_doc_id: &str,
    messages: &[InsertedMessageDoc],
) -> Result<Vec<InsertedActionDoc>> {
    let docs = (0..session.action_count)
        .map(|index| {
            let created_at = timestamp_for(index + session.message_count);
            let action_type = action_type(index);
            let target = action_target(session.kind, index);
            let command = action_command(config, session.kind, index);
            let parent_message = parent_message_for_action(messages, index);
            let tags = serde_json::to_string(&action_tags(action_type, &target))
                .unwrap_or_else(|_| "[]".to_string());

            let body = format!(
                r#"{{
  _messageID: "{message_doc_id}"
  _sessionID: "{session_doc_id}"
  action_type: "{action_type}"
  target: "{target}"
  tags: {tags}
  created_at: "{created_at}"
  command: "{command}"
}}"#,
                message_doc_id = escape_graphql(&parent_message.doc_id),
                session_doc_id = escape_graphql(session_doc_id),
                action_type = escape_graphql(action_type),
                target = escape_graphql(&target),
                tags = tags,
                created_at = created_at.as_str(),
                command = escape_graphql(&command),
            );

            InsertedActionDoc {
                doc_id: String::new(),
                parent_message_id: parent_message.message_id.clone(),
                action_type: action_type.to_string(),
                target,
                created_at,
                command,
                body,
            }
        })
        .collect::<Vec<_>>();
    let bodies = docs.iter().map(|doc| doc.body.clone()).collect::<Vec<_>>();
    let doc_ids =
        execute_batched_add_collect_doc_ids(node, &session.session_id, "CodingAction", &bodies)
            .await?;
    Ok(docs
        .into_iter()
        .zip(doc_ids)
        .map(|(mut doc, doc_id)| {
            doc.doc_id = doc_id;
            doc
        })
        .collect())
}

pub(crate) async fn create_search_chunks(
    node: &EmbeddedNode,
    session: &FixtureSession,
    project_doc_id: &str,
    session_doc_id: &str,
    messages: &[InsertedMessageDoc],
    actions: &[InsertedActionDoc],
) -> Result<()> {
    let mut docs = Vec::new();
    for message in messages {
        let chunks = derive_search_chunks(
            &message.message_id,
            "content",
            &message.content,
            &DEFAULT_CHUNK_CONFIG,
        );
        for chunk in chunks {
            docs.push(format!(
                r#"{{
  chunk_id: "{chunk_id}"
  _projectID: "{project_doc_id}"
  _sessionID: "{session_doc_id}"
  _messageID: "{message_doc_id}"
  target_kind: "message"
  source_field: "content"
  session_id: "{session_id}"
  project_path: "{project_path}"
  parent_external_id: "{parent_external_id}"
  role: "{role}"
  chunk_index: {chunk_index}
  chunk_count: {chunk_count}
  created_at: "{created_at}"
  content: "{content}"
}}"#,
                chunk_id = escape_graphql(&chunk.chunk_id),
                project_doc_id = escape_graphql(project_doc_id),
                session_doc_id = escape_graphql(session_doc_id),
                message_doc_id = escape_graphql(&message.doc_id),
                session_id = escape_graphql(&session.session_id),
                project_path = escape_graphql(&session.project_path),
                parent_external_id = escape_graphql(&message.message_id),
                role = escape_graphql(&message.role),
                chunk_index = chunk.chunk_index,
                chunk_count = chunk.chunk_count,
                created_at = message.created_at.as_str(),
                content = escape_graphql(&chunk.content),
            ));
        }
    }

    for action in actions {
        let chunks = derive_search_chunks(
            &action.parent_message_id,
            "command",
            &action.command,
            &DEFAULT_CHUNK_CONFIG,
        );
        for chunk in chunks {
            docs.push(format!(
                r#"{{
  chunk_id: "{chunk_id}"
  _projectID: "{project_doc_id}"
  _sessionID: "{session_doc_id}"
  _actionID: "{action_doc_id}"
  target_kind: "action"
  source_field: "command"
  session_id: "{session_id}"
  project_path: "{project_path}"
  parent_external_id: "{parent_external_id}"
  action_type: "{action_type}"
  target: "{target}"
  chunk_index: {chunk_index}
  chunk_count: {chunk_count}
  created_at: "{created_at}"
  content: "{content}"
}}"#,
                chunk_id = escape_graphql(&chunk.chunk_id),
                project_doc_id = escape_graphql(project_doc_id),
                session_doc_id = escape_graphql(session_doc_id),
                action_doc_id = escape_graphql(&action.doc_id),
                session_id = escape_graphql(&session.session_id),
                project_path = escape_graphql(&session.project_path),
                parent_external_id = escape_graphql(&action_parent_external_id(action)),
                action_type = escape_graphql(&action.action_type),
                target = escape_graphql(&action.target),
                chunk_index = chunk.chunk_index,
                chunk_count = chunk.chunk_count,
                created_at = action.created_at.as_str(),
                content = escape_graphql(&chunk.content),
            ));
        }
    }

    execute_batched_add(node, &session.session_id, "CodingSearchChunk", &docs).await
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

async fn execute_batched_add_collect_doc_ids(
    node: &EmbeddedNode,
    context: &str,
    collection_name: &str,
    docs: &[String],
) -> Result<Vec<String>> {
    let mut doc_ids = Vec::with_capacity(docs.len());
    for chunk in docs.chunks(INSERT_BATCH_SIZE) {
        let mut query = String::from("mutation {\n");
        writeln!(&mut query, "  add_{collection_name}(input: [").unwrap();
        for doc in chunk {
            writeln!(&mut query, "    {doc}", doc = doc).unwrap();
        }
        writeln!(&mut query, "  ]) {{ _docID }}").unwrap();
        query.push('}');
        let data = ensure_success(node.execute(&query).await, context)?;
        let ids = data
            .get(format!("add_{collection_name}").as_str())
            .and_then(JsonValue::as_array)
            .ok_or_else(|| anyhow!("missing add_{collection_name} array in {data}"))?
            .iter()
            .map(|item| {
                item.get("_docID")
                    .and_then(JsonValue::as_str)
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| anyhow!("missing _docID in {item}"))
            })
            .collect::<Result<Vec<_>>>()?;
        doc_ids.extend(ids);
    }

    Ok(doc_ids)
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

fn repo_info(project_path: &str) -> (Option<String>, Option<String>) {
    for marker in ["github.com/", "github-com/"] {
        if let Some(index) = project_path.find(marker) {
            let after = &project_path[index + marker.len()..];
            let mut parts = after.trim_matches('/').split('/');
            let owner = parts.next().filter(|part| !part.is_empty());
            let repo = parts.next().filter(|part| !part.is_empty());
            return (owner.map(ToOwned::to_owned), repo.map(ToOwned::to_owned));
        }
    }

    (
        None,
        project_path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .filter(|part| !part.is_empty())
            .map(ToOwned::to_owned),
    )
}

fn timestamp_for(index: usize) -> String {
    let day = (index / 86_400) % 27 + 1;
    let hour = (index / 3_600) % 24;
    let minute = (index / 60) % 60;
    let second = index % 60;
    format!("2026-01-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn cli_version_for(source: &str) -> &'static str {
    match source {
        "codex" => "0.117.0",
        "claude" => "2.1.80",
        "gemini" => "0.9.1",
        _ => "0.1.0",
    }
}

fn session_title(session: &FixtureSession) -> String {
    format!(
        "{} search investigation for {}",
        session.source_label(),
        session.project_path.rsplit('/').next().unwrap_or("repo")
    )
}

fn session_summary(session: &FixtureSession) -> String {
    format!(
        "Session {} focuses on BM25 and dense retrieval over coding data in {} with {} messages and {} actions.",
        session.session_id, session.project_path, session.message_count, session.action_count
    )
}

fn first_prompt(session: &FixtureSession) -> String {
    format!(
        "Help investigate search quality in {} for session {}. Focus on BM25, embeddings, relation narrowing, and hybrid retrieval.",
        session.project_path, session.session_id
    )
}

fn fake_git_sha(session: &FixtureSession) -> String {
    format!(
        "{:040x}",
        session.session_id.bytes().map(u64::from).sum::<u64>()
    )
}

fn git_origin_url(project_path: &str) -> String {
    if let (Some(owner), Some(repo)) = repo_info(project_path) {
        format!("git@github.com:{owner}/{repo}.git")
    } else {
        "git@github.com:example/repo.git".to_string()
    }
}

fn agent_role(kind: SessionKind) -> &'static str {
    match kind {
        SessionKind::Hot => "default",
        SessionKind::Medium => "review",
        SessionKind::Background => "explorer",
    }
}

fn reasoning_effort(kind: SessionKind) -> &'static str {
    match kind {
        SessionKind::Hot => "high",
        SessionKind::Medium => "medium",
        SessionKind::Background => "low",
    }
}

fn assistant_tool_uses(index: usize) -> Vec<&'static str> {
    match index % 4 {
        0 => vec!["Read", "Search"],
        1 => vec!["Bash", "Read"],
        2 => vec!["Bash", "Search"],
        _ => vec!["Read"],
    }
}

fn files_referenced(kind: SessionKind, index: usize) -> Vec<String> {
    let primary = action_target(kind, index);
    let secondary = action_target(kind, index + 17);
    vec![primary, secondary]
}

fn action_tags(action_type: &str, target: &str) -> Vec<String> {
    let mut tags = vec![action_type.to_string()];
    if target.contains("nested") {
        tags.push("nested".to_string());
    }
    if target.contains("joins") {
        tags.push("joins".to_string());
    }
    if target.contains("type_join_many") {
        tags.push("type_join_many".to_string());
    }
    tags
}

fn parent_message_for_action(messages: &[InsertedMessageDoc], index: usize) -> &InsertedMessageDoc {
    let assistant_messages = messages
        .iter()
        .filter(|message| message.role == "assistant")
        .collect::<Vec<_>>();
    assistant_messages[index % assistant_messages.len()]
}

fn action_parent_external_id(action: &InsertedActionDoc) -> String {
    format!(
        "{}:{}:{}",
        action.parent_message_id, action.action_type, action.target
    )
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
            "I profiled the nested coding-session query and relation narrowing before scoring is still dominated by join work.\n\
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
        1 => "rg pushdown crates/query/src/planner/joins/mod.rs",
        2 => "rg bm25 crates/query/src/runner/query/nested.rs",
        3 => "cargo clippy --all -- -D warnings",
        4 => "cargo bench -p defra-node --features rocksdb --bin coding-session-bench",
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
    let mut terms = match index {
        1 => vec!["pushdown"],
        2 => vec!["wand"],
        3 => vec!["candidate"],
        _ => Vec::new(),
    };

    terms.extend(
        [
            (29, "wand"),
            (31, "pushdown"),
            (37, "candidate"),
            (43, "turbo"),
            (47, "bm25"),
        ]
        .into_iter()
        .filter_map(|(divisor, term)| index.is_multiple_of(divisor).then_some(term)),
    );
    terms.join(" ")
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
