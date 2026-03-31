//! Search query generation for benchmark fixtures.

use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::benchmark_support::SearchTarget;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RankedQueryOrder {
    Bm25,
    Similarity,
}

impl RankedQueryOrder {
    fn alias(self) -> &'static str {
        match self {
            Self::Bm25 => "bm25",
            Self::Similarity => "sim",
        }
    }
}

pub fn render_message_search_query(
    session_id: &str,
    query: &str,
    limit: usize,
    offset: usize,
    explain: bool,
) -> String {
    wrap_query(
        format!(
            r#"{{
  CodingSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
    session_id
    messages(
      order: {{ _alias: {{ score: DESC }} }}
      limit: {limit}
      offset: {offset}
    ) {{
      message_id
      sequence
      role
      created_at
      score: BM25(query: "{query}", fields: ["content"])
      content
    }}
  }}
}}"#,
            session_id = escape_graphql(session_id),
            query = escape_graphql(query),
        ),
        explain,
    )
}

pub fn render_action_search_query(
    session_id: &str,
    query: &str,
    limit: usize,
    offset: usize,
    explain: bool,
) -> String {
    wrap_query(
        format!(
            r#"{{
  CodingSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
    session_id
    actions(
      order: {{ _alias: {{ score: DESC }} }}
      limit: {limit}
      offset: {offset}
    ) {{
      action_type
      target
      created_at
      score: BM25(query: "{query}", fields: ["command"])
      command
    }}
  }}
}}"#,
            session_id = escape_graphql(session_id),
            query = escape_graphql(query),
        ),
        explain,
    )
}

pub(crate) fn render_message_ranked_query(
    session_doc_id: &str,
    query: &str,
    vector: &[f64],
    limit: usize,
    offset: usize,
    order: RankedQueryOrder,
    explain: bool,
) -> String {
    wrap_query(
        format!(
            r#"{{
  CodingMessage(
    filter: {{ _sessionID: {{ _eq: "{session_doc_id}" }} }}
    order: {{ _alias: {{ {order_alias}: DESC }} }}
    limit: {limit}
    offset: {offset}
  ) {{
    _docID
    message_id
    bm25: BM25(query: "{query}", fields: ["content"])
    sim: SIMILARITY(content_v: {{vector: [{vector}]}})
    content
  }}
}}"#,
            session_doc_id = escape_graphql(session_doc_id),
            order_alias = order.alias(),
            query = escape_graphql(query),
            vector = format_vector(vector),
        ),
        explain,
    )
}

pub(crate) fn render_action_ranked_query(
    session_doc_id: &str,
    query: &str,
    vector: &[f64],
    limit: usize,
    offset: usize,
    order: RankedQueryOrder,
    explain: bool,
) -> String {
    wrap_query(
        format!(
            r#"{{
  CodingAction(
    filter: {{ _sessionID: {{ _eq: "{session_doc_id}" }} }}
    order: {{ _alias: {{ {order_alias}: DESC }} }}
    limit: {limit}
    offset: {offset}
  ) {{
    _docID
    action_type
    target
    bm25: BM25(query: "{query}", fields: ["command"])
    sim: SIMILARITY(command_v: {{vector: [{vector}]}})
    command
  }}
}}"#,
            session_doc_id = escape_graphql(session_doc_id),
            order_alias = order.alias(),
            query = escape_graphql(query),
            vector = format_vector(vector),
        ),
        explain,
    )
}

pub fn count_hits(data: &JsonValue, target: SearchTarget) -> usize {
    data.get("CodingSession")
        .and_then(JsonValue::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get(target.field_name()))
        .and_then(JsonValue::as_array)
        .map_or(0, Vec::len)
}

pub(crate) fn wrap_query(body: String, explain: bool) -> String {
    if explain {
        format!("query @explain(type: execute) {body}")
    } else {
        body
    }
}

pub fn format_duration(duration: Duration) -> String {
    format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
}

pub(crate) fn escape_graphql(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

pub(crate) fn format_vector(vector: &[f64]) -> String {
    vector
        .iter()
        .map(|value| format!("{value:.8}"))
        .collect::<Vec<_>>()
        .join(", ")
}
