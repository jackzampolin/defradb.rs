use std::fmt::Write as _;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value as JsonValue;

use crate::{EmbeddedNode, QueryResponse};

pub const CODING_SESSION_FIXTURE_SDL: &str = r#"
type CodingSession {
    session_id: String @index(unique: true)
    message_count: Int
    user_message_count: Int
    messages: [CodingMessage] @relation(name: "coding_session_messages")
    actions: [CodingAction] @relation(name: "coding_session_actions")
}

type CodingMessage {
    message_id: String @index(unique: true)
    session: CodingSession @relation(name: "coding_session_messages")
    sequence: Int @index
    role: String @index
    created_at: DateTime @index(direction: DESC)
    content: String @fulltext
}

type CodingAction {
    session: CodingSession @relation(name: "coding_session_actions")
    action_type: String @index
    target: String @index
    created_at: DateTime @index(direction: DESC)
    command: String @fulltext
}
"#;

const DEFAULT_SEARCH_LIMIT: usize = 10;
const INSERT_BATCH_SIZE: usize = 25;

#[derive(Debug, Clone)]
pub struct CodingSessionFixtureConfig {
    pub hot_session_messages: usize,
    pub hot_session_actions: usize,
    pub medium_session_messages: usize,
    pub medium_session_actions: usize,
    pub background_sessions: usize,
    pub background_session_messages: usize,
    pub background_session_actions: usize,
    pub user_message_bytes: usize,
    pub assistant_message_bytes: usize,
    pub action_command_bytes: usize,
}

impl Default for CodingSessionFixtureConfig {
    fn default() -> Self {
        Self {
            hot_session_messages: 1_500,
            hot_session_actions: 900,
            medium_session_messages: 500,
            medium_session_actions: 250,
            background_sessions: 12,
            background_session_messages: 120,
            background_session_actions: 60,
            user_message_bytes: 1_536,
            assistant_message_bytes: 6_144,
            action_command_bytes: 640,
        }
    }
}

impl CodingSessionFixtureConfig {
    pub fn smoke_test() -> Self {
        Self {
            hot_session_messages: 18,
            hot_session_actions: 12,
            medium_session_messages: 10,
            medium_session_actions: 6,
            background_sessions: 2,
            background_session_messages: 8,
            background_session_actions: 4,
            user_message_bytes: 320,
            assistant_message_bytes: 768,
            action_command_bytes: 192,
        }
    }

    pub fn large() -> Self {
        Self {
            hot_session_messages: 6_000,
            hot_session_actions: 3_000,
            medium_session_messages: 2_000,
            medium_session_actions: 900,
            background_sessions: 32,
            background_session_messages: 240,
            background_session_actions: 120,
            user_message_bytes: 2_048,
            assistant_message_bytes: 8_192,
            action_command_bytes: 768,
        }
    }

    pub fn layout(&self) -> CodingSessionFixture {
        let hot_session = FixtureSession::new(
            SessionKind::Hot,
            "fixture-hot-session",
            self.hot_session_messages,
            self.hot_session_actions,
        );
        let medium_session = FixtureSession::new(
            SessionKind::Medium,
            "fixture-medium-session",
            self.medium_session_messages,
            self.medium_session_actions,
        );
        let background_sessions = (0..self.background_sessions)
            .map(|index| {
                FixtureSession::new(
                    SessionKind::Background,
                    format!("fixture-background-session-{index:02}"),
                    self.background_session_messages,
                    self.background_session_actions,
                )
            })
            .collect();

        CodingSessionFixture {
            hot_session,
            medium_session,
            background_sessions,
        }
    }

    pub fn estimated_stats(&self) -> FixtureStats {
        let fixture = self.layout();
        let mut sessions = 0usize;
        let mut messages = 0usize;
        let mut actions = 0usize;
        let mut payload_bytes = 0usize;

        for session in fixture.all_sessions() {
            sessions += 1;
            messages += session.message_count;
            actions += session.action_count;
            payload_bytes += self.estimated_session_payload_bytes(session);
        }

        FixtureStats {
            sessions,
            messages,
            actions,
            estimated_payload_bytes: payload_bytes,
        }
    }

    fn estimated_session_payload_bytes(&self, session: &FixtureSession) -> usize {
        let user_messages = session.message_count.div_ceil(3);
        let assistant_messages = session.message_count - user_messages;

        user_messages * self.message_target_bytes(session.kind, "user")
            + assistant_messages * self.message_target_bytes(session.kind, "assistant")
            + session.action_count * self.action_target_bytes(session.kind)
    }

    fn message_target_bytes(&self, kind: SessionKind, role: &str) -> usize {
        let base = if role == "user" {
            self.user_message_bytes
        } else {
            self.assistant_message_bytes
        };
        scale_bytes(base, kind.message_size_scale_percent())
    }

    fn action_target_bytes(&self, kind: SessionKind) -> usize {
        scale_bytes(self.action_command_bytes, kind.action_size_scale_percent())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FixtureStats {
    pub sessions: usize,
    pub messages: usize,
    pub actions: usize,
    pub estimated_payload_bytes: usize,
}

impl FixtureStats {
    pub fn estimated_payload_mib(&self) -> f64 {
        self.estimated_payload_bytes as f64 / (1024.0 * 1024.0)
    }
}

#[derive(Debug, Clone)]
pub struct CodingSessionFixture {
    pub hot_session: FixtureSession,
    pub medium_session: FixtureSession,
    pub background_sessions: Vec<FixtureSession>,
}

impl CodingSessionFixture {
    pub fn all_sessions(&self) -> impl Iterator<Item = &FixtureSession> {
        std::iter::once(&self.hot_session)
            .chain(std::iter::once(&self.medium_session))
            .chain(self.background_sessions.iter())
    }

    pub fn default_cases(&self) -> Vec<SearchQueryCase> {
        vec![
            SearchQueryCase::new(
                "hot_messages_cargo",
                SearchTarget::Messages,
                self.hot_session.session_id.clone(),
                "cargo",
            ),
            SearchQueryCase::new(
                "hot_messages_wand",
                SearchTarget::Messages,
                self.hot_session.session_id.clone(),
                "wand",
            ),
            SearchQueryCase::new(
                "hot_actions_cargo",
                SearchTarget::Actions,
                self.hot_session.session_id.clone(),
                "cargo",
            ),
            SearchQueryCase::new(
                "hot_actions_rg",
                SearchTarget::Actions,
                self.hot_session.session_id.clone(),
                "rg",
            ),
            SearchQueryCase::new(
                "medium_messages_candidate",
                SearchTarget::Messages,
                self.medium_session.session_id.clone(),
                "candidate",
            ),
            SearchQueryCase::new(
                "medium_actions_bench",
                SearchTarget::Actions,
                self.medium_session.session_id.clone(),
                "bench",
            ),
        ]
    }
}

#[derive(Debug, Clone)]
pub struct FixtureSession {
    pub kind: SessionKind,
    pub session_id: String,
    pub message_count: usize,
    pub action_count: usize,
}

impl FixtureSession {
    fn new(
        kind: SessionKind,
        session_id: impl Into<String>,
        message_count: usize,
        action_count: usize,
    ) -> Self {
        Self {
            kind,
            session_id: session_id.into(),
            message_count,
            action_count,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionKind {
    Hot,
    Medium,
    Background,
}

impl SessionKind {
    fn message_size_scale_percent(self) -> usize {
        match self {
            Self::Hot => 100,
            Self::Medium => 70,
            Self::Background => 45,
        }
    }

    fn action_size_scale_percent(self) -> usize {
        match self {
            Self::Hot => 100,
            Self::Medium => 80,
            Self::Background => 55,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchTarget {
    Messages,
    Actions,
}

impl SearchTarget {
    pub fn field_name(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::Actions => "actions",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchQueryCase {
    pub name: String,
    pub target: SearchTarget,
    pub session_id: String,
    pub query: String,
    pub limit: usize,
    pub offset: usize,
}

impl SearchQueryCase {
    pub fn new(
        name: impl Into<String>,
        target: SearchTarget,
        session_id: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            target,
            session_id: session_id.into(),
            query: query.into(),
            limit: DEFAULT_SEARCH_LIMIT,
            offset: 0,
        }
    }

    pub fn render_query(&self, explain: bool) -> String {
        match self.target {
            SearchTarget::Messages => render_message_search_query(
                &self.session_id,
                &self.query,
                self.limit,
                self.offset,
                explain,
            ),
            SearchTarget::Actions => render_action_search_query(
                &self.session_id,
                &self.query,
                self.limit,
                self.offset,
                explain,
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkSummary {
    pub case_name: String,
    pub sample_count: usize,
    pub hit_count: usize,
    pub average: Duration,
    pub minimum: Duration,
    pub maximum: Duration,
    pub p50: Duration,
    pub p95: Duration,
}

impl BenchmarkSummary {
    pub fn render(&self) -> String {
        format!(
            "{}: samples={} hits={} avg={} p50={} p95={} min={} max={}",
            self.case_name,
            self.sample_count,
            self.hit_count,
            format_duration(self.average),
            format_duration(self.p50),
            format_duration(self.p95),
            format_duration(self.minimum),
            format_duration(self.maximum),
        )
    }
}

pub async fn seed_coding_session_fixture(
    node: &EmbeddedNode,
    config: &CodingSessionFixtureConfig,
) -> Result<CodingSessionFixture> {
    node.add_schema(CODING_SESSION_FIXTURE_SDL).await?;

    let fixture = config.layout();
    for session in fixture.all_sessions() {
        let session_doc_id = create_session(node, session).await?;
        create_messages(node, config, session, &session_doc_id).await?;
        create_actions(node, config, session, &session_doc_id).await?;
    }

    Ok(fixture)
}

pub async fn benchmark_case(
    node: &EmbeddedNode,
    case: &SearchQueryCase,
    warmup: usize,
    iterations: usize,
) -> Result<BenchmarkSummary> {
    if iterations == 0 {
        bail!("iterations must be greater than zero");
    }

    let query = case.render_query(false);
    for _ in 0..warmup {
        let response = node.execute(&query).await;
        ensure_success(response, &case.name)?;
    }

    let mut samples = Vec::with_capacity(iterations);
    let mut hit_count = 0usize;

    for _ in 0..iterations {
        let started_at = std::time::Instant::now();
        let response = node.execute(&query).await;
        let data = ensure_success(response, &case.name)?;
        samples.push(started_at.elapsed());
        hit_count = count_hits(&data, case.target);
    }

    Ok(summarize(case.name.clone(), hit_count, samples))
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

pub fn count_hits(data: &JsonValue, target: SearchTarget) -> usize {
    data.get("CodingSession")
        .and_then(JsonValue::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get(target.field_name()))
        .and_then(JsonValue::as_array)
        .map_or(0, Vec::len)
}

pub fn format_duration(duration: Duration) -> String {
    format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
}

fn wrap_query(body: String, explain: bool) -> String {
    if explain {
        format!("query @explain(type: execute) {body}")
    } else {
        body
    }
}

fn summarize(case_name: String, hit_count: usize, mut samples: Vec<Duration>) -> BenchmarkSummary {
    samples.sort_unstable();

    let total = samples
        .iter()
        .copied()
        .fold(Duration::ZERO, |acc, value| acc + value);
    let average = total / (samples.len() as u32);
    let minimum = *samples.first().unwrap_or(&Duration::ZERO);
    let maximum = *samples.last().unwrap_or(&Duration::ZERO);
    let p50 = percentile(&samples, 0.50);
    let p95 = percentile(&samples, 0.95);

    BenchmarkSummary {
        case_name,
        sample_count: samples.len(),
        hit_count,
        average,
        minimum,
        maximum,
        p50,
        p95,
    }
}

fn percentile(samples: &[Duration], percentile: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }

    let last_index = samples.len() - 1;
    let index = ((last_index as f64) * percentile).round() as usize;
    samples[index.min(last_index)]
}

async fn create_session(node: &EmbeddedNode, session: &FixtureSession) -> Result<String> {
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

async fn create_messages(
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

async fn create_actions(
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

fn ensure_success(response: QueryResponse, context: &str) -> Result<JsonValue> {
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

fn escape_graphql(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
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
        SessionKind::Hot if index % 7 == 0 => " -- candidate pushdown cargo",
        SessionKind::Medium if index % 5 == 0 => " -- bench rocksdb",
        SessionKind::Background if index % 11 == 0 => " -- rg noise",
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
    .filter_map(|(divisor, term)| (index % divisor == 0).then_some(term))
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
            ("user", 1) => format!(
                "Paths:\n- crates/query/src/plan/type_join/type_join_many/children.rs\n- crates/query/src/plan/type_join/type_join_many/plan_node.rs\n- crates/query/src/runner/query/nested.rs\n"
            ),
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

fn scale_bytes(base: usize, scale_percent: usize) -> usize {
    base.saturating_mul(scale_percent) / 100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn coding_session_fixture_smoke_test() {
        let node = crate::EmbeddedNode::builder().build().await.unwrap();
        let fixture = seed_coding_session_fixture(&node, &CodingSessionFixtureConfig::smoke_test())
            .await
            .unwrap();

        let message_case = fixture
            .default_cases()
            .into_iter()
            .find(|case| case.name == "hot_messages_cargo")
            .unwrap();
        let raw_message_data = ensure_success(
            node.execute(&message_case.render_query(false)).await,
            "smoke message raw",
        )
        .unwrap();
        assert!(count_hits(&raw_message_data, SearchTarget::Messages) > 0);

        let hot_session_data = ensure_success(
            node.execute(
                "{ CodingSession(filter: { session_id: { _eq: \"fixture-hot-session\" } }, limit: 1) { _docID session_id } }",
            )
            .await,
            "smoke session lookup",
        )
        .unwrap();
        let hot_session_doc_id = hot_session_data["CodingSession"][0]["_docID"]
            .as_str()
            .unwrap()
            .to_string();
        let direct_fk_filter = ensure_success(
            node.execute(&format!(
                "{{ CodingMessage(filter: {{ _sessionID: {{ _eq: \"{}\" }} }}, limit: 5) {{ message_id _sessionID }} }}",
                hot_session_doc_id
            ))
            .await,
            "smoke direct fk filter",
        )
        .unwrap();
        assert!(direct_fk_filter["CodingMessage"]
            .as_array()
            .is_some_and(|messages| !messages.is_empty()));
        let explain_message_data = ensure_success(
            node.execute(&message_case.render_query(true)).await,
            "smoke message explain",
        )
        .unwrap();
        assert!(explain_message_data.get("explain").is_some());
        let message_summary = benchmark_case(&node, &message_case, 0, 1).await.unwrap();
        assert!(message_summary.hit_count > 0);

        let action_query =
            render_action_search_query(&fixture.hot_session.session_id, "cargo", 10, 0, false);
        let action_data =
            ensure_success(node.execute(&action_query).await, "smoke action").unwrap();
        assert!(count_hits(&action_data, SearchTarget::Actions) > 0);
    }
}
