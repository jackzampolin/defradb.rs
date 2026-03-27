use anyhow::{bail, Result};

use crate::benchmark_data_gen::{
    create_actions, create_messages, create_session, ensure_success, scale_bytes,
};
use crate::benchmark_stats::summarize;
use crate::EmbeddedNode;

// Re-export extracted types so `benchmark_support::Foo` paths still resolve.
pub use crate::benchmark_queries::{
    count_hits, format_duration, render_action_search_query, render_message_search_query,
};
pub use crate::benchmark_stats::BenchmarkSummary;

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

    pub(crate) fn message_target_bytes(&self, kind: SessionKind, role: &str) -> usize {
        let base = if role == "user" {
            self.user_message_bytes
        } else {
            self.assistant_message_bytes
        };
        scale_bytes(base, kind.message_size_scale_percent())
    }

    pub(crate) fn action_target_bytes(&self, kind: SessionKind) -> usize {
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
    pub(crate) fn new(
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
#[non_exhaustive]
pub enum SessionKind {
    Hot,
    Medium,
    Background,
}

impl SessionKind {
    pub(crate) fn message_size_scale_percent(self) -> usize {
        match self {
            Self::Hot => 100,
            Self::Medium => 70,
            Self::Background => 45,
        }
    }

    pub(crate) fn action_size_scale_percent(self) -> usize {
        match self {
            Self::Hot => 100,
            Self::Medium => 80,
            Self::Background => 55,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
