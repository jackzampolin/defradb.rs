use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::benchmark_data_gen::{
    create_actions, create_messages, create_project, create_search_chunks, create_session,
    ensure_success, scale_bytes,
};
use crate::benchmark_queries::{
    render_action_ranked_query, render_message_ranked_query, RankedQueryOrder,
};
use crate::benchmark_stats::summarize;
use crate::EmbeddedNode;

// Re-export extracted types so `benchmark_support::Foo` paths still resolve.
pub use crate::benchmark_queries::{
    count_hits, format_duration, render_action_search_query, render_message_search_query,
};
pub use crate::benchmark_stats::BenchmarkSummary;

pub const CODING_DATA_FIXTURE_SDL: &str = r#"
type CodingProject {
    path: String @index(unique: true)
    repo_name: String @index
    repo_owner: String @index
    sessions: [CodingSession]
    search_chunks: [CodingSearchChunk]
}

type CodingSession {
    session_id: String @index(unique: true)
    project: CodingProject
    git_branch: String @index
    source: String @index
    model_primary: String @index
    claude_version: String
    title: String @fulltext
    archived: Boolean @index
    git_sha: String
    git_origin_url: String
    agent_role: String @index
    reasoning_effort: String @index
    created_at: DateTime @index(direction: DESC)
    finished_at: DateTime
    message_count: Int
    user_message_count: Int
    input_tokens: Int
    output_tokens: Int
    tools_used: [String]
    first_prompt: String
    summary: String @fulltext
    messages: [CodingMessage] @relation(name: "coding_session_messages")
    actions: [CodingAction] @relation(name: "coding_session_actions")
    search_chunks: [CodingSearchChunk]
}

type CodingMessage {
    message_id: String @index(unique: true)
    session: CodingSession @relation(name: "coding_session_messages")
    sequence: Int @index
    role: String @index
    model: String
    created_at: DateTime @index(direction: DESC)
    content: String @fulltext
    tool_uses: [String]
    files_referenced: [String]
    input_tokens: Int
    output_tokens: Int
    search_chunks: [CodingSearchChunk]
}

type CodingAction {
    message: CodingMessage
    session: CodingSession @relation(name: "coding_session_actions")
    action_type: String @index
    target: String @index
    tags: [String]
    created_at: DateTime @index(direction: DESC)
    command: String @fulltext
    search_chunks: [CodingSearchChunk]
}

type CodingSearchChunk {
    chunk_id: String @index(unique: true)
    project: CodingProject
    session: CodingSession
    message: CodingMessage
    action: CodingAction
    target_kind: String @index
    source_field: String @index
    session_id: String @index
    project_path: String @index
    parent_external_id: String @index
    role: String @index
    action_type: String @index
    target: String @index
    chunk_index: Int @index
    chunk_count: Int
    created_at: DateTime @index(direction: DESC)
    content: String @fulltext
}
"#;

pub const CODING_DATA_EMBEDDING_FIXTURE_SDL: &str = r#"
type CodingProject {
    path: String @index(unique: true)
    repo_name: String @index
    repo_owner: String @index
    sessions: [CodingSession]
    search_chunks: [CodingSearchChunk]
}

type CodingSession {
    session_id: String @index(unique: true)
    project: CodingProject
    git_branch: String @index
    source: String @index
    model_primary: String @index
    claude_version: String
    title: String @fulltext
    archived: Boolean @index
    git_sha: String
    git_origin_url: String
    agent_role: String @index
    reasoning_effort: String @index
    created_at: DateTime @index(direction: DESC)
    finished_at: DateTime
    message_count: Int
    user_message_count: Int
    input_tokens: Int
    output_tokens: Int
    tools_used: [String]
    first_prompt: String
    summary: String @fulltext
    messages: [CodingMessage] @relation(name: "coding_session_messages")
    actions: [CodingAction] @relation(name: "coding_session_actions")
    search_chunks: [CodingSearchChunk]
}

type CodingMessage {
    message_id: String @index(unique: true)
    session: CodingSession @relation(name: "coding_session_messages")
    sequence: Int @index
    role: String @index
    model: String
    created_at: DateTime @index(direction: DESC)
    content: String @fulltext
    tool_uses: [String]
    files_referenced: [String]
    input_tokens: Int
    output_tokens: Int
    content_v: [Float32!] @index(vector: {dimensions: 14, hnsw: {metric: DOT}}) @embedding(provider: "openai", model: "coding-message-model", fields: ["content"])
    search_chunks: [CodingSearchChunk]
}

type CodingAction {
    message: CodingMessage
    session: CodingSession @relation(name: "coding_session_actions")
    action_type: String @index
    target: String @index
    tags: [String]
    created_at: DateTime @index(direction: DESC)
    command: String @fulltext
    command_v: [Float32!] @index(vector: {dimensions: 14, hnsw: {metric: DOT}}) @embedding(provider: "openai", model: "coding-action-model", fields: ["command"])
    search_chunks: [CodingSearchChunk]
}

type CodingSearchChunk {
    chunk_id: String @index(unique: true)
    project: CodingProject
    session: CodingSession
    message: CodingMessage
    action: CodingAction
    target_kind: String @index
    source_field: String @index
    session_id: String @index
    project_path: String @index
    parent_external_id: String @index
    role: String @index
    action_type: String @index
    target: String @index
    chunk_index: Int @index
    chunk_count: Int
    created_at: DateTime @index(direction: DESC)
    content: String @fulltext
    content_v: [Float32!] @index(vector: {dimensions: 14, hnsw: {metric: DOT}}) @embedding(provider: "openai", model: "coding-search-chunk-model", fields: ["content"])
}
"#;

pub const CODING_SESSION_FIXTURE_SDL: &str = CODING_DATA_FIXTURE_SDL;
pub const CODING_SESSION_EMBEDDING_FIXTURE_SDL: &str = CODING_DATA_EMBEDDING_FIXTURE_SDL;

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
            "/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs",
            self.hot_session_messages,
            self.hot_session_actions,
        );
        let medium_session = FixtureSession::new(
            SessionKind::Medium,
            "fixture-medium-session",
            "/Users/johnzampolin/go/src/github.com/jackzampolin/amygdala",
            self.medium_session_messages,
            self.medium_session_actions,
        );
        let background_projects = [
            "/Users/johnzampolin/go/src/github.com/sourcenetwork/hub-rs",
            "/Users/johnzampolin/go/src/github.com/jackzampolin/amygdala",
            "/Users/johnzampolin/go/src/github.com/mizufinance/bankd",
        ];
        let background_sessions = (0..self.background_sessions)
            .map(|index| {
                FixtureSession::new(
                    SessionKind::Background,
                    format!("fixture-background-session-{index:02}"),
                    background_projects[index % background_projects.len()],
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
    pub project_path: String,
    pub message_count: usize,
    pub action_count: usize,
}

impl FixtureSession {
    pub(crate) fn new(
        kind: SessionKind,
        session_id: impl Into<String>,
        project_path: impl Into<String>,
        message_count: usize,
        action_count: usize,
    ) -> Self {
        Self {
            kind,
            session_id: session_id.into(),
            project_path: project_path.into(),
            message_count,
            action_count,
        }
    }

    pub(crate) fn source_label(&self) -> &'static str {
        match self.kind {
            SessionKind::Hot => "codex",
            SessionKind::Medium => "claude",
            SessionKind::Background => "gemini",
        }
    }

    pub(crate) fn model_primary(&self) -> &'static str {
        match self.kind {
            SessionKind::Hot => "gpt-5.4",
            SessionKind::Medium => "claude-sonnet-4.5",
            SessionKind::Background => "gemini-2.5-pro",
        }
    }

    pub(crate) fn git_branch(&self) -> &'static str {
        match self.kind {
            SessionKind::Hot => "feat/coding-hybrid-search-api",
            SessionKind::Medium => "main",
            SessionKind::Background => "feat/indexing-playground",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodingTaskItem {
    pub id: String,
    pub label: String,
    pub clue_quotes: Vec<String>,
    pub item_quotes: Vec<String>,
    pub contains_truth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodingRetrievalTask {
    pub task_id: String,
    pub level: usize,
    pub target: SearchTarget,
    pub session_id: String,
    pub question: String,
    pub retrieval_query: String,
    pub truth: String,
    pub truth_type: String,
    pub supporting_items: Vec<CodingTaskItem>,
    pub items_and_contents: BTreeMap<String, String>,
    pub distractors: Vec<CodingTaskItem>,
}

impl CodingRetrievalTask {
    pub fn support_ids(&self) -> HashSet<String> {
        self.supporting_items
            .iter()
            .map(|item| item.id.clone())
            .collect()
    }

    pub fn distractor_ids(&self) -> HashSet<String> {
        self.distractors
            .iter()
            .map(|item| item.id.clone())
            .collect()
    }

    fn effective_query(&self) -> &str {
        if self.retrieval_query.trim().is_empty() {
            &self.question
        } else {
            &self.retrieval_query
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalStrategy {
    Bm25,
    Dense,
    Rrf,
}

impl RetrievalStrategy {
    fn label(self) -> &'static str {
        match self {
            Self::Bm25 => "bm25",
            Self::Dense => "dense",
            Self::Rrf => "rrf",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodingRankedResult {
    pub id: String,
    pub label: String,
    pub content: String,
    pub bm25_score: f64,
    pub dense_score: f64,
    pub rrf_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodingTaskRankings {
    pub bm25: Vec<CodingRankedResult>,
    pub dense: Vec<CodingRankedResult>,
    pub rrf: Vec<CodingRankedResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodingTaskMetrics {
    pub strategy: RetrievalStrategy,
    pub limit: usize,
    pub support_hits: usize,
    pub distractor_hits: usize,
    pub precision_at_k: f64,
    pub recall_at_k: f64,
    pub first_support_rank: Option<usize>,
    pub answer_found: bool,
    pub retrieved_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodingTaskEvaluation {
    pub task_id: String,
    pub question: String,
    pub truth: String,
    pub metrics: Vec<CodingTaskMetrics>,
}

impl CodingTaskEvaluation {
    pub fn metric(&self, strategy: RetrievalStrategy) -> Option<&CodingTaskMetrics> {
        self.metrics
            .iter()
            .find(|metric| metric.strategy == strategy)
    }

    pub fn render(&self) -> String {
        let rendered = self
            .metrics
            .iter()
            .map(|metric| {
                format!(
                    "{} hits={} distractors={} precision@{}={:.3} recall@{}={:.3} first_hit={:?}",
                    metric.strategy.label(),
                    metric.support_hits,
                    metric.distractor_hits,
                    metric.limit,
                    metric.precision_at_k,
                    metric.limit,
                    metric.recall_at_k,
                    metric.first_support_rank,
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");

        format!(
            "{} question=\"{}\" truth=\"{}\" {}",
            self.task_id, self.question, self.truth, rendered
        )
    }
}

#[derive(Debug, Clone)]
struct CodingCorpusRecord {
    doc_id: String,
    label: String,
    content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskSessionSelector {
    Hot,
    Medium,
}

#[derive(Debug, Clone, Copy)]
struct CodingTaskTemplate {
    task_id: &'static str,
    target: SearchTarget,
    session: TaskSessionSelector,
    level: usize,
    question: &'static str,
    retrieval_query: &'static str,
    truth_type: &'static str,
    support_all_terms: &'static [&'static str],
    distractor_any_terms: &'static [&'static str],
}

pub async fn build_context1_style_coding_tasks(
    node: &EmbeddedNode,
    fixture: &CodingSessionFixture,
) -> Result<Vec<CodingRetrievalTask>> {
    let hot_messages = fetch_message_records(node, &fixture.hot_session).await?;
    let medium_messages = fetch_message_records(node, &fixture.medium_session).await?;
    let hot_actions = fetch_action_records(node, &fixture.hot_session).await?;
    let medium_actions = fetch_action_records(node, &fixture.medium_session).await?;

    let templates = [
        CodingTaskTemplate {
            task_id: "hot_messages_pushdown",
            target: SearchTarget::Messages,
            session: TaskSessionSelector::Hot,
            level: 0,
            question:
                "Which hot-session messages discuss relation narrowing before BM25 scoring and explicitly mention pushdown?",
            retrieval_query: "relation narrowing bm25 pushdown",
            truth_type: "message_ids",
            support_all_terms: &["pushdown"],
            distractor_any_terms: &["candidate", "wand", "bm25"],
        },
        CodingTaskTemplate {
            task_id: "hot_messages_wand",
            target: SearchTarget::Messages,
            session: TaskSessionSelector::Hot,
            level: 0,
            question:
                "Which hot-session messages mention wand while debugging nested BM25 search behavior?",
            retrieval_query: "wand nested bm25 search",
            truth_type: "message_ids",
            support_all_terms: &["wand"],
            distractor_any_terms: &["pushdown", "candidate", "bm25"],
        },
        CodingTaskTemplate {
            task_id: "medium_messages_candidate",
            target: SearchTarget::Messages,
            session: TaskSessionSelector::Medium,
            level: 0,
            question:
                "Which medium-session messages talk about candidate relevance in the nested BM25 workload?",
            retrieval_query: "candidate relevance nested bm25",
            truth_type: "message_ids",
            support_all_terms: &["candidate"],
            distractor_any_terms: &["pushdown", "wand", "bm25"],
        },
        CodingTaskTemplate {
            task_id: "hot_actions_rg_pushdown",
            target: SearchTarget::Actions,
            session: TaskSessionSelector::Hot,
            level: 0,
            question:
                "Which hot-session commands grep for pushdown behavior in the planner join path?",
            retrieval_query: "rg pushdown planner joins",
            truth_type: "action_commands",
            support_all_terms: &["rg pushdown"],
            distractor_any_terms: &["rg ", "cargo test", "bm25"],
        },
        CodingTaskTemplate {
            task_id: "medium_actions_bench_rocksdb",
            target: SearchTarget::Actions,
            session: TaskSessionSelector::Medium,
            level: 0,
            question:
                "Which medium-session commands run the coding-session benchmark with bench rocksdb arguments?",
            retrieval_query: "coding-session-bench bench rocksdb",
            truth_type: "action_commands",
            support_all_terms: &["coding-session-bench", "bench rocksdb"],
            distractor_any_terms: &["coding-session-bench", "cargo bench", "rocksdb"],
        },
    ];

    let mut tasks = Vec::new();
    for template in templates {
        let (records, session_id) = match (template.target, template.session) {
            (SearchTarget::Messages, TaskSessionSelector::Hot) => {
                (&hot_messages, &fixture.hot_session.session_id)
            }
            (SearchTarget::Messages, TaskSessionSelector::Medium) => {
                (&medium_messages, &fixture.medium_session.session_id)
            }
            (SearchTarget::Actions, TaskSessionSelector::Hot) => {
                (&hot_actions, &fixture.hot_session.session_id)
            }
            (SearchTarget::Actions, TaskSessionSelector::Medium) => {
                (&medium_actions, &fixture.medium_session.session_id)
            }
        };

        if let Some(task) = build_task_from_records(records, session_id, &template) {
            tasks.push(task);
        }
    }

    if tasks.is_empty() {
        bail!("failed to build any context-1-style coding retrieval tasks");
    }

    Ok(tasks)
}

pub async fn retrieve_coding_task_rankings(
    node: &EmbeddedNode,
    task: &CodingRetrievalTask,
    query_embedding: &[f64],
    limit: usize,
) -> Result<CodingTaskRankings> {
    let session_doc_id = lookup_session_doc_id(node, &task.session_id).await?;
    let query = task.effective_query();

    let bm25_query = match task.target {
        SearchTarget::Messages => render_message_ranked_query(
            &session_doc_id,
            query,
            query_embedding,
            limit,
            0,
            RankedQueryOrder::Bm25,
            false,
        ),
        SearchTarget::Actions => render_action_ranked_query(
            &session_doc_id,
            query,
            query_embedding,
            limit,
            0,
            RankedQueryOrder::Bm25,
            false,
        ),
    };
    let dense_query = match task.target {
        SearchTarget::Messages => render_message_ranked_query(
            &session_doc_id,
            query,
            query_embedding,
            limit,
            0,
            RankedQueryOrder::Similarity,
            false,
        ),
        SearchTarget::Actions => render_action_ranked_query(
            &session_doc_id,
            query,
            query_embedding,
            limit,
            0,
            RankedQueryOrder::Similarity,
            false,
        ),
    };

    let bm25_data = ensure_success(node.execute(&bm25_query).await, &task.task_id)?;
    let dense_data = ensure_success(node.execute(&dense_query).await, &task.task_id)?;
    let bm25 = parse_ranked_results(&bm25_data, task.target)?;
    let dense = parse_ranked_results(&dense_data, task.target)?;
    let rrf = fuse_rankings_rrf(&bm25, &dense);

    Ok(CodingTaskRankings { bm25, dense, rrf })
}

pub async fn evaluate_coding_task(
    node: &EmbeddedNode,
    task: &CodingRetrievalTask,
    query_embedding: &[f64],
    limit: usize,
) -> Result<CodingTaskEvaluation> {
    let rankings = retrieve_coding_task_rankings(node, task, query_embedding, limit).await?;
    Ok(score_coding_task(task, &rankings))
}

pub fn score_coding_task(
    task: &CodingRetrievalTask,
    rankings: &CodingTaskRankings,
) -> CodingTaskEvaluation {
    CodingTaskEvaluation {
        task_id: task.task_id.clone(),
        question: task.question.clone(),
        truth: task.truth.clone(),
        metrics: vec![
            build_task_metrics(task, RetrievalStrategy::Bm25, &rankings.bm25),
            build_task_metrics(task, RetrievalStrategy::Dense, &rankings.dense),
            build_task_metrics(task, RetrievalStrategy::Rrf, &rankings.rrf),
        ],
    }
}

async fn fetch_message_records(
    node: &EmbeddedNode,
    session: &FixtureSession,
) -> Result<Vec<CodingCorpusRecord>> {
    let query = format!(
        r#"{{
  CodingSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
    messages(order: {{ sequence: ASC }}, limit: {limit}) {{
      _docID
      message_id
      content
    }}
  }}
}}"#,
        session_id = crate::benchmark_queries::escape_graphql(&session.session_id),
        limit = session.message_count,
    );
    let data = ensure_success(node.execute(&query).await, "fetch message records")?;
    let messages = data
        .get("CodingSession")
        .and_then(JsonValue::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get("messages"))
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("missing messages array for {}", session.session_id))?;

    messages
        .iter()
        .map(|message| {
            Ok(CodingCorpusRecord {
                doc_id: message
                    .get("_docID")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("missing message _docID in {}", message))?
                    .to_string(),
                label: message
                    .get("message_id")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("missing message_id in {}", message))?
                    .to_string(),
                content: message
                    .get("content")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("missing content in {}", message))?
                    .to_string(),
            })
        })
        .collect()
}

async fn fetch_action_records(
    node: &EmbeddedNode,
    session: &FixtureSession,
) -> Result<Vec<CodingCorpusRecord>> {
    let query = format!(
        r#"{{
  CodingSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
    actions(order: {{ created_at: ASC }}, limit: {limit}) {{
      _docID
      action_type
      target
      command
    }}
  }}
}}"#,
        session_id = crate::benchmark_queries::escape_graphql(&session.session_id),
        limit = session.action_count,
    );
    let data = ensure_success(node.execute(&query).await, "fetch action records")?;
    let actions = data
        .get("CodingSession")
        .and_then(JsonValue::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get("actions"))
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("missing actions array for {}", session.session_id))?;

    actions
        .iter()
        .map(|action| {
            let action_type = action
                .get("action_type")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| anyhow!("missing action_type in {}", action))?;
            let target = action
                .get("target")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| anyhow!("missing target in {}", action))?;

            Ok(CodingCorpusRecord {
                doc_id: action
                    .get("_docID")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("missing action _docID in {}", action))?
                    .to_string(),
                label: format!("{action_type} {target}"),
                content: action
                    .get("command")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("missing command in {}", action))?
                    .to_string(),
            })
        })
        .collect()
}

async fn lookup_session_doc_id(node: &EmbeddedNode, session_id: &str) -> Result<String> {
    let query = format!(
        r#"{{
  CodingSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
    _docID
  }}
}}"#,
        session_id = crate::benchmark_queries::escape_graphql(session_id),
    );
    let data = ensure_success(node.execute(&query).await, "lookup session doc id")?;
    data.get("CodingSession")
        .and_then(JsonValue::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get("_docID"))
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing session _docID for {}", session_id))
}

fn build_task_from_records(
    records: &[CodingCorpusRecord],
    session_id: &str,
    template: &CodingTaskTemplate,
) -> Option<CodingRetrievalTask> {
    let support_records = records
        .iter()
        .filter(|record| contains_all_terms(&record.content, template.support_all_terms))
        .take(3)
        .cloned()
        .collect::<Vec<_>>();
    if support_records.is_empty() {
        return None;
    }

    let distractor_records = records
        .iter()
        .filter(|record| {
            !contains_all_terms(&record.content, template.support_all_terms)
                && contains_any_term(&record.content, template.distractor_any_terms)
        })
        .take(3)
        .cloned()
        .collect::<Vec<_>>();
    if distractor_records.is_empty() {
        return None;
    }

    let supporting_items = support_records
        .iter()
        .map(|record| CodingTaskItem {
            id: record.doc_id.clone(),
            label: record.label.clone(),
            clue_quotes: template
                .support_all_terms
                .iter()
                .map(|term| term.to_string())
                .collect(),
            item_quotes: extract_matching_quotes(&record.content, template.support_all_terms),
            contains_truth: true,
        })
        .collect::<Vec<_>>();

    let distractors = distractor_records
        .iter()
        .map(|record| CodingTaskItem {
            id: record.doc_id.clone(),
            label: record.label.clone(),
            clue_quotes: template
                .distractor_any_terms
                .iter()
                .map(|term| term.to_string())
                .collect(),
            item_quotes: extract_matching_quotes(&record.content, template.distractor_any_terms),
            contains_truth: false,
        })
        .collect::<Vec<_>>();

    let mut items_and_contents = BTreeMap::new();
    for record in support_records.iter().chain(distractor_records.iter()) {
        items_and_contents.insert(record.doc_id.clone(), record.content.clone());
    }

    Some(CodingRetrievalTask {
        task_id: template.task_id.to_string(),
        level: template.level,
        target: template.target,
        session_id: session_id.to_string(),
        question: template.question.to_string(),
        retrieval_query: template.retrieval_query.to_string(),
        truth: support_records
            .iter()
            .map(|record| record.label.clone())
            .collect::<Vec<_>>()
            .join(" | "),
        truth_type: template.truth_type.to_string(),
        supporting_items,
        items_and_contents,
        distractors,
    })
}

fn contains_all_terms(content: &str, terms: &[&str]) -> bool {
    let lowercase = content.to_ascii_lowercase();
    terms
        .iter()
        .all(|term| lowercase.contains(&term.to_ascii_lowercase()))
}

fn contains_any_term(content: &str, terms: &[&str]) -> bool {
    let lowercase = content.to_ascii_lowercase();
    terms
        .iter()
        .any(|term| lowercase.contains(&term.to_ascii_lowercase()))
}

fn extract_matching_quotes(content: &str, terms: &[&str]) -> Vec<String> {
    let mut quotes = Vec::new();
    for term in terms {
        if let Some(quote) = extract_quote(content, term) {
            quotes.push(quote);
        }
    }

    if quotes.is_empty() {
        quotes.push(content.lines().next().unwrap_or(content).trim().to_string());
    }

    quotes
}

fn extract_quote(content: &str, term: &str) -> Option<String> {
    let lowercase_content = content.to_ascii_lowercase();
    let lowercase_term = term.to_ascii_lowercase();
    let start = lowercase_content.find(&lowercase_term)?;
    let quote_start = start.saturating_sub(48);
    let quote_end = (start + term.len() + 48).min(content.len());
    Some(content[quote_start..quote_end].trim().replace('\n', " "))
}

fn parse_ranked_results(data: &JsonValue, target: SearchTarget) -> Result<Vec<CodingRankedResult>> {
    let collection_name = match target {
        SearchTarget::Messages => "CodingMessage",
        SearchTarget::Actions => "CodingAction",
    };
    let items = data
        .get(collection_name)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("missing {collection_name} array in {data}"))?;

    items
        .iter()
        .map(|item| {
            let (label, content) = match target {
                SearchTarget::Messages => (
                    item.get("message_id")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow!("missing message_id in {}", item))?
                        .to_string(),
                    item.get("content")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow!("missing content in {}", item))?
                        .to_string(),
                ),
                SearchTarget::Actions => (
                    format!(
                        "{} {}",
                        item.get("action_type")
                            .and_then(JsonValue::as_str)
                            .ok_or_else(|| anyhow!("missing action_type in {}", item))?,
                        item.get("target")
                            .and_then(JsonValue::as_str)
                            .ok_or_else(|| anyhow!("missing target in {}", item))?,
                    ),
                    item.get("command")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow!("missing command in {}", item))?
                        .to_string(),
                ),
            };

            Ok(CodingRankedResult {
                id: item
                    .get("_docID")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| anyhow!("missing _docID in {}", item))?
                    .to_string(),
                label,
                content,
                bm25_score: item
                    .get("bm25")
                    .and_then(JsonValue::as_f64)
                    .unwrap_or_default(),
                dense_score: item
                    .get("sim")
                    .and_then(JsonValue::as_f64)
                    .unwrap_or_default(),
                rrf_score: 0.0,
            })
        })
        .collect()
}

fn fuse_rankings_rrf(
    bm25: &[CodingRankedResult],
    dense: &[CodingRankedResult],
) -> Vec<CodingRankedResult> {
    const RRF_RANK_BIAS: f64 = 60.0;

    let mut fused = HashMap::<String, CodingRankedResult>::new();

    for (index, hit) in bm25.iter().enumerate() {
        let entry = fused.entry(hit.id.clone()).or_insert_with(|| hit.clone());
        entry.rrf_score += 1.0 / (RRF_RANK_BIAS + (index + 1) as f64);
        entry.bm25_score = hit.bm25_score;
        entry.dense_score = hit.dense_score;
    }

    for (index, hit) in dense.iter().enumerate() {
        let entry = fused.entry(hit.id.clone()).or_insert_with(|| hit.clone());
        entry.rrf_score += 1.0 / (RRF_RANK_BIAS + (index + 1) as f64);
        entry.bm25_score = hit.bm25_score;
        entry.dense_score = hit.dense_score;
        if entry.content.is_empty() {
            entry.content = hit.content.clone();
        }
    }

    let mut fused = fused.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .rrf_score
            .partial_cmp(&left.rrf_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right
                    .dense_score
                    .partial_cmp(&left.dense_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                right
                    .bm25_score
                    .partial_cmp(&left.bm25_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.id.cmp(&right.id))
    });

    fused
}

fn build_task_metrics(
    task: &CodingRetrievalTask,
    strategy: RetrievalStrategy,
    results: &[CodingRankedResult],
) -> CodingTaskMetrics {
    let support_ids = task.support_ids();
    let distractor_ids = task.distractor_ids();
    let support_hits = results
        .iter()
        .filter(|result| support_ids.contains(&result.id))
        .count();
    let distractor_hits = results
        .iter()
        .filter(|result| distractor_ids.contains(&result.id))
        .count();
    let first_support_rank = results
        .iter()
        .position(|result| support_ids.contains(&result.id))
        .map(|index| index + 1);
    let limit = results.len();
    let precision_at_k = if limit == 0 {
        0.0
    } else {
        support_hits as f64 / limit as f64
    };
    let recall_at_k = if support_ids.is_empty() {
        0.0
    } else {
        support_hits as f64 / support_ids.len() as f64
    };

    CodingTaskMetrics {
        strategy,
        limit,
        support_hits,
        distractor_hits,
        precision_at_k,
        recall_at_k,
        first_support_rank,
        answer_found: first_support_rank.is_some(),
        retrieved_ids: results.iter().map(|result| result.id.clone()).collect(),
    }
}

pub async fn seed_coding_session_fixture(
    node: &EmbeddedNode,
    config: &CodingSessionFixtureConfig,
) -> Result<CodingSessionFixture> {
    seed_coding_session_fixture_with_schema(node, config, CODING_SESSION_FIXTURE_SDL).await
}

pub async fn seed_coding_session_embedding_fixture(
    node: &EmbeddedNode,
    config: &CodingSessionFixtureConfig,
) -> Result<CodingSessionFixture> {
    seed_coding_session_fixture_with_schema(node, config, CODING_SESSION_EMBEDDING_FIXTURE_SDL)
        .await
}

async fn seed_coding_session_fixture_with_schema(
    node: &EmbeddedNode,
    config: &CodingSessionFixtureConfig,
    sdl: &str,
) -> Result<CodingSessionFixture> {
    let mut project_doc_ids: HashMap<String, String> = HashMap::new();
    node.add_schema(sdl).await?;

    let fixture = config.layout();
    for session in fixture.all_sessions() {
        let project_doc_id = if let Some(doc_id) = project_doc_ids.get(&session.project_path) {
            doc_id.clone()
        } else {
            let doc_id = create_project(node, &session.project_path).await?;
            project_doc_ids.insert(session.project_path.clone(), doc_id.clone());
            doc_id
        };
        let session_doc_id = create_session(node, session, &project_doc_id).await?;
        let messages = create_messages(node, config, session, &session_doc_id).await?;
        let actions = create_actions(node, config, session, &session_doc_id, &messages).await?;
        create_search_chunks(
            node,
            session,
            &project_doc_id,
            &session_doc_id,
            &messages,
            &actions,
        )
        .await?;
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
#[path = "benchmark_support_tests.rs"]
mod tests;
