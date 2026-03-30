use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::benchmark_queries::{escape_graphql, format_vector};
use crate::EmbeddedNode;

const DEFAULT_LIMIT: usize = 10;
const DEFAULT_CANDIDATE_LIMIT: usize = 0;
const DEFAULT_RRF_K: f64 = 60.0;
const MESSAGE_EMBEDDING_MODEL: &str = "coding-message-model";
const ACTION_EMBEDDING_MODEL: &str = "coding-action-model";

/// Coding-corpus search target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodingSearchTarget {
    Messages,
    Actions,
}

impl CodingSearchTarget {
    fn default_embedding_model(self) -> &'static str {
        match self {
            Self::Messages => MESSAGE_EMBEDDING_MODEL,
            Self::Actions => ACTION_EMBEDDING_MODEL,
        }
    }

    fn relation_field(self) -> &'static str {
        "_sessionID"
    }

    fn collection_name(self) -> &'static str {
        match self {
            Self::Messages => "CodingMessage",
            Self::Actions => "CodingAction",
        }
    }
}

/// Request for hybrid BM25 + dense retrieval over the coding corpus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodingHybridSearchRequest {
    pub target: CodingSearchTarget,
    pub query_text: String,
    pub session_id: Option<String>,
    pub limit: usize,
    pub candidate_limit: usize,
    pub exclude_doc_ids: Vec<String>,
    pub embedding_model: Option<String>,
}

impl CodingHybridSearchRequest {
    pub fn new(target: CodingSearchTarget, query_text: impl Into<String>) -> Self {
        Self {
            target,
            query_text: query_text.into(),
            session_id: None,
            limit: DEFAULT_LIMIT,
            candidate_limit: DEFAULT_CANDIDATE_LIMIT,
            exclude_doc_ids: Vec::new(),
            embedding_model: None,
        }
    }

    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_candidate_limit(mut self, candidate_limit: usize) -> Self {
        self.candidate_limit = candidate_limit;
        self
    }

    pub fn with_excluded_doc_ids(mut self, exclude_doc_ids: impl Into<Vec<String>>) -> Self {
        self.exclude_doc_ids = exclude_doc_ids.into();
        self
    }

    pub fn with_embedding_model(mut self, embedding_model: impl Into<String>) -> Self {
        self.embedding_model = Some(embedding_model.into());
        self
    }
}

/// Search hit returned by coding-corpus hybrid retrieval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CodingHybridSearchHit {
    pub doc_id: String,
    pub label: String,
    pub content: String,
    pub bm25_score: f64,
    pub dense_score: f64,
    pub fused_score: f64,
    pub bm25_rank: Option<usize>,
    pub dense_rank: Option<usize>,
}

impl CodingHybridSearchHit {
    fn best_rank(&self) -> usize {
        self.bm25_rank
            .unwrap_or(usize::MAX)
            .min(self.dense_rank.unwrap_or(usize::MAX))
    }
}

/// Response returned by coding-corpus hybrid retrieval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CodingHybridSearchResponse {
    pub target: CodingSearchTarget,
    pub query_text: String,
    pub session_id: Option<String>,
    pub embedding_model: String,
    pub query_vector_dimensions: usize,
    pub bm25_candidates: Vec<CodingHybridSearchHit>,
    pub dense_candidates: Vec<CodingHybridSearchHit>,
    pub hits: Vec<CodingHybridSearchHit>,
}

impl EmbeddedNode {
    /// Embed a free-form query text using the node's configured embedding endpoint.
    ///
    /// `model` defaults to the node-level fallback model when omitted.
    pub async fn embed_text(&self, text: &str, model: Option<&str>) -> Result<Vec<f64>> {
        embed_text_with_config(self.embedding_config(), text, model).await
    }

    /// Run hybrid BM25 + dense retrieval over the coding benchmark corpus.
    ///
    /// Dense retrieval uses the node's configured embedding endpoint and requires
    /// an embedding model compatible with the target collection's stored vectors.
    pub async fn hybrid_search_coding(
        &self,
        request: &CodingHybridSearchRequest,
    ) -> Result<CodingHybridSearchResponse> {
        hybrid_search_coding(self, request).await
    }
}

async fn hybrid_search_coding(
    node: &EmbeddedNode,
    request: &CodingHybridSearchRequest,
) -> Result<CodingHybridSearchResponse> {
    if request.query_text.trim().is_empty() {
        bail!("query_text must not be empty");
    }
    if request.limit == 0 {
        bail!("limit must be greater than zero");
    }

    let candidate_limit = if request.candidate_limit == 0 {
        request.limit
    } else {
        request.candidate_limit.max(request.limit)
    };
    let embedding_model = request
        .embedding_model
        .clone()
        .unwrap_or_else(|| request.target.default_embedding_model().to_string());
    let query_vector = node
        .embed_text(&request.query_text, Some(&embedding_model))
        .await?;
    let session_doc_id = if let Some(session_id) = request.session_id.as_deref() {
        Some(lookup_session_doc_id(node, session_id).await?)
    } else {
        None
    };

    let bm25_query = render_coding_ranked_query(
        request.target,
        session_doc_id.as_deref(),
        &request.exclude_doc_ids,
        &request.query_text,
        &query_vector,
        candidate_limit,
        RankedOrder::Bm25,
    );
    let dense_query = render_coding_ranked_query(
        request.target,
        session_doc_id.as_deref(),
        &request.exclude_doc_ids,
        &request.query_text,
        &query_vector,
        candidate_limit,
        RankedOrder::Dense,
    );

    let bm25_data = require_success(node.execute(&bm25_query).await, "coding bm25 search")?;
    let dense_data = require_success(node.execute(&dense_query).await, "coding dense search")?;
    let mut bm25_candidates = parse_hits(&bm25_data, request.target)?;
    let mut dense_candidates = parse_hits(&dense_data, request.target)?;

    for (index, hit) in bm25_candidates.iter_mut().enumerate() {
        hit.bm25_rank = Some(index + 1);
    }
    for (index, hit) in dense_candidates.iter_mut().enumerate() {
        hit.dense_rank = Some(index + 1);
    }

    let hits = fuse_rankings_rrf(&bm25_candidates, &dense_candidates, request.limit);

    Ok(CodingHybridSearchResponse {
        target: request.target,
        query_text: request.query_text.clone(),
        session_id: request.session_id.clone(),
        embedding_model,
        query_vector_dimensions: query_vector.len(),
        bm25_candidates,
        dense_candidates,
        hits,
    })
}

async fn embed_text_with_config(
    config: &db::EmbeddingClientConfig,
    text: &str,
    model: Option<&str>,
) -> Result<Vec<f64>> {
    let url = config.url.trim();
    if url.is_empty() {
        bail!("embedded node is missing an embedding URL; configure with with_embedding_url(...)");
    }

    let resolved_model = model
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .or_else(|| {
            let default_model = config.model.trim();
            (!default_model.is_empty()).then_some(default_model)
        })
        .ok_or_else(|| anyhow!("embedded node is missing an embedding model"))?;

    let endpoint = format!("{}/embeddings", url.trim_end_matches('/'));
    let mut request = embedding_client().post(endpoint).json(&serde_json::json!({
        "model": resolved_model,
        "input": text,
    }));
    if !config.api_key.is_empty() {
        request = request.bearer_auth(&config.api_key);
    }

    let response = request.send().await?;
    let response = response.error_for_status()?;
    let body: JsonValue = response.json().await?;
    let values = body
        .pointer("/data/0/embedding")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("missing data[0].embedding in embedding response: {}", body))?;

    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_f64()
                .ok_or_else(|| anyhow!("embedding value at index {} is not numeric", index))
        })
        .collect()
}

fn embedding_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

async fn lookup_session_doc_id(node: &EmbeddedNode, session_id: &str) -> Result<String> {
    let query = format!(
        r#"{{
  CodingSession(filter: {{ session_id: {{ _eq: "{session_id}" }} }}, limit: 1) {{
    _docID
  }}
}}"#,
        session_id = escape_graphql(session_id),
    );
    let data = require_success(node.execute(&query).await, "lookup coding session")?;
    data.get("CodingSession")
        .and_then(JsonValue::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get("_docID"))
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("no CodingSession found for session_id={}", session_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RankedOrder {
    Bm25,
    Dense,
}

impl RankedOrder {
    fn alias(self) -> &'static str {
        match self {
            Self::Bm25 => "bm25",
            Self::Dense => "sim",
        }
    }
}

fn render_coding_ranked_query(
    target: CodingSearchTarget,
    session_doc_id: Option<&str>,
    exclude_doc_ids: &[String],
    query_text: &str,
    vector: &[f64],
    limit: usize,
    order: RankedOrder,
) -> String {
    let mut args = Vec::new();
    if let Some(filter_clause) = render_filter_clause(target, session_doc_id, exclude_doc_ids) {
        args.push(filter_clause);
    }
    args.push(format!(
        "order: {{ _alias: {{ {}: DESC }} }}",
        order.alias()
    ));
    args.push(format!("limit: {}", limit));

    match target {
        CodingSearchTarget::Messages => format!(
            r#"{{
  CodingMessage(
    {args}
  ) {{
    _docID
    message_id
    bm25: BM25(query: "{query_text}", fields: ["content"])
    sim: SIMILARITY(content_v: {{vector: [{vector}]}})
    content
  }}
}}"#,
            args = args.join("\n    "),
            query_text = escape_graphql(query_text),
            vector = format_vector(vector),
        ),
        CodingSearchTarget::Actions => format!(
            r#"{{
  CodingAction(
    {args}
  ) {{
    _docID
    action_type
    target
    bm25: BM25(query: "{query_text}", fields: ["command"])
    sim: SIMILARITY(command_v: {{vector: [{vector}]}})
    command
  }}
}}"#,
            args = args.join("\n    "),
            query_text = escape_graphql(query_text),
            vector = format_vector(vector),
        ),
    }
}

fn render_filter_clause(
    target: CodingSearchTarget,
    session_doc_id: Option<&str>,
    exclude_doc_ids: &[String],
) -> Option<String> {
    let mut clauses = Vec::new();

    if let Some(session_doc_id) = session_doc_id {
        clauses.push(format!(
            "{}: {{ _eq: \"{}\" }}",
            target.relation_field(),
            escape_graphql(session_doc_id),
        ));
    }

    if !exclude_doc_ids.is_empty() {
        let values = exclude_doc_ids
            .iter()
            .map(|doc_id| format!("\"{}\"", escape_graphql(doc_id)))
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!("_docID: {{ _nin: [{}] }}", values));
    }

    match clauses.len() {
        0 => None,
        1 => Some(format!("filter: {{ {} }}", clauses[0])),
        _ => Some(format!(
            "filter: {{ _and: [{}] }}",
            clauses
                .into_iter()
                .map(|clause| format!("{{ {} }}", clause))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn parse_hits(data: &JsonValue, target: CodingSearchTarget) -> Result<Vec<CodingHybridSearchHit>> {
    let items = data
        .get(target.collection_name())
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("missing {} array in {}", target.collection_name(), data))?;

    items
        .iter()
        .map(|item| {
            let doc_id = item
                .get("_docID")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| anyhow!("missing _docID in {}", item))?
                .to_string();

            let (label, content) = match target {
                CodingSearchTarget::Messages => (
                    item.get("message_id")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow!("missing message_id in {}", item))?
                        .to_string(),
                    item.get("content")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow!("missing content in {}", item))?
                        .to_string(),
                ),
                CodingSearchTarget::Actions => (
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

            Ok(CodingHybridSearchHit {
                doc_id,
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
                fused_score: 0.0,
                bm25_rank: None,
                dense_rank: None,
            })
        })
        .collect()
}

fn fuse_rankings_rrf(
    bm25_candidates: &[CodingHybridSearchHit],
    dense_candidates: &[CodingHybridSearchHit],
    limit: usize,
) -> Vec<CodingHybridSearchHit> {
    let mut fused = HashMap::<String, CodingHybridSearchHit>::new();

    for hit in bm25_candidates {
        let rank = hit.bm25_rank.unwrap_or(usize::MAX);
        let entry = fused
            .entry(hit.doc_id.clone())
            .or_insert_with(|| hit.clone());
        entry.fused_score += 1.0 / (DEFAULT_RRF_K + rank as f64);
        entry.bm25_rank = hit.bm25_rank;
        entry.bm25_score = hit.bm25_score;
        entry.dense_score = hit.dense_score;
    }

    for hit in dense_candidates {
        let rank = hit.dense_rank.unwrap_or(usize::MAX);
        let entry = fused
            .entry(hit.doc_id.clone())
            .or_insert_with(|| hit.clone());
        entry.fused_score += 1.0 / (DEFAULT_RRF_K + rank as f64);
        entry.dense_rank = hit.dense_rank;
        entry.bm25_score = hit.bm25_score;
        entry.dense_score = hit.dense_score;
        if entry.content.is_empty() {
            entry.content = hit.content.clone();
        }
    }

    let mut fused = fused.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .fused_score
            .partial_cmp(&left.fused_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.best_rank().cmp(&right.best_rank()))
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
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });
    fused.truncate(limit);
    fused
}

fn require_success(response: crate::QueryResponse, context: &str) -> Result<JsonValue> {
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
        .ok_or_else(|| anyhow!("missing response data for {}", context))
}
