use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::dense_search::{require_success, DenseHybridSearchRequest};
use crate::EmbeddedNode;

const DEFAULT_LIMIT: usize = 10;
const DEFAULT_CANDIDATE_LIMIT: usize = 0;
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

    fn dense_request(
        self,
        query_text: &str,
        session_doc_id: Option<&str>,
        limit: usize,
        candidate_limit: usize,
        exclude_doc_ids: &[String],
        embedding_model: Option<&str>,
    ) -> DenseHybridSearchRequest {
        let (collection_name, vector_field, fulltext_fields, return_fields) = match self {
            Self::Messages => (
                "CodingMessage",
                "content_v",
                vec!["content"],
                vec!["message_id", "content"],
            ),
            Self::Actions => (
                "CodingAction",
                "command_v",
                vec!["command"],
                vec!["action_type", "target", "command"],
            ),
        };

        let mut request = DenseHybridSearchRequest::new(
            collection_name,
            query_text,
            vector_field,
            fulltext_fields,
        )
        .with_return_fields(return_fields)
        .with_limit(limit)
        .with_candidate_limit(candidate_limit)
        .with_excluded_doc_ids(exclude_doc_ids.iter().cloned());

        if let Some(session_doc_id) = session_doc_id {
            request = request.with_filter(json!({
                "_sessionID": { "_eq": session_doc_id }
            }));
        }
        request =
            request.with_embedding_model(embedding_model.unwrap_or(self.default_embedding_model()));

        request
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

    pub fn with_excluded_doc_ids<I, S>(mut self, exclude_doc_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.exclude_doc_ids = exclude_doc_ids.into_iter().map(Into::into).collect();
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
    /// Run hybrid BM25 + dense retrieval over the coding benchmark corpus.
    pub async fn hybrid_search_coding(
        &self,
        request: &CodingHybridSearchRequest,
    ) -> Result<CodingHybridSearchResponse> {
        let session_doc_id = if let Some(session_id) = request.session_id.as_deref() {
            Some(lookup_session_doc_id(self, session_id).await?)
        } else {
            None
        };
        let dense_request = request.target.dense_request(
            &request.query_text,
            session_doc_id.as_deref(),
            request.limit,
            request.candidate_limit,
            &request.exclude_doc_ids,
            request.embedding_model.as_deref(),
        );
        let dense_response = self.hybrid_search_dense(&dense_request).await?;

        Ok(CodingHybridSearchResponse {
            target: request.target,
            query_text: request.query_text.clone(),
            session_id: request.session_id.clone(),
            embedding_model: dense_response.embedding_model.clone(),
            query_vector_dimensions: dense_response.query_vector_dimensions,
            bm25_candidates: map_hits(&dense_response.bm25_candidates, request.target)?,
            dense_candidates: map_hits(&dense_response.dense_candidates, request.target)?,
            hits: map_hits(&dense_response.hits, request.target)?,
        })
    }
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
    let data = require_success(node.execute(&query).await, "lookup coding session")?;
    data.get("CodingSession")
        .and_then(JsonValue::as_array)
        .and_then(|sessions| sessions.first())
        .and_then(|session| session.get("_docID"))
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("no CodingSession found for session_id={}", session_id))
}

fn map_hits(
    hits: &[crate::DenseHybridSearchHit],
    target: CodingSearchTarget,
) -> Result<Vec<CodingHybridSearchHit>> {
    hits.iter()
        .map(|hit| {
            let (label, content) = match target {
                CodingSearchTarget::Messages => (
                    required_string_field(&hit.fields, "message_id")?.to_string(),
                    required_string_field(&hit.fields, "content")?.to_string(),
                ),
                CodingSearchTarget::Actions => (
                    format!(
                        "{} {}",
                        required_string_field(&hit.fields, "action_type")?,
                        required_string_field(&hit.fields, "target")?,
                    ),
                    required_string_field(&hit.fields, "command")?.to_string(),
                ),
            };

            Ok(CodingHybridSearchHit {
                doc_id: hit.doc_id.clone(),
                label,
                content,
                bm25_score: hit.bm25_score,
                dense_score: hit.dense_score,
                fused_score: hit.fused_score,
                bm25_rank: hit.bm25_rank,
                dense_rank: hit.dense_rank,
            })
        })
        .collect()
}

fn required_string_field<'a>(
    fields: &'a serde_json::Map<String, JsonValue>,
    field_name: &str,
) -> Result<&'a str> {
    fields
        .get(field_name)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("missing {} in {:?}", field_name, fields))
}
