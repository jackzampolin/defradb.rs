//! Generic dense retrieval v1 primitives.
//!
//! This module intentionally keeps the contract narrow:
//! - one dense vector per configured vector field
//! - query-time embeddings are generated through an OpenAI-compatible
//!   `/embeddings` endpoint
//! - DefraDB scores stored and query vectors with dot product
//! - hybrid ranking is BM25 + dense similarity fused with reciprocal rank fusion
//!
//! Model-specific behavior such as query instructions, pooling strategy,
//! normalization, and any asymmetric query/document preprocessing is expected to
//! be handled by the embedding service itself. DefraDB v1 only sends `model` and
//! `input`, and assumes the returned query vectors are compatible with the
//! vectors already stored in the collection.

use std::cmp::Ordering;
use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};

use crate::{embedding::embed_text, EmbeddingClientConfig};

const DEFAULT_LIMIT: usize = 10;
const DEFAULT_CANDIDATE_LIMIT: usize = 0;
const DEFAULT_RRF_K: f64 = 60.0;
const BM25_ALIAS: &str = "dense_v1_bm25_score";
const SIMILARITY_ALIAS: &str = "dense_v1_similarity_score";

/// Request for production dense-search v1.
///
/// Notes:
/// - `collection_name`, `vector_field`, and `return_fields` are GraphQL field
///   names, not SDL snippets.
/// - `fulltext_fields` are BM25 field paths and may include dot-separated paths.
/// - `filter` must be a JSON object matching DefraDB's GraphQL filter shape.
/// - `embedding_model` must match the family/dimensions used to populate
///   `vector_field`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenseHybridSearchRequest {
    pub collection_name: String,
    pub query_text: String,
    pub vector_field: String,
    pub fulltext_fields: Vec<String>,
    pub return_fields: Vec<String>,
    pub filter: Option<JsonValue>,
    pub limit: usize,
    pub candidate_limit: usize,
    pub exclude_doc_ids: Vec<String>,
    pub embedding_model: Option<String>,
}

impl DenseHybridSearchRequest {
    pub fn new<I, S>(
        collection_name: impl Into<String>,
        query_text: impl Into<String>,
        vector_field: impl Into<String>,
        fulltext_fields: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            collection_name: collection_name.into(),
            query_text: query_text.into(),
            vector_field: vector_field.into(),
            fulltext_fields: fulltext_fields.into_iter().map(Into::into).collect(),
            return_fields: Vec::new(),
            filter: None,
            limit: DEFAULT_LIMIT,
            candidate_limit: DEFAULT_CANDIDATE_LIMIT,
            exclude_doc_ids: Vec::new(),
            embedding_model: None,
        }
    }

    pub fn with_return_fields<I, S>(mut self, return_fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.return_fields = return_fields.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_filter(mut self, filter: JsonValue) -> Self {
        self.filter = Some(filter);
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

/// Hybrid search hit for dense-search v1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DenseHybridSearchHit {
    pub doc_id: String,
    pub fields: Map<String, JsonValue>,
    pub bm25_score: f64,
    pub dense_score: f64,
    pub fused_score: f64,
    pub bm25_rank: Option<usize>,
    pub dense_rank: Option<usize>,
}

impl DenseHybridSearchHit {
    fn best_rank(&self) -> usize {
        self.bm25_rank
            .unwrap_or(usize::MAX)
            .min(self.dense_rank.unwrap_or(usize::MAX))
    }
}

/// Hybrid response for dense-search v1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DenseHybridSearchResponse {
    pub collection_name: String,
    pub query_text: String,
    pub vector_field: String,
    pub fulltext_fields: Vec<String>,
    pub embedding_model: String,
    pub query_vector_dimensions: usize,
    pub bm25_candidates: Vec<DenseHybridSearchHit>,
    pub dense_candidates: Vec<DenseHybridSearchHit>,
    pub hits: Vec<DenseHybridSearchHit>,
}

/// Run dense-search v1 over an arbitrary collection using any `QueryExecutor`.
pub async fn hybrid_search_dense<E: query::QueryExecutor + ?Sized>(
    executor: &E,
    embedding_config: &EmbeddingClientConfig,
    request: &DenseHybridSearchRequest,
) -> Result<DenseHybridSearchResponse> {
    validate_request(request)?;

    let candidate_limit = if request.candidate_limit == 0 {
        request.limit
    } else {
        request.candidate_limit.max(request.limit)
    };
    let embedding_model = request
        .embedding_model
        .clone()
        .or_else(|| {
            let default_model = embedding_config.model.trim();
            (!default_model.is_empty()).then_some(default_model.to_string())
        })
        .ok_or_else(|| anyhow!("dense-search request is missing an embedding model"))?;
    let query_vector = embed_text(
        embedding_config,
        &request.query_text,
        Some(&embedding_model),
    )
    .await?;
    let bm25_query =
        render_dense_ranked_query(request, &query_vector, candidate_limit, RankedOrder::Bm25)?;
    let dense_query =
        render_dense_ranked_query(request, &query_vector, candidate_limit, RankedOrder::Dense)?;

    let bm25_data = require_query_success(
        executor
            .execute(query::QueryRequest::new(&bm25_query))
            .await,
        "dense-search bm25",
    )?;
    let dense_data = require_query_success(
        executor
            .execute(query::QueryRequest::new(&dense_query))
            .await,
        "dense-search similarity",
    )?;
    let mut bm25_candidates = parse_hits(&bm25_data, &request.collection_name)?;
    let mut dense_candidates = parse_hits(&dense_data, &request.collection_name)?;

    for (index, hit) in bm25_candidates.iter_mut().enumerate() {
        hit.bm25_rank = Some(index + 1);
    }
    for (index, hit) in dense_candidates.iter_mut().enumerate() {
        hit.dense_rank = Some(index + 1);
    }

    Ok(DenseHybridSearchResponse {
        collection_name: request.collection_name.clone(),
        query_text: request.query_text.clone(),
        vector_field: request.vector_field.clone(),
        fulltext_fields: request.fulltext_fields.clone(),
        embedding_model,
        query_vector_dimensions: query_vector.len(),
        hits: fuse_rankings_rrf(&bm25_candidates, &dense_candidates, request.limit),
        bm25_candidates,
        dense_candidates,
    })
}

pub fn require_query_success(response: query::QueryResponse, context: &str) -> Result<JsonValue> {
    if response.has_errors() {
        let messages = response
            .errors
            .into_iter()
            .map(|error| error.message)
            .collect::<Vec<_>>()
            .join("; ");
        bail!("{} failed: {}", context, messages);
    }

    response
        .data
        .ok_or_else(|| anyhow!("{} returned no data", context))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RankedOrder {
    Bm25,
    Dense,
}

impl RankedOrder {
    fn alias(self) -> &'static str {
        match self {
            Self::Bm25 => BM25_ALIAS,
            Self::Dense => SIMILARITY_ALIAS,
        }
    }
}

fn validate_request(request: &DenseHybridSearchRequest) -> Result<()> {
    if request.query_text.trim().is_empty() {
        bail!("query_text must not be empty");
    }
    if request.limit == 0 {
        bail!("limit must be greater than zero");
    }

    validate_graphql_name(&request.collection_name, "collection_name")?;
    validate_graphql_name(&request.vector_field, "vector_field")?;

    if request.fulltext_fields.is_empty() {
        bail!("fulltext_fields must not be empty");
    }
    for field in &request.fulltext_fields {
        validate_graphql_field_path(field, "fulltext_fields")?;
    }

    for field in &request.return_fields {
        validate_graphql_name(field, "return_fields")?;
    }

    if let Some(filter) = request.filter.as_ref() {
        let JsonValue::Object(_) = filter else {
            bail!("filter must be a JSON object matching DefraDB GraphQL filter syntax");
        };
        validate_graphql_input_keys(filter, "filter")?;
    }

    Ok(())
}

fn validate_graphql_name(name: &str, context: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        bail!("{context} must not be empty");
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        bail!("{context} must start with a letter or underscore: {name}");
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        bail!("{context} contains an invalid GraphQL identifier: {name}");
    }
    Ok(())
}

fn validate_graphql_field_path(path: &str, context: &str) -> Result<()> {
    if path.is_empty() {
        bail!("{context} must not be empty");
    }
    for segment in path.split('.') {
        validate_graphql_name(segment, context)?;
    }
    Ok(())
}

fn validate_graphql_input_keys(value: &JsonValue, context: &str) -> Result<()> {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                validate_graphql_input_keys(value, context)?;
            }
        }
        JsonValue::Object(obj) => {
            for (key, value) in obj {
                validate_graphql_name(key, context)?;
                validate_graphql_input_keys(value, context)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn render_dense_ranked_query(
    request: &DenseHybridSearchRequest,
    vector: &[f64],
    limit: usize,
    order: RankedOrder,
) -> Result<String> {
    let mut args = Vec::new();
    if let Some(filter) = merged_filter(request.filter.as_ref(), &request.exclude_doc_ids)? {
        args.push(format!("filter: {}", json_to_graphql_input(&filter)));
    }
    args.push(format!(
        "order: {{ _alias: {{ {}: DESC }} }}",
        order.alias()
    ));
    args.push(format!("limit: {}", limit));

    let return_fields = dedupe_fields(&request.return_fields);
    let selection = if return_fields.is_empty() {
        String::new()
    } else {
        format!(
            "\n{}",
            return_fields
                .iter()
                .map(|field| format!("    {}", field))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };

    Ok(format!(
        r#"{{
  {collection_name}(
    {args}
  ) {{
    _docID
    {bm25_alias}: BM25(query: "{query_text}", fields: [{fulltext_fields}])
    {similarity_alias}: SIMILARITY({vector_field}: {{vector: [{vector}]}}){selection}
  }}
}}"#,
        collection_name = request.collection_name,
        args = args.join("\n    "),
        bm25_alias = BM25_ALIAS,
        similarity_alias = SIMILARITY_ALIAS,
        query_text = escape_graphql(&request.query_text),
        fulltext_fields = request
            .fulltext_fields
            .iter()
            .map(|field| format!("\"{}\"", escape_graphql(field)))
            .collect::<Vec<_>>()
            .join(", "),
        vector_field = request.vector_field,
        vector = format_vector(vector),
        selection = selection,
    ))
}

fn dedupe_fields(fields: &[String]) -> Vec<String> {
    let mut deduped = Vec::new();
    for field in fields {
        if field != "_docID" && !deduped.iter().any(|existing| existing == field) {
            deduped.push(field.clone());
        }
    }
    deduped
}

fn merged_filter(
    filter: Option<&JsonValue>,
    exclude_doc_ids: &[String],
) -> Result<Option<JsonValue>> {
    let exclusion = (!exclude_doc_ids.is_empty()).then(|| {
        json!({
            "_docID": { "_nin": exclude_doc_ids }
        })
    });

    match (filter.cloned(), exclusion) {
        (None, None) => Ok(None),
        (Some(filter), None) => Ok(Some(filter)),
        (None, Some(exclusion)) => Ok(Some(exclusion)),
        (Some(filter), Some(exclusion)) => Ok(Some(json!({
            "_and": [filter, exclusion]
        }))),
    }
}

fn json_to_graphql_input(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("\"{}\"", escape_graphql(s)),
        JsonValue::Array(arr) => {
            let items = arr.iter().map(json_to_graphql_input).collect::<Vec<_>>();
            format!("[{}]", items.join(", "))
        }
        JsonValue::Object(obj) => {
            let fields = obj
                .iter()
                .map(|(key, value)| format!("{}: {}", key, json_to_graphql_input(value)))
                .collect::<Vec<_>>();
            format!("{{{}}}", fields.join(", "))
        }
    }
}

fn parse_hits(data: &JsonValue, collection_name: &str) -> Result<Vec<DenseHybridSearchHit>> {
    let items = data
        .get(collection_name)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("missing {} array in {}", collection_name, data))?;

    items
        .iter()
        .map(|item| {
            let mut fields = item
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!("expected object item in {}", item))?;
            let doc_id = fields
                .remove("_docID")
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .ok_or_else(|| anyhow!("missing _docID in {}", item))?;
            let bm25_score = fields
                .remove(BM25_ALIAS)
                .and_then(|value| value.as_f64())
                .unwrap_or_default();
            let dense_score = fields
                .remove(SIMILARITY_ALIAS)
                .and_then(|value| value.as_f64())
                .unwrap_or_default();

            Ok(DenseHybridSearchHit {
                doc_id,
                fields,
                bm25_score,
                dense_score,
                fused_score: 0.0,
                bm25_rank: None,
                dense_rank: None,
            })
        })
        .collect()
}

fn fuse_rankings_rrf(
    bm25_candidates: &[DenseHybridSearchHit],
    dense_candidates: &[DenseHybridSearchHit],
    limit: usize,
) -> Vec<DenseHybridSearchHit> {
    let mut fused = HashMap::<String, DenseHybridSearchHit>::new();

    for hit in bm25_candidates {
        let rank = hit.bm25_rank.unwrap_or(usize::MAX);
        let entry = fused
            .entry(hit.doc_id.clone())
            .or_insert_with(|| hit.clone());
        entry.fused_score += 1.0 / (DEFAULT_RRF_K + rank as f64);
        entry.bm25_rank = hit.bm25_rank;
        entry.bm25_score = hit.bm25_score;
        if entry.fields.is_empty() {
            entry.fields = hit.fields.clone();
        }
    }

    for hit in dense_candidates {
        let rank = hit.dense_rank.unwrap_or(usize::MAX);
        let entry = fused
            .entry(hit.doc_id.clone())
            .or_insert_with(|| hit.clone());
        entry.fused_score += 1.0 / (DEFAULT_RRF_K + rank as f64);
        entry.dense_rank = hit.dense_rank;
        entry.dense_score = hit.dense_score;
        if entry.fields.is_empty() {
            entry.fields = hit.fields.clone();
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

fn escape_graphql(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn format_vector(vector: &[f64]) -> String {
    vector
        .iter()
        .map(|value| format!("{value:.8}"))
        .collect::<Vec<_>>()
        .join(", ")
}
