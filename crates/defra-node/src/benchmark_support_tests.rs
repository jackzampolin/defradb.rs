use super::*;
use crate::benchmark_queries::{
    format_vector, render_action_ranked_query, render_message_ranked_query, RankedQueryOrder,
};
use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

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
    let action_data = ensure_success(node.execute(&action_query).await, "smoke action").unwrap();
    assert!(count_hits(&action_data, SearchTarget::Actions) > 0);
}

#[tokio::test]
async fn coding_session_fixture_exports_context1_style_tasks() {
    let node = crate::EmbeddedNode::builder().build().await.unwrap();
    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 40;
    config.hot_session_actions = 16;
    config.medium_session_messages = 12;
    config.medium_session_actions = 6;

    let fixture = seed_coding_session_fixture(&node, &config).await.unwrap();

    let tasks = build_context1_style_coding_tasks(&node, &fixture)
        .await
        .unwrap();
    assert!(!tasks.is_empty());
    assert!(tasks
        .iter()
        .any(|task| task.task_id == "hot_messages_pushdown"));
    assert!(tasks
        .iter()
        .any(|task| task.task_id == "hot_actions_rg_pushdown"));

    for task in &tasks {
        assert!(
            !task.supporting_items.is_empty(),
            "{} missing supports",
            task.task_id
        );
        assert!(
            !task.distractors.is_empty(),
            "{} missing distractors",
            task.task_id
        );
        assert_eq!(
            task.items_and_contents.len(),
            task.supporting_items.len() + task.distractors.len(),
            "{} items_and_contents should cover support + distractor items",
            task.task_id
        );
    }

    let serialized = serde_json::to_value(&tasks).unwrap();
    assert!(serialized.is_array());
    assert!(serialized
        .as_array()
        .unwrap()
        .iter()
        .all(|task| task.get("question").is_some() && task.get("supporting_items").is_some()));
}

#[derive(Clone, Default)]
struct MockEmbeddingState {
    requests: Arc<Mutex<Vec<EmbeddingRequest>>>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct EmbeddingRequest {
    model: String,
    input: String,
}

#[derive(Debug, Serialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingResponseItem>,
}

#[derive(Debug, Serialize)]
struct EmbeddingResponseItem {
    embedding: Vec<f64>,
}

struct MockEmbeddingServer {
    base_url: String,
    state: MockEmbeddingState,
    task: tokio::task::JoinHandle<()>,
    _fixture_permit: tokio::sync::SemaphorePermit<'static>,
}

static EMBEDDING_FIXTURE_TEST: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

impl MockEmbeddingServer {
    async fn start() -> Self {
        let fixture_permit = EMBEDDING_FIXTURE_TEST.acquire().await.unwrap();
        let state = MockEmbeddingState::default();
        let app = Router::new()
            .route("/embeddings", post(mock_embedding_handler))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{}", addr),
            state,
            task,
            _fixture_permit: fixture_permit,
        }
    }

    fn requests(&self) -> Vec<EmbeddingRequest> {
        self.state.requests.lock().unwrap().clone()
    }
}

impl Drop for MockEmbeddingServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn mock_embedding_handler(
    State(state): State<MockEmbeddingState>,
    Json(request): Json<EmbeddingRequest>,
) -> Json<EmbeddingResponse> {
    state.requests.lock().unwrap().push(request.clone());

    Json(EmbeddingResponse {
        data: vec![EmbeddingResponseItem {
            embedding: deterministic_embedding(&request.model, &request.input),
        }],
    })
}

fn deterministic_embedding(model: &str, input: &str) -> Vec<f64> {
    let lowercase = input.to_ascii_lowercase();
    let tokens = [
        "cargo",
        "query",
        "planner",
        "rocksdb",
        "bm25",
        "rg",
        "bench",
        "pushdown",
        "candidate",
        "wand",
        "clippy",
        "type_join_many",
    ];

    let mut vector = tokens
        .iter()
        .map(|token| lowercase.matches(token).count() as f64)
        .collect::<Vec<_>>();

    let model_bias = match model {
        "coding-message-model" => [1.0, 0.0],
        "coding-action-model" => [0.0, 1.0],
        "coding-search-chunk-model" => [0.5, 0.5],
        _ => [0.0, 0.0],
    };
    vector.extend(model_bias);
    vector
}

fn task_embedding_model(target: SearchTarget) -> &'static str {
    match target {
        SearchTarget::Messages => "coding-message-model",
        SearchTarget::Actions => "coding-action-model",
    }
}

fn task_search_target(target: SearchTarget) -> crate::CodingSearchTarget {
    match target {
        SearchTarget::Messages => crate::CodingSearchTarget::Messages,
        SearchTarget::Actions => crate::CodingSearchTarget::Actions,
    }
}

fn dense_request_for_target(
    target: SearchTarget,
    query: &str,
    session_doc_id: Option<&str>,
) -> crate::DenseHybridSearchRequest {
    let (collection_name, vector_field, fulltext_fields, return_fields, embedding_model) =
        match target {
            SearchTarget::Messages => (
                "CodingMessage",
                "content_v",
                vec!["content"],
                vec!["message_id", "content"],
                "coding-message-model",
            ),
            SearchTarget::Actions => (
                "CodingAction",
                "command_v",
                vec!["command"],
                vec!["action_type", "target", "command"],
                "coding-action-model",
            ),
        };

    let mut request =
        crate::DenseHybridSearchRequest::new(collection_name, query, vector_field, fulltext_fields)
            .with_return_fields(return_fields)
            .with_embedding_model(embedding_model);
    if let Some(session_doc_id) = session_doc_id {
        request = request.with_filter(serde_json::json!({
            "_sessionID": { "_eq": session_doc_id }
        }));
    }

    request
}

fn dense_chunk_request(
    target_kind: &str,
    query: &str,
    session_id: Option<&str>,
    project_path: Option<&str>,
) -> crate::DenseHybridSearchRequest {
    let mut filter = serde_json::Map::new();
    filter.insert(
        "target_kind".to_string(),
        serde_json::json!({ "_eq": target_kind }),
    );
    if let Some(session_id) = session_id {
        filter.insert(
            "session_id".to_string(),
            serde_json::json!({ "_eq": session_id }),
        );
    }
    if let Some(project_path) = project_path {
        filter.insert(
            "project_path".to_string(),
            serde_json::json!({ "_eq": project_path }),
        );
    }

    crate::DenseHybridSearchRequest::new("CodingSearchChunk", query, "content_v", vec!["content"])
        .with_return_fields(vec![
            "chunk_id",
            "target_kind",
            "session_id",
            "project_path",
            "parent_external_id",
            "role",
            "action_type",
            "target",
            "content",
            "chunk_index",
            "chunk_count",
        ])
        .with_filter(JsonValue::Object(filter))
        .with_embedding_model("coding-search-chunk-model")
}

#[derive(Debug, Clone)]
struct RankedHit {
    doc_id: String,
    label: String,
    preview: String,
    bm25: f64,
    sim: f64,
}

#[derive(Debug, Clone)]
struct FusedRankedHit {
    doc_id: String,
    label: String,
    preview: String,
    fused_score: f64,
    bm25_rank: Option<usize>,
    dense_rank: Option<usize>,
    bm25: f64,
    sim: f64,
}

impl FusedRankedHit {
    fn best_rank(&self) -> usize {
        self.bm25_rank
            .unwrap_or(usize::MAX)
            .min(self.dense_rank.unwrap_or(usize::MAX))
    }
}

#[derive(Debug, Clone)]
struct HybridComparisonSummary {
    case_name: String,
    query: String,
    bm25: Vec<RankedHit>,
    dense: Vec<RankedHit>,
    fused: Vec<FusedRankedHit>,
    overlap: usize,
    bm25_only: usize,
    dense_only: usize,
}

impl HybridComparisonSummary {
    fn render(&self) -> String {
        format!(
            "{} query=\"{}\" overlap={} bm25_only={} dense_only={}\n  bm25: {}\n  dense: {}\n  rrf: {}",
            self.case_name,
            self.query,
            self.overlap,
            self.bm25_only,
            self.dense_only,
            render_ranked_hits(&self.bm25),
            render_ranked_hits(&self.dense),
            render_fused_hits(&self.fused),
        )
    }
}

fn render_ranked_hits(hits: &[RankedHit]) -> String {
    hits.iter()
        .take(3)
        .enumerate()
        .map(|(index, hit)| {
            format!(
                "#{} {} bm25={:.3} sim={:.3} {}",
                index + 1,
                hit.label,
                hit.bm25,
                hit.sim,
                preview_excerpt(&hit.preview),
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn render_fused_hits(hits: &[FusedRankedHit]) -> String {
    hits.iter()
        .take(3)
        .enumerate()
        .map(|(index, hit)| {
            format!(
                "#{} {} fused={:.4} ranks=b{:?}/d{:?} {}",
                index + 1,
                hit.label,
                hit.fused_score,
                hit.bm25_rank,
                hit.dense_rank,
                preview_excerpt(&hit.preview),
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn preview_excerpt(value: &str) -> String {
    const MAX_PREVIEW_LEN: usize = 96;

    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= MAX_PREVIEW_LEN {
        compact
    } else {
        format!("{}...", &compact[..MAX_PREVIEW_LEN])
    }
}

fn render_message_similarity_query(term: &str, vector: &[f64]) -> String {
    format!(
        r#"{{
  CodingMessage(order: {{ _alias: {{ sim: DESC }} }}, limit: 5) {{
message_id
bm25: BM25(query: "{term}", fields: ["content"])
sim: SIMILARITY(content_v: {{vector: [{vector}]}})
content
  }}
}}"#,
        term = crate::benchmark_queries::escape_graphql(term),
        vector = format_vector(vector),
    )
}

fn render_action_similarity_query(vector: &[f64]) -> String {
    format!(
        r#"{{
  CodingAction(order: {{ _alias: {{ sim: DESC }} }}, limit: 5) {{
action_type
sim: SIMILARITY(command_v: {{vector: [{vector}]}})
command
  }}
}}"#,
        vector = format_vector(vector),
    )
}

async fn total_embedding_documents(node: &crate::EmbeddedNode) -> usize {
    let data = ensure_success(
        node.execute(
            r#"{
  CodingMessage { _docID }
  CodingAction { _docID }
  CodingSearchChunk { _docID }
}"#,
        )
        .await,
        "count embedding documents",
    )
    .unwrap();

    ["CodingMessage", "CodingAction", "CodingSearchChunk"]
        .into_iter()
        .map(|collection_name| {
            data.get(collection_name)
                .and_then(JsonValue::as_array)
                .map_or(0, Vec::len)
        })
        .sum()
}

async fn lookup_session_doc_id(
    node: &crate::EmbeddedNode,
    session_id: &str,
) -> anyhow::Result<String> {
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
        .ok_or_else(|| anyhow::anyhow!("missing session _docID for {}", session_id))
}

async fn run_hybrid_comparison(
    node: &crate::EmbeddedNode,
    target: SearchTarget,
    session_id: &str,
    case_name: &str,
    query: &str,
    vector: &[f64],
    limit: usize,
) -> anyhow::Result<HybridComparisonSummary> {
    let session_doc_id = lookup_session_doc_id(node, session_id).await?;

    let bm25_query = match target {
        SearchTarget::Messages => render_message_ranked_query(
            &session_doc_id,
            query,
            vector,
            limit,
            0,
            RankedQueryOrder::Bm25,
            false,
        ),
        SearchTarget::Actions => render_action_ranked_query(
            &session_doc_id,
            query,
            vector,
            limit,
            0,
            RankedQueryOrder::Bm25,
            false,
        ),
    };
    let dense_query = match target {
        SearchTarget::Messages => render_message_ranked_query(
            &session_doc_id,
            query,
            vector,
            limit,
            0,
            RankedQueryOrder::Similarity,
            false,
        ),
        SearchTarget::Actions => render_action_ranked_query(
            &session_doc_id,
            query,
            vector,
            limit,
            0,
            RankedQueryOrder::Similarity,
            false,
        ),
    };

    let bm25_data = ensure_success(node.execute(&bm25_query).await, case_name)?;
    let dense_data = ensure_success(node.execute(&dense_query).await, case_name)?;
    let bm25 = parse_ranked_hits(&bm25_data, target)?;
    let dense = parse_ranked_hits(&dense_data, target)?;
    Ok(compare_rankings(case_name, query, bm25, dense))
}

async fn assert_hits_belong_to_session(
    node: &crate::EmbeddedNode,
    target: crate::CodingSearchTarget,
    hits: &[crate::CodingHybridSearchHit],
    expected_session_doc_id: &str,
) -> anyhow::Result<()> {
    if hits.is_empty() {
        anyhow::bail!("expected at least one hit to validate session scope");
    }

    let collection_name = match target {
        crate::CodingSearchTarget::Messages => "CodingMessage",
        crate::CodingSearchTarget::Actions => "CodingAction",
    };
    let doc_ids = hits
        .iter()
        .map(|hit| {
            format!(
                "\"{}\"",
                crate::benchmark_queries::escape_graphql(&hit.doc_id)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        r#"{{
  {collection_name}(filter: {{ _docID: {{ _in: [{doc_ids}] }} }}, limit: {limit}) {{
_docID
_sessionID
  }}
}}"#,
        limit = hits.len(),
    );
    let data = ensure_success(node.execute(&query).await, "hybrid search session scope")?;
    let items = data
        .get(collection_name)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow::anyhow!("missing {collection_name} array in {data}"))?;

    assert_eq!(
        items.len(),
        hits.len(),
        "expected {} {} rows, got {}",
        hits.len(),
        collection_name,
        items.len()
    );
    for item in items {
        assert_eq!(
            item.get("_sessionID").and_then(JsonValue::as_str),
            Some(expected_session_doc_id),
            "hit belonged to unexpected session: {}",
            item
        );
    }

    Ok(())
}

async fn assert_doc_ids_belong_to_session(
    node: &crate::EmbeddedNode,
    collection_name: &str,
    doc_ids: &[String],
    expected_session_doc_id: &str,
) -> anyhow::Result<()> {
    if doc_ids.is_empty() {
        anyhow::bail!("expected at least one hit to validate session scope");
    }

    let rendered_doc_ids = doc_ids
        .iter()
        .map(|doc_id| format!("\"{}\"", crate::benchmark_queries::escape_graphql(doc_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        r#"{{
  {collection_name}(filter: {{ _docID: {{ _in: [{rendered_doc_ids}] }} }}, limit: {limit}) {{
_docID
_sessionID
  }}
}}"#,
        limit = doc_ids.len(),
    );
    let data = ensure_success(node.execute(&query).await, "dense search session scope")?;
    let items = data
        .get(collection_name)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow::anyhow!("missing {collection_name} array in {data}"))?;

    assert_eq!(
        items.len(),
        doc_ids.len(),
        "expected {} {} rows, got {}",
        doc_ids.len(),
        collection_name,
        items.len()
    );
    for item in items {
        assert_eq!(
            item.get("_sessionID").and_then(JsonValue::as_str),
            Some(expected_session_doc_id),
            "hit belonged to unexpected session: {}",
            item
        );
    }

    Ok(())
}

fn parse_ranked_hits(data: &JsonValue, target: SearchTarget) -> anyhow::Result<Vec<RankedHit>> {
    let collection_name = match target {
        SearchTarget::Messages => "CodingMessage",
        SearchTarget::Actions => "CodingAction",
    };
    let items = data
        .get(collection_name)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow::anyhow!("missing {collection_name} array in {data}"))?;

    items
        .iter()
        .map(|item| {
            let doc_id = item
                .get("_docID")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing _docID in {item}"))?
                .to_string();
            let (label, preview) = match target {
                SearchTarget::Messages => (
                    item.get("message_id")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow::anyhow!("missing message_id in {item}"))?
                        .to_string(),
                    item.get("content")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow::anyhow!("missing content in {item}"))?
                        .to_string(),
                ),
                SearchTarget::Actions => (
                    format!(
                        "{} {}",
                        item.get("action_type")
                            .and_then(JsonValue::as_str)
                            .ok_or_else(|| anyhow::anyhow!("missing action_type in {item}"))?,
                        item.get("target")
                            .and_then(JsonValue::as_str)
                            .ok_or_else(|| anyhow::anyhow!("missing target in {item}"))?,
                    ),
                    item.get("command")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| anyhow::anyhow!("missing command in {item}"))?
                        .to_string(),
                ),
            };

            Ok(RankedHit {
                doc_id,
                label,
                preview,
                bm25: item
                    .get("bm25")
                    .and_then(JsonValue::as_f64)
                    .unwrap_or_default(),
                sim: item
                    .get("sim")
                    .and_then(JsonValue::as_f64)
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn compare_rankings(
    case_name: &str,
    query: &str,
    bm25: Vec<RankedHit>,
    dense: Vec<RankedHit>,
) -> HybridComparisonSummary {
    const RRF_RANK_BIAS: f64 = 60.0;

    let bm25_ids = bm25
        .iter()
        .map(|hit| hit.doc_id.as_str())
        .collect::<HashSet<_>>();
    let dense_ids = dense
        .iter()
        .map(|hit| hit.doc_id.as_str())
        .collect::<HashSet<_>>();
    let overlap = bm25_ids.intersection(&dense_ids).count();
    let bm25_only = bm25_ids.difference(&dense_ids).count();
    let dense_only = dense_ids.difference(&bm25_ids).count();

    let mut fused = HashMap::<String, FusedRankedHit>::new();

    for (index, hit) in bm25.iter().enumerate() {
        let rank = index + 1;
        let entry = fused
            .entry(hit.doc_id.clone())
            .or_insert_with(|| FusedRankedHit {
                doc_id: hit.doc_id.clone(),
                label: hit.label.clone(),
                preview: hit.preview.clone(),
                fused_score: 0.0,
                bm25_rank: None,
                dense_rank: None,
                bm25: hit.bm25,
                sim: hit.sim,
            });
        entry.fused_score += 1.0 / (RRF_RANK_BIAS + rank as f64);
        entry.bm25_rank = Some(rank);
        entry.bm25 = hit.bm25;
        entry.sim = hit.sim;
    }

    for (index, hit) in dense.iter().enumerate() {
        let rank = index + 1;
        let entry = fused
            .entry(hit.doc_id.clone())
            .or_insert_with(|| FusedRankedHit {
                doc_id: hit.doc_id.clone(),
                label: hit.label.clone(),
                preview: hit.preview.clone(),
                fused_score: 0.0,
                bm25_rank: None,
                dense_rank: None,
                bm25: hit.bm25,
                sim: hit.sim,
            });
        entry.fused_score += 1.0 / (RRF_RANK_BIAS + rank as f64);
        entry.dense_rank = Some(rank);
        entry.bm25 = hit.bm25;
        entry.sim = hit.sim;
        if entry.preview.is_empty() {
            entry.preview = hit.preview.clone();
        }
    }

    let mut fused = fused.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .fused_score
            .partial_cmp(&left.fused_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.best_rank().cmp(&right.best_rank()))
            .then_with(|| right.sim.partial_cmp(&left.sim).unwrap_or(Ordering::Equal))
            .then_with(|| {
                right
                    .bm25
                    .partial_cmp(&left.bm25)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });

    HybridComparisonSummary {
        case_name: case_name.to_string(),
        query: query.to_string(),
        bm25,
        dense,
        fused,
        overlap,
        bm25_only,
        dense_only,
    }
}

fn assert_hybrid_summary(summary: &HybridComparisonSummary, expected_term: &str) {
    assert!(
        !summary.bm25.is_empty(),
        "{} bm25 ranking was empty",
        summary.case_name
    );
    assert!(
        !summary.dense.is_empty(),
        "{} dense ranking was empty",
        summary.case_name
    );
    assert!(
        !summary.fused.is_empty(),
        "{} fused ranking was empty",
        summary.case_name
    );
    assert!(
        summary.bm25.iter().any(|hit| hit.bm25 > 0.0),
        "{} bm25 ranking never produced a positive score",
        summary.case_name
    );
    assert!(
        summary.dense.iter().any(|hit| hit.sim > 0.0),
        "{} dense ranking never produced a positive score",
        summary.case_name
    );
    assert!(
        summary.overlap > 0,
        "{} had no overlap between bm25 and dense top-k",
        summary.case_name
    );

    let top_fused = &summary.fused[0].doc_id;
    let top_fused_in_source_top3 = summary
        .bm25
        .iter()
        .take(3)
        .chain(summary.dense.iter().take(3))
        .any(|hit| hit.doc_id == *top_fused);
    assert!(
        top_fused_in_source_top3,
        "{} fused top result did not come from either source top-3",
        summary.case_name
    );
    assert!(
        summary
            .fused
            .iter()
            .take(3)
            .any(|hit| hit.preview.to_ascii_lowercase().contains(expected_term)),
        "{} fused top-3 did not contain expected term '{}'",
        summary.case_name,
        expected_term
    );
}

#[tokio::test]
async fn coding_session_embedding_fixture_supports_similarity_queries() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 40;
    config.hot_session_actions = 16;
    config.medium_session_messages = 12;
    config.medium_session_actions = 6;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let _fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();

    let requests = server.requests();
    assert_eq!(requests.len(), total_embedding_documents(&node).await);
    assert!(requests
        .iter()
        .any(|request| request.model == "coding-message-model"));
    assert!(requests
        .iter()
        .any(|request| request.model == "coding-action-model"));

    let message_vector = deterministic_embedding("coding-message-model", "pushdown");
    let message_data = ensure_success(
        node.execute(&render_message_similarity_query(
            "pushdown",
            &message_vector,
        ))
        .await,
        "message similarity",
    )
    .unwrap();
    let top_message = &message_data["CodingMessage"][0];
    assert!(top_message["content"]
        .as_str()
        .is_some_and(|content| content.contains("pushdown")));
    assert!(top_message["sim"].as_f64().unwrap_or_default() > 0.0);
    assert!(top_message["bm25"].as_f64().unwrap_or_default() > 0.0);

    let action_vector = deterministic_embedding("coding-action-model", "rg");
    let action_data = ensure_success(
        node.execute(&render_action_similarity_query(&action_vector))
            .await,
        "action similarity",
    )
    .unwrap();
    let top_action = &action_data["CodingAction"][0];
    assert!(top_action["command"]
        .as_str()
        .is_some_and(|command| command.contains("rg")));
    assert!(top_action["sim"].as_f64().unwrap_or_default() > 0.0);
}

#[tokio::test]
async fn coding_session_embedding_fixture_scores_context1_style_tasks() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 64;
    config.hot_session_actions = 24;
    config.medium_session_messages = 16;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let tasks = build_context1_style_coding_tasks(&node, &fixture)
        .await
        .unwrap();

    for task_id in ["hot_messages_pushdown", "hot_actions_rg_pushdown"] {
        let task = tasks.iter().find(|task| task.task_id == task_id).unwrap();
        let query_vector =
            deterministic_embedding(task_embedding_model(task.target), task.effective_query());
        let evaluation = evaluate_coding_task(&node, task, &query_vector, 6)
            .await
            .unwrap();
        eprintln!("{}", evaluation.render());

        let bm25 = evaluation.metric(RetrievalStrategy::Bm25).unwrap();
        let dense = evaluation.metric(RetrievalStrategy::Dense).unwrap();
        let rrf = evaluation.metric(RetrievalStrategy::Rrf).unwrap();

        assert!(bm25.answer_found, "{} bm25 missed supports", task.task_id);
        assert!(rrf.answer_found, "{} rrf missed supports", task.task_id);
        assert!(dense.limit > 0, "{} dense ranking was empty", task.task_id);
        assert!(
            rrf.support_hits >= 1,
            "{} rrf should recover at least one support item",
            task.task_id
        );
        assert!(
            rrf.distractor_hits < rrf.limit,
            "{} rrf ranking should not be all distractors",
            task.task_id
        );
    }
}

#[tokio::test]
async fn coding_session_embedding_fixture_supports_hybrid_rank_comparison() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 64;
    config.hot_session_actions = 24;
    config.medium_session_messages = 18;
    config.medium_session_actions = 10;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();

    let message_query = "pushdown candidate";
    let message_vector = deterministic_embedding("coding-message-model", message_query);
    let message_summary = run_hybrid_comparison(
        &node,
        SearchTarget::Messages,
        &fixture.hot_session.session_id,
        "mock_hot_messages_pushdown_candidate",
        message_query,
        &message_vector,
        8,
    )
    .await
    .unwrap();
    eprintln!("{}", message_summary.render());
    assert_hybrid_summary(&message_summary, "pushdown");

    let action_query = "rg pushdown";
    let action_vector = deterministic_embedding("coding-action-model", action_query);
    let action_summary = run_hybrid_comparison(
        &node,
        SearchTarget::Actions,
        &fixture.hot_session.session_id,
        "mock_hot_actions_rg_pushdown",
        action_query,
        &action_vector,
        6,
    )
    .await
    .unwrap();
    eprintln!("{}", action_summary.render());
    assert_hybrid_summary(&action_summary, "rg");
}

#[tokio::test]
async fn coding_session_embedding_fixture_supports_query_text_hybrid_search_api() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 64;
    config.hot_session_actions = 24;
    config.medium_session_messages = 16;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let tasks = build_context1_style_coding_tasks(&node, &fixture)
        .await
        .unwrap();

    for task_id in ["hot_messages_pushdown", "hot_actions_rg_pushdown"] {
        let task = tasks.iter().find(|task| task.task_id == task_id).unwrap();
        let response = node
            .hybrid_search_coding(
                &crate::CodingHybridSearchRequest::new(
                    task_search_target(task.target),
                    task.effective_query(),
                )
                .with_session_id(task.session_id.clone())
                .with_limit(6),
            )
            .await
            .unwrap();
        eprintln!(
            "{} query=\"{}\" hits={}",
            task.task_id,
            response.query_text,
            response.hits.len()
        );

        assert_eq!(response.target, task_search_target(task.target));
        assert_eq!(response.embedding_model, task_embedding_model(task.target));
        assert!(response.query_vector_dimensions > 0);
        assert!(!response.bm25_candidates.is_empty());
        assert!(!response.dense_candidates.is_empty());
        assert!(!response.hits.is_empty());
        assert!(
            response
                .hits
                .iter()
                .any(|hit| task.support_ids().contains(&hit.doc_id)),
            "{} hybrid_search_coding missed all labeled supports",
            task.task_id
        );
    }

    let requests = server.requests();
    assert_eq!(requests.len(), total_embedding_documents(&node).await + 2);
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.model == "coding-message-model"
                    && request.input == "relation narrowing bm25 pushdown"
            })
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.model == "coding-action-model"
                    && request.input == "rg pushdown planner joins"
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn coding_session_embedding_fixture_hybrid_search_api_honors_session_scope_and_exclusions() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 48;
    config.hot_session_actions = 20;
    config.medium_session_messages = 14;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let session_doc_id = lookup_session_doc_id(&node, &fixture.hot_session.session_id)
        .await
        .unwrap();
    let request = crate::CodingHybridSearchRequest::new(
        crate::CodingSearchTarget::Messages,
        "pushdown candidate",
    )
    .with_session_id(fixture.hot_session.session_id.clone())
    .with_limit(5);

    let first_response = node.hybrid_search_coding(&request).await.unwrap();
    assert!(!first_response.hits.is_empty());
    assert_hits_belong_to_session(
        &node,
        crate::CodingSearchTarget::Messages,
        &first_response.hits,
        &session_doc_id,
    )
    .await
    .unwrap();

    let excluded_doc_id = first_response.hits[0].doc_id.clone();
    let second_response = node
        .hybrid_search_coding(
            &request
                .clone()
                .with_excluded_doc_ids(vec![excluded_doc_id.clone()]),
        )
        .await
        .unwrap();

    assert!(!second_response.hits.is_empty());
    assert!(
        second_response
            .hits
            .iter()
            .all(|hit| hit.doc_id != excluded_doc_id),
        "excluded doc_id {} was still returned",
        excluded_doc_id
    );
    assert_hits_belong_to_session(
        &node,
        crate::CodingSearchTarget::Messages,
        &second_response.hits,
        &session_doc_id,
    )
    .await
    .unwrap();

    assert_eq!(
        server.requests().len(),
        total_embedding_documents(&node).await + 2
    );
}

#[tokio::test]
async fn coding_data_fixture_materializes_search_chunks_for_large_messages() {
    let node = crate::EmbeddedNode::builder().build().await.unwrap();
    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 24;
    config.hot_session_actions = 10;
    config.assistant_message_bytes = 1_600;
    config.action_command_bytes = 420;

    let fixture = seed_coding_session_fixture(&node, &config).await.unwrap();
    let data = ensure_success(
        node.execute(&format!(
            r#"{{
  CodingSearchChunk(
filter: {{
  session_id: {{ _eq: "{session_id}" }}
  target_kind: {{ _eq: "message" }}
  chunk_count: {{ _gt: 1 }}
}}
order: {{ chunk_count: DESC }}
limit: 20
  ) {{
chunk_id
session_id
project_path
parent_external_id
role
chunk_index
chunk_count
content
  }}
}}"#,
            session_id = crate::benchmark_queries::escape_graphql(&fixture.hot_session.session_id),
        ))
        .await,
        "coding search chunk materialization",
    )
    .unwrap();

    let chunks = data["CodingSearchChunk"].as_array().unwrap();
    assert!(!chunks.is_empty());
    assert!(chunks.iter().all(|chunk| {
        chunk["session_id"].as_str() == Some(fixture.hot_session.session_id.as_str())
            && chunk["project_path"].as_str() == Some(fixture.hot_session.project_path.as_str())
            && chunk["role"].as_str() == Some("assistant")
    }));
    assert!(chunks
        .iter()
        .any(|chunk| chunk["chunk_index"].as_i64() == Some(1)));
    assert!(chunks
        .iter()
        .all(|chunk| chunk["chunk_count"].as_i64().unwrap_or_default() > 1));
}

#[tokio::test]
async fn dense_search_v1_supports_production_coding_search_chunks() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 48;
    config.hot_session_actions = 24;
    config.assistant_message_bytes = 1_600;
    config.action_command_bytes = 480;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();

    let message_response = node
        .hybrid_search_dense(
            &dense_chunk_request(
                "message",
                "relation narrowing bm25 pushdown",
                Some(&fixture.hot_session.session_id),
                Some(&fixture.hot_session.project_path),
            )
            .with_limit(6),
        )
        .await
        .unwrap();
    assert_eq!(message_response.collection_name, "CodingSearchChunk");
    assert_eq!(
        message_response.embedding_model,
        "coding-search-chunk-model"
    );
    assert!(message_response.query_vector_dimensions > 0);
    assert!(!message_response.hits.is_empty());
    assert!(message_response.hits.iter().all(|hit| {
        hit.fields["target_kind"].as_str() == Some("message")
            && hit.fields["session_id"].as_str() == Some(fixture.hot_session.session_id.as_str())
            && hit.fields["project_path"].as_str()
                == Some(fixture.hot_session.project_path.as_str())
    }));
    assert!(message_response.hits.iter().any(|hit| {
        hit.fields["content"]
            .as_str()
            .is_some_and(|content| content.contains("relation narrowing"))
    }));

    let action_response = node
        .hybrid_search_dense(
            &dense_chunk_request(
                "action",
                "rg pushdown planner joins",
                Some(&fixture.hot_session.session_id),
                Some(&fixture.hot_session.project_path),
            )
            .with_limit(6),
        )
        .await
        .unwrap();
    assert_eq!(action_response.collection_name, "CodingSearchChunk");
    assert!(!action_response.hits.is_empty());
    assert!(action_response.hits.iter().all(|hit| {
        hit.fields["target_kind"].as_str() == Some("action")
            && hit.fields["session_id"].as_str() == Some(fixture.hot_session.session_id.as_str())
            && hit.fields["project_path"].as_str()
                == Some(fixture.hot_session.project_path.as_str())
    }));
    assert!(action_response.hits.iter().any(|hit| {
        hit.fields["content"]
            .as_str()
            .is_some_and(|content| content.contains("rg"))
    }));

    let requests = server.requests();
    assert_eq!(requests.len(), total_embedding_documents(&node).await + 2);
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.model == "coding-search-chunk-model"
                    && request.input == "relation narrowing bm25 pushdown"
            })
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.model == "coding-search-chunk-model"
                    && request.input == "rg pushdown planner joins"
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn dense_search_v1_supports_query_text_api() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 64;
    config.hot_session_actions = 24;
    config.medium_session_messages = 16;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let tasks = build_context1_style_coding_tasks(&node, &fixture)
        .await
        .unwrap();

    for task_id in ["hot_messages_pushdown", "hot_actions_rg_pushdown"] {
        let task = tasks.iter().find(|task| task.task_id == task_id).unwrap();
        let session_doc_id = lookup_session_doc_id(&node, &task.session_id)
            .await
            .unwrap();
        let response = node
            .hybrid_search_dense(
                &dense_request_for_target(
                    task.target,
                    task.effective_query(),
                    Some(&session_doc_id),
                )
                .with_limit(6),
            )
            .await
            .unwrap();
        eprintln!(
            "{} dense v1 query=\"{}\" hits={}",
            task.task_id,
            response.query_text,
            response.hits.len()
        );

        assert!(response.query_vector_dimensions > 0);
        assert_eq!(response.embedding_model, task_embedding_model(task.target));
        assert!(!response.bm25_candidates.is_empty());
        assert!(!response.dense_candidates.is_empty());
        assert!(!response.hits.is_empty());

        match task.target {
            SearchTarget::Messages => {
                assert_eq!(response.collection_name, "CodingMessage");
                assert_eq!(response.vector_field, "content_v");
                assert!(response
                    .hits
                    .iter()
                    .all(|hit| hit.fields.get("message_id").is_some()
                        && hit.fields.get("content").is_some()));
            }
            SearchTarget::Actions => {
                assert_eq!(response.collection_name, "CodingAction");
                assert_eq!(response.vector_field, "command_v");
                assert!(response.hits.iter().all(|hit| {
                    hit.fields.get("action_type").is_some()
                        && hit.fields.get("target").is_some()
                        && hit.fields.get("command").is_some()
                }));
            }
        }

        assert!(
            response
                .hits
                .iter()
                .any(|hit| task.support_ids().contains(&hit.doc_id)),
            "{} dense v1 search missed all labeled supports",
            task.task_id
        );
    }

    let requests = server.requests();
    assert_eq!(requests.len(), total_embedding_documents(&node).await + 2);
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.model == "coding-message-model"
                    && request.input == "relation narrowing bm25 pushdown"
            })
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.model == "coding-action-model"
                    && request.input == "rg pushdown planner joins"
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn dense_search_v1_honors_filter_and_exclusions() {
    let server = MockEmbeddingServer::start().await;

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 48;
    config.hot_session_actions = 20;
    config.medium_session_messages = 14;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let session_doc_id = lookup_session_doc_id(&node, &fixture.hot_session.session_id)
        .await
        .unwrap();
    let request = dense_request_for_target(
        SearchTarget::Messages,
        "pushdown candidate",
        Some(&session_doc_id),
    )
    .with_limit(5);

    let first_response = node.hybrid_search_dense(&request).await.unwrap();
    assert!(!first_response.hits.is_empty());
    assert_doc_ids_belong_to_session(
        &node,
        "CodingMessage",
        &first_response
            .hits
            .iter()
            .map(|hit| hit.doc_id.clone())
            .collect::<Vec<_>>(),
        &session_doc_id,
    )
    .await
    .unwrap();

    let excluded_doc_id = first_response.hits[0].doc_id.clone();
    let second_response = node
        .hybrid_search_dense(
            &request
                .clone()
                .with_excluded_doc_ids(vec![excluded_doc_id.clone()]),
        )
        .await
        .unwrap();

    assert!(!second_response.hits.is_empty());
    assert!(
        second_response
            .hits
            .iter()
            .all(|hit| hit.doc_id != excluded_doc_id),
        "excluded doc_id {} was still returned",
        excluded_doc_id
    );
    assert_doc_ids_belong_to_session(
        &node,
        "CodingMessage",
        &second_response
            .hits
            .iter()
            .map(|hit| hit.doc_id.clone())
            .collect::<Vec<_>>(),
        &session_doc_id,
    )
    .await
    .unwrap();

    assert_eq!(
        server.requests().len(),
        total_embedding_documents(&node).await + 2
    );
}

struct RealEmbeddingServer {
    base_url: String,
    child: Child,
}

impl RealEmbeddingServer {
    async fn start() -> anyhow::Result<Self> {
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()?;
        let script = repo_root.join("tools/hf_embedding_server.py");

        let child = Command::new("python3")
            .arg(script)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--device")
            .arg("cpu")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        let server = Self {
            base_url: format!("http://127.0.0.1:{port}/v1"),
            child,
        };
        server.wait_until_ready().await?;
        Ok(server)
    }

    async fn wait_until_ready(&self) -> anyhow::Result<()> {
        let client = reqwest::Client::new();
        let health_url = self.base_url.trim_end_matches("/v1").to_string() + "/health";

        for _ in 0..120 {
            if let Ok(response) = client.get(&health_url).send().await {
                if response.status().is_success() {
                    return Ok(());
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        anyhow::bail!(
            "timed out waiting for local embedding server at {}",
            health_url
        );
    }
}

impl Drop for RealEmbeddingServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn request_real_embedding(
    base_url: &str,
    model: &str,
    input: &str,
) -> anyhow::Result<Vec<f64>> {
    let response = reqwest::Client::new()
        .post(format!("{}/embeddings", base_url.trim_end_matches('/')))
        .json(&serde_json::json!({
            "model": model,
            "input": input,
        }))
        .send()
        .await?;

    let response = response.error_for_status()?;
    let body: serde_json::Value = response.json().await?;
    let values = body
        .pointer("/data/0/embedding")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("missing data[0].embedding in response: {}", body))?;

    values
        .iter()
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("non-numeric embedding value: {}", value))
        })
        .collect()
}

fn render_real_message_similarity_query(vector: &[f64]) -> String {
    format!(
        r#"{{
  CodingMessage(order: {{ _alias: {{ sim: DESC }} }}, limit: 5) {{
message_id
sim: SIMILARITY(content_v: {{vector: [{vector}]}})
content
  }}
}}"#,
        vector = format_vector(vector),
    )
}

fn render_real_action_similarity_query(vector: &[f64]) -> String {
    format!(
        r#"{{
  CodingAction(order: {{ _alias: {{ sim: DESC }} }}, limit: 5) {{
action_type
sim: SIMILARITY(command_v: {{vector: [{vector}]}})
command
  }}
}}"#,
        vector = format_vector(vector),
    )
}

#[tokio::test]
#[ignore = "downloads local Hugging Face embedding models and runs a full real-model e2e"]
async fn coding_session_embedding_fixture_real_models_e2e() {
    let server = RealEmbeddingServer::start().await.unwrap();

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 40;
    config.hot_session_actions = 20;
    config.medium_session_messages = 12;
    config.medium_session_actions = 6;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let _fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    assert!(total_embedding_documents(&node).await > 102);

    let message_vector = request_real_embedding(
        &server.base_url,
        "coding-message-model",
        "relation narrowing before bm25 scoring pushdown",
    )
    .await
    .unwrap();
    let message_data = ensure_success(
        node.execute(&render_real_message_similarity_query(&message_vector))
            .await,
        "real message similarity",
    )
    .unwrap();
    let top_messages = message_data["CodingMessage"].as_array().unwrap();
    assert!(!top_messages.is_empty());
    assert!(top_messages.iter().any(|message| {
        message["content"]
            .as_str()
            .is_some_and(|content| content.contains("bm25"))
    }));
    assert!(top_messages
        .iter()
        .any(|message| message["sim"].as_f64().unwrap_or_default() > 0.0));

    let action_vector = request_real_embedding(
        &server.base_url,
        "coding-action-model",
        "rg bm25 nested query planner",
    )
    .await
    .unwrap();
    let action_data = ensure_success(
        node.execute(&render_real_action_similarity_query(&action_vector))
            .await,
        "real action similarity",
    )
    .unwrap();
    let top_actions = action_data["CodingAction"].as_array().unwrap();
    assert!(!top_actions.is_empty());
    assert!(top_actions.iter().any(|action| {
        action["command"]
            .as_str()
            .is_some_and(|command| command.contains("rg") || command.contains("cargo"))
    }));
    assert!(top_actions
        .iter()
        .any(|action| action["sim"].as_f64().unwrap_or_default() > 0.0));
}

#[tokio::test]
#[ignore = "downloads local Hugging Face embedding models and runs a hybrid bm25+dense comparison"]
async fn coding_session_embedding_fixture_real_models_hybrid_rank_comparison() {
    let server = RealEmbeddingServer::start().await.unwrap();

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 48;
    config.hot_session_actions = 24;
    config.medium_session_messages = 16;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();

    let message_query = "pushdown candidate relation narrowing";
    let message_vector =
        request_real_embedding(&server.base_url, "coding-message-model", message_query)
            .await
            .unwrap();
    let message_summary = run_hybrid_comparison(
        &node,
        SearchTarget::Messages,
        &fixture.hot_session.session_id,
        "real_hot_messages_pushdown_candidate",
        message_query,
        &message_vector,
        8,
    )
    .await
    .unwrap();
    eprintln!("{}", message_summary.render());
    assert_hybrid_summary(&message_summary, "pushdown");

    let action_query = "rg pushdown planner";
    let action_vector =
        request_real_embedding(&server.base_url, "coding-action-model", action_query)
            .await
            .unwrap();
    let action_summary = run_hybrid_comparison(
        &node,
        SearchTarget::Actions,
        &fixture.hot_session.session_id,
        "real_hot_actions_rg_pushdown",
        action_query,
        &action_vector,
        6,
    )
    .await
    .unwrap();
    eprintln!("{}", action_summary.render());
    assert_hybrid_summary(&action_summary, "rg");
}

#[tokio::test]
#[ignore = "downloads local Hugging Face embedding models and runs query-text hybrid search"]
async fn coding_session_embedding_fixture_real_models_hybrid_search_api() {
    let server = RealEmbeddingServer::start().await.unwrap();

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 48;
    config.hot_session_actions = 24;
    config.medium_session_messages = 16;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let tasks = build_context1_style_coding_tasks(&node, &fixture)
        .await
        .unwrap();

    for task_id in ["hot_messages_pushdown", "hot_actions_rg_pushdown"] {
        let task = tasks.iter().find(|task| task.task_id == task_id).unwrap();
        let response = node
            .hybrid_search_coding(
                &crate::CodingHybridSearchRequest::new(
                    task_search_target(task.target),
                    task.effective_query(),
                )
                .with_session_id(task.session_id.clone())
                .with_limit(6),
            )
            .await
            .unwrap();
        eprintln!(
            "{} real hybrid_search_coding query=\"{}\" hits={}",
            task.task_id,
            response.query_text,
            response.hits.len()
        );

        assert_eq!(response.embedding_model, task_embedding_model(task.target));
        assert!(response.query_vector_dimensions > 0);
        assert!(!response.hits.is_empty());
        assert!(
            response
                .hits
                .iter()
                .any(|hit| task.support_ids().contains(&hit.doc_id)),
            "{} real hybrid_search_coding missed all labeled supports",
            task.task_id
        );
    }
}

#[tokio::test]
#[ignore = "downloads local Hugging Face embedding models and runs generic dense-search v1"]
async fn dense_search_v1_real_models_query_text_api() {
    let server = RealEmbeddingServer::start().await.unwrap();

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 48;
    config.hot_session_actions = 24;
    config.medium_session_messages = 16;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let tasks = build_context1_style_coding_tasks(&node, &fixture)
        .await
        .unwrap();

    for task_id in ["hot_messages_pushdown", "hot_actions_rg_pushdown"] {
        let task = tasks.iter().find(|task| task.task_id == task_id).unwrap();
        let session_doc_id = lookup_session_doc_id(&node, &task.session_id)
            .await
            .unwrap();
        let response = node
            .hybrid_search_dense(
                &dense_request_for_target(
                    task.target,
                    task.effective_query(),
                    Some(&session_doc_id),
                )
                .with_limit(6),
            )
            .await
            .unwrap();
        eprintln!(
            "{} real dense v1 query=\"{}\" hits={}",
            task.task_id,
            response.query_text,
            response.hits.len()
        );

        assert_eq!(response.embedding_model, task_embedding_model(task.target));
        assert!(response.query_vector_dimensions > 0);
        assert!(!response.hits.is_empty());
        assert!(
            response
                .hits
                .iter()
                .any(|hit| task.support_ids().contains(&hit.doc_id)),
            "{} real dense v1 search missed all labeled supports",
            task.task_id
        );
    }
}

#[tokio::test]
#[ignore = "downloads local Hugging Face embedding models and evaluates context-1-style coding tasks"]
async fn coding_session_embedding_fixture_real_models_context1_task_eval() {
    let server = RealEmbeddingServer::start().await.unwrap();

    let mut config = CodingSessionFixtureConfig::smoke_test();
    config.hot_session_messages = 48;
    config.hot_session_actions = 24;
    config.medium_session_messages = 16;
    config.medium_session_actions = 8;

    let node = crate::EmbeddedNode::builder()
        .with_embedding_url(server.base_url.clone())
        .build()
        .await
        .unwrap();

    let fixture = seed_coding_session_embedding_fixture(&node, &config)
        .await
        .unwrap();
    let tasks = build_context1_style_coding_tasks(&node, &fixture)
        .await
        .unwrap();

    for task_id in ["hot_messages_pushdown", "hot_actions_rg_pushdown"] {
        let task = tasks.iter().find(|task| task.task_id == task_id).unwrap();
        let query_vector = request_real_embedding(
            &server.base_url,
            task_embedding_model(task.target),
            task.effective_query(),
        )
        .await
        .unwrap();
        let evaluation = evaluate_coding_task(&node, task, &query_vector, 6)
            .await
            .unwrap();
        eprintln!("{}", evaluation.render());

        let rrf = evaluation.metric(RetrievalStrategy::Rrf).unwrap();
        assert!(rrf.answer_found, "{} rrf missed all supports", task.task_id);
        assert!(
            rrf.support_hits >= 1,
            "{} rrf did not recover any labeled support item",
            task.task_id
        );
    }
}
