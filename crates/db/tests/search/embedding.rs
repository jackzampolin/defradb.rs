use db::search::embedding::ResolvedEmbeddingConfig;
use db::search::embedding::*;
use db::search::EmbeddingClientConfig;
use document::Document;
use schema::VectorEmbeddingDescription;

fn embedding_with(url: &str, model: &str) -> VectorEmbeddingDescription {
    embedding_with_provider("openai", url, model)
}

fn embedding_with_provider(provider: &str, url: &str, model: &str) -> VectorEmbeddingDescription {
    VectorEmbeddingDescription {
        field_name: "content_v".to_string(),
        fields: vec!["content".to_string()],
        model: model.to_string(),
        provider: provider.to_string(),
        template: String::new(),
        url: url.to_string(),
    }
}

#[test]
fn resolve_embedding_config_prefers_schema_values() {
    let embedding = embedding_with("https://schema.example/v1", "schema-model");
    let config = EmbeddingClientConfig::new()
        .with_url("https://node.example/v1")
        .with_model("node-model")
        .with_api_key("secret");

    let resolved = resolve_embedding_config(&embedding, &config).unwrap();

    assert_eq!(
        resolved,
        ResolvedEmbeddingConfig {
            url: "https://schema.example/v1",
            model: "schema-model",
        }
    );
}

#[test]
fn resolve_embedding_config_falls_back_to_node_values() {
    let embedding = embedding_with("", "");
    let config = EmbeddingClientConfig::new()
        .with_url("https://node.example/v1")
        .with_model("node-model");

    let resolved = resolve_embedding_config(&embedding, &config).unwrap();

    assert_eq!(
        resolved,
        ResolvedEmbeddingConfig {
            url: "https://node.example/v1",
            model: "node-model",
        }
    );
}

#[test]
fn resolve_embedding_config_uses_openai_default_url() {
    let embedding = embedding_with("", "schema-model");
    let config = EmbeddingClientConfig::new();

    assert_eq!(
        resolve_embedding_config(&embedding, &config).unwrap(),
        ResolvedEmbeddingConfig {
            url: "https://api.openai.com/v1",
            model: "schema-model",
        }
    );
}

#[test]
fn resolve_embedding_config_uses_ollama_default_url() {
    let embedding = embedding_with_provider("ollama", "", "schema-model");
    let config = EmbeddingClientConfig::new();

    assert_eq!(
        resolve_embedding_config(&embedding, &config).unwrap(),
        ResolvedEmbeddingConfig {
            url: "http://localhost:11434/api",
            model: "schema-model",
        }
    );
}

#[test]
fn resolve_embedding_config_requires_model() {
    let embedding = embedding_with("https://schema.example/v1", "");
    let config = EmbeddingClientConfig::new();

    assert_eq!(
        resolve_embedding_config(&embedding, &config),
        Err(MissingEmbeddingConfig::Model)
    );
}

#[test]
fn parse_embedding_vector_errors_on_non_numeric_value() {
    let values = vec![serde_json::json!(1.0), serde_json::json!("bad")];

    let err = parse_embedding_vector(&values).unwrap_err();

    assert_eq!(err, "embedding value at index 1 is not numeric");
}

#[tokio::test]
async fn set_embedding_skips_when_effective_config_is_missing() {
    let embeddings = vec![embedding_with_provider("unknown", "", "")];
    let mut doc = Document::new();
    doc.set("content", "hello");

    let generated = set_embedding(
        &embeddings,
        &mut doc,
        true,
        None,
        &EmbeddingClientConfig::new(),
    )
    .await
    .unwrap();

    assert!(generated.is_empty());
    assert!(doc.get("content_v").is_none());
}

#[tokio::test]
async fn set_embedding_uses_each_field_provider_and_model() {
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::net::TcpListener;

    async fn openai(Json(request): Json<Value>) -> Json<Value> {
        assert_eq!(
            request,
            json!({"model": "text-embedding-3-small", "input": "hello\n"})
        );
        Json(json!({"data": [{"embedding": [1.0, 0.0]}]}))
    }

    async fn ollama(Json(request): Json<Value>) -> Json<Value> {
        assert_eq!(
            request,
            json!({"model": "nomic-embed-text", "prompt": "world\n"})
        );
        Json(json!({"embedding": [0.0, 2.0]}))
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/openai/embeddings", post(openai))
                .route("/ollama/embeddings", post(ollama)),
        )
        .await
        .unwrap();
    });

    let openai = embedding_with_provider(
        "openai",
        &format!("http://{address}/openai"),
        "text-embedding-3-small",
    );
    let mut ollama = embedding_with_provider(
        "ollama",
        &format!("http://{address}/ollama"),
        "nomic-embed-text",
    );
    ollama.field_name = "summary_v".to_string();
    ollama.fields = vec!["summary".to_string()];

    let mut doc = Document::new();
    doc.set("content", "hello");
    doc.set("summary", "world");

    let generated = set_embedding(
        &[openai, ollama],
        &mut doc,
        true,
        None,
        &EmbeddingClientConfig::new(),
    )
    .await
    .unwrap();
    server.abort();

    assert_eq!(generated, vec!["content_v", "summary_v"]);
    assert_eq!(
        doc.get("content_v"),
        Some(&document::NormalValue::Float64Array(vec![1.0, 0.0]))
    );
    assert_eq!(
        doc.get("summary_v"),
        Some(&document::NormalValue::Float64Array(vec![0.0, 1.0]))
    );
}
