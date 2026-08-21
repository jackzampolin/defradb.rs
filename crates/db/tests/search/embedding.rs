use db::search::embedding::ResolvedEmbeddingConfig;
use db::search::embedding::*;
use db::search::EmbeddingClientConfig;
use document::Document;
use schema::VectorEmbeddingDescription;

fn embedding_with(url: &str, model: &str) -> VectorEmbeddingDescription {
    VectorEmbeddingDescription {
        field_name: "content_v".to_string(),
        fields: vec!["content".to_string()],
        model: model.to_string(),
        provider: "openai".to_string(),
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
fn resolve_embedding_config_requires_url() {
    let embedding = embedding_with("", "schema-model");
    let config = EmbeddingClientConfig::new();

    assert_eq!(
        resolve_embedding_config(&embedding, &config),
        Err(MissingEmbeddingConfig::Url)
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
    let embeddings = vec![embedding_with("", "")];
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
