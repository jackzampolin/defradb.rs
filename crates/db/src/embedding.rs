use crate::EmbeddingClientConfig;
use document::{Document, NormalValue};
use schema::VectorEmbeddingDescription;
use std::collections::HashSet;
use std::sync::OnceLock;
use tracing::warn;

#[cfg(not(target_arch = "wasm32"))]
type EmbeddingError = Box<dyn std::error::Error + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type EmbeddingError = Box<dyn std::error::Error>;

/// Generate and set embedding vectors on a document based on the collection's
/// vector embedding configuration.
///
/// Returns the names of fields that were generated (so the caller can add them
/// to modified_fields for block creation).
///
/// On create: generates embeddings for all configured fields unless the user
/// already provided the vector value. Runs BEFORE doc ID generation so the
/// content-addressed ID includes embedding values.
///
/// On update: generates embeddings only if a source field was modified.
/// Skips generation if the user explicitly set the vector field.
pub async fn set_embedding(
    embeddings: &[VectorEmbeddingDescription],
    doc: &mut Document,
    is_create: bool,
    modified_fields: Option<&HashSet<String>>,
    embedding_config: &EmbeddingClientConfig,
) -> Result<Vec<String>, EmbeddingError> {
    let mut generated = Vec::new();

    for embedding in embeddings {
        // Skip if user explicitly provided the embedding vector
        if is_create {
            if let Some(fv) = doc.get_field_value(&embedding.field_name) {
                if fv.is_dirty() {
                    continue;
                }
            }
        } else if let Some(fields) = modified_fields {
            if fields.contains(&embedding.field_name) {
                continue;
            }
        }

        // On update, check if any source field was modified
        if !is_create {
            if let Some(fields) = modified_fields {
                let any_source_modified = embedding.fields.iter().any(|f| fields.contains(f));
                if !any_source_modified {
                    continue;
                }
            }
        }

        if !embedding.provider.is_empty() && embedding.provider != "openai" {
            warn!(
                provider = %embedding.provider,
                field = %embedding.field_name,
                "embedding provider is not recognized, using OpenAI-compatible API"
            );
        }

        let resolved_config = match resolve_embedding_config(embedding, embedding_config) {
            Ok(config) => config,
            // Missing runtime config is non-fatal by design: collection schemas may
            // declare embeddings while node-level defaults are intentionally absent.
            Err(MissingEmbeddingConfig::Url) => {
                warn!(
                    field = %embedding.field_name,
                    "embedding URL is empty, skipping embedding generation"
                );
                continue;
            }
            Err(MissingEmbeddingConfig::Model) => {
                warn!(
                    field = %embedding.field_name,
                    "embedding model is empty, skipping embedding generation"
                );
                continue;
            }
        };

        // Build text from source field values
        let mut text = String::new();
        for field_name in &embedding.fields {
            if let Some(val) = doc.get(field_name) {
                let s = normal_value_to_string(val);
                text.push_str(&s);
                text.push('\n');
            }
        }

        let vec = call_embedding(
            resolved_config.url,
            resolved_config.model,
            &embedding_config.api_key,
            &text,
        )
        .await?;

        doc.set(&embedding.field_name, NormalValue::Float64Array(vec));
        generated.push(embedding.field_name.clone());
    }

    Ok(generated)
}

fn normal_value_to_string(val: &NormalValue) -> String {
    match val {
        NormalValue::String(s) => s.clone(),
        NormalValue::Int(i) => i.to_string(),
        NormalValue::Float64(f) => format!("{}", f),
        NormalValue::Float32(f) => format!("{}", f),
        NormalValue::Bool(b) => b.to_string(),
        other => format!("{:?}", other),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum MissingEmbeddingConfig {
    Url,
    Model,
}

#[derive(Debug, PartialEq, Eq)]
struct ResolvedEmbeddingConfig<'a> {
    url: &'a str,
    model: &'a str,
}

fn resolve_embedding_config<'a>(
    embedding: &'a VectorEmbeddingDescription,
    embedding_config: &'a EmbeddingClientConfig,
) -> Result<ResolvedEmbeddingConfig<'a>, MissingEmbeddingConfig> {
    let url = if embedding.url.is_empty() {
        embedding_config.url.as_str()
    } else {
        embedding.url.as_str()
    };
    if url.is_empty() {
        return Err(MissingEmbeddingConfig::Url);
    }

    let model = if embedding.model.is_empty() {
        embedding_config.model.as_str()
    } else {
        embedding.model.as_str()
    };
    if model.is_empty() {
        return Err(MissingEmbeddingConfig::Model);
    }

    Ok(ResolvedEmbeddingConfig { url, model })
}

async fn call_embedding(
    url: &str,
    model: &str,
    api_key: &str,
    text: &str,
) -> Result<Vec<f64>, EmbeddingError> {
    let endpoint = format!("{}/embeddings", url.trim_end_matches('/'));
    let body = serde_json::json!({
        "model": model,
        "input": text,
    });

    let mut request = embedding_client().post(&endpoint);
    if !api_key.is_empty() {
        request = request.bearer_auth(api_key);
    }

    let resp = request.json(&body).send().await?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("embedding provider returned {}: {}", status, text).into());
    }

    let result: serde_json::Value = resp.json().await?;
    let embedding = result
        .pointer("/data/0/embedding")
        .and_then(|v| v.as_array())
        .ok_or("embedding response missing data[0].embedding")?;

    let vec = parse_embedding_vector(embedding).map_err(|err| -> EmbeddingError { err.into() })?;

    Ok(vec)
}

fn embedding_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

fn parse_embedding_vector(embedding: &[serde_json::Value]) -> Result<Vec<f64>, String> {
    embedding
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_f64()
                .ok_or_else(|| format!("embedding value at index {} is not numeric", index))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
