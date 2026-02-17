use document::{Document, NormalValue};
use schema::VectorEmbeddingDescription;
use std::collections::HashSet;

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

        // Build text from source field values
        let mut text = String::new();
        for field_name in &embedding.fields {
            if let Some(val) = doc.get(field_name) {
                let s = normal_value_to_string(val);
                text.push_str(&s);
                text.push('\n');
            }
        }

        // Call embedding provider
        let vec =
            call_embedding_provider(&embedding.provider, &embedding.model, &embedding.url, &text)
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

async fn call_embedding_provider(
    provider: &str,
    model: &str,
    url: &str,
    text: &str,
) -> Result<Vec<f64>, EmbeddingError> {
    match provider {
        "ollama" => call_ollama(model, url, text).await,
        "openai" => call_openai(model, url, text).await,
        other => Err(format!("unsupported embedding provider: {}", other).into()),
    }
}

async fn call_ollama(model: &str, url: &str, text: &str) -> Result<Vec<f64>, EmbeddingError> {
    let base = if url.is_empty() {
        "http://localhost:11434/api"
    } else {
        url
    };
    let endpoint = format!("{}/embeddings", base.trim_end_matches('/'));

    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "model": model,
        "prompt": text,
    });

    let resp = client.post(&endpoint).json(&body).send().await?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("ollama returned {}: {}", status, text).into());
    }

    let result: serde_json::Value = resp.json().await?;
    let embedding = result
        .get("embedding")
        .and_then(|v| v.as_array())
        .ok_or("ollama response missing 'embedding' array")?;

    let vec: Vec<f64> = embedding
        .iter()
        .map(|v| v.as_f64().unwrap_or(0.0))
        .collect();

    Ok(vec)
}

async fn call_openai(model: &str, url: &str, text: &str) -> Result<Vec<f64>, EmbeddingError> {
    let base = if url.is_empty() {
        "https://api.openai.com/v1"
    } else {
        url
    };
    let endpoint = format!("{}/embeddings", base.trim_end_matches('/'));

    let api_key = std::env::var("OPENAI_API_KEY").unwrap_or_default();

    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "model": model,
        "input": text,
    });

    let resp = client
        .post(&endpoint)
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&body)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("openai returned {}: {}", status, text).into());
    }

    let result: serde_json::Value = resp.json().await?;
    let embedding = result
        .pointer("/data/0/embedding")
        .and_then(|v| v.as_array())
        .ok_or("openai response missing embedding data")?;

    let vec: Vec<f64> = embedding
        .iter()
        .map(|v| v.as_f64().unwrap_or(0.0))
        .collect();

    Ok(vec)
}
