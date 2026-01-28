//! Vector embedding types for AI/ML integration.
//!
//! Matches Go's VectorEmbeddingDescription in client/collection_description.go

use serde::{Deserialize, Serialize};

/// Describes configuration for generating embedding vectors.
/// Matches Go's VectorEmbeddingDescription.
///
/// Embeddings are AI/ML specific vector representations of document content.
/// When configured, embeddings may call 3rd party APIs inline with document mutations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorEmbeddingDescription {
    /// Name of the field on the collection that this embedding applies to.
    #[serde(rename = "FieldName", default)]
    pub field_name: String,

    /// Fields in the parent schema used as basis for vector generation.
    #[serde(rename = "Fields", default)]
    pub fields: Vec<String>,

    /// The LLM model to use for generating embeddings.
    /// Example: "text-embedding-3-small"
    #[serde(rename = "Model", default)]
    pub model: String,

    /// The API provider for generating embeddings.
    /// Example: "openai"
    #[serde(rename = "Provider", default)]
    pub provider: String,

    /// Optional template path for formatting field values.
    /// Uses Go template syntax with field names as variables.
    #[serde(rename = "Template", default)]
    pub template: String,

    /// URL endpoint of the provider's API.
    /// Example: "https://api.openai.com/v1"
    /// If not provided, uses the default URL for the given provider.
    #[serde(rename = "URL", default)]
    pub url: String,
}

impl VectorEmbeddingDescription {
    /// Create a new vector embedding description.
    pub fn new(
        field_name: impl Into<String>,
        model: impl Into<String>,
        provider: impl Into<String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            fields: Vec::new(),
            model: model.into(),
            provider: provider.into(),
            template: String::new(),
            url: String::new(),
        }
    }

    /// Add source fields for the embedding.
    pub fn with_fields(mut self, fields: Vec<String>) -> Self {
        self.fields = fields;
        self
    }

    /// Set the template.
    pub fn with_template(mut self, template: impl Into<String>) -> Self {
        self.template = template.into();
        self
    }

    /// Set the API URL.
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_builder() {
        let embedding =
            VectorEmbeddingDescription::new("name_embedding", "text-embedding-3-small", "openai")
                .with_fields(vec!["name".to_string(), "description".to_string()])
                .with_url("https://api.openai.com/v1");

        assert_eq!(embedding.field_name, "name_embedding");
        assert_eq!(embedding.model, "text-embedding-3-small");
        assert_eq!(embedding.provider, "openai");
        assert_eq!(embedding.fields.len(), 2);
    }

    #[test]
    fn test_embedding_serialization() {
        let embedding =
            VectorEmbeddingDescription::new("vec_field", "text-embedding-3-small", "openai")
                .with_fields(vec!["name".to_string()]);

        let json = serde_json::to_string(&embedding).unwrap();
        assert!(json.contains("\"FieldName\""));
        assert!(json.contains("\"Model\""));
        assert!(json.contains("\"Provider\""));

        let parsed: VectorEmbeddingDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(embedding, parsed);
    }
}
