//! Embedding client configuration (moved from `db::database`).

/// Embedding client configuration for OpenAI-compatible endpoints.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct EmbeddingClientConfig {
    pub url: String,
    pub model: String,
    pub api_key: String,
}

impl std::fmt::Debug for EmbeddingClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingClientConfig")
            .field("url", &self.url)
            .field("model", &self.model)
            .field("api_key_configured", &!self.api_key.is_empty())
            .finish()
    }
}

impl EmbeddingClientConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = api_key.into();
        self
    }
}
