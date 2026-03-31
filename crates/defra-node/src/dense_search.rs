//! Thin embedded-node wrappers over the generic db dense-search primitives.

use anyhow::Result;

use crate::EmbeddedNode;

pub use db::{DenseHybridSearchHit, DenseHybridSearchRequest, DenseHybridSearchResponse};

impl EmbeddedNode {
    /// Embed free-form text using the node's configured embedding runtime.
    pub async fn embed_text(&self, text: &str, model: Option<&str>) -> Result<Vec<f64>> {
        db::embed_text(self.embedding_config(), text, model).await
    }

    /// Run dense-search v1 over an arbitrary collection.
    pub async fn hybrid_search_dense(
        &self,
        request: &DenseHybridSearchRequest,
    ) -> Result<DenseHybridSearchResponse> {
        db::hybrid_search_dense(self.runner.as_ref(), self.embedding_config(), request).await
    }
}

pub(crate) fn require_success(
    response: crate::QueryResponse,
    context: &str,
) -> Result<serde_json::Value> {
    db::require_query_success(response, context)
}
