//! Vector search, embeddings and hybrid BM25 + dense retrieval.

mod config;
pub mod dense;
pub mod embedding;

pub use config::EmbeddingClientConfig;
pub use dense::{
    hybrid_search_dense, require_query_success, DenseHybridSearchHit, DenseHybridSearchRequest,
    DenseHybridSearchResponse,
};
pub use embedding::{embed_text, set_embedding};
