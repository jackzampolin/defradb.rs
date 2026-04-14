//! Vector search, embedding client, and hybrid BM25+dense retrieval for DefraDB.
//!
//! Extracted from the `db` crate as Phase 6 of #796. This crate holds:
//!
//! - [`EmbeddingClientConfig`] — OpenAI-compatible embedding API configuration
//! - [`embed_text`] — generate query-time embedding vectors via the configured endpoint
//! - [`set_embedding`] — populate document field embeddings on create/update
//! - [`hybrid_search_dense`] — BM25 + dense similarity hybrid retrieval with
//!   reciprocal rank fusion
//! - [`DenseHybridSearchRequest`] / [`DenseHybridSearchResponse`] / [`DenseHybridSearchHit`]
//!   — public request/response types
//! - [`require_query_success`] — helper to unwrap a [`query::QueryResponse`]
//!
//! This crate depends only on `query`, `schema`, `document`, and `reqwest` —
//! no dependency back to `db`, so `db` depends on this crate cleanly.

mod config;
pub mod dense_search;
pub mod embedding;

pub use config::EmbeddingClientConfig;
pub use dense_search::{
    hybrid_search_dense, require_query_success, DenseHybridSearchHit, DenseHybridSearchRequest,
    DenseHybridSearchResponse,
};
pub use embedding::{embed_text, set_embedding};
