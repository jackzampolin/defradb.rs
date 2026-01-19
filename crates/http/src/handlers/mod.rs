//! HTTP request handlers.
//!
//! This module contains handlers for all HTTP endpoints:
//! - GraphQL and transaction endpoints
//! - REST collection endpoints
//! - REST document endpoints

pub mod collections;
pub mod documents;
pub mod graphql;

// Re-export GraphQL handlers for backwards compatibility
pub use graphql::{
    graphql, graphql_get, graphql_transactional, health_check, schema, tx_begin, tx_commit,
    tx_rollback, version, GraphqlQueryParams, TransactionalQueryRequest, TxBeginRequest,
    TxBeginResponse, TxRequest, TxSuccessResponse, VersionResponse,
};

// Re-export REST handlers
pub use collections::{
    get_collection_doc_ids, list_collections, CollectionsResponse, DocIdsResponse,
};
pub use documents::{
    create_document, delete_document, get_document, update_document, DeleteResponse,
};
