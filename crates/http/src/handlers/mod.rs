//! HTTP request handlers.
//!
//! This module contains handlers for all HTTP endpoints:
//! - GraphQL and transaction endpoints
//! - REST collection endpoints
//! - REST document endpoints
//! - P2P endpoints
//! - ACP (Access Control Policy) endpoints
//! - Index management endpoints
//! - Backup endpoints

pub mod acp;
pub mod backup;
pub mod collections;
pub mod documents;
pub mod graphql;
pub mod index;
pub mod p2p;
pub mod schema;

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
