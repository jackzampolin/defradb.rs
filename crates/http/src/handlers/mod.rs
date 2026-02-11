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
pub mod encrypted_index;
pub mod graphql;
pub mod index;
pub mod lens;
pub mod nac;
pub mod p2p;
pub mod schema;
pub mod utility;
pub mod views;

// Re-export GraphQL handlers for backwards compatibility
pub use graphql::{
    graphql, graphql_get, graphql_transactional, graphql_ws_handler, health_check, schema,
    tx_begin, tx_begin_concurrent, tx_commit, tx_discard, version, GraphqlQueryParams,
    TransactionalQueryRequest, TxBeginQuery, TxBeginResponse, TxPathParam, VersionResponse,
};

// Re-export REST handlers
pub use collections::{
    collection_exists, delete_collection, delete_collection_versions, describe_collection,
    find_collection_by_id, get_all_collections, get_collection_by_version_id,
    get_collection_doc_ids, list_collections, patch_collection, set_active, truncate_collection,
    CollectionsResponse, DocIdsResponse,
};
pub use documents::{create_document, delete_document, get_document, update_document};
