//! GraphQL and transaction HTTP handlers.
//!
//! # NAC Permission Model
//!
//! GraphQL endpoint permissions are checked based on the operation type:
//! - Query operations require `DocumentRead` permission
//! - Mutation operations require `DocumentUpdate` permission
//!
//! This matches Go DefraDB's per-operation permission model more closely,
//! where each operation type has its own permission requirement.

mod meta;
mod query;
mod transactions;

/// Go DefraDB transaction header name.
pub(crate) const TX_HEADER_NAME: &str = "x-defradb-tx";

pub use meta::{health_check, schema, version};
pub(crate) use query::{check_encrypted_fields, graphql_required_permission};
pub use query::{
    graphql, graphql_get, graphql_transactional, graphql_ws_handler, GraphqlQueryParams,
    TransactionalQueryRequest,
};
pub use transactions::{
    tx_begin, tx_commit, tx_discard, TxBeginQuery, TxBeginResponse, TxPathParam,
};
