pub(crate) mod auth;
mod cascade;
mod catalog;
mod execute;
mod protocol;
mod query_aggregate;
mod query_distinct;
mod query_join;
mod query_set_ops;

use std::sync::Arc;

use futures::Sink;
use identity::Did;
use pgwire::api::results::{Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use query::{CollectionProvider, QueryExecutor, QueryRequest, TransactionHandle};
use tokio::sync::RwLock;
use tracing::debug;

use crate::bridge::MutationKind;
use crate::encode;
use crate::metadata::DdlMetadata;

pub use protocol::DefraHandlerFactory;
use protocol::DefraQueryParser;

const TXN_ID_KEY: &str = "txn_id";

/// Trait for creating DefraDB schemas from SQL DDL.
#[async_trait::async_trait]
pub trait SchemaManager: Send + Sync {
    /// Add a schema from a GraphQL SDL string.
    async fn add_schema(&self, sdl: &str) -> Result<(), String>;
}

/// Handler for Postgres wire protocol queries.
///
/// Translates SQL to GraphQL, executes via QueryExecutor, and encodes results.
/// Implements both SimpleQueryHandler and ExtendedQueryHandler.
pub struct DefraQueryHandler {
    executor: Arc<dyn QueryExecutor>,
    collections: Arc<dyn CollectionProvider>,
    parser: Arc<DefraQueryParser>,
    schema_manager: Option<Arc<dyn SchemaManager>>,
    ddl_metadata: Arc<RwLock<DdlMetadata>>,
}

impl DefraQueryHandler {
    pub fn new(
        executor: Arc<dyn QueryExecutor>,
        collections: Arc<dyn CollectionProvider>,
        schema_manager: Option<Arc<dyn SchemaManager>>,
    ) -> Self {
        let parser = Arc::new(DefraQueryParser);
        Self {
            executor,
            collections,
            parser,
            schema_manager,
            ddl_metadata: Arc::new(RwLock::new(DdlMetadata::default())),
        }
    }

    async fn execute_graphql(
        &self,
        graphql: &str,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<query::QueryResponse> {
        let mut request = QueryRequest::new(graphql);
        if let Some(did_str) = identity_did {
            request = request.with_identity(Did::new(did_str).ok());
        }
        match txn_id {
            Some(id) => {
                let handle: TransactionHandle = id
                    .parse()
                    .map_err(|e: query::TransactionError| pg_error("XX000", e.to_string()))?;
                Ok(self.executor.execute_in_txn(request, &handle).await)
            }
            None => Ok(self.executor.execute(request).await),
        }
    }
}

// -- Core query/mutation handlers --

impl DefraQueryHandler {
    async fn handle_query_single(
        &self,
        graphql: &str,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        debug!(graphql, "Translated to GraphQL query");

        let table_name = extract_table_name_from_graphql(graphql);
        let response = self.execute_graphql(graphql, txn_id, identity_did).await?;

        if response.has_errors() {
            return Err(pg_error("XX000", format_errors(&response.errors)));
        }

        let data = match &response.data {
            Some(d) => d,
            None => return Ok(encode::encode_empty_response("SELECT 0")),
        };

        let docs = match data.get(&table_name) {
            Some(serde_json::Value::Array(arr)) => arr,
            _ => return Ok(encode::encode_empty_response("SELECT 0")),
        };

        let collection = self
            .collections
            .get_collection(&table_name)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match collection {
            Some(col) => {
                let fields_str = extract_fields_from_graphql(graphql);
                let requested = encode::extract_requested_fields(&fields_str);
                encode::encode_response(docs, &col, &requested)
            }
            None => Err(pg_error(
                "42P01",
                format!("relation \"{}\" does not exist", table_name),
            )),
        }
    }

    async fn handle_mutation_single(
        &self,
        graphql: &str,
        table_name: &str,
        mutation_name: &str,
        kind: MutationKind,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        debug!(graphql, "Translated to GraphQL mutation");

        let response = self.execute_graphql(graphql, txn_id, identity_did).await?;

        if response.has_errors() {
            return Err(pg_error("XX000", format_errors(&response.errors)));
        }

        let data = match &response.data {
            Some(d) => d,
            None => return Ok(execution_tag(&kind, 0)),
        };

        let docs = match data.get(mutation_name) {
            Some(serde_json::Value::Array(arr)) => arr,
            _ => return Ok(execution_tag(&kind, 0)),
        };

        let row_count = docs.len();

        let fields_str = extract_fields_from_graphql(graphql);
        let has_returning = fields_str != "_docID";

        if has_returning {
            let collection = self
                .collections
                .get_collection(table_name)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            if let Some(col) = collection {
                let requested = encode::extract_requested_fields(&fields_str);
                return encode::encode_response(docs, &col, &requested);
            }
        }

        Ok(execution_tag(&kind, row_count))
    }

    async fn handle_begin_single<C>(&self, client: &mut C) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        if client.metadata().contains_key(TXN_ID_KEY) {
            return Err(pg_error(
                "25001",
                "there is already a transaction in progress".to_string(),
            ));
        }

        let handle = self
            .executor
            .begin_txn(false)
            .await
            .map_err(|e| pg_error("XX000", e.to_string()))?;

        client
            .metadata_mut()
            .insert(TXN_ID_KEY.to_string(), handle.to_string());

        Ok(Response::TransactionStart(Tag::new("BEGIN")))
    }

    async fn handle_commit_single<C>(&self, client: &mut C) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let txn_id =
            client.metadata().get(TXN_ID_KEY).cloned().ok_or_else(|| {
                pg_error("25P01", "there is no transaction in progress".to_string())
            })?;

        let handle: TransactionHandle = txn_id
            .parse()
            .map_err(|e: query::TransactionError| pg_error("XX000", e.to_string()))?;

        self.executor
            .commit_txn(&handle)
            .await
            .map_err(|e| pg_error("XX000", e.to_string()))?;

        client.metadata_mut().remove(TXN_ID_KEY);

        Ok(Response::TransactionEnd(Tag::new("COMMIT")))
    }

    async fn handle_rollback_single<C>(&self, client: &mut C) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let txn_id =
            client.metadata().get(TXN_ID_KEY).cloned().ok_or_else(|| {
                pg_error("25P01", "there is no transaction in progress".to_string())
            })?;

        let handle: TransactionHandle = txn_id
            .parse()
            .map_err(|e: query::TransactionError| pg_error("XX000", e.to_string()))?;

        self.executor
            .rollback_txn(&handle)
            .await
            .map_err(|e| pg_error("XX000", e.to_string()))?;

        client.metadata_mut().remove(TXN_ID_KEY);

        Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
    }
}

// -- Helpers --

fn execution_tag(kind: &MutationKind, row_count: usize) -> Response {
    let tag = match kind {
        MutationKind::Insert => Tag::new("INSERT").with_oid(0).with_rows(row_count),
        MutationKind::Update => Tag::new("UPDATE").with_rows(row_count),
        MutationKind::Delete => Tag::new("DELETE").with_rows(row_count),
    };
    Response::Execution(tag)
}

fn pg_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

fn format_errors(errors: &[query::QueryResponseError]) -> String {
    errors
        .iter()
        .map(|e| e.message.as_str())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Extract table name from GraphQL: "query { TableName(...) { ... } }" or "mutation { ... }"
fn extract_table_name_from_graphql(gql: &str) -> String {
    let after_brace = gql.find('{').map(|i| &gql[i + 1..]).unwrap_or(gql);
    let trimmed = after_brace.trim_start();
    trimmed
        .split(|c: char| c == '(' || c == '{' || c.is_whitespace())
        .next()
        .unwrap_or("")
        .to_string()
}

/// Extract field list from GraphQL: "query { Table(...) { field1 field2 } }"
fn extract_fields_from_graphql(gql: &str) -> String {
    let trimmed = gql.trim().trim_end_matches('}').trim();
    if let Some(last_open) = trimmed.rfind('{') {
        let fields_section = &trimmed[last_open + 1..];
        let fields = fields_section.trim().trim_end_matches('}').trim();
        return fields.to_string();
    }
    String::new()
}

/// Extract 'table_name'::regclass from SQL (for PK queries).
fn extract_regclass_table(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let idx = lower.find("::regclass")?;
    let before = &sql[..idx];
    let quote_end = before.rfind('\'')?;
    let before_quote = &before[..quote_end];
    let quote_start = before_quote.rfind('\'')?;
    Some(before[quote_start + 1..quote_end].to_string())
}

/// Extract (field, value) from a GraphQL filter pattern like `filter: {field: {_eq: "value"}}`.
fn extract_filter_from_graphql(gql: &str) -> (String, String) {
    static RE_STR: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"filter:\s*\{(\w+):\s*\{_eq:\s*"([^"]*)"\}\}"#)
            .expect("valid regex literal")
    });
    static RE_NUM: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"filter:\s*\{(\w+):\s*\{_eq:\s*(\d+)\}\}"#)
            .expect("valid regex literal")
    });

    if let Some(caps) = RE_STR.captures(gql) {
        return (caps[1].to_string(), caps[2].to_string());
    }
    if let Some(caps) = RE_NUM.captures(gql) {
        return (caps[1].to_string(), caps[2].to_string());
    }
    (String::new(), String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_table_from_graphql() {
        assert_eq!(
            extract_table_name_from_graphql("query { User(filter: {age: {_gt: 25}}) { name } }"),
            "User"
        );
        assert_eq!(
            extract_table_name_from_graphql("query { User { name age } }"),
            "User"
        );
    }

    #[test]
    fn extract_table_from_mutation() {
        assert_eq!(
            extract_table_name_from_graphql(
                "mutation { add_User(input: {name: \"Alice\"}) { _docID } }"
            ),
            "add_User"
        );
    }

    #[test]
    fn extract_fields() {
        let fields =
            extract_fields_from_graphql("query { User(filter: {age: {_gt: 25}}) { name age } }");
        assert_eq!(fields, "name age");
    }

    #[test]
    fn extract_fields_from_mutation() {
        let fields = extract_fields_from_graphql(
            "mutation { add_User(input: {name: \"Alice\"}) { _docID name } }",
        );
        assert_eq!(fields, "_docID name");
    }
}
