use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{Response, Tag};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use query::{CollectionProvider, QueryExecutor, QueryRequest, TransactionHandle};
use tracing::{debug, warn};

use crate::bridge::{sql_to_graphql, MutationKind, SqlStatement};
use crate::encode;

const TXN_ID_KEY: &str = "txn_id";

/// Handler for Postgres wire protocol queries.
///
/// Translates SQL to GraphQL, executes via QueryExecutor, and encodes results.
pub struct DefraQueryHandler {
    executor: Arc<dyn QueryExecutor>,
    collections: Arc<dyn CollectionProvider>,
}

impl DefraQueryHandler {
    pub fn new(executor: Arc<dyn QueryExecutor>, collections: Arc<dyn CollectionProvider>) -> Self {
        Self {
            executor,
            collections,
        }
    }

    async fn execute_graphql(&self, graphql: &str, txn_id: Option<&str>) -> query::QueryResponse {
        let request = QueryRequest::new(graphql);
        match txn_id {
            Some(id) => {
                let handle: TransactionHandle = id.parse().expect("valid txn handle");
                self.executor.execute_in_txn(request, &handle).await
            }
            None => self.executor.execute(request).await,
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DefraQueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        debug!(sql = query, "PG query received");

        let statement = match sql_to_graphql(query) {
            Ok(stmt) => stmt,
            Err(e) => {
                warn!(error = %e, "SQL translation failed");
                return Err(pg_error("42601", e.to_string()));
            }
        };

        let txn_id = client.metadata().get(TXN_ID_KEY).cloned();

        match statement {
            SqlStatement::Query(graphql) => self.handle_query(&graphql, txn_id.as_deref()).await,
            SqlStatement::Mutation {
                graphql,
                table_name,
                mutation_name,
                kind,
            } => {
                self.handle_mutation(
                    &graphql,
                    &table_name,
                    &mutation_name,
                    kind,
                    txn_id.as_deref(),
                )
                .await
            }
            SqlStatement::Begin => self.handle_begin(client).await,
            SqlStatement::Commit => self.handle_commit(client).await,
            SqlStatement::Rollback => self.handle_rollback(client).await,
        }
    }
}

impl DefraQueryHandler {
    async fn handle_query(
        &self,
        graphql: &str,
        txn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response>> {
        debug!(graphql, "Translated to GraphQL query");

        let table_name = extract_table_name_from_graphql(graphql);
        let response = self.execute_graphql(graphql, txn_id).await;

        if response.has_errors() {
            return Err(pg_error("XX000", format_errors(&response.errors)));
        }

        let data = match &response.data {
            Some(d) => d,
            None => return Ok(vec![encode::encode_empty_response("SELECT 0")]),
        };

        let docs = match data.get(&table_name) {
            Some(serde_json::Value::Array(arr)) => arr,
            _ => return Ok(vec![encode::encode_empty_response("SELECT 0")]),
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
                let resp = encode::encode_response(docs, &col, &requested)?;
                Ok(vec![resp])
            }
            None => Err(pg_error(
                "42P01",
                format!("relation \"{}\" does not exist", table_name),
            )),
        }
    }

    async fn handle_mutation(
        &self,
        graphql: &str,
        table_name: &str,
        mutation_name: &str,
        kind: MutationKind,
        txn_id: Option<&str>,
    ) -> PgWireResult<Vec<Response>> {
        debug!(graphql, "Translated to GraphQL mutation");

        let response = self.execute_graphql(graphql, txn_id).await;

        if response.has_errors() {
            return Err(pg_error("XX000", format_errors(&response.errors)));
        }

        let data = match &response.data {
            Some(d) => d,
            None => return Ok(vec![execution_tag(&kind, 0)]),
        };

        let docs = match data.get(mutation_name) {
            Some(serde_json::Value::Array(arr)) => arr,
            _ => return Ok(vec![execution_tag(&kind, 0)]),
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
                let resp = encode::encode_response(docs, &col, &requested)?;
                return Ok(vec![resp]);
            }
        }

        Ok(vec![execution_tag(&kind, row_count)])
    }

    async fn handle_begin<C>(&self, client: &mut C) -> PgWireResult<Vec<Response>>
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

        Ok(vec![Response::TransactionStart(Tag::new("BEGIN"))])
    }

    async fn handle_commit<C>(&self, client: &mut C) -> PgWireResult<Vec<Response>>
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

        Ok(vec![Response::TransactionEnd(Tag::new("COMMIT"))])
    }

    async fn handle_rollback<C>(&self, client: &mut C) -> PgWireResult<Vec<Response>>
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

        Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))])
    }
}

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

/// Factory that produces handlers for each PG connection.
pub struct DefraHandlerFactory {
    handler: Arc<DefraQueryHandler>,
}

impl DefraHandlerFactory {
    pub fn new(handler: Arc<DefraQueryHandler>) -> Self {
        Self { handler }
    }
}

impl PgWireServerHandlers for DefraHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(NoopHandler)
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(NoopStartupHandlerImpl)
    }
}

/// Noop startup handler that accepts all connections without authentication.
pub struct NoopStartupHandlerImpl;

impl NoopStartupHandler for NoopStartupHandlerImpl {}

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
                "mutation { create_User(input: {name: \"Alice\"}) { _docID } }"
            ),
            "create_User"
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
            "mutation { create_User(input: {name: \"Alice\"}) { _docID name } }",
        );
        assert_eq!(fields, "_docID name");
    }
}
