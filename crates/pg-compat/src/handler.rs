use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use query::{CollectionProvider, QueryExecutor, QueryRequest, TransactionHandle};
use tracing::{debug, warn};

use crate::bridge::{
    count_params, extract_table_from_sql, is_select_or_returning, is_transaction_control,
    sql_to_graphql, substitute_params, MutationKind, SqlStatement,
};
use crate::encode;

const TXN_ID_KEY: &str = "txn_id";

/// Handler for Postgres wire protocol queries.
///
/// Translates SQL to GraphQL, executes via QueryExecutor, and encodes results.
/// Implements both SimpleQueryHandler and ExtendedQueryHandler.
pub struct DefraQueryHandler {
    executor: Arc<dyn QueryExecutor>,
    collections: Arc<dyn CollectionProvider>,
    parser: Arc<DefraQueryParser>,
}

impl DefraQueryHandler {
    pub fn new(executor: Arc<dyn QueryExecutor>, collections: Arc<dyn CollectionProvider>) -> Self {
        let parser = Arc::new(DefraQueryParser);
        Self {
            executor,
            collections,
            parser,
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

// ── Core query/mutation handlers (return single Response) ──

impl DefraQueryHandler {
    async fn handle_query_single(
        &self,
        graphql: &str,
        txn_id: Option<&str>,
    ) -> PgWireResult<Response> {
        debug!(graphql, "Translated to GraphQL query");

        let table_name = extract_table_name_from_graphql(graphql);
        let response = self.execute_graphql(graphql, txn_id).await;

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
    ) -> PgWireResult<Response> {
        debug!(graphql, "Translated to GraphQL mutation");

        let response = self.execute_graphql(graphql, txn_id).await;

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

    /// Translate SQL, execute, and return a single Response.
    async fn execute_sql<C>(&self, client: &mut C, sql: &str) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let statement = match sql_to_graphql(sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                warn!(error = %e, "SQL translation failed");
                return Err(pg_error("42601", e.to_string()));
            }
        };

        let txn_id = client.metadata().get(TXN_ID_KEY).cloned();

        match statement {
            SqlStatement::Query(graphql) => {
                self.handle_query_single(&graphql, txn_id.as_deref()).await
            }
            SqlStatement::Mutation {
                graphql,
                table_name,
                mutation_name,
                kind,
            } => {
                self.handle_mutation_single(
                    &graphql,
                    &table_name,
                    &mutation_name,
                    kind,
                    txn_id.as_deref(),
                )
                .await
            }
            SqlStatement::Begin => self.handle_begin_single(client).await,
            SqlStatement::Commit => self.handle_commit_single(client).await,
            SqlStatement::Rollback => self.handle_rollback_single(client).await,
        }
    }
}

// ── Simple Query Protocol ──

#[async_trait]
impl SimpleQueryHandler for DefraQueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        debug!(sql = query, "PG simple query received");
        let resp = self.execute_sql(client, query).await?;
        Ok(vec![resp])
    }
}

// ── Query Parser for Extended Protocol ──

pub struct DefraQueryParser;

#[async_trait]
impl QueryParser for DefraQueryParser {
    type Statement = String;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(sql.to_string())
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        let count = count_params(stmt);
        Ok(vec![Type::TEXT; count])
    }

    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // Full schema resolution happens in do_describe_* methods which have
        // async access to the collection provider. Return empty here as the
        // default on_describe implementation calls do_describe_* anyway.
        Ok(vec![])
    }
}

// ── Extended Query Protocol ──

#[async_trait]
impl ExtendedQueryHandler for DefraQueryHandler {
    type Statement = String;
    type QueryParser = DefraQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let raw_sql = &portal.statement.statement;
        let params = extract_params(portal);
        let sql = substitute_params(raw_sql, &params);

        debug!(raw_sql, substituted = %sql, "PG extended query received");

        self.execute_sql(client, &sql).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &target.statement;
        let param_types = self.parser.get_parameter_types(sql)?;

        if is_transaction_control(sql) || !is_select_or_returning(sql) {
            return Ok(DescribeStatementResponse::new(param_types, vec![]));
        }

        let fields = self.build_field_infos_from_sql(sql).await;
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &target.statement.statement;

        if is_transaction_control(sql) || !is_select_or_returning(sql) {
            return Ok(DescribePortalResponse::new(vec![]));
        }

        let fields = self.build_field_infos_from_sql(sql).await;
        Ok(DescribePortalResponse::new(fields))
    }
}

impl DefraQueryHandler {
    async fn build_field_infos_from_sql(&self, sql: &str) -> Vec<FieldInfo> {
        let table_name = match extract_table_from_sql(sql) {
            Some(name) => name,
            None => return vec![],
        };

        let collection = match self.collections.get_collection(&table_name).await {
            Ok(Some(col)) => col,
            _ => return vec![],
        };

        encode::build_field_infos_from_collection(&collection)
    }
}

// ── Handler Factory ──

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
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(NoopStartupHandlerImpl)
    }
}

/// Noop startup handler that accepts all connections without authentication.
pub struct NoopStartupHandlerImpl;

impl NoopStartupHandler for NoopStartupHandlerImpl {}

// ── Helpers ──

fn extract_params(portal: &Portal<String>) -> Vec<Option<String>> {
    portal
        .parameters
        .iter()
        .map(|p| {
            p.as_ref()
                .map(|bytes| String::from_utf8_lossy(bytes).to_string())
        })
        .collect()
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
