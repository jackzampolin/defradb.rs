pub(crate) mod auth;
mod cascade;
mod catalog;
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
use tracing::{debug, warn};

use schema::FieldKind;

use crate::bridge::{
    extract_table_from_sql, is_system_catalog_query, sql_to_graphql_typed, FieldTypeMap,
    MutationKind, SqlStatement,
};
use crate::encode;
use crate::metadata::{DdlMetadata, IndexInfo, PrimaryKeyInfo};

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

// ── Core query/mutation handlers ──

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

    /// Translate SQL, execute, and return a single Response.
    async fn execute_sql<C>(&self, client: &mut C, sql: &str) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        if is_system_catalog_query(sql) {
            debug!(sql, "Handling system catalog query");
            return self.handle_system_catalog(sql).await;
        }

        let field_types = self.build_field_type_map(sql).await;
        let statement = match sql_to_graphql_typed(sql, field_types.as_ref()) {
            Ok(stmt) => stmt,
            Err(e) => {
                warn!(error = %e, sql, "SQL translation failed");
                return Err(pg_error("42601", e.to_string()));
            }
        };

        let txn_id = client.metadata().get(TXN_ID_KEY).cloned();
        let identity_did = client.metadata().get(auth::IDENTITY_DID_KEY).cloned();

        match statement {
            SqlStatement::Query(graphql) => {
                self.handle_query_single(&graphql, txn_id.as_deref(), identity_did.as_deref())
                    .await
            }
            SqlStatement::Mutation {
                graphql,
                table_name,
                mutation_name,
                kind,
            } => {
                if kind == MutationKind::Delete {
                    let has_cascade = !self
                        .ddl_metadata
                        .read()
                        .await
                        .cascade_children_of(&table_name)
                        .is_empty();
                    if has_cascade {
                        return self
                            .handle_delete_with_cascade(
                                &graphql,
                                &table_name,
                                txn_id.as_deref(),
                                identity_did.as_deref(),
                            )
                            .await;
                    }
                }

                self.handle_mutation_single(
                    &graphql,
                    &table_name,
                    &mutation_name,
                    kind,
                    txn_id.as_deref(),
                    identity_did.as_deref(),
                )
                .await
            }
            SqlStatement::Upsert {
                insert_graphql,
                update_graphql,
                check_graphql,
                table_name,
                insert_mutation_name,
                update_mutation_name,
            } => {
                self.handle_upsert(
                    &insert_graphql,
                    &update_graphql,
                    &check_graphql,
                    &table_name,
                    &insert_mutation_name,
                    &update_mutation_name,
                    txn_id.as_deref(),
                    identity_did.as_deref(),
                )
                .await
            }
            SqlStatement::SyntheticQuery { columns } => encode::encode_synthetic_response(&columns),
            SqlStatement::Begin => self.handle_begin_single(client).await,
            SqlStatement::Commit => self.handle_commit_single(client).await,
            SqlStatement::Rollback => self.handle_rollback_single(client).await,
            SqlStatement::CreateTable {
                sdl,
                table_name,
                primary_key_columns,
                inline_foreign_keys,
            } => {
                {
                    let mut meta = self.ddl_metadata.write().await;
                    if !primary_key_columns.is_empty() {
                        meta.add_primary_key(PrimaryKeyInfo {
                            table_name: table_name.clone(),
                            columns: primary_key_columns,
                        });
                    }
                    for fk in &inline_foreign_keys {
                        meta.add_foreign_key(&table_name, fk);
                    }
                }
                self.handle_create_table(&sdl).await
            }
            SqlStatement::CreateIndex {
                index_name,
                table_name,
                columns,
            } => {
                if let Some(name) = &index_name {
                    self.ddl_metadata.write().await.add_index(IndexInfo {
                        index_name: name.clone(),
                        table_name: table_name.clone(),
                        columns: columns.clone(),
                    });
                }
                debug!(sql, "DDL CREATE INDEX accepted");
                Ok(encode::encode_empty_response("CREATE INDEX"))
            }
            SqlStatement::AlterTable {
                table_name,
                foreign_keys,
            } => {
                {
                    let mut meta = self.ddl_metadata.write().await;
                    for fk in &foreign_keys {
                        meta.add_foreign_key(&table_name, fk);
                    }
                }
                debug!(sql, "DDL ALTER TABLE accepted");
                Ok(encode::encode_empty_response("ALTER TABLE"))
            }
            SqlStatement::DropTable => {
                debug!(sql, "DDL DROP TABLE accepted");
                Ok(encode::encode_empty_response("DROP TABLE"))
            }
            SqlStatement::Aggregate {
                table_name,
                aggregates,
                filter,
            } => {
                self.handle_aggregate(
                    &table_name,
                    &aggregates,
                    filter.as_deref(),
                    txn_id.as_deref(),
                    identity_did.as_deref(),
                )
                .await
            }
            SqlStatement::GroupBy {
                table_name,
                group_columns,
                aggregates,
                non_agg_columns,
                filter,
                having_filter,
            } => {
                self.handle_group_by(
                    &table_name,
                    &group_columns,
                    &aggregates,
                    &non_agg_columns,
                    filter.as_deref(),
                    having_filter.as_deref(),
                    txn_id.as_deref(),
                    identity_did.as_deref(),
                )
                .await
            }
            SqlStatement::Join {
                primary_table,
                joins,
                filter,
                order,
                limit,
                offset,
                all_select_columns,
                group_columns,
                group_aggregates,
            } => {
                self.handle_join(
                    &primary_table,
                    &joins,
                    filter.as_deref(),
                    order.as_deref(),
                    limit.as_deref(),
                    offset.as_deref(),
                    &all_select_columns,
                    &group_columns,
                    &group_aggregates,
                    txn_id.as_deref(),
                    identity_did.as_deref(),
                )
                .await
            }
            SqlStatement::Distinct { inner } => {
                self.handle_distinct(*inner, client, txn_id, identity_did)
                    .await
            }
            SqlStatement::SetOperation {
                left_query,
                right_query,
                op,
            } => {
                self.handle_set_operation(
                    &left_query,
                    &right_query,
                    &op,
                    txn_id.as_deref(),
                    identity_did.as_deref(),
                )
                .await
            }
            SqlStatement::Subquery {
                outer_table,
                outer_filter,
                outer_fields,
                inner_table,
                inner_column,
                join_column,
                negated,
            } => {
                self.handle_subquery(
                    &outer_table,
                    outer_filter.as_deref(),
                    &outer_fields,
                    &inner_table,
                    &inner_column,
                    &join_column,
                    negated,
                    txn_id.as_deref(),
                    identity_did.as_deref(),
                )
                .await
            }
        }
    }

    async fn build_field_type_map(&self, sql: &str) -> Option<FieldTypeMap> {
        let table_name = extract_table_from_sql(sql)?;
        let collection = self.collections.get_collection(&table_name).await.ok()??;
        let mut types = FieldTypeMap::new();
        for field in &collection.fields {
            if let FieldKind::Scalar(scalar) = &field.kind {
                types.insert(field.name.clone(), *scalar);
            }
        }
        Some(types)
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_upsert(
        &self,
        insert_graphql: &str,
        update_graphql: &str,
        check_graphql: &str,
        table_name: &str,
        insert_mutation_name: &str,
        update_mutation_name: &str,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        debug!(check_graphql, "Upsert: checking for existing row");

        let check_response = self
            .execute_graphql(check_graphql, txn_id, identity_did)
            .await?;
        let exists = check_response
            .data
            .as_ref()
            .and_then(|d| {
                let table = extract_table_name_from_graphql(check_graphql);
                d.get(&table)
            })
            .and_then(|v| v.as_array())
            .is_some_and(|arr| !arr.is_empty());

        if exists {
            debug!(update_graphql, "Upsert: row exists, updating");
            self.handle_mutation_single(
                update_graphql,
                table_name,
                update_mutation_name,
                MutationKind::Update,
                txn_id,
                identity_did,
            )
            .await
        } else {
            debug!(insert_graphql, "Upsert: no existing row, inserting");
            self.handle_mutation_single(
                insert_graphql,
                table_name,
                insert_mutation_name,
                MutationKind::Insert,
                txn_id,
                identity_did,
            )
            .await
        }
    }

    async fn handle_create_table(&self, sdl: &str) -> PgWireResult<Response> {
        let mgr = self.schema_manager.as_ref().ok_or_else(|| {
            pg_error(
                "0A000",
                "CREATE TABLE not supported: no schema manager configured".to_string(),
            )
        })?;

        debug!(sdl, "Creating collection from DDL");
        mgr.add_schema(sdl)
            .await
            .map_err(|e| pg_error("42000", e))?;

        Ok(encode::encode_empty_response("CREATE TABLE"))
    }
}

// ── Helpers ──

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
    let re = regex::Regex::new(r#"filter:\s*\{(\w+):\s*\{_eq:\s*"([^"]*)"\}\}"#).unwrap();
    if let Some(caps) = re.captures(gql) {
        return (caps[1].to_string(), caps[2].to_string());
    }
    let re_num = regex::Regex::new(r#"filter:\s*\{(\w+):\s*\{_eq:\s*(\d+)\}\}"#).unwrap();
    if let Some(caps) = re_num.captures(gql) {
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
