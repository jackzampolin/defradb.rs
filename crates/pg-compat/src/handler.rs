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
use tokio::sync::RwLock;
use tracing::{debug, warn};

use schema::FieldKind;

use crate::bridge::{
    count_params, escape_graphql_string, extract_table_from_sql, is_select_or_returning,
    is_system_catalog_query, is_transaction_control, sql_to_graphql_typed, substitute_params,
    FieldTypeMap, MutationKind, SqlStatement,
};
use crate::encode;
use crate::metadata::{DdlMetadata, IndexInfo, PrimaryKeyInfo};

const TXN_ID_KEY: &str = "txn_id";

/// Trait for creating DefraDB schemas from SQL DDL.
#[async_trait]
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
    ) -> PgWireResult<query::QueryResponse> {
        let request = QueryRequest::new(graphql);
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

// ── Core query/mutation handlers (return single Response) ──

impl DefraQueryHandler {
    async fn handle_query_single(
        &self,
        graphql: &str,
        txn_id: Option<&str>,
    ) -> PgWireResult<Response> {
        debug!(graphql, "Translated to GraphQL query");

        let table_name = extract_table_name_from_graphql(graphql);
        let response = self.execute_graphql(graphql, txn_id).await?;

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

        let response = self.execute_graphql(graphql, txn_id).await?;

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
                if kind == MutationKind::Delete {
                    let has_cascade = !self
                        .ddl_metadata
                        .read()
                        .await
                        .cascade_children_of(&table_name)
                        .is_empty();
                    if has_cascade {
                        return self
                            .handle_delete_with_cascade(&graphql, &table_name, txn_id.as_deref())
                            .await;
                    }
                }

                self.handle_mutation_single(
                    &graphql,
                    &table_name,
                    &mutation_name,
                    kind,
                    txn_id.as_deref(),
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
        }
    }

    /// Build a field type map for the target table in a SQL statement.
    ///
    /// Returns `None` for queries where schema lookup isn't needed or fails.
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
    ) -> PgWireResult<Response> {
        debug!(check_graphql, "Upsert: checking for existing row");

        // Check if a row with the conflict key already exists
        let check_response = self.execute_graphql(check_graphql, txn_id).await?;
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
            )
            .await
        }
    }

    async fn handle_system_catalog(&self, sql: &str) -> PgWireResult<Response> {
        let lower = sql.to_lowercase();

        // SELECT current_schema()
        if lower.contains("current_schema") && !lower.contains("information_schema") {
            return encode::encode_single_value_response("current_schema", "public");
        }

        // information_schema.tables → return actual collection names
        if lower.contains("information_schema.tables") && !lower.contains("table_constraints") {
            return self.handle_info_schema_tables().await;
        }

        // information_schema.columns → return actual field metadata
        if lower.contains("information_schema.columns") {
            return self.handle_info_schema_columns().await;
        }

        // pg_indexes → return stored index metadata
        if lower.contains("pg_indexes") {
            return self.handle_pg_indexes().await;
        }

        // FK constraints via table_constraints + constraint_column_usage
        if lower.contains("table_constraints") && lower.contains("constraint_column_usage") {
            return self.handle_fk_constraints().await;
        }

        // Primary key columns via pg_index + pg_attribute
        if lower.contains("pg_index") && lower.contains("pg_attribute") {
            return self.handle_pk_columns(sql).await;
        }

        // pg_catalog.pg_type → return basic type stubs (but not for enum queries)
        if lower.contains("pg_type") && !lower.contains("pg_enum") {
            return Ok(encode::encode_pg_types());
        }

        // Other catalog queries (pg_class, pg_namespace, pg_roles, etc.)
        Ok(encode::encode_empty_select_with_columns(sql))
    }

    async fn handle_info_schema_tables(&self) -> PgWireResult<Response> {
        let names = self
            .collections
            .list_collections()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let rows: Vec<Vec<(String, String)>> = names
            .into_iter()
            .map(|name| {
                vec![
                    ("table_schema".to_string(), "public".to_string()),
                    ("table_name".to_string(), name),
                    ("table_type".to_string(), "BASE TABLE".to_string()),
                ]
            })
            .collect();

        encode::encode_text_rows(&rows)
    }

    async fn handle_info_schema_columns(&self) -> PgWireResult<Response> {
        let names = self
            .collections
            .list_collections()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let mut rows = Vec::new();
        for name in &names {
            if let Ok(Some(col)) = self.collections.get_collection(name).await {
                for (pos, field) in col.fields.iter().enumerate() {
                    if !field.kind.is_scalar() {
                        continue;
                    }
                    rows.push(vec![
                        ("table_schema".to_string(), "public".to_string()),
                        ("table_name".to_string(), name.clone()),
                        ("column_name".to_string(), field.name.clone()),
                        ("ordinal_position".to_string(), (pos + 1).to_string()),
                        (
                            "data_type".to_string(),
                            encode::field_kind_to_pg_type_name(&field.kind),
                        ),
                        ("is_nullable".to_string(), "YES".to_string()),
                    ]);
                }
            }
        }

        encode::encode_text_rows(&rows)
    }

    async fn handle_pg_indexes(&self) -> PgWireResult<Response> {
        let meta = self.ddl_metadata.read().await;
        let rows: Vec<Vec<(String, String)>> = meta
            .indexes
            .iter()
            .map(|idx| {
                vec![
                    ("schemaname".to_string(), "public".to_string()),
                    ("tablename".to_string(), idx.table_name.clone()),
                    ("indexname".to_string(), idx.index_name.clone()),
                ]
            })
            .collect();
        encode::encode_text_rows(&rows)
    }

    async fn handle_fk_constraints(&self) -> PgWireResult<Response> {
        let meta = self.ddl_metadata.read().await;
        let rows: Vec<Vec<(String, String)>> = meta
            .foreign_keys
            .iter()
            .map(|fk| {
                vec![
                    ("table_name".to_string(), fk.from_table.clone()),
                    ("constraint_name".to_string(), fk.constraint_name.clone()),
                    ("foreign_table_name".to_string(), fk.to_table.clone()),
                ]
            })
            .collect();
        encode::encode_text_rows(&rows)
    }

    async fn handle_pk_columns(&self, sql: &str) -> PgWireResult<Response> {
        // Extract table name from 'table_name'::regclass pattern
        let table_name = extract_regclass_table(sql).unwrap_or_default();
        let meta = self.ddl_metadata.read().await;
        let rows: Vec<Vec<(String, String)>> = meta
            .primary_key_for(&table_name)
            .map(|pk| {
                pk.columns
                    .iter()
                    .map(|col| vec![("attname".to_string(), col.clone())])
                    .collect()
            })
            .unwrap_or_default();
        encode::encode_text_rows(&rows)
    }

    async fn handle_delete_with_cascade(
        &self,
        graphql: &str,
        table_name: &str,
        txn_id: Option<&str>,
    ) -> PgWireResult<Response> {
        // Extract the filter field and value from the GraphQL delete mutation.
        // Pattern: `delete_Table(filter: {field: {_eq: "value"}}) { ... }`
        let (filter_field, filter_value) = extract_filter_from_graphql(graphql);

        // Recursively delete children first (depth-first)
        Box::pin(self.cascade_delete_children(table_name, &filter_field, &filter_value, txn_id))
            .await?;

        // Delete from parent
        let mutation_name = format!("delete_{}", table_name);
        self.handle_mutation_single(
            graphql,
            table_name,
            &mutation_name,
            MutationKind::Delete,
            txn_id,
        )
        .await
    }

    async fn cascade_delete_children(
        &self,
        parent_table: &str,
        filter_field: &str,
        filter_value: &str,
        txn_id: Option<&str>,
    ) -> PgWireResult<()> {
        let children = {
            let meta = self.ddl_metadata.read().await;
            meta.cascade_children_of(parent_table)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        };

        for fk in &children {
            // If the FK points to the same column we're filtering on, we can
            // use the parent's filter value directly. Otherwise we need to
            // query the parent to get the linking column values.
            let child_filter_value = if fk.to_column == filter_field {
                filter_value.to_string()
            } else {
                // Query parent table for the FK target column values
                let escaped = escape_graphql_string(filter_value);
                let query_gql = format!(
                    "query {{ {}(filter: {{{}: {{_eq: \"{}\"}}}}) {{ {} }} }}",
                    parent_table, filter_field, escaped, fk.to_column
                );
                let response = self.execute_graphql(&query_gql, txn_id).await?;
                let values = response
                    .data
                    .as_ref()
                    .and_then(|d| d.get(parent_table))
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|doc| {
                                doc.get(&fk.to_column).map(|v| match v {
                                    serde_json::Value::String(s) => s.clone(),
                                    _ => v.to_string(),
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                if values.is_empty() {
                    continue;
                }
                values[0].clone()
            };

            // Recursively cascade into grandchildren
            Box::pin(self.cascade_delete_children(
                &fk.from_table,
                &fk.from_column,
                &child_filter_value,
                txn_id,
            ))
            .await?;

            // Delete from child table
            let child_mutation_name = format!("delete_{}", fk.from_table);
            let escaped_child = escape_graphql_string(&child_filter_value);
            let child_gql = format!(
                "mutation {{ {}(filter: {{{}: {{_eq: \"{}\"}}}}) {{ _docID }} }}",
                child_mutation_name, fk.from_column, escaped_child
            );
            debug!(child_gql, "CASCADE delete child");
            let response = self.execute_graphql(&child_gql, txn_id).await?;
            if response.has_errors() {
                debug!(errors = ?response.errors, "CASCADE delete child errors (non-fatal)");
            }
        }
        Ok(())
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

        if is_transaction_control(sql) {
            return Ok(DescribeStatementResponse::new(param_types, vec![]));
        }

        // For non-SELECT DML without RETURNING, return no columns
        if !is_select_or_returning(sql) && !is_system_catalog_query(sql) {
            return Ok(DescribeStatementResponse::new(param_types, vec![]));
        }

        let fields = self.build_field_infos_for_describe(sql).await;
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

        if is_transaction_control(sql) {
            return Ok(DescribePortalResponse::new(vec![]));
        }

        if !is_select_or_returning(sql) && !is_system_catalog_query(sql) {
            return Ok(DescribePortalResponse::new(vec![]));
        }

        let fields = self.build_field_infos_for_describe(sql).await;
        Ok(DescribePortalResponse::new(fields))
    }
}

impl DefraQueryHandler {
    async fn build_field_infos_for_describe(&self, sql: &str) -> Vec<FieldInfo> {
        // System catalog queries
        if is_system_catalog_query(sql) {
            return encode::describe_system_catalog(sql);
        }

        // Try to resolve from table
        let table_name = match extract_table_from_sql(sql) {
            Some(name) => name,
            None => {
                // SELECT without FROM — try to extract synthetic column names
                return encode::describe_synthetic_query(sql);
            }
        };

        let collection = match self.collections.get_collection(&table_name).await {
            Ok(Some(col)) => col,
            _ => return vec![],
        };

        // Extract requested columns from the SQL to match what execute returns.
        // For SELECT, parse the column list; for DML with RETURNING, parse the RETURNING clause.
        let columns = {
            let select_cols = encode::extract_select_columns(sql);
            if select_cols.is_empty() {
                encode::extract_returning_columns(sql)
            } else {
                select_cols
            }
        };

        if columns.is_empty() || columns.iter().any(|c| c == "*") {
            return encode::build_field_infos_from_collection(&collection);
        }

        encode::build_field_infos_for_columns(&collection, &columns)
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
    // Match: filter: {field: {_eq: "value"}}
    let re = regex::Regex::new(r#"filter:\s*\{(\w+):\s*\{_eq:\s*"([^"]*)"\}\}"#).unwrap();
    if let Some(caps) = re.captures(gql) {
        return (caps[1].to_string(), caps[2].to_string());
    }
    // Also match numeric: filter: {field: {_eq: 123}}
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
