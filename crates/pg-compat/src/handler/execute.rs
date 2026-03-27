//! SQL execution dispatch for the Postgres wire protocol handler.

use futures::Sink;
use pgwire::api::results::Response;
use pgwire::api::ClientInfo;
use pgwire::error::PgWireResult;
use pgwire::messages::PgWireBackendMessage;
use tracing::{debug, warn};

use crate::bridge::{
    extract_table_from_sql, is_system_catalog_query, sql_to_graphql_typed, MutationKind,
    SqlStatement,
};
use crate::encode;
use crate::metadata::{IndexInfo, PrimaryKeyInfo};

use super::auth;
use super::{pg_error, DefraQueryHandler, TXN_ID_KEY};

impl DefraQueryHandler {
    /// Translate SQL, execute, and return a single Response.
    pub(crate) async fn execute_sql<C>(&self, client: &mut C, sql: &str) -> PgWireResult<Response>
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

    pub(crate) async fn build_field_type_map(
        &self,
        sql: &str,
    ) -> Option<crate::bridge::FieldTypeMap> {
        use schema::FieldKind;

        let table_name = extract_table_from_sql(sql)?;
        let collection = self.collections.get_collection(&table_name).await.ok()??;
        let mut types = crate::bridge::FieldTypeMap::new();
        for field in &collection.fields {
            if let FieldKind::Scalar(scalar) = &field.kind {
                types.insert(field.name.clone(), *scalar);
            }
        }
        Some(types)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_upsert(
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
                let table = super::extract_table_name_from_graphql(check_graphql);
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

    pub(crate) async fn handle_create_table(&self, sdl: &str) -> PgWireResult<Response> {
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
