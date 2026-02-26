use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use tracing::debug;

use crate::bridge::{
    count_params, extract_table_from_sql, is_select_or_returning, is_system_catalog_query,
    is_transaction_control, substitute_params,
};
use crate::encode;

use super::auth::DIDAuthHandler;
use super::DefraQueryHandler;

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
        if is_system_catalog_query(sql) {
            return encode::describe_system_catalog(sql);
        }

        let table_name = match extract_table_from_sql(sql) {
            Some(name) => name,
            None => return encode::describe_synthetic_query(sql),
        };

        let collection = match self.collections.get_collection(&table_name).await {
            Ok(Some(col)) => col,
            _ => return vec![],
        };

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

pub struct DefraHandlerFactory {
    handler: Arc<DefraQueryHandler>,
    auth_handler: Arc<DIDAuthHandler>,
}

impl DefraHandlerFactory {
    pub fn new(handler: Arc<DefraQueryHandler>, audience: String) -> Self {
        Self {
            handler,
            auth_handler: Arc::new(DIDAuthHandler::new(audience)),
        }
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
        self.auth_handler.clone()
    }
}

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
