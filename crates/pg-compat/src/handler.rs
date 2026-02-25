use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use query::{CollectionProvider, QueryExecutor, QueryRequest};
use tracing::{debug, warn};

use crate::bridge::sql_to_graphql;
use crate::encode;

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
}

#[async_trait]
impl SimpleQueryHandler for DefraQueryHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        debug!(sql = query, "PG query received");

        // Translate SQL → GraphQL
        let graphql = match sql_to_graphql(query) {
            Ok(gql) => gql,
            Err(e) => {
                warn!(error = %e, "SQL translation failed");
                return Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        e.to_string(),
                    ),
                )));
            }
        };

        debug!(graphql = graphql.as_str(), "Translated to GraphQL");

        // Extract table name from the GraphQL query for collection lookup.
        let table_name = extract_table_name_from_graphql(&graphql);

        // Execute the GraphQL query
        let request = QueryRequest::new(&graphql);
        let response = self.executor.execute(request).await;

        if response.has_errors() {
            let msg = response
                .errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), msg),
            )));
        }

        // Extract data array from response
        let data = match &response.data {
            Some(d) => d,
            None => {
                return Ok(vec![encode::encode_empty_response("SELECT 0")]);
            }
        };

        // The GraphQL response is { "TableName": [...] }
        let docs = match data.get(&table_name) {
            Some(serde_json::Value::Array(arr)) => arr,
            _ => {
                return Ok(vec![encode::encode_empty_response("SELECT 0")]);
            }
        };

        // Look up collection schema for type-correct encoding
        let collection = self
            .collections
            .get_collection(&table_name)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match collection {
            Some(col) => {
                let fields_str = extract_fields_from_graphql(&graphql);
                let requested = encode::extract_requested_fields(&fields_str);
                let resp = encode::encode_response(docs, &col, &requested)?;
                Ok(vec![resp])
            }
            None => Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42P01".to_owned(),
                    format!("relation \"{}\" does not exist", table_name),
                ),
            ))),
        }
    }
}

/// Extract table name from GraphQL: "query { TableName(...) { ... } }"
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
///
/// Finds the last matching `{ ... }` pair before the final closing brace.
fn extract_fields_from_graphql(gql: &str) -> String {
    // Strip the outer "query { ... }" wrapper by removing the last '}'
    let trimmed = gql.trim().trim_end_matches('}').trim();
    // Now find the last '{ ... }' which contains the field list
    if let Some(last_open) = trimmed.rfind('{') {
        let fields_section = &trimmed[last_open + 1..];
        // Strip any trailing '}' that might remain from nested filters
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
    fn extract_fields() {
        let fields =
            extract_fields_from_graphql("query { User(filter: {age: {_gt: 25}}) { name age } }");
        assert_eq!(fields, "name age");
    }
}
