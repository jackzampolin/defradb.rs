//! Query execution methods for QueryRunner.

mod aggregate;
mod nested;
mod relation_aggregate;
mod select;
mod simple;

use identity::Did;
use serde_json::{Map, Value as JsonValue};
use tracing::instrument;

use crate::error::Result;
use crate::mapper::Select;
use crate::query_parse::parse_query_with_variables;
use crate::txn::TransactionRegistry;

use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a GraphQL query and return JSON results.
    pub async fn execute_query(&self, query: &str) -> Result<JsonValue> {
        self.execute_query_internal(query, self.fetcher.as_ref(), None)
            .await
    }

    /// Execute a GraphQL query with identity for ACP permission checks.
    pub async fn execute_query_with_identity(
        &self,
        query: &str,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        self.execute_query_internal(query, self.fetcher.as_ref(), caller_identity)
            .await
    }

    /// Execute a GraphQL query with identity and variables.
    pub async fn execute_query_with_identity_and_vars(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        self.execute_query_internal_with_vars(
            query,
            self.fetcher.as_ref(),
            caller_identity,
            variables,
        )
        .await
    }

    /// Execute a GraphQL query with a specific fetcher and identity.
    ///
    /// This is used internally for both regular queries (using the default fetcher)
    /// and transactional queries (using a transaction-scoped fetcher).
    pub(crate) async fn execute_query_internal(
        &self,
        query: &str,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        self.execute_query_internal_with_vars(query, fetcher, caller_identity, None)
            .await
    }

    /// Execute a GraphQL query with a specific fetcher, identity, and variables.
    pub(crate) async fn execute_query_internal_with_vars(
        &self,
        query: &str,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        let selects = parse_query_with_variables(query, variables)?;

        let mut results = Map::new();

        for select in selects {
            let result = self
                .execute_select_internal(&select, fetcher, caller_identity.clone())
                .await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute already-parsed Select operations with a specific fetcher and identity.
    #[instrument(
        name = "query.execute",
        skip(self, selects, fetcher, caller_identity),
        fields(select_count = selects.len())
    )]
    pub(crate) async fn execute_selects_internal(
        &self,
        selects: Vec<Select>,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        let mut results = Map::new();

        for select in selects {
            let result = self
                .execute_select_internal(&select, fetcher, caller_identity.clone())
                .await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }
}
