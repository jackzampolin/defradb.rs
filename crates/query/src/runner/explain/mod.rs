mod aggregate;
mod execute;
mod mutation;
mod select;

use identity::Did;
use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::query_parse::{parse_query_with_variables, ExplainType};
use crate::txn::TransactionRegistry;

use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Generate an explanation of the query plan.
    ///
    /// Used when queries include the @explain directive.
    /// Supports three modes:
    /// - Simple: Query plan structure without execution
    /// - Execute: Run the query and return plan structure with execution metrics
    /// - Debug: All plan nodes including internal ones
    ///
    /// Output format matches Go DefraDB:
    /// ```json
    /// {
    ///   "explain": {
    ///     "operationNode": [
    ///       {
    ///         "selectTopNode": {
    ///           "selectNode": { ... "scanNode": { ... } }
    ///         }
    ///       }
    ///     ]
    ///   }
    /// }
    /// ```
    pub async fn explain_query_with_identity(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        self.explain_query_with_identity_and_vars(query, caller_identity, explain_type, None)
            .await
    }

    /// Generate an explanation of the query plan with variable support.
    pub async fn explain_query_with_identity_and_vars(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        explain_type: ExplainType,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        match explain_type {
            ExplainType::Simple | ExplainType::Debug => {
                // Simple and Debug modes: explain without execution
                let selects = parse_query_with_variables(query, variables)?;
                let mut operation_children: Vec<JsonValue> = Vec::new();

                for select in selects {
                    // Check if this is a top-level aggregate query (e.g., _avg, _count, _sum)
                    let is_top_level_aggregate = Self::is_top_level_aggregate(&select);

                    // Build the plan explanation for this select
                    let select_node_content = self.explain_select(&select, explain_type).await?;

                    if is_top_level_aggregate {
                        // Top-level aggregates use topLevelNode wrapper
                        let top_level_node = self.build_top_level_aggregate_explain(
                            &select,
                            select_node_content,
                            explain_type,
                        );
                        operation_children.push(top_level_node);
                    } else {
                        // Regular queries use selectTopNode wrapper
                        let select_top_node = serde_json::json!({
                            "selectTopNode": select_node_content
                        });
                        operation_children.push(select_top_node);
                    }
                }

                // Wrap all selects in operationNode array (Go's MultiNode pattern)
                Ok(serde_json::json!({
                    "explain": {
                        "operationNode": operation_children
                    }
                }))
            }
            ExplainType::Execute => {
                // Execute mode: run the query and collect metrics
                self.execute_explain_with_vars(query, caller_identity, variables)
                    .await
            }
        }
    }

    /// Generate an explanation of the mutation plan.
    ///
    /// Used when mutations include the @explain directive.
    /// Output format matches Go DefraDB with addNode/deleteNode/updateNode/upsertNode.
    pub async fn explain_mutation_with_identity(
        &self,
        mutation_str: &str,
        caller_identity: Option<Did>,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        use crate::query_parse::parse_mutations;

        match explain_type {
            ExplainType::Simple | ExplainType::Debug => {
                // Simple and Debug modes: explain without execution
                let mutations = parse_mutations(mutation_str)?;
                let mut operation_children: Vec<JsonValue> = Vec::new();

                for mutation in mutations {
                    let mutation_explain = self
                        .explain_single_mutation(&mutation, explain_type)
                        .await?;
                    operation_children.push(mutation_explain);
                }

                // Wrap all mutations in operationNode array (Go's MultiNode pattern)
                Ok(serde_json::json!({
                    "explain": {
                        "operationNode": operation_children
                    }
                }))
            }
            ExplainType::Execute => {
                // Execute mode: run the mutation and collect metrics
                self.execute_mutation_explain(mutation_str, caller_identity)
                    .await
            }
        }
    }
}
