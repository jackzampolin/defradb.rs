use std::collections::HashSet;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
use tracing::debug;

use crate::bridge::{self, SetOp, SqlStatement};
use crate::encode;

use super::{pg_error, DefraQueryHandler};

impl DefraQueryHandler {
    pub(super) async fn handle_set_operation(
        &self,
        left_query: &sqlparser::ast::Query,
        right_query: &sqlparser::ast::Query,
        op: &SetOp,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        // Translate both sides to SqlStatements
        let left_stmt = bridge::dml::translate_query(left_query)
            .map_err(|e| pg_error("42601", e.to_string()))?;
        let right_stmt = bridge::dml::translate_query(right_query)
            .map_err(|e| pg_error("42601", e.to_string()))?;

        let left_graphql = match &left_stmt {
            SqlStatement::Query(g) => g.clone(),
            _ => return Err(pg_error("0A000", "set operation requires SELECT".into())),
        };
        let right_graphql = match &right_stmt {
            SqlStatement::Query(g) => g.clone(),
            _ => return Err(pg_error("0A000", "set operation requires SELECT".into())),
        };

        debug!(left = %left_graphql, right = %right_graphql, "Executing set operation");

        // Execute both
        let left_response = self
            .execute_graphql(&left_graphql, txn_id, identity_did)
            .await?;
        let right_response = self
            .execute_graphql(&right_graphql, txn_id, identity_did)
            .await?;

        let left_table = super::extract_table_name_from_graphql(&left_graphql);
        let right_table = super::extract_table_name_from_graphql(&right_graphql);

        let left_docs = left_response
            .data
            .as_ref()
            .and_then(|d| d.get(&left_table))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let right_docs = right_response
            .data
            .as_ref()
            .and_then(|d| d.get(&right_table))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // Get collection for encoding
        let collection = self
            .collections
            .get_collection(&left_table)
            .await
            .ok()
            .flatten();

        let left_fields_str = super::extract_fields_from_graphql(&left_graphql);
        let requested = encode::extract_requested_fields(&left_fields_str);

        // Apply set operation
        let result_docs = apply_set_op(&left_docs, &right_docs, op);

        match collection {
            Some(col) => encode::encode_response(&result_docs, &col, &requested),
            None => Ok(encode::encode_empty_response("SELECT 0")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_subquery(
        &self,
        outer_table: &str,
        outer_filter: Option<&str>,
        outer_fields: &str,
        inner_table: &str,
        inner_column: &str,
        join_column: &str,
        negated: bool,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        // Execute inner query to get values
        let inner_graphql = format!("query {{ {} {{ {} }} }}", inner_table, inner_column);

        debug!(graphql = %inner_graphql, "Executing subquery inner");

        let inner_response = self
            .execute_graphql(&inner_graphql, txn_id, identity_did)
            .await?;

        let inner_docs = inner_response
            .data
            .as_ref()
            .and_then(|d| d.get(inner_table))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let inner_values: Vec<String> = inner_docs
            .iter()
            .filter_map(|doc| {
                doc.get(inner_column).and_then(|v| match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
            })
            .collect();

        // Build outer query with _in or _nin filter
        let op = if negated { "_nin" } else { "_in" };
        let in_values = inner_values
            .iter()
            .map(|v| format!("\"{}\"", v))
            .collect::<Vec<_>>()
            .join(", ");

        let mut filter_parts = Vec::new();
        filter_parts.push(format!("{}: {{{}: [{}]}}", join_column, op, in_values));
        if let Some(f) = outer_filter {
            filter_parts.push(f.to_string());
        }

        let outer_graphql = format!(
            "query {{ {}(filter: {{{}}}) {{ {} }} }}",
            outer_table,
            filter_parts.join(", "),
            outer_fields
        );

        debug!(graphql = %outer_graphql, "Executing subquery outer");

        self.handle_query_single(&outer_graphql, txn_id, identity_did)
            .await
    }
}

fn apply_set_op(
    left: &[serde_json::Value],
    right: &[serde_json::Value],
    op: &SetOp,
) -> Vec<serde_json::Value> {
    match op {
        SetOp::Union => {
            let mut result = left.to_vec();
            let mut seen: HashSet<String> = left.iter().map(|v| v.to_string()).collect();
            for doc in right {
                let key = doc.to_string();
                if seen.insert(key) {
                    result.push(doc.clone());
                }
            }
            result
        }
        SetOp::UnionAll => {
            let mut result = left.to_vec();
            result.extend(right.iter().cloned());
            result
        }
        SetOp::Intersect => {
            let right_set: HashSet<String> = right.iter().map(|v| v.to_string()).collect();
            left.iter()
                .filter(|doc| right_set.contains(&doc.to_string()))
                .cloned()
                .collect()
        }
        SetOp::Except => {
            let right_set: HashSet<String> = right.iter().map(|v| v.to_string()).collect();
            left.iter()
                .filter(|doc| !right_set.contains(&doc.to_string()))
                .cloned()
                .collect()
        }
    }
}
