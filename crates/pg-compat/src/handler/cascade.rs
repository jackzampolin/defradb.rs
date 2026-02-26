use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
use tracing::debug;

use crate::bridge::{escape_graphql_string, MutationKind};

use super::{extract_filter_from_graphql, DefraQueryHandler};

impl DefraQueryHandler {
    pub(super) async fn handle_delete_with_cascade(
        &self,
        graphql: &str,
        table_name: &str,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        let (filter_field, filter_value) = extract_filter_from_graphql(graphql);

        Box::pin(self.cascade_delete_children(
            table_name,
            &filter_field,
            &filter_value,
            txn_id,
            identity_did,
        ))
        .await?;

        let mutation_name = format!("delete_{}", table_name);
        self.handle_mutation_single(
            graphql,
            table_name,
            &mutation_name,
            MutationKind::Delete,
            txn_id,
            identity_did,
        )
        .await
    }

    async fn cascade_delete_children(
        &self,
        parent_table: &str,
        filter_field: &str,
        filter_value: &str,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<()> {
        let children = {
            let meta = self.ddl_metadata.read().await;
            meta.cascade_children_of(parent_table)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        };

        for fk in &children {
            let child_filter_value = if fk.to_column == filter_field {
                filter_value.to_string()
            } else {
                let escaped = escape_graphql_string(filter_value);
                let query_gql = format!(
                    "query {{ {}(filter: {{{}: {{_eq: \"{}\"}}}}) {{ {} }} }}",
                    parent_table, filter_field, escaped, fk.to_column
                );
                let response = self
                    .execute_graphql(&query_gql, txn_id, identity_did)
                    .await?;
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

            Box::pin(self.cascade_delete_children(
                &fk.from_table,
                &fk.from_column,
                &child_filter_value,
                txn_id,
                identity_did,
            ))
            .await?;

            let child_mutation_name = format!("delete_{}", fk.from_table);
            let escaped_child = escape_graphql_string(&child_filter_value);
            let child_gql = format!(
                "mutation {{ {}(filter: {{{}: {{_eq: \"{}\"}}}}) {{ _docID }} }}",
                child_mutation_name, fk.from_column, escaped_child
            );
            debug!(child_gql, "CASCADE delete child");
            let response = self
                .execute_graphql(&child_gql, txn_id, identity_did)
                .await?;
            if response.has_errors() {
                debug!(errors = ?response.errors, "CASCADE delete child errors (non-fatal)");
            }
        }
        Ok(())
    }
}
