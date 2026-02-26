use std::collections::HashSet;

use futures::Sink;
use pgwire::api::results::Response;
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::bridge::SqlStatement;
use crate::encode;

use super::{pg_error, DefraQueryHandler};

impl DefraQueryHandler {
    pub(super) async fn handle_distinct<C>(
        &self,
        inner: SqlStatement,
        _client: &mut C,
        txn_id: Option<String>,
        identity_did: Option<String>,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    {
        let graphql = match &inner {
            SqlStatement::Query(g) => g.clone(),
            _ => {
                return Err(pg_error(
                    "0A000",
                    "DISTINCT only supported on simple SELECT".into(),
                ));
            }
        };

        let table_name = super::extract_table_name_from_graphql(&graphql);
        let response = self
            .execute_graphql(&graphql, txn_id.as_deref(), identity_did.as_deref())
            .await?;

        if response.has_errors() {
            return Err(pg_error("XX000", super::format_errors(&response.errors)));
        }

        let data = match &response.data {
            Some(d) => d,
            None => return Ok(encode::encode_empty_response("SELECT 0")),
        };

        let docs = match data.get(&table_name) {
            Some(serde_json::Value::Array(arr)) => arr,
            _ => return Ok(encode::encode_empty_response("SELECT 0")),
        };

        // Deduplicate documents
        let mut seen = HashSet::new();
        let deduped: Vec<serde_json::Value> = docs
            .iter()
            .filter(|doc| {
                let key = doc.to_string();
                seen.insert(key)
            })
            .cloned()
            .collect();

        let collection = self
            .collections
            .get_collection(&table_name)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match collection {
            Some(col) => {
                let fields_str = super::extract_fields_from_graphql(&graphql);
                let requested = encode::extract_requested_fields(&fields_str);
                encode::encode_response(&deduped, &col, &requested)
            }
            None => Err(pg_error(
                "42P01",
                format!("relation \"{}\" does not exist", table_name),
            )),
        }
    }
}
