use std::collections::HashSet;
use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::PgWireResult;
use tracing::debug;

use crate::bridge::{JoinClause, JoinType};
use crate::encode;

use super::{pg_error, DefraQueryHandler};

impl DefraQueryHandler {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_join(
        &self,
        primary_table: &str,
        joins: &[JoinClause],
        filter: Option<&str>,
        _order: Option<&str>,
        limit: Option<&str>,
        _offset: Option<&str>,
        all_select_columns: &[(String, String, String)],
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        // Build field lists for each table (need join keys + selected fields)
        let primary_fields = build_field_list(primary_table, all_select_columns, joins, true);
        let mut joined_field_lists: Vec<(String, String)> = Vec::new();
        for jc in joins {
            let fields = build_field_list(&jc.table_name, all_select_columns, joins, false);
            joined_field_lists.push((jc.table_name.clone(), fields));
        }

        // 1. Query the primary table (LIMIT/OFFSET applied post-join)
        let mut primary_args = Vec::new();
        if let Some(f) = filter {
            primary_args.push(format!("filter: {{{}}}", f));
        }

        let primary_args_str = if primary_args.is_empty() {
            String::new()
        } else {
            format!("({})", primary_args.join(", "))
        };

        let primary_graphql = format!(
            "query {{ {}{} {{ {} }} }}",
            primary_table, primary_args_str, primary_fields
        );

        debug!(graphql = %primary_graphql, "JOIN: querying primary table");

        let primary_response = self
            .execute_graphql(&primary_graphql, txn_id, identity_did)
            .await?;

        if primary_response.has_errors() {
            return Err(pg_error(
                "XX000",
                format!(
                    "JOIN primary query failed: {}",
                    super::format_errors(&primary_response.errors)
                ),
            ));
        }

        let primary_docs = primary_response
            .data
            .as_ref()
            .and_then(|d| d.get(primary_table))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        debug!(count = primary_docs.len(), "JOIN: primary docs fetched");

        // 2. For each join, query the joined table
        let mut join_results: Vec<(String, JoinType, String, String, Vec<serde_json::Value>)> =
            Vec::new();

        for jc in joins {
            let join_values: Vec<String> = primary_docs
                .iter()
                .filter_map(|doc| {
                    doc.get(&jc.left_col).and_then(|v| match v {
                        serde_json::Value::String(s) => Some(s.clone()),
                        serde_json::Value::Number(n) => Some(n.to_string()),
                        _ => None,
                    })
                })
                .collect();

            if join_values.is_empty() && jc.join_type == JoinType::Inner {
                return Ok(encode::encode_empty_query_response());
            }

            let join_fields = joined_field_lists
                .iter()
                .find(|(t, _)| t == &jc.table_name)
                .map(|(_, f)| f.as_str())
                .unwrap_or("_docID");

            let filter_str = if join_values.is_empty() {
                String::new()
            } else {
                let in_values = join_values
                    .iter()
                    .map(|v| format!("\"{}\"", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("(filter: {{{}: {{_in: [{}]}}}})", jc.right_col, in_values)
            };

            let joined_graphql = format!(
                "query {{ {}{} {{ {} }} }}",
                jc.table_name, filter_str, join_fields
            );

            debug!(graphql = %joined_graphql, "JOIN: querying joined table");

            let joined_response = self
                .execute_graphql(&joined_graphql, txn_id, identity_did)
                .await?;

            let joined_docs = joined_response
                .data
                .as_ref()
                .and_then(|d| d.get(&jc.table_name))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            join_results.push((
                jc.table_name.clone(),
                jc.join_type.clone(),
                jc.left_col.clone(),
                jc.right_col.clone(),
                joined_docs,
            ));
        }

        // 3. In-memory join + project selected columns
        let mut result_rows: Vec<Vec<(String, Option<serde_json::Value>)>> = Vec::new();

        for primary_doc in &primary_docs {
            let mut matched_any = true;
            let mut combined: Vec<Vec<(String, Option<serde_json::Value>)>> = vec![vec![]];

            let primary_cols = project_columns(primary_table, primary_doc, all_select_columns);
            for row in &mut combined {
                row.extend(primary_cols.clone());
            }

            for (join_table, join_type, left_col, right_col, joined_docs) in &join_results {
                let left_val = primary_doc.get(left_col.as_str());

                let matching: Vec<&serde_json::Value> = joined_docs
                    .iter()
                    .filter(|jdoc| values_match(left_val, jdoc.get(right_col.as_str())))
                    .collect();

                if matching.is_empty() {
                    match join_type {
                        JoinType::Left => {
                            for row in &mut combined {
                                let null_cols =
                                    null_columns_for_table(join_table, all_select_columns);
                                row.extend(null_cols);
                            }
                        }
                        JoinType::Inner => {
                            matched_any = false;
                            break;
                        }
                    }
                } else {
                    let mut new_combined = Vec::new();
                    for existing in &combined {
                        for jdoc in &matching {
                            let mut row = existing.clone();
                            let join_cols = project_columns(join_table, jdoc, all_select_columns);
                            row.extend(join_cols);
                            new_combined.push(row);
                        }
                    }
                    combined = new_combined;
                }
            }

            if matched_any {
                result_rows.extend(combined);
            }
        }

        // Apply OFFSET/LIMIT to the final joined result
        let offset_n: usize = _offset.and_then(|o| o.parse().ok()).unwrap_or(0);
        let limit_n: Option<usize> = limit.and_then(|l| l.parse().ok());

        let result_rows: Vec<_> = result_rows
            .into_iter()
            .skip(offset_n)
            .take(limit_n.unwrap_or(usize::MAX))
            .collect();

        encode_join_rows(&result_rows)
    }
}

/// Build the GraphQL field list for a table query.
///
/// Includes selected columns from all_select_columns plus join key columns.
fn build_field_list(
    table: &str,
    all_select_columns: &[(String, String, String)],
    joins: &[JoinClause],
    is_primary: bool,
) -> String {
    let mut fields: HashSet<String> = HashSet::new();

    // Add selected fields for this table
    for (tbl, field, _alias) in all_select_columns {
        if tbl == table && field != "*" {
            fields.insert(field.clone());
        }
    }

    // Add join key columns
    for jc in joins {
        if is_primary {
            fields.insert(jc.left_col.clone());
        }
        if jc.table_name == table {
            fields.insert(jc.right_col.clone());
        }
    }

    if fields.is_empty() {
        "_docID".to_string()
    } else {
        fields.into_iter().collect::<Vec<_>>().join(" ")
    }
}

fn project_columns(
    table: &str,
    doc: &serde_json::Value,
    all_columns: &[(String, String, String)],
) -> Vec<(String, Option<serde_json::Value>)> {
    let mut result = Vec::new();

    for (tbl, field, alias) in all_columns {
        if tbl != table {
            continue;
        }
        if field == "*" {
            if let Some(obj) = doc.as_object() {
                for (k, v) in obj {
                    result.push((k.clone(), Some(v.clone())));
                }
            }
        } else {
            let val = doc.get(field).cloned();
            result.push((alias.clone(), val));
        }
    }

    result
}

fn null_columns_for_table(
    table: &str,
    all_columns: &[(String, String, String)],
) -> Vec<(String, Option<serde_json::Value>)> {
    all_columns
        .iter()
        .filter(|(t, _, _)| t == table)
        .map(|(_, _, alias)| (alias.clone(), None))
        .collect()
}

fn values_match(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => {
            let a_str = value_as_str(a);
            let b_str = value_as_str(b);
            a_str == b_str
        }
        _ => false,
    }
}

fn value_as_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

fn encode_join_rows(rows: &[Vec<(String, Option<serde_json::Value>)>]) -> PgWireResult<Response> {
    if rows.is_empty() {
        return Ok(encode::encode_empty_query_response());
    }

    let field_infos: Vec<FieldInfo> = rows[0]
        .iter()
        .map(|(name, val)| {
            let pg_type = match val {
                Some(serde_json::Value::Number(n)) if n.is_f64() => Type::FLOAT8,
                Some(serde_json::Value::Number(_)) => Type::INT8,
                Some(serde_json::Value::Bool(_)) => Type::BOOL,
                _ => Type::TEXT,
            };
            FieldInfo::new(name.clone(), None, None, pg_type, FieldFormat::Text)
        })
        .collect();

    let schema = Arc::new(field_infos);
    let mut encoded_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (idx, (_name, val)) in row.iter().enumerate() {
            let pg_type = schema[idx].datatype();
            match val {
                None | Some(serde_json::Value::Null) => {
                    encoder.encode_field(&None::<&str>)?;
                }
                Some(serde_json::Value::String(s)) => {
                    encoder.encode_field(&s.as_str())?;
                }
                Some(serde_json::Value::Number(n)) => {
                    if pg_type == &Type::INT8 {
                        encoder.encode_field(&n.as_i64().unwrap_or(0))?;
                    } else if pg_type == &Type::FLOAT8 {
                        encoder.encode_field(&n.as_f64().unwrap_or(0.0))?;
                    } else {
                        encoder.encode_field(&n.to_string().as_str())?;
                    }
                }
                Some(serde_json::Value::Bool(b)) => {
                    encoder.encode_field(b)?;
                }
                Some(other) => {
                    let s = other.to_string();
                    encoder.encode_field(&s.as_str())?;
                }
            }
        }
        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(encoded_rows),
    )))
}
