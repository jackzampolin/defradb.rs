use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::PgWireResult;
use tracing::debug;

use crate::bridge::{AggFunc, AggregateExpr, JoinClause, JoinType};
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
        group_columns: &[String],
        group_aggregates: &[AggregateExpr],
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

        // 2. For each join, query the joined table.
        //    Track all fetched docs by table so chained joins can source
        //    join keys from a previously-joined table (not just the primary).
        let mut table_docs: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        table_docs.insert(primary_table.to_string(), primary_docs.clone());

        let mut join_results: Vec<(String, JoinType, String, String, Vec<serde_json::Value>)> =
            Vec::new();

        for jc in joins {
            let source_table = jc.left_table.as_deref().unwrap_or(primary_table);
            let source_docs = table_docs.get(source_table).cloned().unwrap_or_default();

            let join_values: Vec<String> = source_docs
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

            table_docs.insert(jc.table_name.clone(), joined_docs.clone());

            join_results.push((
                jc.table_name.clone(),
                jc.join_type.clone(),
                jc.left_col.clone(),
                jc.right_col.clone(),
                joined_docs,
            ));
        }

        // 3. In-memory join using row contexts that track raw docs per table.
        //    This allows chained joins to look up left_col from the correct table.
        let mut contexts: Vec<RowContext> = primary_docs
            .iter()
            .map(|doc| {
                let cols = project_columns(primary_table, doc, all_select_columns);
                let mut docs = HashMap::new();
                docs.insert(primary_table.to_string(), doc.clone());
                RowContext {
                    table_docs: docs,
                    columns: cols,
                }
            })
            .collect();

        for (idx, (join_table, join_type, _left_col, right_col, joined_docs)) in
            join_results.iter().enumerate()
        {
            let left_table_name = joins[idx].left_table.as_deref().unwrap_or(primary_table);

            let mut new_contexts = Vec::new();

            for ctx in &contexts {
                let left_val = ctx
                    .table_docs
                    .get(left_table_name)
                    .and_then(|d| d.get(_left_col.as_str()));

                let matching: Vec<&serde_json::Value> = joined_docs
                    .iter()
                    .filter(|jdoc| values_match(left_val, jdoc.get(right_col.as_str())))
                    .collect();

                if matching.is_empty() {
                    match join_type {
                        JoinType::Left => {
                            let mut new_ctx = ctx.clone();
                            let null_cols = null_columns_for_table(join_table, all_select_columns);
                            new_ctx.columns.extend(null_cols);
                            new_contexts.push(new_ctx);
                        }
                        JoinType::Inner => { /* skip — no match */ }
                    }
                } else {
                    for jdoc in &matching {
                        let mut new_ctx = ctx.clone();
                        new_ctx
                            .table_docs
                            .insert(join_table.clone(), (*jdoc).clone());
                        let join_cols = project_columns(join_table, jdoc, all_select_columns);
                        new_ctx.columns.extend(join_cols);
                        new_contexts.push(new_ctx);
                    }
                }
            }

            contexts = new_contexts;
        }

        let mut result_rows: Vec<Vec<(String, Option<serde_json::Value>)>> =
            contexts.into_iter().map(|ctx| ctx.columns).collect();

        // Apply OFFSET/LIMIT to the final joined result
        let offset_n: usize = _offset.and_then(|o| o.parse().ok()).unwrap_or(0);
        let limit_n: Option<usize> = limit.and_then(|l| l.parse().ok());

        result_rows = result_rows
            .into_iter()
            .skip(offset_n)
            .take(limit_n.unwrap_or(usize::MAX))
            .collect();

        // Post-join GROUP BY + aggregation
        if !group_columns.is_empty() && !group_aggregates.is_empty() {
            return encode_grouped_join_rows(&result_rows, group_columns, group_aggregates);
        }

        encode_join_rows(&result_rows)
    }
}

#[derive(Clone)]
struct RowContext {
    table_docs: HashMap<String, serde_json::Value>,
    columns: Vec<(String, Option<serde_json::Value>)>,
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

    // Add join key columns — include left_col for the table it belongs to
    for jc in joins {
        let left_tbl = jc.left_table.as_deref().unwrap_or("");
        if is_primary && (left_tbl.is_empty() || left_tbl == table) {
            fields.insert(jc.left_col.clone());
        }
        if !is_primary && left_tbl == table {
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

fn encode_grouped_join_rows(
    rows: &[Vec<(String, Option<serde_json::Value>)>],
    group_columns: &[String],
    aggregates: &[AggregateExpr],
) -> PgWireResult<Response> {
    // Group rows by the GROUP BY column values
    let mut groups: Vec<(Vec<String>, usize)> = Vec::new();

    for row in rows {
        let key: Vec<String> = group_columns
            .iter()
            .map(|gc| {
                row.iter()
                    .find(|(name, _)| name == gc)
                    .and_then(|(_, val)| val.as_ref())
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();

        if let Some(existing) = groups.iter_mut().find(|(k, _)| k == &key) {
            existing.1 += 1;
        } else {
            groups.push((key, 1));
        }
    }

    // Build field infos: group columns + aggregates
    let mut field_infos = Vec::new();
    for gc in group_columns {
        field_infos.push(FieldInfo::new(
            gc.clone(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ));
    }
    for agg in aggregates {
        let pg_type = match agg.func {
            AggFunc::Count => Type::INT8,
            AggFunc::Avg => Type::FLOAT8,
            _ => Type::INT8,
        };
        field_infos.push(FieldInfo::new(
            agg.alias.clone(),
            None,
            None,
            pg_type,
            FieldFormat::Text,
        ));
    }

    let schema = Arc::new(field_infos);
    let mut encoded_rows = Vec::new();

    for (key, count) in &groups {
        let mut encoder = DataRowEncoder::new(schema.clone());

        for val in key {
            encoder.encode_field(&val.as_str())?;
        }

        for agg in aggregates {
            match agg.func {
                AggFunc::Count => encoder.encode_field(&(*count as i64))?,
                _ => encoder.encode_field(&(*count as i64))?,
            }
        }

        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(encoded_rows),
    )))
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
