use std::collections::HashSet;
use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::PgWireResult;
use tracing::debug;

use crate::bridge::{AggFunc, AggregateExpr};

use super::DefraQueryHandler;

impl DefraQueryHandler {
    pub(super) async fn handle_aggregate(
        &self,
        table_name: &str,
        aggregates: &[AggregateExpr],
        filter: Option<&str>,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        let mut result_values: Vec<(String, serde_json::Value)> = Vec::new();

        for agg in aggregates {
            if agg.distinct && agg.func == AggFunc::Count {
                let value = self
                    .count_distinct(table_name, agg, filter, txn_id, identity_did)
                    .await?;
                result_values.push((agg.alias.clone(), value));
                continue;
            }

            let gql_name = agg_func_name(&agg.func);
            let mut inner_args = Vec::new();
            if let Some(ref field) = agg.field {
                inner_args.push(format!("field: \"{}\"", field));
            }
            if let Some(f) = filter {
                inner_args.push(format!("filter: {{{}}}", f));
            }
            let inner = if inner_args.is_empty() {
                String::new()
            } else {
                inner_args.join(", ")
            };
            let graphql = format!("query {{ {}({}: {{{}}}) }}", gql_name, table_name, inner);
            debug!(graphql = %graphql, "Executing aggregate query");

            let response = self.execute_graphql(&graphql, txn_id, identity_did).await?;

            if response.has_errors() {
                return Err(super::pg_error(
                    "XX000",
                    super::format_errors(&response.errors),
                ));
            }

            let value = response
                .data
                .as_ref()
                .and_then(|d| d.get(gql_name))
                .cloned()
                .unwrap_or(serde_json::Value::Number(0.into()));
            result_values.push((agg.alias.clone(), value));
        }

        encode_aggregate_response(&result_values)
    }

    async fn count_distinct(
        &self,
        table_name: &str,
        agg: &AggregateExpr,
        filter: Option<&str>,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<serde_json::Value> {
        let field = agg.field.as_deref().unwrap_or("_docID");
        let filter_part = match filter {
            Some(f) => format!("(filter: {{{}}})", f),
            None => String::new(),
        };
        let graphql = format!("query {{ {}{} {{ {} }} }}", table_name, filter_part, field);
        debug!(graphql = %graphql, "Executing COUNT(DISTINCT) via field query");

        let response = self.execute_graphql(&graphql, txn_id, identity_did).await?;

        if response.has_errors() {
            return Err(super::pg_error(
                "XX000",
                super::format_errors(&response.errors),
            ));
        }

        let docs = response
            .data
            .as_ref()
            .and_then(|d| d.get(table_name))
            .and_then(|v| v.as_array());

        let count = match docs {
            Some(arr) => {
                let mut unique = HashSet::new();
                for doc in arr {
                    let val = doc
                        .get(field)
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .unwrap_or_default();
                    unique.insert(val);
                }
                unique.len() as i64
            }
            None => 0,
        };

        Ok(serde_json::Value::Number(count.into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_group_by(
        &self,
        table_name: &str,
        group_columns: &[String],
        aggregates: &[AggregateExpr],
        _non_agg_columns: &[String],
        filter: Option<&str>,
        having_filter: Option<&str>,
        txn_id: Option<&str>,
        identity_did: Option<&str>,
    ) -> PgWireResult<Response> {
        // DefraDB GROUP BY syntax:
        //   Collection(groupBy: [field]) { field COUNT(GROUP: {}) SUM(GROUP: {field: "col"}) }
        let group_by_str = group_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut args = vec![format!("groupBy: [{}]", group_by_str)];
        if let Some(f) = filter {
            args.push(format!("filter: {{{}}}", f));
        }

        let mut select_fields = Vec::new();
        for col in group_columns {
            select_fields.push(col.clone());
        }
        for agg in aggregates {
            let gql_name = agg_func_name(&agg.func);
            let inner = if let Some(ref field) = agg.field {
                format!("field: \"{}\"", field)
            } else {
                String::new()
            };
            select_fields.push(format!("{}(GROUP: {{{}}})", gql_name, inner));
        }

        let graphql = format!(
            "query {{ {}({}) {{ {} }} }}",
            table_name,
            args.join(", "),
            select_fields.join(" ")
        );

        debug!(graphql = %graphql, "Executing GROUP BY query");

        let response = self.execute_graphql(&graphql, txn_id, identity_did).await?;

        if response.has_errors() {
            return Err(super::pg_error(
                "XX000",
                super::format_errors(&response.errors),
            ));
        }

        let data = match &response.data {
            Some(d) => d,
            None => return Ok(crate::encode::encode_empty_response("SELECT 0")),
        };

        let docs = match data.get(table_name) {
            Some(serde_json::Value::Array(arr)) => arr,
            _ => return Ok(crate::encode::encode_empty_response("SELECT 0")),
        };

        encode_grouped_response(docs, group_columns, aggregates, having_filter)
    }
}

fn agg_func_name(func: &AggFunc) -> &'static str {
    match func {
        AggFunc::Count => "COUNT",
        AggFunc::Sum => "SUM",
        AggFunc::Avg => "AVG",
        AggFunc::Min => "MIN",
        AggFunc::Max => "MAX",
    }
}

fn encode_aggregate_response(values: &[(String, serde_json::Value)]) -> PgWireResult<Response> {
    let field_infos: Vec<FieldInfo> = values
        .iter()
        .map(|(alias, val)| {
            let pg_type = match val {
                serde_json::Value::Number(n) if n.is_f64() => Type::FLOAT8,
                serde_json::Value::Number(_) => Type::INT8,
                _ => Type::TEXT,
            };
            FieldInfo::new(alias.clone(), None, None, pg_type, FieldFormat::Text)
        })
        .collect();

    let schema = Arc::new(field_infos);
    let mut encoder = DataRowEncoder::new(schema.clone());

    for (_alias, val) in values {
        match val {
            serde_json::Value::Number(n) if n.is_f64() => {
                encoder.encode_field(&n.as_f64().unwrap_or(0.0))?;
            }
            serde_json::Value::Number(n) => {
                encoder.encode_field(&n.as_i64().unwrap_or(0))?;
            }
            serde_json::Value::String(s) => {
                encoder.encode_field(&s.as_str())?;
            }
            serde_json::Value::Null => {
                encoder.encode_field(&None::<&str>)?;
            }
            other => {
                let s = other.to_string();
                encoder.encode_field(&s.as_str())?;
            }
        }
    }

    let row = encoder.take_row();
    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    )))
}

fn encode_grouped_response(
    docs: &[serde_json::Value],
    group_columns: &[String],
    aggregates: &[AggregateExpr],
    having_filter: Option<&str>,
) -> PgWireResult<Response> {
    let mut field_infos = Vec::new();
    for col in group_columns {
        field_infos.push(FieldInfo::new(
            col.clone(),
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
    let mut rows = Vec::new();

    for doc in docs {
        // Apply HAVING filter
        if let Some(having) = having_filter {
            if !evaluate_having(doc, aggregates, having) {
                continue;
            }
        }

        let mut encoder = DataRowEncoder::new(schema.clone());

        for col in group_columns {
            let val = doc.get(col);
            match val {
                Some(serde_json::Value::String(s)) => encoder.encode_field(&s.as_str())?,
                Some(serde_json::Value::Number(n)) => {
                    encoder.encode_field(&n.as_i64().unwrap_or(0))?
                }
                Some(serde_json::Value::Null) | None => encoder.encode_field(&None::<&str>)?,
                Some(other) => {
                    let s = other.to_string();
                    encoder.encode_field(&s.as_str())?;
                }
            }
        }

        for agg in aggregates {
            let gql_name = agg_func_name(&agg.func);
            let val = doc.get(gql_name);
            match val {
                Some(serde_json::Value::Number(n)) if n.is_f64() => {
                    encoder.encode_field(&n.as_f64().unwrap_or(0.0))?;
                }
                Some(serde_json::Value::Number(n)) => {
                    encoder.encode_field(&n.as_i64().unwrap_or(0))?;
                }
                _ => {
                    encoder.encode_field(&0_i64)?;
                }
            }
        }

        rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    )))
}

fn evaluate_having(doc: &serde_json::Value, aggregates: &[AggregateExpr], having: &str) -> bool {
    let trimmed = having.trim();

    // Split on top-level AND/OR (respecting parentheses)
    if let Some((left, right)) = split_compound(trimmed, " AND ") {
        return evaluate_having(doc, aggregates, left) && evaluate_having(doc, aggregates, right);
    }
    if let Some((left, right)) = split_compound(trimmed, " OR ") {
        return evaluate_having(doc, aggregates, left) || evaluate_having(doc, aggregates, right);
    }

    // Strip outer parentheses
    if trimmed.starts_with('(') && trimmed.ends_with(')') {
        let inner = &trimmed[1..trimmed.len() - 1];
        if parentheses_balanced(inner) {
            return evaluate_having(doc, aggregates, inner);
        }
    }

    evaluate_single_having(doc, aggregates, trimmed)
}

fn split_compound<'a>(s: &'a str, delimiter: &str) -> Option<(&'a str, &'a str)> {
    let mut depth = 0;
    let bytes = s.as_bytes();
    let delim_bytes = delimiter.as_bytes();

    for i in 0..bytes.len() {
        if bytes[i] == b'(' {
            depth += 1;
        } else if bytes[i] == b')' {
            depth -= 1;
        } else if depth == 0
            && i + delim_bytes.len() <= bytes.len()
            && &bytes[i..i + delim_bytes.len()] == delim_bytes
        {
            return Some((&s[..i], &s[i + delim_bytes.len()..]));
        }
    }
    None
}

fn parentheses_balanced(s: &str) -> bool {
    let mut depth = 0i32;
    for b in s.bytes() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

fn evaluate_single_having(
    doc: &serde_json::Value,
    aggregates: &[AggregateExpr],
    having: &str,
) -> bool {
    let having_lower = having.to_lowercase();

    for agg in aggregates {
        let func_name = match agg.func {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        };

        if !having_lower.contains(func_name) {
            continue;
        }

        let gql_name = agg_func_name(&agg.func);
        let agg_val = doc.get(gql_name).and_then(|v| v.as_f64()).unwrap_or(0.0);

        let ops = [">=", "<=", "!=", "==", ">", "<"];

        for op_str in &ops {
            if let Some(idx) = having.find(op_str) {
                let threshold_str = having[idx + op_str.len()..].trim();
                if let Ok(threshold) = threshold_str.parse::<f64>() {
                    return match *op_str {
                        ">=" => agg_val >= threshold,
                        "<=" => agg_val <= threshold,
                        "!=" => (agg_val - threshold).abs() > f64::EPSILON,
                        "==" => (agg_val - threshold).abs() < f64::EPSILON,
                        ">" => agg_val > threshold,
                        "<" => agg_val < threshold,
                        _ => true,
                    };
                }
            }
        }
    }

    true
}
