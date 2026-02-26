use regex::Regex;
use sqlparser::ast::{FromTable, SetExpr, Statement, TableObject};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::{extract_table_name, object_name_to_string};

/// Replace `$1`, `$2`, ... placeholders with literal values.
///
/// String values are single-quoted, NULLs become `NULL`, numeric values stay as-is.
pub fn substitute_params(sql: &str, params: &[Option<String>]) -> String {
    if params.is_empty() {
        return sql.to_string();
    }

    let re = Regex::new(r"\$(\d+)").expect("valid regex");
    re.replace_all(sql, |caps: &regex::Captures| {
        let idx: usize = caps[1].parse().unwrap_or(0);
        if idx == 0 || idx > params.len() {
            return caps[0].to_string();
        }
        match &params[idx - 1] {
            None => "NULL".to_string(),
            Some(val) => {
                let is_literal = val.parse::<f64>().is_ok()
                    || val.eq_ignore_ascii_case("true")
                    || val.eq_ignore_ascii_case("false");
                if is_literal {
                    val.clone()
                } else {
                    format!("'{}'", val.replace('\'', "''"))
                }
            }
        }
    })
    .into_owned()
}

/// Escape a string for safe interpolation into a GraphQL string literal.
pub fn escape_graphql_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Count the number of `$N` parameter placeholders in a SQL string.
pub fn count_params(sql: &str) -> usize {
    let re = Regex::new(r"\$(\d+)").expect("valid regex");
    let mut max = 0usize;
    for caps in re.captures_iter(sql) {
        if let Ok(n) = caps[1].parse::<usize>() {
            max = max.max(n);
        }
    }
    max
}

/// Extract the table name from a SQL string (for describe responses).
///
/// Returns the first table referenced in FROM or INTO clauses.
pub fn extract_table_from_sql(sql: &str) -> Option<String> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    let stmt = statements.first()?;
    match stmt {
        Statement::Query(query) => {
            if let SetExpr::Select(select) = query.body.as_ref() {
                if let Some(from) = select.from.first() {
                    return extract_table_name(&from.relation).ok();
                }
            }
            None
        }
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => Some(object_name_to_string(name)),
            _ => None,
        },
        Statement::Update { table, .. } => extract_table_name(&table.relation).ok(),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            tables
                .first()
                .and_then(|t| extract_table_name(&t.relation).ok())
        }
        _ => None,
    }
}

/// Check if a SQL string is a SELECT query (or has RETURNING clause).
pub fn is_select_or_returning(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("SELECT") || upper.contains("RETURNING")
}

/// Check if a SQL string references system catalog tables.
///
/// postgres.js sends type discovery queries against pg_catalog on connect.
/// We return empty results for these rather than failing translation.
pub fn is_system_catalog_query(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    lower.contains("pg_catalog")
        || lower.contains("information_schema")
        || lower.contains("current_schema")
        || has_pg_system_table(&lower)
}

/// Check if the query references a Postgres system table (pg_*).
fn has_pg_system_table(lower: &str) -> bool {
    for keyword in ["from ", "join "] {
        for segment in lower.split(keyword).skip(1) {
            let table = segment.split_whitespace().next().unwrap_or("");
            let table = table.trim_matches('"');
            if table.starts_with("pg_") {
                return true;
            }
        }
    }
    false
}

/// Check if a SQL string is a transaction control statement.
pub fn is_transaction_control(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("BEGIN")
        || upper.starts_with("START TRANSACTION")
        || upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK")
}
