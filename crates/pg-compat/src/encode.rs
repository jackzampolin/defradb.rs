use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use schema::{CollectionVersion, FieldKind, ScalarKind};
use serde_json::Value as JsonValue;

use crate::types::field_kind_to_pg_type;

/// Build a PG response from a GraphQL JSON result and collection schema.
///
/// `data` is the JSON array of documents returned by QueryExecutor.
/// `collection` provides field metadata for type-correct encoding.
/// `requested_fields` constrains which columns to include. If it contains "*",
/// all scalar fields from the collection are included.
pub fn encode_response(
    data: &[JsonValue],
    collection: &CollectionVersion,
    requested_fields: &[String],
) -> PgWireResult<Response> {
    let fields = resolve_fields(collection, requested_fields);
    let field_infos = build_field_infos(&fields);
    let schema = Arc::new(field_infos);

    let mut rows = Vec::with_capacity(data.len());
    for doc in data {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (name, pg_type) in &fields {
            let value = doc.get(name);
            encode_value(&mut encoder, value, pg_type)?;
        }
        rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    )))
}

/// Build a response for an empty result set (table not found or no data).
pub fn encode_empty_response(message: &str) -> Response {
    Response::Execution(Tag::new(message))
}

/// Build a SELECT response with zero rows and no columns.
pub fn encode_empty_query_response() -> Response {
    let schema = Arc::new(vec![]);
    Response::Query(QueryResponse::new(schema, stream::iter(vec![])))
}

/// Build a zero-row SELECT response with proper column headers.
///
/// Parses the SQL to extract column names/aliases and returns a RowDescription
/// with those columns but zero DataRows. This matches real Postgres behavior
/// where an empty result still includes column metadata.
pub fn encode_empty_select_with_columns(sql: &str) -> Response {
    let columns = extract_select_columns(sql);
    if columns.is_empty() {
        return encode_empty_query_response();
    }
    let field_infos: Vec<FieldInfo> = columns.iter().map(|name| text_field(name)).collect();
    let schema = Arc::new(field_infos);
    Response::Query(QueryResponse::new(schema, stream::iter(vec![])))
}

/// Build a single-row, single-column response with a text value.
///
/// Used for synthetic responses like `SELECT current_schema()`.
pub fn encode_single_value_response(column_name: &str, value: &str) -> PgWireResult<Response> {
    let field_info = FieldInfo::new(
        column_name.to_string(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    );
    let schema = Arc::new(vec![field_info]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder.encode_field(&value)?;
    let row = encoder.take_row();
    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    )))
}

/// Build a multi-row response where all columns are TEXT.
///
/// Each row is a Vec of (column_name, value) pairs. Column names come from
/// the first row (all rows must have the same columns).
pub fn encode_text_rows(rows: &[Vec<(String, String)>]) -> PgWireResult<Response> {
    if rows.is_empty() {
        return Ok(encode_empty_query_response());
    }

    let col_names: Vec<&str> = rows[0].iter().map(|(name, _)| name.as_str()).collect();
    let field_infos: Vec<FieldInfo> = col_names
        .iter()
        .map(|name| FieldInfo::new(name.to_string(), None, None, Type::TEXT, FieldFormat::Text))
        .collect();
    let schema = Arc::new(field_infos);

    let mut encoded_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (_name, value) in row {
            encoder.encode_field(&value.as_str())?;
        }
        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(encoded_rows),
    )))
}

/// Build a response for a synthetic query (SELECT without FROM).
pub fn encode_synthetic_response(columns: &[(String, String)]) -> PgWireResult<Response> {
    if columns.is_empty() {
        return Ok(encode_empty_query_response());
    }

    let field_infos: Vec<FieldInfo> = columns
        .iter()
        .map(|(name, _)| FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text))
        .collect();
    let schema = Arc::new(field_infos);

    let mut encoder = DataRowEncoder::new(schema.clone());
    for (_name, value) in columns {
        encoder.encode_field(&value.as_str())?;
    }
    let row = encoder.take_row();

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    )))
}

/// Return basic pg_type rows for type discovery queries.
///
/// postgres.js queries pg_catalog.pg_type on connect to discover type OIDs.
/// We return a minimal set of common types.
pub fn encode_pg_types() -> Response {
    let types = vec![
        ("16", "bool"),
        ("20", "int8"),
        ("21", "int2"),
        ("23", "int4"),
        ("25", "text"),
        ("114", "json"),
        ("700", "float4"),
        ("701", "float8"),
        ("1043", "varchar"),
        ("1114", "timestamp"),
        ("1184", "timestamptz"),
        ("3802", "jsonb"),
    ];

    let field_infos = vec![
        FieldInfo::new("oid".to_string(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new(
            "typname".to_string(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
    ];
    let schema = Arc::new(field_infos);

    let mut rows = Vec::new();
    for (oid, name) in types {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&oid);
        let _ = encoder.encode_field(&name);
        rows.push(Ok(encoder.take_row()));
    }

    Response::Query(QueryResponse::new(schema, stream::iter(rows)))
}

/// Return field infos for describe responses on system catalog queries.
pub fn describe_system_catalog(sql: &str) -> Vec<FieldInfo> {
    let lower = sql.to_lowercase();

    if lower.contains("current_schema") && !lower.contains("information_schema") {
        return vec![text_field("current_schema")];
    }

    if lower.contains("information_schema.tables") {
        return vec![
            text_field("table_schema"),
            text_field("table_name"),
            text_field("table_type"),
        ];
    }

    if lower.contains("information_schema.columns") {
        return vec![
            text_field("table_schema"),
            text_field("table_name"),
            text_field("column_name"),
            text_field("ordinal_position"),
            text_field("data_type"),
            text_field("is_nullable"),
        ];
    }

    if lower.contains("pg_indexes") {
        return vec![
            text_field("schemaname"),
            text_field("tablename"),
            text_field("indexname"),
        ];
    }

    if lower.contains("table_constraints") && lower.contains("constraint_column_usage") {
        return vec![
            text_field("table_name"),
            text_field("constraint_name"),
            text_field("foreign_table_name"),
        ];
    }

    if lower.contains("pg_index") && lower.contains("pg_attribute") {
        return vec![text_field("attname")];
    }

    if lower.contains("pg_type") {
        return vec![text_field("oid"), text_field("typname")];
    }

    // Fallback: extract column names from the SELECT clause
    extract_select_columns(sql)
        .iter()
        .map(|name| text_field(name))
        .collect()
}

/// Return field infos for describe responses on synthetic queries (SELECT without FROM).
pub fn describe_synthetic_query(sql: &str) -> Vec<FieldInfo> {
    // Parse SQL to extract column aliases
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    let stmt = match statements.first() {
        Some(s) => s,
        None => return vec![],
    };

    if let sqlparser::ast::Statement::Query(query) = stmt {
        if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
            if select.from.is_empty() {
                return select
                    .projection
                    .iter()
                    .map(|item| match item {
                        sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                            text_field(&alias.value)
                        }
                        sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                            text_field(&format!("{}", expr))
                        }
                        _ => text_field("?column?"),
                    })
                    .collect();
            }
        }
    }

    vec![]
}

/// Map a DefraDB FieldKind to a PostgreSQL type name string.
pub fn field_kind_to_pg_type_name(kind: &FieldKind) -> String {
    match kind {
        FieldKind::Scalar(scalar) => match scalar {
            ScalarKind::Bool => "boolean".to_string(),
            ScalarKind::Int => "bigint".to_string(),
            ScalarKind::Float64 => "double precision".to_string(),
            ScalarKind::Float32 => "real".to_string(),
            ScalarKind::String | ScalarKind::DocID | ScalarKind::None => "text".to_string(),
            ScalarKind::DateTime => "timestamp with time zone".to_string(),
            ScalarKind::Blob => "bytea".to_string(),
            ScalarKind::Json => "jsonb".to_string(),
        },
        _ => "text".to_string(),
    }
}

/// Extract column names from a RETURNING clause (INSERT/UPDATE/DELETE).
pub fn extract_returning_columns(sql: &str) -> Vec<String> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    let stmt = match statements.first() {
        Some(s) => s,
        None => return vec![],
    };

    let returning = match stmt {
        sqlparser::ast::Statement::Insert(insert) => insert.returning.as_ref(),
        sqlparser::ast::Statement::Update { returning, .. } => returning.as_ref(),
        sqlparser::ast::Statement::Delete(delete) => delete.returning.as_ref(),
        _ => return vec![],
    };

    match returning {
        Some(items) => items
            .iter()
            .map(|item| match item {
                sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                    extract_column_name_from_expr(expr)
                }
                sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
                sqlparser::ast::SelectItem::QualifiedWildcard(name, _) => {
                    format!("{}.*", name)
                }
            })
            .collect(),
        None => vec![],
    }
}

/// Extract output column names from a SELECT statement.
///
/// Handles aliases (`AS name`), qualified columns (`t.col`), and expressions.
/// Falls back to `?column?` for unparseable items.
pub fn extract_select_columns(sql: &str) -> Vec<String> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    let stmt = match statements.first() {
        Some(s) => s,
        None => return vec![],
    };

    if let sqlparser::ast::Statement::Query(query) = stmt {
        return extract_columns_from_set_expr(query.body.as_ref());
    }

    vec![]
}

fn extract_columns_from_set_expr(body: &sqlparser::ast::SetExpr) -> Vec<String> {
    match body {
        sqlparser::ast::SetExpr::Select(select) => select
            .projection
            .iter()
            .map(|item| match item {
                sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                    extract_column_name_from_expr(expr)
                }
                sqlparser::ast::SelectItem::QualifiedWildcard(name, _) => {
                    format!("{}.*", name)
                }
                sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
            })
            .collect(),
        sqlparser::ast::SetExpr::SetOperation { left, .. } => extract_columns_from_set_expr(left),
        sqlparser::ast::SetExpr::Query(inner) => extract_columns_from_set_expr(inner.body.as_ref()),
        _ => vec![],
    }
}

fn extract_column_name_from_expr(expr: &sqlparser::ast::Expr) -> String {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
        sqlparser::ast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        _ => "?column?".to_string(),
    }
}

fn text_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_string(), None, None, Type::TEXT, FieldFormat::Text)
}

/// Resolve which fields to include, returning (name, pg_type) pairs.
fn resolve_fields(collection: &CollectionVersion, requested: &[String]) -> Vec<(String, Type)> {
    let use_all = requested.is_empty() || requested.iter().any(|f| f == "*");

    if use_all {
        collection
            .fields
            .iter()
            .filter(|f| f.kind.is_scalar())
            .map(|f| (f.name.clone(), field_kind_to_pg_type(&f.kind)))
            .collect()
    } else {
        requested
            .iter()
            .filter_map(|name| {
                collection
                    .field_by_name(name)
                    .map(|f| (f.name.clone(), field_kind_to_pg_type(&f.kind)))
            })
            .collect()
    }
}

fn build_field_infos(fields: &[(String, Type)]) -> Vec<FieldInfo> {
    fields
        .iter()
        .map(|(name, pg_type)| {
            FieldInfo::new(name.clone(), None, None, pg_type.clone(), FieldFormat::Text)
        })
        .collect()
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    value: Option<&JsonValue>,
    pg_type: &Type,
) -> PgWireResult<()> {
    match value {
        None | Some(JsonValue::Null) => encoder.encode_field(&None::<&str>),
        Some(JsonValue::Bool(b)) => encoder.encode_field(b),
        Some(JsonValue::Number(n)) => encode_number(encoder, n, pg_type),
        Some(JsonValue::String(s)) => encoder.encode_field(&s.as_str()),
        Some(JsonValue::Array(_)) | Some(JsonValue::Object(_)) => {
            let s = serde_json::to_string(value.unwrap())
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encoder.encode_field(&s.as_str())
        }
    }
}

fn encode_number(
    encoder: &mut DataRowEncoder,
    n: &serde_json::Number,
    pg_type: &Type,
) -> PgWireResult<()> {
    match *pg_type {
        Type::INT8 => {
            let v = n.as_i64().unwrap_or(0);
            encoder.encode_field(&v)
        }
        Type::FLOAT4 => {
            let v = n.as_f64().unwrap_or(0.0) as f32;
            encoder.encode_field(&v)
        }
        Type::FLOAT8 => {
            let v = n.as_f64().unwrap_or(0.0);
            encoder.encode_field(&v)
        }
        _ => {
            let s = n.to_string();
            encoder.encode_field(&s.as_str())
        }
    }
}

/// Build FieldInfo metadata for all scalar fields in a collection.
///
/// Used by describe responses so clients know column types before execution.
pub fn build_field_infos_from_collection(collection: &CollectionVersion) -> Vec<FieldInfo> {
    let fields = resolve_fields(collection, &[]);
    build_field_infos(&fields)
}

/// Build FieldInfo metadata for specific columns from a collection.
///
/// Returns field infos matching the requested column names in order.
/// Columns not found in the collection are returned as TEXT type.
pub fn build_field_infos_for_columns(
    collection: &CollectionVersion,
    columns: &[String],
) -> Vec<FieldInfo> {
    columns
        .iter()
        .map(|col| {
            let pg_type = collection
                .field_by_name(col)
                .map(|f| field_kind_to_pg_type(&f.kind))
                .unwrap_or(Type::TEXT);
            FieldInfo::new(col.clone(), None, None, pg_type, FieldFormat::Text)
        })
        .collect()
}

/// Extract field names from a SELECT * or explicit column list.
pub fn extract_requested_fields(fields_str: &str) -> Vec<String> {
    fields_str
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}
