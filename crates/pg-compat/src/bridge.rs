use std::collections::HashMap;

use regex::Regex;
use schema::ScalarKind;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, ColumnDef, DataType, Expr, FromTable, ObjectName,
    OnConflictAction, OnInsert, OrderByExpr, OrderByKind, SelectItem, SetExpr, Statement,
    TableFactor, TableObject, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::PgCompatError;

/// Map of field name → scalar kind, used for schema-aware type coercion.
pub type FieldTypeMap = HashMap<String, ScalarKind>;

#[derive(Debug, PartialEq)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, PartialEq)]
pub enum SqlStatement {
    Query(String),
    Mutation {
        graphql: String,
        table_name: String,
        mutation_name: String,
        kind: MutationKind,
    },
    Begin,
    Commit,
    Rollback,
    /// CREATE TABLE translated to DefraDB SDL.
    CreateTable(String),
    /// DROP TABLE — treated as a no-op.
    DropTable,
    /// CREATE INDEX — treated as a no-op.
    CreateIndex,
    /// ALTER TABLE — treated as a no-op (FK constraints, etc.).
    AlterTable,
    /// SELECT without FROM — synthetic result from expression evaluation.
    SyntheticQuery {
        columns: Vec<(String, String)>,
    },
    /// INSERT ... ON CONFLICT DO UPDATE — check-then-insert-or-update.
    Upsert {
        insert_graphql: String,
        update_graphql: String,
        check_graphql: String,
        table_name: String,
        insert_mutation_name: String,
        update_mutation_name: String,
    },
}

/// Parse a SQL string and translate it to a structured statement (no type coercion).
#[cfg(test)]
pub fn sql_to_graphql(sql: &str) -> Result<SqlStatement, PgCompatError> {
    sql_to_graphql_typed(sql, None)
}

/// Parse a SQL string with schema-aware type coercion for mutations.
///
/// When `field_types` is provided, numeric values targeting String fields
/// are quoted as GraphQL strings instead of bare numbers. This fixes the
/// extended query protocol case where postgres.js sends all params as text
/// and `substitute_params` treats numeric-looking values as bare numbers.
pub fn sql_to_graphql_typed(
    sql: &str,
    field_types: Option<&FieldTypeMap>,
) -> Result<SqlStatement, PgCompatError> {
    let dialect = PostgreSqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| PgCompatError::SqlParse(e.to_string()))?;

    if statements.is_empty() {
        return Err(PgCompatError::SqlParse("empty query".into()));
    }

    match &statements[0] {
        Statement::Query(query) => translate_query(query),
        Statement::Insert(insert) => translate_insert(insert, field_types),
        Statement::Update {
            table,
            assignments,
            selection,
            returning,
            ..
        } => translate_update(
            table,
            assignments,
            selection.as_ref(),
            returning.as_deref(),
            field_types,
        ),
        Statement::Delete(delete) => translate_delete(delete),
        Statement::StartTransaction { .. } => Ok(SqlStatement::Begin),
        Statement::Commit { .. } => Ok(SqlStatement::Commit),
        Statement::Rollback { .. } => Ok(SqlStatement::Rollback),
        Statement::CreateTable(ct) => translate_create_table(ct),
        Statement::Drop { .. } => Ok(SqlStatement::DropTable),
        Statement::CreateIndex(_) => Ok(SqlStatement::CreateIndex),
        Statement::AlterTable { .. } => Ok(SqlStatement::AlterTable),
        other => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported statement: {}",
            statement_kind(other)
        ))),
    }
}

/// Replace `$1`, `$2`, ... placeholders with literal values for `sql_to_graphql()`.
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
///
/// Matches tables in FROM/JOIN clauses that start with `pg_`.
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

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::CreateIndex { .. } => "CREATE INDEX",
        _ => "unsupported statement",
    }
}

/// Translate a CREATE TABLE statement to a DefraDB GraphQL SDL type definition.
fn translate_create_table(ct: &sqlparser::ast::CreateTable) -> Result<SqlStatement, PgCompatError> {
    let table_name = object_name_to_string(&ct.name);

    let mut fields = Vec::new();
    for col in &ct.columns {
        if let Some(sdl_field) = column_to_sdl_field(col) {
            fields.push(sdl_field);
        }
    }

    if fields.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "CREATE TABLE requires at least one column".into(),
        ));
    }

    let sdl = format!("type {} {{\n  {}\n}}", table_name, fields.join("\n  "));
    Ok(SqlStatement::CreateTable(sdl))
}

/// Convert a SQL column definition to a DefraDB SDL field string.
fn column_to_sdl_field(col: &ColumnDef) -> Option<String> {
    let name = &col.name.value;
    let defra_type = sql_type_to_defra(&col.data_type)?;
    Some(format!("{}: {}", name, defra_type))
}

/// Map SQL data types to DefraDB scalar type names.
fn sql_type_to_defra(dt: &DataType) -> Option<&'static str> {
    match dt {
        DataType::Text
        | DataType::Varchar(_)
        | DataType::CharVarying(_)
        | DataType::Character(_)
        | DataType::Char(_) => Some("String"),

        DataType::Integer(_) | DataType::Int(_) | DataType::SmallInt(_) | DataType::Int4(_) => {
            Some("Int")
        }

        DataType::BigInt(_) | DataType::Int8(_) => Some("Int"),

        DataType::Real | DataType::Float4 | DataType::Float(_) => Some("Float"),

        DataType::Double(_) | DataType::DoublePrecision | DataType::Float8 => Some("Float"),

        DataType::Boolean => Some("Boolean"),

        DataType::Timestamp(_, _) | DataType::Date => Some("DateTime"),

        DataType::JSON | DataType::JSONB => Some("JSON"),

        DataType::Bytea => Some("Blob"),

        _ => Some("String"),
    }
}

fn translate_query(query: &sqlparser::ast::Query) -> Result<SqlStatement, PgCompatError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(PgCompatError::UnsupportedSql(
                "only simple SELECT statements are supported".into(),
            ))
        }
    };

    // Handle SELECT without FROM (synthetic expressions like SELECT $1::text AS greeting)
    if select.from.is_empty() {
        return translate_synthetic_query(&select.projection);
    }

    if select.from.len() != 1 {
        return Err(PgCompatError::UnsupportedSql(
            "exactly one FROM table is required".into(),
        ));
    }
    let table_name = extract_table_name(&select.from[0].relation)?;

    let fields = translate_projection(&select.projection)?;

    let mut args = Vec::new();

    if let Some(ref selection) = select.selection {
        let filter = translate_where(selection)?;
        args.push(format!("filter: {{{}}}", filter));
    }

    if let Some(ref order_by) = query.order_by {
        match order_by {
            sqlparser::ast::OrderBy {
                kind: OrderByKind::All { .. },
                ..
            } => {}
            sqlparser::ast::OrderBy {
                kind: OrderByKind::Expressions(exprs),
                ..
            } => {
                if !exprs.is_empty() {
                    let order = translate_order(exprs)?;
                    args.push(format!("order: {}", order));
                }
            }
        }
    }

    if let Some(ref limit_expr) = query.limit {
        let limit = translate_limit_expr(limit_expr)?;
        args.push(format!("limit: {}", limit));
    }

    if let Some(ref offset) = query.offset {
        let off = translate_limit_expr(&offset.value)?;
        args.push(format!("offset: {}", off));
    }

    let args_str = if args.is_empty() {
        String::new()
    } else {
        format!("({})", args.join(", "))
    };

    Ok(SqlStatement::Query(format!(
        "query {{ {}{} {{ {} }} }}",
        table_name, args_str, fields
    )))
}

fn translate_insert(
    insert: &sqlparser::ast::Insert,
    field_types: Option<&FieldTypeMap>,
) -> Result<SqlStatement, PgCompatError> {
    let table_name = match &insert.table {
        TableObject::TableName(name) => object_name_to_string(name),
        _ => {
            return Err(PgCompatError::UnsupportedSql(
                "only simple table names are supported for INSERT".into(),
            ))
        }
    };

    if insert.columns.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "INSERT requires explicit column list".into(),
        ));
    }

    // Check for ON CONFLICT DO UPDATE (upsert) — translate as INSERT + UPDATE
    if let Some(OnInsert::OnConflict(conflict)) = &insert.on {
        return translate_upsert(insert, &table_name, conflict, field_types);
    }

    let col_names: Vec<&str> = insert.columns.iter().map(|c| c.value.as_str()).collect();

    let rows = extract_insert_values(insert)?;

    let mut input_objects = Vec::with_capacity(rows.len());
    for row in &rows {
        if row.len() != col_names.len() {
            return Err(PgCompatError::UnsupportedSql(format!(
                "VALUES row has {} values but {} columns specified",
                row.len(),
                col_names.len()
            )));
        }
        let fields: Result<Vec<String>, _> = col_names
            .iter()
            .zip(row.iter())
            .map(|(col, val)| {
                let type_hint = field_types.and_then(|ft| ft.get(*col));
                let v = typed_graphql_value(val, type_hint)?;
                Ok(format!("{}: {}", col, v))
            })
            .collect();
        input_objects.push(format!("{{{}}}", fields?.join(", ")));
    }

    let input_str = if input_objects.len() == 1 {
        input_objects.into_iter().next().unwrap()
    } else {
        format!("[{}]", input_objects.join(", "))
    };

    let return_fields = translate_returning(&insert.returning);
    let mutation_name = format!("create_{}", table_name);

    let graphql = format!(
        "mutation {{ {}(input: {}) {{ {} }} }}",
        mutation_name, input_str, return_fields
    );

    Ok(SqlStatement::Mutation {
        graphql,
        table_name,
        mutation_name,
        kind: MutationKind::Insert,
    })
}

/// Translate INSERT ... ON CONFLICT DO UPDATE into an Upsert statement.
///
/// Since DefraDB doesn't have native upsert, we translate this to two operations:
/// first try an INSERT, then if it fails (conflict) do an UPDATE.
/// We represent this as a special Upsert variant.
fn translate_upsert(
    insert: &sqlparser::ast::Insert,
    table_name: &str,
    conflict: &sqlparser::ast::OnConflict,
    field_types: Option<&FieldTypeMap>,
) -> Result<SqlStatement, PgCompatError> {
    let col_names: Vec<&str> = insert.columns.iter().map(|c| c.value.as_str()).collect();
    let rows = extract_insert_values(insert)?;

    if rows.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "INSERT ON CONFLICT requires at least one VALUES row".into(),
        ));
    }

    let row = &rows[0];
    let fields: Result<Vec<String>, _> = col_names
        .iter()
        .zip(row.iter())
        .map(|(col, val)| {
            let type_hint = field_types.and_then(|ft| ft.get(*col));
            let v = typed_graphql_value(val, type_hint)?;
            Ok(format!("{}: {}", col, v))
        })
        .collect();
    let input_str = format!("{{{}}}", fields?.join(", "));

    let return_fields = translate_returning(&insert.returning);
    let mutation_name = format!("create_{}", table_name);

    let insert_graphql = format!(
        "mutation {{ {}(input: {}) {{ {} }} }}",
        mutation_name, input_str, return_fields
    );

    // Extract update fields from ON CONFLICT DO UPDATE SET ...
    let update_fields = match &conflict.action {
        OnConflictAction::DoUpdate(do_update) => {
            let mut fields = Vec::new();
            for assign in &do_update.assignments {
                let col = assignment_target_name(&assign.target)?;
                let type_hint = field_types.and_then(|ft| ft.get(col.as_str()));
                let val = typed_graphql_value(&assign.value, type_hint)?;
                fields.push(format!("{}: {}", col, val));
            }
            format!("{{{}}}", fields.join(", "))
        }
        OnConflictAction::DoNothing => {
            // ON CONFLICT DO NOTHING — just treat as regular insert
            return Ok(SqlStatement::Mutation {
                graphql: insert_graphql,
                table_name: table_name.to_string(),
                mutation_name,
                kind: MutationKind::Insert,
            });
        }
    };

    // Extract conflict target column(s) for building the WHERE filter
    let conflict_cols: Vec<String> = match &conflict.conflict_target {
        Some(sqlparser::ast::ConflictTarget::Columns(cols)) => {
            cols.iter().map(|c| c.value.clone()).collect()
        }
        _ => vec![],
    };

    // Build update filter from conflict columns + inserted values
    let mut filter_parts = Vec::new();
    for col in &conflict_cols {
        if let Some(idx) = col_names.iter().position(|c| *c == col.as_str()) {
            let val = expr_to_graphql_value(&row[idx])?;
            filter_parts.push(format!("{}: {{_eq: {}}}", col, val));
        }
    }

    let update_mutation_name = format!("update_{}", table_name);
    let update_args = if filter_parts.is_empty() {
        format!("input: {}", update_fields)
    } else {
        format!(
            "filter: {{{}}}, input: {}",
            filter_parts.join(", "),
            update_fields
        )
    };

    let update_graphql = format!(
        "mutation {{ {}({}) {{ {} }} }}",
        update_mutation_name, update_args, return_fields
    );

    // Check query: does a row with the conflict key already exist?
    let check_graphql = if filter_parts.is_empty() {
        format!("query {{ {} {{ _docID }} }}", table_name)
    } else {
        format!(
            "query {{ {}(filter: {{{}}}) {{ _docID }} }}",
            table_name,
            filter_parts.join(", ")
        )
    };

    Ok(SqlStatement::Upsert {
        insert_graphql,
        update_graphql,
        check_graphql,
        table_name: table_name.to_string(),
        insert_mutation_name: mutation_name,
        update_mutation_name,
    })
}

fn extract_insert_values(insert: &sqlparser::ast::Insert) -> Result<Vec<Vec<Expr>>, PgCompatError> {
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| PgCompatError::UnsupportedSql("INSERT requires VALUES clause".into()))?;

    match source.body.as_ref() {
        SetExpr::Values(values) => Ok(values.rows.clone()),
        _ => Err(PgCompatError::UnsupportedSql(
            "only INSERT ... VALUES is supported".into(),
        )),
    }
}

fn translate_update(
    table: &sqlparser::ast::TableWithJoins,
    assignments: &[sqlparser::ast::Assignment],
    selection: Option<&Expr>,
    returning: Option<&[SelectItem]>,
    field_types: Option<&FieldTypeMap>,
) -> Result<SqlStatement, PgCompatError> {
    let table_name = extract_table_name(&table.relation)?;

    if assignments.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "UPDATE requires at least one SET assignment".into(),
        ));
    }

    let mut input_fields = Vec::with_capacity(assignments.len());
    for assign in assignments {
        let col = assignment_target_name(&assign.target)?;
        validate_assignment_value(&col, &assign.value)?;
        let type_hint = field_types.and_then(|ft| ft.get(col.as_str()));
        let val = typed_graphql_value(&assign.value, type_hint)?;
        input_fields.push(format!("{}: {}", col, val));
    }
    let input_str = format!("{{{}}}", input_fields.join(", "));

    let mut args = Vec::new();

    if let Some(sel) = selection {
        if let Some(doc_id) = try_extract_docid(sel) {
            args.push(format!("docID: \"{}\"", doc_id));
        } else {
            let filter = translate_where(sel)?;
            args.push(format!("filter: {{{}}}", filter));
        }
    }

    args.push(format!("input: {}", input_str));

    let return_fields = translate_returning(&returning.map(|s| s.to_vec()));
    let mutation_name = format!("update_{}", table_name);

    let graphql = format!(
        "mutation {{ {}({}) {{ {} }} }}",
        mutation_name,
        args.join(", "),
        return_fields
    );

    Ok(SqlStatement::Mutation {
        graphql,
        table_name,
        mutation_name,
        kind: MutationKind::Update,
    })
}

fn translate_delete(delete: &sqlparser::ast::Delete) -> Result<SqlStatement, PgCompatError> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };

    if tables.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "DELETE requires a FROM table".into(),
        ));
    }

    let table_name = extract_table_name(&tables[0].relation)?;

    let mut args = Vec::new();

    if let Some(sel) = &delete.selection {
        if let Some(doc_id) = try_extract_docid(sel) {
            args.push(format!("docID: \"{}\"", doc_id));
        } else {
            let filter = translate_where(sel)?;
            args.push(format!("filter: {{{}}}", filter));
        }
    }

    let return_fields = translate_returning(&delete.returning);
    let mutation_name = format!("delete_{}", table_name);

    let args_str = if args.is_empty() {
        String::new()
    } else {
        format!("({})", args.join(", "))
    };

    let graphql = format!(
        "mutation {{ {}{} {{ {} }} }}",
        mutation_name, args_str, return_fields
    );

    Ok(SqlStatement::Mutation {
        graphql,
        table_name,
        mutation_name,
        kind: MutationKind::Delete,
    })
}

/// Check if a WHERE clause is `_docID = 'value'` and extract the value.
fn try_extract_docid(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            let field = expr_to_field_name(left).ok()?;
            if field == "_docID" {
                if let Ok(val) = extract_string_value(right) {
                    return Some(val);
                }
            }
            None
        }
        Expr::Nested(inner) => try_extract_docid(inner),
        _ => None,
    }
}

fn extract_string_value(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
            _ => Err(PgCompatError::UnsupportedSql(
                "expected string value".into(),
            )),
        },
        _ => Err(PgCompatError::UnsupportedSql(
            "expected string value".into(),
        )),
    }
}

fn assignment_target_name(target: &AssignmentTarget) -> Result<String, PgCompatError> {
    match target {
        AssignmentTarget::ColumnName(name) => Ok(object_name_to_string(name)),
        _ => Err(PgCompatError::UnsupportedSql(
            "only simple column assignments are supported".into(),
        )),
    }
}

/// Reject arithmetic expressions like `counter = counter + 1`.
fn validate_assignment_value(col: &str, value: &Expr) -> Result<(), PgCompatError> {
    match value {
        Expr::BinaryOp { .. } => Err(PgCompatError::UnsupportedSql(format!(
            "arithmetic expressions in SET clause for '{}' are not supported; \
             use GraphQL _increment/_decrement mutations instead",
            col
        ))),
        _ => Ok(()),
    }
}

fn translate_returning(returning: &Option<Vec<SelectItem>>) -> String {
    match returning {
        Some(items) if !items.is_empty() => {
            let fields: Vec<String> = items
                .iter()
                .map(|item| match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => ident.value.clone(),
                    SelectItem::Wildcard(_) => "*".to_string(),
                    other => format!("{}", other),
                })
                .collect();
            fields.join(" ")
        }
        _ => "_docID".to_string(),
    }
}

fn extract_table_name(table: &TableFactor) -> Result<String, PgCompatError> {
    match table {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(PgCompatError::UnsupportedSql(
            "only simple table references are supported".into(),
        )),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
        .collect::<Vec<_>>()
        .join(".")
}

fn translate_projection(items: &[SelectItem]) -> Result<String, PgCompatError> {
    let mut fields = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let name = projection_expr_to_field(expr)?;
                fields.push(name);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let _name = projection_expr_to_field(expr)?;
                fields.push(alias.value.clone());
            }
            SelectItem::Wildcard(_) => {
                fields.push("*".to_string());
            }
            _ => {
                return Err(PgCompatError::UnsupportedSql(format!(
                    "unsupported SELECT item: {}",
                    item
                )));
            }
        }
    }
    Ok(fields.join(" "))
}

/// Extract a field name from a projection expression, handling casts and qualifiers.
fn projection_expr_to_field(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .ok_or_else(|| PgCompatError::UnsupportedSql("empty compound identifier".into())),
        Expr::Cast { expr, .. } => projection_expr_to_field(expr),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported SELECT expression: {}",
            expr
        ))),
    }
}

fn translate_where(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::BinaryOp { left, op, right } => translate_binary_op(left, op, right),
        Expr::Nested(inner) => translate_where(inner),
        Expr::IsNull(inner) => {
            let field = expr_to_field_name(inner)?;
            Ok(format!("{}: {{_eq: null}}", field))
        }
        Expr::IsNotNull(inner) => {
            let field = expr_to_field_name(inner)?;
            Ok(format!("{}: {{_ne: null}}", field))
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let field = expr_to_field_name(inner)?;
            let values: Result<Vec<String>, _> = list.iter().map(expr_to_graphql_value).collect();
            let op = if *negated { "_nin" } else { "_in" };
            Ok(format!("{}: {{{}: [{}]}}", field, op, values?.join(", ")))
        }
        Expr::Like {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            let field = expr_to_field_name(inner)?;
            let pat = expr_to_graphql_value(pattern)?;
            let op = if *negated { "_nlike" } else { "_like" };
            Ok(format!("{}: {{{}: {}}}", field, op, pat))
        }
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported WHERE expression: {}",
            expr
        ))),
    }
}

/// Translate a SELECT without FROM into a SyntheticQuery.
fn translate_synthetic_query(items: &[SelectItem]) -> Result<SqlStatement, PgCompatError> {
    let mut columns = Vec::new();
    for item in items {
        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                let value = eval_const_expr(expr)?;
                columns.push((alias.value.clone(), value));
            }
            SelectItem::UnnamedExpr(expr) => {
                let value = eval_const_expr(expr)?;
                let name = format!("{}", expr);
                columns.push((name, value));
            }
            _ => {
                return Err(PgCompatError::UnsupportedSql(
                    "unsupported synthetic SELECT item".into(),
                ))
            }
        }
    }
    Ok(SqlStatement::SyntheticQuery { columns })
}

/// Evaluate a constant expression to a string value.
fn eval_const_expr(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
            Value::Number(n, _) => Ok(n.clone()),
            Value::Boolean(b) => Ok(b.to_string()),
            Value::Null => Ok("".to_string()),
            _ => Err(PgCompatError::UnsupportedSql(format!(
                "unsupported constant: {}",
                vws.value
            ))),
        },
        Expr::Cast { expr, .. } => eval_const_expr(expr),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported synthetic expression: {}",
            expr
        ))),
    }
}

fn translate_binary_op(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
) -> Result<String, PgCompatError> {
    match op {
        BinaryOperator::And => {
            let l = translate_where(left)?;
            let r = translate_where(right)?;
            Ok(format!("_and: [{{{}}}, {{{}}}]", l, r))
        }
        BinaryOperator::Or => {
            let l = translate_where(left)?;
            let r = translate_where(right)?;
            Ok(format!("_or: [{{{}}}, {{{}}}]", l, r))
        }
        _ => {
            let field = expr_to_field_name(left)?;
            let gql_op = sql_op_to_graphql(op)?;
            let value = expr_to_graphql_value(right)?;
            Ok(format!("{}: {{{}: {}}}", field, gql_op, value))
        }
    }
}

fn sql_op_to_graphql(op: &BinaryOperator) -> Result<&'static str, PgCompatError> {
    match op {
        BinaryOperator::Eq => Ok("_eq"),
        BinaryOperator::NotEq => Ok("_ne"),
        BinaryOperator::Gt => Ok("_gt"),
        BinaryOperator::GtEq => Ok("_ge"),
        BinaryOperator::Lt => Ok("_lt"),
        BinaryOperator::LtEq => Ok("_le"),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported SQL operator: {}",
            op
        ))),
    }
}

fn expr_to_field_name(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // "table"."column" → just use the column name (last part)
            parts
                .last()
                .map(|p| p.value.clone())
                .ok_or_else(|| PgCompatError::UnsupportedSql("empty compound identifier".into()))
        }
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "expected column name, got: {}",
            expr
        ))),
    }
}

fn expr_to_graphql_value(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::Value(vws) => value_to_graphql(&vws.value),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let val = expr_to_graphql_value(expr)?;
            Ok(format!("-{}", val))
        }
        Expr::Cast { expr, .. } => expr_to_graphql_value(expr),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported value expression: {}",
            expr
        ))),
    }
}

fn value_to_graphql(value: &Value) -> Result<String, PgCompatError> {
    match value {
        Value::Number(n, _) => Ok(n.clone()),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            Ok(format!("\"{}\"", escaped))
        }
        Value::Boolean(b) => Ok(b.to_string()),
        Value::Null => Ok("null".to_string()),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported value: {}",
            value
        ))),
    }
}

/// Convert a SQL expression to a GraphQL value with optional schema type hint.
///
/// When `type_hint` indicates a String field, numeric values are quoted as
/// GraphQL strings. This handles the extended query protocol case where
/// postgres.js sends `"2"` as a text param that `substitute_params` treats
/// as a bare number, but DefraDB expects a string for the target field.
fn typed_graphql_value(
    expr: &Expr,
    type_hint: Option<&ScalarKind>,
) -> Result<String, PgCompatError> {
    match expr {
        Expr::Value(vws) => typed_value_to_graphql(&vws.value, type_hint),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let val = typed_graphql_value(expr, type_hint)?;
            Ok(format!("-{}", val))
        }
        Expr::Cast { expr, .. } => typed_graphql_value(expr, type_hint),
        _ => expr_to_graphql_value(expr),
    }
}

fn typed_value_to_graphql(
    value: &Value,
    type_hint: Option<&ScalarKind>,
) -> Result<String, PgCompatError> {
    let is_string_field = matches!(
        type_hint,
        Some(ScalarKind::String | ScalarKind::DocID | ScalarKind::None | ScalarKind::DateTime)
    );

    match value {
        Value::Number(n, _) if is_string_field => {
            // Field expects a string but SQL has a bare number (from param substitution).
            Ok(format!("\"{}\"", n))
        }
        _ => value_to_graphql(value),
    }
}

fn translate_order(exprs: &[OrderByExpr]) -> Result<String, PgCompatError> {
    let mut parts = Vec::new();
    for expr in exprs {
        let field = expr_to_field_name(&expr.expr)?;
        let dir = if expr.options.asc == Some(false) {
            "DESC"
        } else {
            "ASC"
        };
        parts.push(format!("{{{}: {}}}", field, dir));
    }
    if parts.len() == 1 {
        Ok(parts.into_iter().next().unwrap())
    } else {
        Ok(format!("[{}]", parts.join(", ")))
    }
}

fn translate_limit_expr(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => Ok(n.clone()),
            _ => Err(PgCompatError::UnsupportedSql(format!(
                "unsupported LIMIT/OFFSET value: {}",
                vws.value
            ))),
        },
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported LIMIT/OFFSET expression: {}",
            expr
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── SELECT tests (unchanged from Phase 1) ──

    #[test]
    fn simple_select_all() {
        let stmt = sql_to_graphql("SELECT name, age FROM User").unwrap();
        assert_eq!(
            stmt,
            SqlStatement::Query("query { User { name age } }".into())
        );
    }

    #[test]
    fn select_with_where() {
        let stmt = sql_to_graphql("SELECT name FROM User WHERE age > 25").unwrap();
        assert_eq!(
            stmt,
            SqlStatement::Query("query { User(filter: {age: {_gt: 25}}) { name } }".into())
        );
    }

    #[test]
    fn select_with_order() {
        let stmt = sql_to_graphql("SELECT name FROM User ORDER BY name").unwrap();
        assert_eq!(
            stmt,
            SqlStatement::Query("query { User(order: {name: ASC}) { name } }".into())
        );
    }

    #[test]
    fn select_with_limit_offset() {
        let stmt = sql_to_graphql("SELECT name FROM User LIMIT 10 OFFSET 5").unwrap();
        assert_eq!(
            stmt,
            SqlStatement::Query("query { User(limit: 10, offset: 5) { name } }".into())
        );
    }

    #[test]
    fn select_with_string_where() {
        let stmt = sql_to_graphql("SELECT name FROM User WHERE name = 'Alice'").unwrap();
        assert_eq!(
            stmt,
            SqlStatement::Query("query { User(filter: {name: {_eq: \"Alice\"}}) { name } }".into())
        );
    }

    #[test]
    fn select_with_and() {
        let stmt =
            sql_to_graphql("SELECT name FROM User WHERE age > 25 AND name = 'Alice'").unwrap();
        match stmt {
            SqlStatement::Query(gql) => {
                assert!(gql.contains("_and"));
                assert!(gql.contains("_gt: 25"));
                assert!(gql.contains("_eq: \"Alice\""));
            }
            _ => panic!("expected Query"),
        }
    }

    // ── INSERT tests ──

    #[test]
    fn insert_single_row() {
        let stmt = sql_to_graphql("INSERT INTO User (name, age) VALUES ('Alice', 30)").unwrap();
        match stmt {
            SqlStatement::Mutation {
                graphql,
                table_name,
                mutation_name,
                kind,
            } => {
                assert_eq!(kind, MutationKind::Insert);
                assert_eq!(table_name, "User");
                assert_eq!(mutation_name, "create_User");
                assert_eq!(
                    graphql,
                    "mutation { create_User(input: {name: \"Alice\", age: 30}) { _docID } }"
                );
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn insert_multi_row() {
        let stmt = sql_to_graphql("INSERT INTO User (name, age) VALUES ('Alice', 30), ('Bob', 25)")
            .unwrap();
        match stmt {
            SqlStatement::Mutation { graphql, kind, .. } => {
                assert_eq!(kind, MutationKind::Insert);
                assert!(graphql.contains("[{name: \"Alice\", age: 30}, {name: \"Bob\", age: 25}]"));
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn insert_with_returning() {
        let stmt = sql_to_graphql(
            "INSERT INTO User (name, age) VALUES ('Alice', 30) RETURNING _docID, name",
        )
        .unwrap();
        match stmt {
            SqlStatement::Mutation { graphql, .. } => {
                assert!(graphql.contains("{ _docID name }"));
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn insert_without_columns_fails() {
        let result = sql_to_graphql("INSERT INTO User VALUES ('Alice', 30)");
        assert!(result.is_err());
    }

    // ── UPDATE tests ──

    #[test]
    fn update_with_where() {
        let stmt =
            sql_to_graphql("UPDATE User SET age = 31, name = 'Bob' WHERE name = 'Alice'").unwrap();
        match stmt {
            SqlStatement::Mutation {
                graphql,
                table_name,
                mutation_name,
                kind,
            } => {
                assert_eq!(kind, MutationKind::Update);
                assert_eq!(table_name, "User");
                assert_eq!(mutation_name, "update_User");
                assert_eq!(
                    graphql,
                    "mutation { update_User(filter: {name: {_eq: \"Alice\"}}, input: {age: 31, name: \"Bob\"}) { _docID } }"
                );
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn update_with_docid() {
        let stmt = sql_to_graphql("UPDATE User SET age = 31 WHERE _docID = 'bae-abc123'").unwrap();
        match stmt {
            SqlStatement::Mutation { graphql, kind, .. } => {
                assert_eq!(kind, MutationKind::Update);
                assert!(graphql.contains("docID: \"bae-abc123\""));
                assert!(graphql.contains("input: {age: 31}"));
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn update_without_where() {
        let stmt = sql_to_graphql("UPDATE User SET age = 0").unwrap();
        match stmt {
            SqlStatement::Mutation { graphql, kind, .. } => {
                assert_eq!(kind, MutationKind::Update);
                assert_eq!(
                    graphql,
                    "mutation { update_User(input: {age: 0}) { _docID } }"
                );
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn update_arithmetic_rejected() {
        let result = sql_to_graphql("UPDATE User SET age = age + 1 WHERE name = 'Alice'");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("arithmetic"));
    }

    // ── DELETE tests ──

    #[test]
    fn delete_with_where() {
        let stmt = sql_to_graphql("DELETE FROM User WHERE name = 'Alice'").unwrap();
        match stmt {
            SqlStatement::Mutation {
                graphql,
                table_name,
                mutation_name,
                kind,
            } => {
                assert_eq!(kind, MutationKind::Delete);
                assert_eq!(table_name, "User");
                assert_eq!(mutation_name, "delete_User");
                assert_eq!(
                    graphql,
                    "mutation { delete_User(filter: {name: {_eq: \"Alice\"}}) { _docID } }"
                );
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn delete_with_docid() {
        let stmt = sql_to_graphql("DELETE FROM User WHERE _docID = 'bae-abc123'").unwrap();
        match stmt {
            SqlStatement::Mutation { graphql, kind, .. } => {
                assert_eq!(kind, MutationKind::Delete);
                assert!(graphql.contains("docID: \"bae-abc123\""));
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn delete_without_where() {
        let stmt = sql_to_graphql("DELETE FROM User").unwrap();
        match stmt {
            SqlStatement::Mutation { graphql, kind, .. } => {
                assert_eq!(kind, MutationKind::Delete);
                assert_eq!(graphql, "mutation { delete_User { _docID } }");
            }
            _ => panic!("expected Mutation"),
        }
    }

    // ── Transaction tests ──

    #[test]
    fn begin_commit_rollback() {
        assert_eq!(sql_to_graphql("BEGIN").unwrap(), SqlStatement::Begin);
        assert_eq!(
            sql_to_graphql("START TRANSACTION").unwrap(),
            SqlStatement::Begin
        );
        assert_eq!(sql_to_graphql("COMMIT").unwrap(), SqlStatement::Commit);
        assert_eq!(sql_to_graphql("ROLLBACK").unwrap(), SqlStatement::Rollback);
    }

    // ── Parameter substitution tests ──

    #[test]
    fn substitute_string_param() {
        let sql = "SELECT * FROM users WHERE name = $1";
        let result = substitute_params(sql, &[Some("Alice".into())]);
        assert_eq!(result, "SELECT * FROM users WHERE name = 'Alice'");
    }

    #[test]
    fn substitute_numeric_param() {
        let sql = "SELECT * FROM users WHERE age > $1";
        let result = substitute_params(sql, &[Some("25".into())]);
        assert_eq!(result, "SELECT * FROM users WHERE age > 25");
    }

    #[test]
    fn substitute_null_param() {
        let sql = "INSERT INTO users (name) VALUES ($1)";
        let result = substitute_params(sql, &[None]);
        assert_eq!(result, "INSERT INTO users (name) VALUES (NULL)");
    }

    #[test]
    fn substitute_multiple_params() {
        let sql = "INSERT INTO users (name, age) VALUES ($1, $2)";
        let result = substitute_params(sql, &[Some("Bob".into()), Some("30".into())]);
        assert_eq!(result, "INSERT INTO users (name, age) VALUES ('Bob', 30)");
    }

    #[test]
    fn substitute_escapes_quotes() {
        let sql = "SELECT * FROM users WHERE name = $1";
        let result = substitute_params(sql, &[Some("O'Brien".into())]);
        assert_eq!(result, "SELECT * FROM users WHERE name = 'O''Brien'");
    }

    #[test]
    fn substitute_no_params() {
        let sql = "SELECT * FROM users";
        let result = substitute_params(sql, &[]);
        assert_eq!(result, "SELECT * FROM users");
    }

    #[test]
    fn substitute_boolean_param() {
        let sql = "SELECT * FROM users WHERE active = $1";
        let result = substitute_params(sql, &[Some("true".into())]);
        assert_eq!(result, "SELECT * FROM users WHERE active = true");
    }

    #[test]
    fn count_params_basic() {
        assert_eq!(
            count_params("SELECT * FROM users WHERE name = $1 AND age > $2"),
            2
        );
        assert_eq!(count_params("SELECT * FROM users"), 0);
        assert_eq!(
            count_params("INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"),
            3
        );
    }

    #[test]
    fn extract_table_from_select() {
        assert_eq!(
            extract_table_from_sql("SELECT name FROM users WHERE id = 1"),
            Some("users".into())
        );
    }

    #[test]
    fn extract_table_from_insert() {
        assert_eq!(
            extract_table_from_sql("INSERT INTO users (name) VALUES ('a')"),
            Some("users".into())
        );
    }

    #[test]
    fn extract_table_from_begin() {
        assert_eq!(extract_table_from_sql("BEGIN"), None);
    }

    // ── Schema-aware type coercion tests ──

    #[test]
    fn insert_coerces_number_to_string_for_string_field() {
        let mut types = FieldTypeMap::new();
        types.insert("version".to_string(), ScalarKind::String);
        types.insert("count".to_string(), ScalarKind::Int);

        let stmt = sql_to_graphql_typed(
            "INSERT INTO session (version, count) VALUES (2, 42)",
            Some(&types),
        )
        .unwrap();

        match stmt {
            SqlStatement::Mutation { graphql, .. } => {
                // version should be quoted (String field), count should be bare (Int field)
                assert!(
                    graphql.contains("version: \"2\""),
                    "expected version: \"2\", got: {}",
                    graphql
                );
                assert!(
                    graphql.contains("count: 42"),
                    "expected count: 42, got: {}",
                    graphql
                );
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn insert_without_types_preserves_numbers() {
        let stmt = sql_to_graphql_typed("INSERT INTO session (version) VALUES (2)", None).unwrap();

        match stmt {
            SqlStatement::Mutation { graphql, .. } => {
                assert!(
                    graphql.contains("version: 2"),
                    "expected bare version: 2, got: {}",
                    graphql
                );
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn update_coerces_number_to_string_for_string_field() {
        let mut types = FieldTypeMap::new();
        types.insert("version".to_string(), ScalarKind::String);

        let stmt = sql_to_graphql_typed(
            "UPDATE session SET version = 3 WHERE id = 'abc'",
            Some(&types),
        )
        .unwrap();

        match stmt {
            SqlStatement::Mutation { graphql, .. } => {
                assert!(
                    graphql.contains("version: \"3\""),
                    "expected version: \"3\", got: {}",
                    graphql
                );
            }
            _ => panic!("expected Mutation"),
        }
    }
}
