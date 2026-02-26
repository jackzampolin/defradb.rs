use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, FromTable, OnConflictAction, OnInsert, OrderByKind,
    SelectItem, SetExpr, TableObject, Value,
};

use crate::error::PgCompatError;

use super::aggregate;
use super::expr::{
    expr_to_field_name, expr_to_graphql_value, translate_limit_expr, translate_order,
    translate_projection, translate_synthetic_query, translate_where, typed_graphql_value,
};
use super::join;
use super::set_ops;
use super::{extract_table_name, object_name_to_string, FieldTypeMap, MutationKind, SqlStatement};

pub(crate) fn translate_query(
    query: &sqlparser::ast::Query,
) -> Result<SqlStatement, PgCompatError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { .. } => {
            return set_ops::translate_set_operation(query.body.as_ref(), query);
        }
        SetExpr::Query(inner) => {
            return translate_query(inner);
        }
        _ => {
            return Err(PgCompatError::UnsupportedSql(
                "only simple SELECT statements are supported".into(),
            ))
        }
    };

    if select.from.is_empty() {
        return translate_synthetic_query(&select.projection);
    }

    // Check for aggregates in projection
    if aggregate::is_aggregate_query(select) {
        if aggregate::has_group_by(select) {
            return aggregate::translate_group_by(select, query);
        }
        return aggregate::translate_aggregate(select, query);
    }

    // Check for JOINs
    if join::has_joins(select) {
        return join::translate_join(select, query);
    }

    // Check for DISTINCT
    let is_distinct = select.distinct.is_some();

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

    let stmt = SqlStatement::Query(format!(
        "query {{ {}{} {{ {} }} }}",
        table_name, args_str, fields
    ));

    if is_distinct {
        return Ok(SqlStatement::Distinct {
            inner: Box::new(stmt),
        });
    }

    Ok(stmt)
}

pub(super) fn translate_insert(
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
            return Ok(SqlStatement::Mutation {
                graphql: insert_graphql,
                table_name: table_name.to_string(),
                mutation_name,
                kind: MutationKind::Insert,
            });
        }
    };

    let conflict_cols: Vec<String> = match &conflict.conflict_target {
        Some(sqlparser::ast::ConflictTarget::Columns(cols)) => {
            cols.iter().map(|c| c.value.clone()).collect()
        }
        _ => vec![],
    };

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

pub(super) fn translate_update(
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

pub(super) fn translate_delete(
    delete: &sqlparser::ast::Delete,
) -> Result<SqlStatement, PgCompatError> {
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
