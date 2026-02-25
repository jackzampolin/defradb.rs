use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, OrderByExpr, OrderByKind, SelectItem, SetExpr, Statement,
    TableFactor, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::PgCompatError;

/// Parse a SQL string and translate it to a GraphQL query string.
pub fn sql_to_graphql(sql: &str) -> Result<String, PgCompatError> {
    let dialect = PostgreSqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| PgCompatError::SqlParse(e.to_string()))?;

    if statements.is_empty() {
        return Err(PgCompatError::SqlParse("empty query".into()));
    }

    match &statements[0] {
        Statement::Query(query) => translate_query(query),
        other => Err(PgCompatError::UnsupportedSql(format!(
            "only SELECT queries are supported, got: {}",
            statement_kind(other)
        ))),
    }
}

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert { .. } => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        _ => "unsupported statement",
    }
}

fn translate_query(query: &sqlparser::ast::Query) -> Result<String, PgCompatError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(PgCompatError::UnsupportedSql(
                "only simple SELECT statements are supported".into(),
            ))
        }
    };

    // Extract table name
    if select.from.len() != 1 {
        return Err(PgCompatError::UnsupportedSql(
            "exactly one FROM table is required".into(),
        ));
    }
    let table_name = extract_table_name(&select.from[0].relation)?;

    // Extract selected fields
    let fields = translate_projection(&select.projection)?;

    // Build GraphQL arguments
    let mut args = Vec::new();

    // WHERE → filter
    if let Some(ref selection) = select.selection {
        let filter = translate_where(selection)?;
        args.push(format!("filter: {{{}}}", filter));
    }

    // ORDER BY
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
                    args.push(format!("order: {{{}}}", order));
                }
            }
        }
    }

    // LIMIT
    if let Some(ref limit_expr) = query.limit {
        let limit = translate_limit_expr(limit_expr)?;
        args.push(format!("limit: {}", limit));
    }

    // OFFSET
    if let Some(ref offset) = query.offset {
        let off = translate_limit_expr(&offset.value)?;
        args.push(format!("offset: {}", off));
    }

    // Build the GraphQL query
    let args_str = if args.is_empty() {
        String::new()
    } else {
        format!("({})", args.join(", "))
    };

    Ok(format!(
        "query {{ {}{} {{ {} }} }}",
        table_name, args_str, fields
    ))
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
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                fields.push(ident.value.clone());
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
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported WHERE expression: {}",
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
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported value expression: {}",
            expr
        ))),
    }
}

fn value_to_graphql(value: &Value) -> Result<String, PgCompatError> {
    match value {
        Value::Number(n, _) => Ok(n.clone()),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(format!("\"{}\"", s)),
        Value::Boolean(b) => Ok(b.to_string()),
        Value::Null => Ok("null".to_string()),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported value: {}",
            value
        ))),
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
        parts.push(format!("{}: {}", field, dir));
    }
    Ok(parts.join(", "))
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

    #[test]
    fn simple_select_all() {
        let gql = sql_to_graphql("SELECT name, age FROM User").unwrap();
        assert_eq!(gql, "query { User { name age } }");
    }

    #[test]
    fn select_with_where() {
        let gql = sql_to_graphql("SELECT name FROM User WHERE age > 25").unwrap();
        assert_eq!(gql, "query { User(filter: {age: {_gt: 25}}) { name } }");
    }

    #[test]
    fn select_with_order() {
        let gql = sql_to_graphql("SELECT name FROM User ORDER BY name").unwrap();
        assert_eq!(gql, "query { User(order: {name: ASC}) { name } }");
    }

    #[test]
    fn select_with_limit_offset() {
        let gql = sql_to_graphql("SELECT name FROM User LIMIT 10 OFFSET 5").unwrap();
        assert_eq!(gql, "query { User(limit: 10, offset: 5) { name } }");
    }

    #[test]
    fn select_with_string_where() {
        let gql = sql_to_graphql("SELECT name FROM User WHERE name = 'Alice'").unwrap();
        assert_eq!(
            gql,
            "query { User(filter: {name: {_eq: \"Alice\"}}) { name } }"
        );
    }

    #[test]
    fn select_with_and() {
        let gql =
            sql_to_graphql("SELECT name FROM User WHERE age > 25 AND name = 'Alice'").unwrap();
        assert!(gql.contains("_and"));
        assert!(gql.contains("_gt: 25"));
        assert!(gql.contains("_eq: \"Alice\""));
    }

    #[test]
    fn rejects_insert() {
        let result = sql_to_graphql("INSERT INTO User (name) VALUES ('Alice')");
        assert!(result.is_err());
    }
}
