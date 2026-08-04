use schema::ScalarKind;
use sqlparser::ast::{BinaryOperator, Expr, OrderByExpr, SelectItem, Value};

use crate::error::PgCompatError;

use super::SqlStatement;

pub(crate) fn translate_where(expr: &Expr) -> Result<String, PgCompatError> {
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
        Expr::ILike {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            let field = expr_to_field_name(inner)?;
            let pat = expr_to_graphql_value(pattern)?;
            let op = if *negated { "_nilike" } else { "_ilike" };
            Ok(format!("{}: {{{}: {}}}", field, op, pat))
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let field = expr_to_field_name(inner)?;
            let low_val = expr_to_graphql_value(low)?;
            let high_val = expr_to_graphql_value(high)?;
            if *negated {
                Ok(format!(
                    "_or: [{{{}: {{_lt: {}}}}}, {{{}: {{_gt: {}}}}}]",
                    field, low_val, field, high_val
                ))
            } else {
                Ok(format!(
                    "_and: [{{{}: {{_ge: {}}}}}, {{{}: {{_le: {}}}}}]",
                    field, low_val, field, high_val
                ))
            }
        }
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr: inner,
        } => {
            let inner_filter = translate_where(inner)?;
            Ok(format!("_not: {{{}}}", inner_filter))
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

pub(crate) fn expr_to_field_name(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .ok_or_else(|| PgCompatError::UnsupportedSql("empty compound identifier".into())),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "expected column name, got: {}",
            expr
        ))),
    }
}

pub(crate) fn expr_to_graphql_value(expr: &Expr) -> Result<String, PgCompatError> {
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

pub(crate) fn value_to_graphql(value: &Value) -> Result<String, PgCompatError> {
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
/// postgres.js sends all params as text and `substitute_params` treats
/// numeric-looking values as bare numbers.
pub(crate) fn typed_graphql_value(
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
    let is_string_field = type_hint.is_some_and(|kind| {
        matches!(
            kind.base_kind(),
            ScalarKind::String | ScalarKind::DocID | ScalarKind::None | ScalarKind::DateTime
        )
    });

    match value {
        Value::Number(n, _) if is_string_field => Ok(format!("\"{}\"", n)),
        _ => value_to_graphql(value),
    }
}

pub(crate) fn translate_order(exprs: &[OrderByExpr]) -> Result<String, PgCompatError> {
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

pub(crate) fn translate_limit_expr(expr: &Expr) -> Result<String, PgCompatError> {
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

pub(crate) fn translate_projection(items: &[SelectItem]) -> Result<String, PgCompatError> {
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

pub(crate) fn translate_synthetic_query(
    items: &[SelectItem],
) -> Result<SqlStatement, PgCompatError> {
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
