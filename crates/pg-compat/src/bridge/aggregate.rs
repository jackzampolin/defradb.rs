use sqlparser::ast::{DuplicateTreatment, Expr, FunctionArgExpr, GroupByExpr, SelectItem};

use crate::error::PgCompatError;

use super::expr::{expr_to_field_name, translate_where};
use super::{extract_table_name, AggFunc, AggregateExpr, SqlStatement};

pub(super) fn is_aggregate_query(select: &sqlparser::ast::Select) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            parse_aggregate_function(expr).is_some()
        }
        _ => false,
    })
}

pub(super) fn has_group_by(select: &sqlparser::ast::Select) -> bool {
    !matches!(select.group_by, GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty())
        && !matches!(select.group_by, GroupByExpr::All(_))
}

pub(super) fn translate_aggregate(
    select: &sqlparser::ast::Select,
    _query: &sqlparser::ast::Query,
) -> Result<SqlStatement, PgCompatError> {
    let table_name = if select.from.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "aggregate query requires FROM".into(),
        ));
    } else {
        extract_table_name(&select.from[0].relation)?
    };

    let mut aggregates = Vec::new();
    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::UnnamedExpr(expr) => (expr, None),
            _ => continue,
        };

        if let Some(mut agg) = parse_aggregate_function(expr) {
            if let Some(a) = alias {
                agg.alias = a;
            } else if agg.alias.is_empty() {
                agg.alias = format!("agg_{}", idx);
            }
            aggregates.push(agg);
        }
    }

    if aggregates.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "no aggregate functions found".into(),
        ));
    }

    let filter = select.selection.as_ref().map(translate_where).transpose()?;

    Ok(SqlStatement::Aggregate {
        table_name,
        aggregates,
        filter,
    })
}

pub(super) fn translate_group_by(
    select: &sqlparser::ast::Select,
    _query: &sqlparser::ast::Query,
) -> Result<SqlStatement, PgCompatError> {
    let table_name = if select.from.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "GROUP BY query requires FROM".into(),
        ));
    } else {
        extract_table_name(&select.from[0].relation)?
    };

    let group_columns = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            let mut cols = Vec::new();
            for expr in exprs {
                cols.push(expr_to_field_name(expr)?);
            }
            cols
        }
        _ => vec![],
    };

    let mut aggregates = Vec::new();
    let mut non_agg_columns = Vec::new();

    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::UnnamedExpr(expr) => (expr, None),
            _ => continue,
        };

        if let Some(mut agg) = parse_aggregate_function(expr) {
            if let Some(a) = alias {
                agg.alias = a;
            } else if agg.alias.is_empty() {
                agg.alias = format!("agg_{}", idx);
            }
            aggregates.push(agg);
        } else if let Ok(field) = expr_to_field_name(expr) {
            let col_alias = alias.unwrap_or_else(|| field.clone());
            non_agg_columns.push(col_alias);
        }
    }

    let filter = select.selection.as_ref().map(translate_where).transpose()?;

    let having_filter = select.having.as_ref().map(translate_having).transpose()?;

    Ok(SqlStatement::GroupBy {
        table_name,
        group_columns,
        aggregates,
        non_agg_columns,
        filter,
        having_filter,
    })
}

fn parse_aggregate_function(expr: &Expr) -> Option<AggregateExpr> {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            let agg_func = match name.as_str() {
                "count" => AggFunc::Count,
                "sum" => AggFunc::Sum,
                "avg" => AggFunc::Avg,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                _ => return None,
            };

            let (field, distinct) = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => {
                    let is_distinct = matches!(
                        arg_list.duplicate_treatment,
                        Some(DuplicateTreatment::Distinct)
                    );
                    let f = if arg_list.args.is_empty() {
                        None
                    } else {
                        match &arg_list.args[0] {
                            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
                            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                                expr_to_field_name(e).ok()
                            }
                            _ => None,
                        }
                    };
                    (f, is_distinct)
                }
                sqlparser::ast::FunctionArguments::None => (None, false),
                sqlparser::ast::FunctionArguments::Subquery(_) => (None, false),
            };

            let alias = match &agg_func {
                AggFunc::Count => "count".to_string(),
                AggFunc::Sum => format!("sum_{}", field.as_deref().unwrap_or("all")),
                AggFunc::Avg => format!("avg_{}", field.as_deref().unwrap_or("all")),
                AggFunc::Min => format!("min_{}", field.as_deref().unwrap_or("all")),
                AggFunc::Max => format!("max_{}", field.as_deref().unwrap_or("all")),
            };

            Some(AggregateExpr {
                func: agg_func,
                field,
                alias,
                distinct,
            })
        }
        _ => None,
    }
}

fn translate_having(expr: &Expr) -> Result<String, PgCompatError> {
    match expr {
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And,
            right,
        } => Ok(format!(
            "({}) AND ({})",
            translate_having(left)?,
            translate_having(right)?
        )),
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::Or,
            right,
        } => Ok(format!(
            "({}) OR ({})",
            translate_having(left)?,
            translate_having(right)?
        )),
        Expr::BinaryOp { left, op, right } => {
            let left_str = format!("{}", left);
            let right_str = format!("{}", right);
            let op_str = match op {
                sqlparser::ast::BinaryOperator::Gt => ">",
                sqlparser::ast::BinaryOperator::GtEq => ">=",
                sqlparser::ast::BinaryOperator::Lt => "<",
                sqlparser::ast::BinaryOperator::LtEq => "<=",
                sqlparser::ast::BinaryOperator::Eq => "==",
                sqlparser::ast::BinaryOperator::NotEq => "!=",
                _ => {
                    return Err(PgCompatError::UnsupportedSql(format!(
                        "unsupported HAVING operator: {}",
                        op
                    )))
                }
            };
            Ok(format!("{} {} {}", left_str, op_str, right_str))
        }
        Expr::Nested(inner) => translate_having(inner),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported HAVING expression: {}",
            expr
        ))),
    }
}
