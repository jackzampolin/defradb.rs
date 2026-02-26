use sqlparser::ast::{Expr, JoinConstraint, JoinOperator, OrderByKind, SelectItem, TableFactor};

use crate::error::PgCompatError;

use super::expr::{expr_to_field_name, translate_limit_expr, translate_order, translate_where};
use super::{extract_table_name, JoinClause, JoinType, SqlStatement};

pub(super) fn has_joins(select: &sqlparser::ast::Select) -> bool {
    !select.from.is_empty() && !select.from[0].joins.is_empty()
}

pub(super) fn translate_join(
    select: &sqlparser::ast::Select,
    query: &sqlparser::ast::Query,
) -> Result<SqlStatement, PgCompatError> {
    let primary_table = extract_table_name(&select.from[0].relation)?;
    let primary_alias = extract_alias(&select.from[0].relation).unwrap_or(primary_table.clone());

    let mut joins = Vec::new();
    for j in &select.from[0].joins {
        joins.push(parse_join_clause(j)?);
    }

    let mut all_select_columns: Vec<(String, String, String)> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let (tbl, col) = expr_to_qualified_name(expr, &primary_alias)?;
                all_select_columns.push((tbl, col.clone(), col));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let (tbl, col) = expr_to_qualified_name(expr, &primary_alias)?;
                all_select_columns.push((tbl, col, alias.value.clone()));
            }
            SelectItem::Wildcard(_) => {
                all_select_columns.push((primary_alias.clone(), "*".to_string(), "*".to_string()));
                for j in &joins {
                    all_select_columns.push((
                        j.table_name.clone(),
                        "*".to_string(),
                        "*".to_string(),
                    ));
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let tbl_name = match kind {
                    sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => name
                        .0
                        .last()
                        .and_then(|p| p.as_ident())
                        .map(|i| i.value.clone())
                        .unwrap_or_default(),
                    _ => String::new(),
                };
                all_select_columns.push((tbl_name, "*".to_string(), "*".to_string()));
            }
        }
    }

    let filter = select.selection.as_ref().map(translate_where).transpose()?;

    let order = if let Some(ref order_by) = query.order_by {
        match order_by {
            sqlparser::ast::OrderBy {
                kind: OrderByKind::Expressions(exprs),
                ..
            } if !exprs.is_empty() => Some(translate_order(exprs)?),
            _ => None,
        }
    } else {
        None
    };

    let limit = query.limit.as_ref().map(translate_limit_expr).transpose()?;

    let offset = query
        .offset
        .as_ref()
        .map(|o| translate_limit_expr(&o.value))
        .transpose()?;

    Ok(SqlStatement::Join {
        primary_table,
        joins,
        filter,
        order,
        limit,
        offset,
        all_select_columns,
    })
}

fn parse_join_clause(join: &sqlparser::ast::Join) -> Result<JoinClause, PgCompatError> {
    let table_name = match &join.relation {
        TableFactor::Table { name, .. } => name
            .0
            .iter()
            .rev()
            .find_map(|p| p.as_ident().map(|i| i.value.clone()))
            .unwrap_or_default(),
        _ => {
            return Err(PgCompatError::UnsupportedSql(
                "only simple table references in JOIN".into(),
            ))
        }
    };

    let join_type = match &join.join_operator {
        JoinOperator::Inner(constraint) => (JoinType::Inner, constraint),
        JoinOperator::LeftOuter(constraint) | JoinOperator::Left(constraint) => {
            (JoinType::Left, constraint)
        }
        other => {
            return Err(PgCompatError::UnsupportedSql(format!(
                "unsupported JOIN type: {:?}",
                other
            )))
        }
    };

    let (left_col, right_col) = parse_join_on(join_type.1)?;

    Ok(JoinClause {
        table_name,
        join_type: join_type.0,
        left_col,
        right_col,
    })
}

fn parse_join_on(constraint: &JoinConstraint) -> Result<(String, String), PgCompatError> {
    match constraint {
        JoinConstraint::On(expr) => match expr {
            Expr::BinaryOp { left, right, .. } => {
                let l = expr_to_field_name(left)?;
                let r = expr_to_field_name(right)?;
                Ok((l, r))
            }
            _ => Err(PgCompatError::UnsupportedSql(
                "only simple ON conditions supported".into(),
            )),
        },
        _ => Err(PgCompatError::UnsupportedSql(
            "only ON clause supported in JOIN".into(),
        )),
    }
}

fn expr_to_qualified_name(
    expr: &Expr,
    default_table: &str,
) -> Result<(String, String), PgCompatError> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table = parts[parts.len() - 2].value.clone();
            let col = parts[parts.len() - 1].value.clone();
            Ok((table, col))
        }
        Expr::Identifier(ident) => Ok((default_table.to_string(), ident.value.clone())),
        _ => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported JOIN select expression: {}",
            expr
        ))),
    }
}

fn extract_alias(table: &TableFactor) -> Option<String> {
    match table {
        TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        _ => None,
    }
}
