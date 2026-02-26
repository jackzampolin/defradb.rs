use sqlparser::ast::SetExpr;

use crate::error::PgCompatError;

use super::{SetOp, SqlStatement};

pub(super) fn translate_set_operation(
    body: &SetExpr,
    query: &sqlparser::ast::Query,
) -> Result<SqlStatement, PgCompatError> {
    match body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let set_op = match op {
                sqlparser::ast::SetOperator::Union => {
                    if matches!(
                        set_quantifier,
                        sqlparser::ast::SetQuantifier::All
                            | sqlparser::ast::SetQuantifier::AllByName
                    ) {
                        SetOp::UnionAll
                    } else {
                        SetOp::Union
                    }
                }
                sqlparser::ast::SetOperator::Intersect => SetOp::Intersect,
                sqlparser::ast::SetOperator::Except => SetOp::Except,
                _ => {
                    return Err(PgCompatError::UnsupportedSql(
                        "unsupported set operator".into(),
                    ))
                }
            };

            let left_query = wrap_in_query(left, query);
            let right_query = wrap_in_query(right, query);

            Ok(SqlStatement::SetOperation {
                left_query: Box::new(left_query),
                right_query: Box::new(right_query),
                op: set_op,
            })
        }
        _ => Err(PgCompatError::UnsupportedSql(
            "expected set operation".into(),
        )),
    }
}

fn wrap_in_query(set_expr: &SetExpr, _parent: &sqlparser::ast::Query) -> sqlparser::ast::Query {
    sqlparser::ast::Query {
        with: None,
        body: Box::new(set_expr.clone()),
        order_by: None,
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    }
}
