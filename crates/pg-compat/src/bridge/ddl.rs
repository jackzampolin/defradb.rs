use sqlparser::ast::{
    AlterTableOperation, ColumnDef, ColumnOption, DataType, Expr, ObjectName, ReferentialAction,
    TableConstraint,
};

use crate::error::PgCompatError;
use crate::metadata::ParsedForeignKey;

use super::{object_name_to_string, table_name_only, SqlStatement};

pub(super) fn translate_create_table(
    ct: &sqlparser::ast::CreateTable,
) -> Result<SqlStatement, PgCompatError> {
    let table_name = object_name_to_string(&ct.name);

    let mut fields = Vec::new();
    let mut pk_columns = Vec::new();
    let mut inline_fks = Vec::new();

    for col in &ct.columns {
        if let Some(sdl_field) = column_to_sdl_field(col) {
            fields.push(sdl_field);
        }

        for opt_def in &col.options {
            match &opt_def.option {
                ColumnOption::Unique {
                    is_primary: true, ..
                } => {
                    pk_columns.push(col.name.value.clone());
                }
                ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    ..
                } => {
                    let to_table = table_name_only(foreign_table);
                    let to_column = referred_columns
                        .first()
                        .map(|c| c.value.clone())
                        .unwrap_or_else(|| "id".to_string());
                    let cascade = matches!(on_delete, Some(ReferentialAction::Cascade));
                    let constraint_name =
                        format!("{}_{}_{}_fk", table_name, col.name.value, to_table);
                    inline_fks.push(ParsedForeignKey {
                        constraint_name,
                        from_column: col.name.value.clone(),
                        to_table,
                        to_column,
                        on_delete_cascade: cascade,
                    });
                }
                _ => {}
            }
        }
    }

    for constraint in &ct.constraints {
        match constraint {
            TableConstraint::PrimaryKey { columns, .. } => {
                for col in columns {
                    let name = col.value.clone();
                    if !pk_columns.contains(&name) {
                        pk_columns.push(name);
                    }
                }
            }
            TableConstraint::ForeignKey {
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                name,
                ..
            } => {
                if let Some(from_col) = columns.first() {
                    let to_table = table_name_only(foreign_table);
                    let to_column = referred_columns
                        .first()
                        .map(|c| c.value.clone())
                        .unwrap_or_else(|| "id".to_string());
                    let cascade = matches!(on_delete, Some(ReferentialAction::Cascade));
                    let constraint_name =
                        name.as_ref().map(|n| n.value.clone()).unwrap_or_else(|| {
                            format!("{}_{}_{}_fk", table_name, from_col.value, to_table)
                        });
                    inline_fks.push(ParsedForeignKey {
                        constraint_name,
                        from_column: from_col.value.clone(),
                        to_table,
                        to_column,
                        on_delete_cascade: cascade,
                    });
                }
            }
            _ => {}
        }
    }

    if fields.is_empty() {
        return Err(PgCompatError::UnsupportedSql(
            "CREATE TABLE requires at least one column".into(),
        ));
    }

    let sdl = format!("type {} {{\n  {}\n}}", table_name, fields.join("\n  "));
    Ok(SqlStatement::CreateTable {
        sdl,
        table_name,
        primary_key_columns: pk_columns,
        inline_foreign_keys: inline_fks,
    })
}

pub(super) fn translate_create_index(
    ci: &sqlparser::ast::CreateIndex,
) -> Result<SqlStatement, PgCompatError> {
    let index_name = ci.name.as_ref().map(object_name_to_string);
    let table_name = object_name_to_string(&ci.table_name);
    let columns: Vec<String> = ci
        .columns
        .iter()
        .map(|c| match &c.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => format!("{}", c.expr),
        })
        .collect();
    Ok(SqlStatement::CreateIndex {
        index_name,
        table_name,
        columns,
    })
}

pub(super) fn translate_alter_table(
    name: &ObjectName,
    operations: &[AlterTableOperation],
) -> Result<SqlStatement, PgCompatError> {
    let table_name = table_name_only(name);
    let mut foreign_keys = Vec::new();

    for op in operations {
        if let AlterTableOperation::AddConstraint(TableConstraint::ForeignKey {
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            name: constraint_name_opt,
            ..
        }) = op
        {
            if let Some(from_col) = columns.first() {
                let to_table = table_name_only(foreign_table);
                let to_column = referred_columns
                    .first()
                    .map(|c| c.value.clone())
                    .unwrap_or_else(|| "id".to_string());
                let cascade = matches!(on_delete, Some(ReferentialAction::Cascade));
                let cname = constraint_name_opt
                    .as_ref()
                    .map(|n| n.value.clone())
                    .unwrap_or_else(|| {
                        format!("{}_{}_{}_fk", table_name, from_col.value, to_table)
                    });
                foreign_keys.push(ParsedForeignKey {
                    constraint_name: cname,
                    from_column: from_col.value.clone(),
                    to_table,
                    to_column,
                    on_delete_cascade: cascade,
                });
            }
        }
    }

    Ok(SqlStatement::AlterTable {
        table_name,
        foreign_keys,
    })
}

fn column_to_sdl_field(col: &ColumnDef) -> Option<String> {
    let name = &col.name.value;
    let defra_type = sql_type_to_defra(&col.data_type)?;
    Some(format!("{}: {}", name, defra_type))
}

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
