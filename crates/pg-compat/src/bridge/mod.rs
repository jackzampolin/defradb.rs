mod ddl;
mod dml;
mod expr;
mod params;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

use schema::ScalarKind;
use sqlparser::ast::{ObjectName, Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::PgCompatError;
use crate::metadata::ParsedForeignKey;

pub use params::{
    count_params, escape_graphql_string, extract_table_from_sql, is_select_or_returning,
    is_system_catalog_query, is_transaction_control, substitute_params,
};

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
    CreateTable {
        sdl: String,
        table_name: String,
        primary_key_columns: Vec<String>,
        inline_foreign_keys: Vec<ParsedForeignKey>,
    },
    DropTable,
    CreateIndex {
        index_name: Option<String>,
        table_name: String,
        columns: Vec<String>,
    },
    AlterTable {
        table_name: String,
        foreign_keys: Vec<ParsedForeignKey>,
    },
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
/// are quoted as GraphQL strings instead of bare numbers.
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
        Statement::Query(query) => dml::translate_query(query),
        Statement::Insert(insert) => dml::translate_insert(insert, field_types),
        Statement::Update {
            table,
            assignments,
            selection,
            returning,
            ..
        } => dml::translate_update(
            table,
            assignments,
            selection.as_ref(),
            returning.as_deref(),
            field_types,
        ),
        Statement::Delete(delete) => dml::translate_delete(delete),
        Statement::StartTransaction { .. } => Ok(SqlStatement::Begin),
        Statement::Commit { .. } => Ok(SqlStatement::Commit),
        Statement::Rollback { .. } => Ok(SqlStatement::Rollback),
        Statement::CreateTable(ct) => ddl::translate_create_table(ct),
        Statement::Drop { .. } => Ok(SqlStatement::DropTable),
        Statement::CreateIndex(ci) => ddl::translate_create_index(ci),
        Statement::AlterTable {
            name, operations, ..
        } => ddl::translate_alter_table(name, operations),
        other => Err(PgCompatError::UnsupportedSql(format!(
            "unsupported statement: {}",
            statement_kind(other)
        ))),
    }
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

// ── Shared helpers used across bridge submodules ──

pub(crate) fn extract_table_name(table: &TableFactor) -> Result<String, PgCompatError> {
    match table {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(PgCompatError::UnsupportedSql(
            "only simple table references are supported".into(),
        )),
    }
}

pub(crate) fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) fn table_name_only(name: &ObjectName) -> String {
    name.0
        .iter()
        .rev()
        .find_map(|p| p.as_ident().map(|i| i.value.clone()))
        .unwrap_or_default()
}
