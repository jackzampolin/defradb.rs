use pgwire::api::results::Response;
use pgwire::error::{PgWireError, PgWireResult};

use crate::encode;

use super::{extract_regclass_table, DefraQueryHandler};

impl DefraQueryHandler {
    pub(super) async fn handle_system_catalog(&self, sql: &str) -> PgWireResult<Response> {
        let lower = sql.to_lowercase();

        if lower.contains("current_schema") && !lower.contains("information_schema") {
            return encode::encode_single_value_response("current_schema", "public");
        }

        if lower.contains("information_schema.tables") && !lower.contains("table_constraints") {
            return self.handle_info_schema_tables().await;
        }

        if lower.contains("information_schema.columns") {
            return self.handle_info_schema_columns().await;
        }

        if lower.contains("pg_indexes") {
            return self.handle_pg_indexes().await;
        }

        if lower.contains("table_constraints") && lower.contains("constraint_column_usage") {
            return self.handle_fk_constraints().await;
        }

        if lower.contains("pg_index") && lower.contains("pg_attribute") {
            return self.handle_pk_columns(sql).await;
        }

        if lower.contains("pg_type") && !lower.contains("pg_enum") {
            return Ok(encode::encode_pg_types());
        }

        Ok(encode::encode_empty_select_with_columns(sql))
    }

    async fn handle_info_schema_tables(&self) -> PgWireResult<Response> {
        let names = self
            .collections
            .list_collections()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let rows: Vec<Vec<(String, String)>> = names
            .into_iter()
            .map(|name| {
                vec![
                    ("table_schema".to_string(), "public".to_string()),
                    ("table_name".to_string(), name),
                    ("table_type".to_string(), "BASE TABLE".to_string()),
                ]
            })
            .collect();

        encode::encode_text_rows(&rows)
    }

    async fn handle_info_schema_columns(&self) -> PgWireResult<Response> {
        let names = self
            .collections
            .list_collections()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let mut rows = Vec::new();
        for name in &names {
            if let Ok(Some(col)) = self.collections.get_collection(name).await {
                for (pos, field) in col.fields.iter().enumerate() {
                    if !field.kind.is_scalar() {
                        continue;
                    }
                    rows.push(vec![
                        ("table_schema".to_string(), "public".to_string()),
                        ("table_name".to_string(), name.clone()),
                        ("column_name".to_string(), field.name.clone()),
                        ("ordinal_position".to_string(), (pos + 1).to_string()),
                        (
                            "data_type".to_string(),
                            encode::field_kind_to_pg_type_name(&field.kind),
                        ),
                        ("is_nullable".to_string(), "YES".to_string()),
                    ]);
                }
            }
        }

        encode::encode_text_rows(&rows)
    }

    async fn handle_pg_indexes(&self) -> PgWireResult<Response> {
        let meta = self.ddl_metadata.read().await;
        let rows: Vec<Vec<(String, String)>> = meta
            .indexes
            .iter()
            .map(|idx| {
                vec![
                    ("schemaname".to_string(), "public".to_string()),
                    ("tablename".to_string(), idx.table_name.clone()),
                    ("indexname".to_string(), idx.index_name.clone()),
                ]
            })
            .collect();
        encode::encode_text_rows(&rows)
    }

    async fn handle_fk_constraints(&self) -> PgWireResult<Response> {
        let meta = self.ddl_metadata.read().await;
        let rows: Vec<Vec<(String, String)>> = meta
            .foreign_keys
            .iter()
            .map(|fk| {
                vec![
                    ("table_name".to_string(), fk.from_table.clone()),
                    ("constraint_name".to_string(), fk.constraint_name.clone()),
                    ("foreign_table_name".to_string(), fk.to_table.clone()),
                ]
            })
            .collect();
        encode::encode_text_rows(&rows)
    }

    async fn handle_pk_columns(&self, sql: &str) -> PgWireResult<Response> {
        let table_name = extract_regclass_table(sql).unwrap_or_default();
        let meta = self.ddl_metadata.read().await;
        let rows: Vec<Vec<(String, String)>> = meta
            .primary_key_for(&table_name)
            .map(|pk| {
                pk.columns
                    .iter()
                    .map(|col| vec![("attname".to_string(), col.clone())])
                    .collect()
            })
            .unwrap_or_default();
        encode::encode_text_rows(&rows)
    }
}
