use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use schema::CollectionVersion;
use serde_json::Value as JsonValue;

use crate::types::field_kind_to_pg_type;

/// Build a PG response from a GraphQL JSON result and collection schema.
///
/// `data` is the JSON array of documents returned by QueryExecutor.
/// `collection` provides field metadata for type-correct encoding.
/// `requested_fields` constrains which columns to include. If it contains "*",
/// all scalar fields from the collection are included.
pub fn encode_response(
    data: &[JsonValue],
    collection: &CollectionVersion,
    requested_fields: &[String],
) -> PgWireResult<Response> {
    let fields = resolve_fields(collection, requested_fields);
    let field_infos = build_field_infos(&fields);
    let schema = Arc::new(field_infos);

    let mut rows = Vec::with_capacity(data.len());
    for doc in data {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (name, pg_type) in &fields {
            let value = doc.get(name);
            encode_value(&mut encoder, value, pg_type)?;
        }
        rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    )))
}

/// Build a response for an empty result set (table not found or no data).
pub fn encode_empty_response(message: &str) -> Response {
    Response::Execution(Tag::new(message))
}

/// Resolve which fields to include, returning (name, pg_type) pairs.
fn resolve_fields(collection: &CollectionVersion, requested: &[String]) -> Vec<(String, Type)> {
    let use_all = requested.is_empty() || requested.iter().any(|f| f == "*");

    if use_all {
        collection
            .fields
            .iter()
            .filter(|f| f.kind.is_scalar())
            .map(|f| (f.name.clone(), field_kind_to_pg_type(&f.kind)))
            .collect()
    } else {
        requested
            .iter()
            .filter_map(|name| {
                collection
                    .field_by_name(name)
                    .map(|f| (f.name.clone(), field_kind_to_pg_type(&f.kind)))
            })
            .collect()
    }
}

fn build_field_infos(fields: &[(String, Type)]) -> Vec<FieldInfo> {
    fields
        .iter()
        .map(|(name, pg_type)| {
            FieldInfo::new(name.clone(), None, None, pg_type.clone(), FieldFormat::Text)
        })
        .collect()
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    value: Option<&JsonValue>,
    pg_type: &Type,
) -> PgWireResult<()> {
    match value {
        None | Some(JsonValue::Null) => encoder.encode_field(&None::<&str>),
        Some(JsonValue::Bool(b)) => encoder.encode_field(b),
        Some(JsonValue::Number(n)) => encode_number(encoder, n, pg_type),
        Some(JsonValue::String(s)) => encoder.encode_field(&s.as_str()),
        Some(JsonValue::Array(_)) | Some(JsonValue::Object(_)) => {
            let s = serde_json::to_string(value.unwrap())
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encoder.encode_field(&s.as_str())
        }
    }
}

fn encode_number(
    encoder: &mut DataRowEncoder,
    n: &serde_json::Number,
    pg_type: &Type,
) -> PgWireResult<()> {
    match *pg_type {
        Type::INT8 => {
            let v = n.as_i64().unwrap_or(0);
            encoder.encode_field(&v)
        }
        Type::FLOAT4 => {
            let v = n.as_f64().unwrap_or(0.0) as f32;
            encoder.encode_field(&v)
        }
        Type::FLOAT8 => {
            let v = n.as_f64().unwrap_or(0.0);
            encoder.encode_field(&v)
        }
        _ => {
            let s = n.to_string();
            encoder.encode_field(&s.as_str())
        }
    }
}

/// Build FieldInfo metadata for all scalar fields in a collection.
///
/// Used by describe responses so clients know column types before execution.
pub fn build_field_infos_from_collection(collection: &CollectionVersion) -> Vec<FieldInfo> {
    let fields = resolve_fields(collection, &[]);
    build_field_infos(&fields)
}

/// Extract field names from a SELECT * or explicit column list.
pub fn extract_requested_fields(fields_str: &str) -> Vec<String> {
    fields_str
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}
