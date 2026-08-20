//! Mutation parsing helpers
//!
//! Standalone functions for parsing GraphQL mutation operations:
//! - `parse_field_to_mutation()` - Parse a field into a Mutation
//! - `parse_create_input()` - Parse CREATE input
//! - `parse_update_input()` - Parse UPDATE input
//! - `parse_document_input()` - Parse document input object
//! - `parse_similarity_field()` - Parse _similarity field
//! - `parse_vector_value()` - Parse vector from GraphQL list
//! - `parse_json_vector()` - Parse vector from JSON array

use graphql_parser::query::{Field, Value};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

use crate::error::{QueryError, Result};
use crate::mapper::{parse_mutation_name, FullTextSearch, Mutation, MutationType, Similarity};

use super::filters::parse_filter_value;
use super::parser::{parse_selection_set, FragmentMap};
use super::values::{graphql_value_to_json, parse_doc_ids_value};

/// Parse a single GraphQL field into a Mutation operation.
///
/// Mutation field names follow the format: `operation_collection`
/// Examples: `create_Users`, `update_Posts`, `delete_Comments`
pub(super) fn parse_field_to_mutation(
    field: &Field<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Mutation> {
    let field_name = &field.name;

    // Parse mutation name to get operation type and collection
    let (mutation_type, collection_name) =
        parse_mutation_name(field_name).map_err(QueryError::parse)?;

    // Create base mutation
    let mut mutation = match mutation_type {
        MutationType::Create => Mutation::create(&collection_name),
        MutationType::Update => Mutation::update(&collection_name),
        MutationType::Delete => Mutation::delete(&collection_name),
        MutationType::Upsert => Mutation::upsert(&collection_name),
    };

    // Capture GraphQL alias if present (e.g., "john: update_Users(...)")
    if let Some(ref alias) = field.alias {
        mutation = mutation.with_alias(alias.clone());
    }

    // Track if input argument was present (even if null)
    let mut has_input_arg = false;

    // Parse arguments based on mutation type
    for (arg_name, arg_value) in &field.arguments {
        match (mutation_type, arg_name.as_str()) {
            // CREATE: input is array of documents (null means empty operation)
            (MutationType::Create, "input") => {
                has_input_arg = true;
                if !matches!(arg_value, Value::Null) {
                    let input = parse_create_input(arg_value, variables, &collection_name)?;
                    mutation.create_input = input;
                }
                // null input is valid - leaves create_input empty for empty result
            }

            // UPDATE: input is patch object (null means empty operation)
            (MutationType::Update, "input") => {
                has_input_arg = true;
                if !matches!(arg_value, Value::Null) {
                    let input = parse_update_input(arg_value, variables)?;
                    mutation.update_input = input;
                }
            }

            // UPSERT: add is the document to create if no match (single object, not array)
            // Go uses "add" as the argument name for upsert create input
            (MutationType::Upsert, "add") => {
                if matches!(arg_value, Value::Null) {
                    return Err(QueryError::parse(
                        "Argument \"add\" has invalid value <nil>.".to_string(),
                    ));
                }
                let input = parse_update_input(arg_value, variables)?;
                // Store create input as single-element array for consistency
                mutation.create_input = vec![input];
            }

            // UPSERT: update is the fields to update if match found
            (MutationType::Upsert, "update") => {
                if matches!(arg_value, Value::Null) {
                    return Err(QueryError::parse(
                        "Argument \"update\" has invalid value <nil>.".to_string(),
                    ));
                }
                let input = parse_update_input(arg_value, variables)?;
                mutation.update_input = input;
            }

            // UPDATE/DELETE: docID or docIDs to target (Go uses singular docID)
            (MutationType::Update | MutationType::Delete, "docID" | "docIDs" | "_docIDs") => {
                // Null docIDs is valid and means "no specific docIDs" (use filter or all)
                if !matches!(arg_value, Value::Null) {
                    let doc_ids = parse_doc_ids_value(arg_value, variables)?;
                    mutation.doc_ids = Some(doc_ids);
                }
            }

            // UPDATE/DELETE: filter to find documents
            (MutationType::Update | MutationType::Delete, "filter") => {
                // Null filter is valid and means "no filter" (operate on all docs)
                if !matches!(arg_value, Value::Null) {
                    let filter = parse_filter_value(arg_value, variables)?;
                    mutation.filter = Some(filter);
                }
            }

            // UPSERT: filter is required and cannot be null
            (MutationType::Upsert, "filter") => {
                if matches!(arg_value, Value::Null) {
                    return Err(QueryError::parse(
                        "Argument \"filter\" has invalid value <nil>.".to_string(),
                    ));
                }
                let filter = parse_filter_value(arg_value, variables)?;
                mutation.filter = Some(filter);
            }

            // Encryption: encrypt entire document
            (_, "encrypt") => {
                if let Value::Boolean(b) = arg_value {
                    mutation.encrypt_doc = *b;
                }
            }

            // Encryption: encrypt specific fields
            (_, "encryptFields") => {
                if let Value::List(fields) = arg_value {
                    mutation.encrypt_fields = fields
                        .iter()
                        .filter_map(|v| match v {
                            Value::Enum(name) => Some(name.clone()),
                            Value::String(name) => Some(name.clone()),
                            _ => None,
                        })
                        .collect();
                }
            }

            // Unknown argument
            _ => {
                return Err(QueryError::parse(format!(
                    "Unknown argument '{}' for {} mutation on '{}'",
                    arg_name,
                    mutation_type.as_prefix(),
                    collection_name
                )));
            }
        }
    }

    // Validate mutation has required arguments
    match mutation_type {
        MutationType::Create => {
            if mutation.create_input.is_empty() && !has_input_arg {
                return Err(QueryError::parse(format!(
                    "add_{} mutation requires 'input' argument with array of documents",
                    collection_name
                )));
            }
        }
        MutationType::Update => {
            if mutation.update_input.is_empty() && !has_input_arg {
                return Err(QueryError::parse(format!(
                    "update_{} mutation requires 'input' argument with fields to update",
                    collection_name
                )));
            }
            // Note: Go DefraDB allows update without docIDs or filter
            // (meaning update all documents in the collection)
        }
        MutationType::Delete => {
            // Note: Go DefraDB allows delete without docIDs or filter
            // (meaning delete all documents in the collection)
        }
        MutationType::Upsert => {
            // Go DefraDB requires all three: filter, create, update
            if mutation.filter.is_none() {
                return Err(QueryError::parse(format!(
                    "upsert_{} mutation requires 'filter' argument",
                    collection_name
                )));
            }
            if mutation.create_input.is_empty() {
                return Err(QueryError::parse(format!(
                    "upsert_{} mutation requires 'add' argument with document to create if no match",
                    collection_name
                )));
            }
            if mutation.update_input.is_empty() {
                return Err(QueryError::parse(format!(
                    "upsert_{} mutation requires 'update' argument with fields to update if match found",
                    collection_name
                )));
            }
        }
    }

    // Parse selection set (fields to return after mutation)
    // For mutations, we don't support fragments in return fields
    let empty_fragments: FragmentMap<'_> = HashMap::new();
    let mut empty_visiting = HashSet::new();
    let (fields, mapping) = parse_selection_set(
        &field.selection_set,
        &collection_name,
        variables,
        &empty_fragments,
        &mut empty_visiting,
    )?;

    // All mutations return [TypeName], which is an object type requiring a sub selection.
    if fields.is_empty() {
        return Err(QueryError::parse(format!(
            "Field \"{}\" of type \"[{}]\" must have a sub selection.",
            field_name, collection_name
        )));
    }

    mutation.fields = fields;
    mutation.document_mapping = mapping;

    Ok(mutation)
}

/// Parse CREATE mutation input (array of documents).
fn parse_create_input(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    collection_name: &str,
) -> Result<Vec<HashMap<String, JsonValue>>> {
    match value {
        Value::List(items) => {
            let mut docs = Vec::new();
            for item in items {
                match item {
                    Value::Object(obj) => {
                        let doc = parse_document_input(obj, variables)?;
                        docs.push(doc);
                    }
                    Value::Null => {
                        return Err(QueryError::parse(format!(
                            "Expected \"{}MutationInputArg!\", found null.",
                            collection_name
                        )))
                    }
                    _ => return Err(QueryError::parse("CREATE input items must be objects")),
                }
            }
            Ok(docs)
        }
        Value::Object(obj) => {
            // Single document (wrap in array)
            let doc = parse_document_input(obj, variables)?;
            Ok(vec![doc])
        }
        // Null input is valid and means "no documents to create"
        Value::Null => Ok(vec![]),
        // Variable reference - resolve from variables map
        Value::Variable(var_name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", var_name))
            })?;
            let json_val = vars.get(var_name).ok_or_else(|| {
                QueryError::parse(format!("variable '{}' not found in variables", var_name))
            })?;
            // Convert JSON value to documents
            match json_val {
                JsonValue::Array(items) => {
                    let mut docs = Vec::new();
                    for item in items {
                        if let JsonValue::Object(obj) = item {
                            let doc: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            docs.push(doc);
                        } else if item.is_null() {
                            return Err(QueryError::parse(format!(
                                "Expected \"{}MutationInputArg!\", found null.",
                                collection_name
                            )));
                        } else {
                            return Err(QueryError::parse("CREATE input items must be objects"));
                        }
                    }
                    Ok(docs)
                }
                JsonValue::Object(obj) => {
                    let doc: HashMap<String, JsonValue> =
                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    Ok(vec![doc])
                }
                JsonValue::Null => Ok(vec![]),
                _ => Err(QueryError::parse(
                    "CREATE input variable must be an array of objects or a single object",
                )),
            }
        }
        _ => Err(QueryError::parse(
            "CREATE input must be an array of objects or a single object",
        )),
    }
}

/// Parse UPDATE mutation input (patch object).
/// Non-object input (e.g., array "patch") is treated as empty/no-op (Go compatibility).
fn parse_update_input(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<HashMap<String, JsonValue>> {
    match value {
        Value::Object(obj) => parse_document_input(obj, variables),
        _ => Ok(HashMap::new()),
    }
}

/// Parse a document input object into field-value map.
fn parse_document_input(
    obj: &std::collections::BTreeMap<String, Value<'_, String>>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<HashMap<String, JsonValue>> {
    let mut fields = HashMap::new();
    for (key, value) in obj {
        let json_value = graphql_value_to_json(value, variables)?;
        fields.insert(key.clone(), json_value);
    }
    Ok(fields)
}

/// Parse a BM25 field from a GraphQL query.
///
/// Format: `BM25(query: "search terms", fields: ["title", "body"])`
pub(super) fn parse_bm25_field(
    field: &Field<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<FullTextSearch> {
    let mut query_str = None;
    let mut fields = Vec::new();

    for (arg_name, arg_value) in &field.arguments {
        match arg_name.as_str() {
            "query" => {
                query_str = Some(match arg_value {
                    Value::String(s) => s.clone(),
                    Value::Variable(var_name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "Variable \"${}\" was not provided.",
                                var_name
                            ))
                        })?;
                        vars.get(var_name.as_str())
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                QueryError::parse(format!(
                                    "Variable \"${}\" must be a string",
                                    var_name
                                ))
                            })?
                            .to_string()
                    }
                    _ => {
                        return Err(QueryError::parse("BM25 query argument must be a string"));
                    }
                });
            }
            "fields" => {
                fields = match arg_value {
                    Value::List(items) => items
                        .iter()
                        .filter_map(|v| match v {
                            Value::String(s) => Some(s.clone()),
                            Value::Enum(s) => Some(s.clone()),
                            _ => None,
                        })
                        .collect(),
                    _ => {
                        return Err(QueryError::parse(
                            "BM25 fields argument must be a list of strings",
                        ));
                    }
                };
            }
            _ => {}
        }
    }

    let query_str =
        query_str.ok_or_else(|| QueryError::parse("BM25 requires a 'query' argument"))?;

    if fields.is_empty() {
        return Err(QueryError::parse(
            "BM25 requires a non-empty 'fields' argument",
        ));
    }

    Ok(FullTextSearch::new(fields, query_str))
}

/// Parse a _similarity field from a GraphQL query.
///
/// Format: `_similarity(fieldName: {vector: [1, 2, 3]})`
/// The argument name is the target field containing the document's vector.
/// The value is an object with a `vector` key containing the query vector.
pub(super) fn parse_similarity_field(
    field: &Field<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Similarity> {
    if field.arguments.is_empty() {
        return Err(QueryError::parse("_similarity requires a field argument"));
    }

    let (target_field, value) = &field.arguments[0];

    // Parse the value: {vector: [1, 2, 3]}
    let vector = match value {
        Value::Object(obj) => {
            let vec_value = obj.get("vector").ok_or_else(|| {
                QueryError::parse("_similarity argument must contain a 'vector' key")
            })?;
            parse_vector_value(vec_value, variables)?
        }
        Value::Variable(var_name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided.", var_name))
            })?;
            let json_val = vars.get(var_name.as_str()).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", var_name))
            })?;
            if let JsonValue::Object(obj) = json_val {
                let vec_val = obj.get("vector").ok_or_else(|| {
                    QueryError::parse("_similarity variable must contain a 'vector' key")
                })?;
                parse_json_vector(vec_val)?
            } else {
                return Err(QueryError::parse("_similarity variable must be an object"));
            }
        }
        _ => {
            return Err(QueryError::parse(
                "_similarity argument must be an object with 'vector' key",
            ));
        }
    };

    Ok(Similarity::new(target_field.clone(), vector))
}

/// Parse a vector value from a GraphQL list literal.
fn parse_vector_value(
    value: &Value<'_, String>,
    _variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<f64>> {
    match value {
        Value::List(items) => {
            let mut vec = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Int(n) => {
                        vec.push(
                            n.as_i64()
                                .ok_or_else(|| QueryError::parse("integer out of range"))?
                                as f64,
                        );
                    }
                    Value::Float(f) => {
                        vec.push(*f);
                    }
                    _ => {
                        return Err(QueryError::parse("vector values must be numeric"));
                    }
                }
            }
            Ok(vec)
        }
        _ => Err(QueryError::parse("vector must be an array")),
    }
}

/// Parse a vector from a JSON array value.
fn parse_json_vector(value: &JsonValue) -> Result<Vec<f64>> {
    match value {
        JsonValue::Array(items) => {
            let mut vec = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    JsonValue::Number(n) => {
                        vec.push(
                            n.as_f64()
                                .ok_or_else(|| QueryError::parse("invalid number in vector"))?,
                        );
                    }
                    _ => {
                        return Err(QueryError::parse("vector values must be numeric"));
                    }
                }
            }
            Ok(vec)
        }
        _ => Err(QueryError::parse("vector must be an array")),
    }
}
