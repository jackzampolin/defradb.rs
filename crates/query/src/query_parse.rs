//! GraphQL query parser
//!
//! Parses GraphQL query strings into Select operations for execution.

use graphql_parser::query::{
    Definition, Document, Field, OperationDefinition, Selection, SelectionSet, Value,
};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{
    Field as SelectField, Filter, GroupBy, Limit, OrderBy, OrderCondition, OrderDirection,
    Requestable, Select,
};

/// Parse a GraphQL query string into Select operations.
///
/// Returns a vector of Select operations, one for each top-level field in the query.
pub fn parse_query(query: &str) -> Result<Vec<Select>> {
    let doc: Document<'_, String> =
        graphql_parser::parse_query(query).map_err(|e| QueryError::parse(e.to_string()))?;

    let mut selects = Vec::new();

    for def in doc.definitions {
        match def {
            Definition::Operation(op) => {
                let selections = match op {
                    OperationDefinition::Query(q) => q.selection_set.items,
                    OperationDefinition::SelectionSet(ss) => ss.items,
                    OperationDefinition::Mutation(_) => {
                        return Err(QueryError::parse("mutations not yet supported"))
                    }
                    OperationDefinition::Subscription(_) => {
                        return Err(QueryError::parse("subscriptions not supported"))
                    }
                };

                for selection in selections {
                    if let Selection::Field(field) = selection {
                        let select = parse_field_to_select(&field)?;
                        selects.push(select);
                    }
                }
            }
            Definition::Fragment(_) => {
                return Err(QueryError::parse("fragments not yet supported"))
            }
        }
    }

    Ok(selects)
}

/// Parse a single GraphQL field into a Select operation.
fn parse_field_to_select(field: &Field<'_, String>) -> Result<Select> {
    let collection_name = field.name.clone();
    let alias = field.alias.clone();

    let mut select = Select::new(&collection_name);
    if let Some(a) = alias {
        select.field = SelectField::with_alias(&collection_name, a);
    }

    // Parse arguments (filter, limit, offset, order, docIDs, etc.)
    for (arg_name, arg_value) in &field.arguments {
        match arg_name.as_str() {
            "filter" => {
                let filter = parse_filter_value(arg_value)?;
                select.filter = Some(filter);
            }
            "limit" => {
                let limit_val = parse_int_value(arg_value)?;
                if limit_val < 0 {
                    return Err(QueryError::parse("limit must be non-negative"));
                }
                select.limit = Some(Limit::new(
                    Some(limit_val as u64),
                    select.limit.as_ref().map(|l| l.offset).unwrap_or(0),
                ));
            }
            "offset" => {
                let offset_val = parse_int_value(arg_value)?;
                if offset_val < 0 {
                    return Err(QueryError::parse("offset must be non-negative"));
                }
                select.limit = Some(Limit::new(
                    select.limit.as_ref().and_then(|l| l.limit),
                    offset_val as u64,
                ));
            }
            "order" => {
                let order_by = parse_order_value(arg_value)?;
                select.order_by = Some(order_by);
            }
            "groupBy" => {
                let group_by = parse_group_by_value(arg_value)?;
                select.group_by = Some(group_by);
            }
            "docIDs" | "docID" => {
                let doc_ids = parse_doc_ids_value(arg_value)?;
                select.doc_ids = Some(doc_ids);
            }
            "cid" => match arg_value {
                Value::String(s) => select.cid = Some(s.clone()),
                _ => return Err(QueryError::parse("cid argument must be a string")),
            },
            "showDeleted" => match arg_value {
                Value::Boolean(b) => select.show_deleted = *b,
                _ => return Err(QueryError::parse("showDeleted argument must be a boolean")),
            },
            _ => {
                return Err(QueryError::parse(format!(
                    "unknown argument '{}' on collection '{}'. Valid arguments are: filter, limit, offset, order, groupBy, docIDs, docID, cid, showDeleted",
                    arg_name, collection_name
                )));
            }
        }
    }

    // Parse selection set (child fields)
    let (fields, mapping) = parse_selection_set(&field.selection_set, &collection_name)?;
    select.fields = fields;
    select.document_mapping = mapping;

    Ok(select)
}

/// Parse a selection set into fields and document mapping.
fn parse_selection_set(
    selection_set: &SelectionSet<'_, String>,
    _collection_name: &str,
) -> Result<(Vec<Requestable>, DocumentMapping)> {
    let mut fields = Vec::new();
    let mut mapping = DocumentMapping::new();

    for selection in &selection_set.items {
        match selection {
            Selection::Field(field) => {
                let field_name = field.name.clone();
                let alias = field.alias.clone();

                // Check if this is a nested selection (relation)
                if !field.selection_set.items.is_empty() {
                    // This is a nested select (relation)
                    let nested = parse_field_to_select(field)?;
                    fields.push(Requestable::Select(Box::new(nested)));
                } else {
                    // Simple field
                    let select_field = if let Some(a) = alias {
                        SelectField::with_alias(&field_name, a)
                    } else {
                        SelectField::new(&field_name)
                    };

                    // Add to document mapping
                    let index = mapping.next_index();
                    mapping.add(index, &field_name);
                    mapping.add_render_key(index, select_field.output_name());

                    fields.push(Requestable::Field(select_field));
                }
            }
            Selection::FragmentSpread(_) => {
                return Err(QueryError::parse("fragment spreads not yet supported"))
            }
            Selection::InlineFragment(_) => {
                return Err(QueryError::parse("inline fragments not yet supported"))
            }
        }
    }

    Ok((fields, mapping))
}

/// Parse a filter argument value into a Filter.
fn parse_filter_value(value: &Value<'_, String>) -> Result<Filter> {
    match value {
        Value::Object(obj) => {
            let conditions = parse_filter_object(obj)?;
            Ok(Filter::from_conditions(conditions))
        }
        _ => Err(QueryError::parse("filter must be an object")),
    }
}

/// Parse a filter object into conditions map.
fn parse_filter_object(
    obj: &BTreeMap<String, Value<'_, String>>,
) -> Result<HashMap<String, JsonValue>> {
    let mut conditions = HashMap::new();

    for (key, val) in obj {
        let json_val = graphql_value_to_json(val)?;
        conditions.insert(key.clone(), json_val);
    }

    Ok(conditions)
}

/// Convert GraphQL Value to JSON Value.
fn graphql_value_to_json(value: &Value<'_, String>) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Int(n) => n
            .as_i64()
            .map(|i| JsonValue::Number(i.into()))
            .ok_or_else(|| QueryError::parse("integer out of range")),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .ok_or_else(|| QueryError::parse("invalid float value")),
        Value::String(s) => Ok(JsonValue::String(s.clone())),
        Value::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Value::Enum(e) => Ok(JsonValue::String(e.clone())),
        Value::List(items) => {
            let arr: Result<Vec<JsonValue>> = items.iter().map(graphql_value_to_json).collect();
            Ok(JsonValue::Array(arr?))
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), graphql_value_to_json(v)?);
            }
            Ok(JsonValue::Object(map))
        }
        Value::Variable(_) => Err(QueryError::parse("variables not yet supported")),
    }
}

/// Parse an integer value from GraphQL Value.
fn parse_int_value(value: &Value<'_, String>) -> Result<i64> {
    match value {
        Value::Int(n) => n
            .as_i64()
            .ok_or_else(|| QueryError::parse("integer out of range")),
        _ => Err(QueryError::parse("expected integer value")),
    }
}

/// Parse order argument into OrderBy.
fn parse_order_value(value: &Value<'_, String>) -> Result<OrderBy> {
    let mut order_by = OrderBy::new();

    match value {
        Value::Object(obj) => {
            for (field_name, direction_val) in obj {
                let direction = match direction_val {
                    Value::Enum(s) | Value::String(s) => {
                        OrderDirection::parse(s).ok_or_else(|| {
                            QueryError::parse(format!(
                                "invalid order direction '{}', expected ASC or DESC",
                                s
                            ))
                        })?
                    }
                    _ => return Err(QueryError::parse("order direction must be ASC or DESC")),
                };
                order_by = order_by.with_condition(OrderCondition::new(field_name, direction));
            }
        }
        _ => return Err(QueryError::parse("order must be an object")),
    }

    Ok(order_by)
}

/// Parse groupBy argument into GroupBy.
fn parse_group_by_value(value: &Value<'_, String>) -> Result<GroupBy> {
    match value {
        Value::List(items) => {
            let fields: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) | Value::Enum(s) => Ok(s.clone()),
                    _ => Err(QueryError::parse("groupBy items must be strings")),
                })
                .collect();
            Ok(GroupBy::new(fields?))
        }
        _ => Err(QueryError::parse("groupBy must be a list")),
    }
}

/// Parse docIDs argument into vector of strings.
fn parse_doc_ids_value(value: &Value<'_, String>) -> Result<Vec<String>> {
    match value {
        Value::List(items) => {
            let ids: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s.clone()),
                    _ => Err(QueryError::parse("docIDs items must be strings")),
                })
                .collect();
            ids
        }
        Value::String(s) => Ok(vec![s.clone()]),
        _ => Err(QueryError::parse("docIDs must be a string or list")),
    }
}
