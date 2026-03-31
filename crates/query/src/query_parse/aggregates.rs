//! Aggregate parsing helpers
//!
//! Standalone functions for parsing GraphQL aggregate arguments:
//! - `parse_group_by_value()` - Parse groupBy argument
//! - `parse_aggregate_target_obj()` - Parse aggregate target from GraphQL object
//! - `parse_aggregate_target_from_json()` - Parse aggregate target from JSON
//! - `parse_aggregate_field()` - Parse an aggregate field into Aggregate
//! - `parse_top_level_aggregate()` - Parse a top-level aggregate query

use graphql_parser::query::{Field, Value};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::error::{QueryError, Result};
use crate::mapper::{
    Aggregate, AggregateTarget, AggregateType, Field as SelectField, Filter, GroupBy, Limit,
    OrderBy, OrderCondition, OrderDirection, Requestable, Select,
};

use super::filters::parse_filter_value;
use super::ordering::{parse_order_from_json, parse_order_value};
use super::values::parse_int_value;

/// Parse groupBy argument into GroupBy.
pub(super) fn parse_group_by_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<GroupBy> {
    match value {
        Value::List(items) => {
            let fields: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) | Value::Enum(s) => Ok(s.clone()),
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "variable '{}' used but no variables provided",
                                name
                            ))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        json_val.as_str().map(|s| s.to_string()).ok_or_else(|| {
                            QueryError::parse(format!(
                                "Variable \"${}\" must be of type String",
                                name
                            ))
                        })
                    }
                    _ => Err(QueryError::parse("groupBy items must be strings")),
                })
                .collect();
            Ok(GroupBy::new(fields?))
        }
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            let arr = json_val.as_array().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type [String]", name))
            })?;
            let fields: Result<Vec<String>> = arr
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| QueryError::parse("groupBy items must be strings"))
                })
                .collect();
            Ok(GroupBy::new(fields?))
        }
        _ => Err(QueryError::parse("groupBy must be a list")),
    }
}

/// Parse an aggregate target from a GraphQL Object value.
pub(super) fn parse_aggregate_target_obj(
    arg_name: &str,
    obj: &std::collections::BTreeMap<String, Value<'_, String>>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<AggregateTarget> {
    let mut target = AggregateTarget::new(arg_name.to_string());
    for (key, val) in obj {
        match key.as_str() {
            "field" => {
                let field_name = match val {
                    Value::String(s) => s.clone(),
                    Value::Enum(s) => s.clone(),
                    _ => {
                        return Err(QueryError::parse(
                            "field in relation aggregate must be a string",
                        ))
                    }
                };
                target.field_name = Some(field_name);
            }
            "filter" => {
                let filter = parse_filter_value(val, variables)?;
                target.filter = Some(filter);
            }
            "limit" => {
                let limit_val = parse_int_value(val, variables)?;
                target.limit = Some(Limit::new(Some(limit_val as u64), 0));
            }
            "offset" => {
                let offset_val = parse_int_value(val, variables)?;
                if let Some(ref mut limit) = target.limit {
                    limit.offset = offset_val as u64;
                } else {
                    target.limit = Some(Limit::new(None, offset_val as u64));
                }
            }
            "order" => {
                let order = match val {
                    Value::Enum(s) | Value::String(s) => {
                        let direction = OrderDirection::parse(s).ok_or_else(|| {
                            QueryError::parse(format!(
                                "Argument \"order\" has invalid value {{order: {}}}",
                                s
                            ))
                        })?;
                        OrderBy::new().with_condition(OrderCondition::new("", direction))
                    }
                    _ => parse_order_value(val, variables)?,
                };
                target.order = Some(order);
            }
            _ => {}
        }
    }
    Ok(target)
}

/// Parse an aggregate target from a resolved JSON variable value.
pub(super) fn parse_aggregate_target_from_json(
    arg_name: &str,
    json: &JsonValue,
    _variables: Option<&HashMap<String, JsonValue>>,
) -> Result<AggregateTarget> {
    let mut target = AggregateTarget::new(arg_name.to_string());
    if let JsonValue::Object(obj) = json {
        for (key, val) in obj {
            match key.as_str() {
                "field" => {
                    if let Some(s) = val.as_str() {
                        target.field_name = Some(s.to_string());
                    }
                }
                "filter" => {
                    // Convert JSON filter to a Filter
                    if let JsonValue::Object(filter_obj) = val {
                        target.filter = Some(Filter::from_conditions(filter_obj.clone()));
                    }
                }
                "limit" => {
                    if let Some(n) = val.as_i64() {
                        target.limit = Some(Limit::new(Some(n as u64), 0));
                    }
                }
                "offset" => {
                    if let Some(n) = val.as_i64() {
                        if let Some(ref mut limit) = target.limit {
                            limit.offset = n as u64;
                        } else {
                            target.limit = Some(Limit::new(None, n as u64));
                        }
                    }
                }
                "order" => {
                    target.order = Some(parse_order_from_json(val)?);
                }
                _ => {}
            }
        }
    }
    Ok(target)
}

/// Parse an aggregate field into an Aggregate.
///
/// Handles aggregate functions like `_count`, `_sum(field: "age")`, etc.
/// Also supports relation aggregates like `_count(books: {})`, `_sum(books: {field: score}, articles: {field: rating})`.
pub(super) fn parse_aggregate_field(
    field: &Field<'_, String>,
    agg_type: AggregateType,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Aggregate> {
    let mut target_field: Option<String> = None;
    let mut relation_targets: Vec<AggregateTarget> = Vec::new();

    // Parse arguments (e.g., `field: "age"` for _sum, or `books: {}` for relation aggregates)
    for (arg_name, arg_value) in &field.arguments {
        match arg_name.as_str() {
            "field" => {
                target_field = Some(match arg_value {
                    Value::String(s) => s.clone(),
                    Value::Enum(s) => s.clone(),
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "variable '{}' used but no variables provided",
                                name
                            ))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        json_val
                            .as_str()
                            .ok_or_else(|| {
                                QueryError::parse(format!(
                                    "Variable \"${}\" must be of type String",
                                    name
                                ))
                            })?
                            .to_string()
                    }
                    _ => return Err(QueryError::parse("field argument must be a string")),
                });
            }
            _ => {
                // This might be a relation name argument like `books: {field: score}`
                // Relation arguments take an object value with optional field, filter, limit, order.
                // Also handle variables that resolve to objects.
                match arg_value {
                    Value::Object(obj) => {
                        let target = parse_aggregate_target_obj(arg_name, obj, variables)?;
                        relation_targets.push(target);
                    }
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "variable '{}' used but no variables provided",
                                name
                            ))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        let target =
                            parse_aggregate_target_from_json(arg_name, json_val, variables)?;
                        relation_targets.push(target);
                    }
                    _ => {
                        return Err(QueryError::parse(format!(
                            "unknown argument '{}' on aggregate '{}', expected an object value for relation aggregates",
                            arg_name,
                            agg_type.as_str()
                        )));
                    }
                }
            }
        }
    }

    // Create the appropriate aggregate
    let aggregate = if !relation_targets.is_empty() {
        // Relation aggregate mode - create aggregate with targets directly (no empty initial target)
        Aggregate {
            aggregate_type: agg_type,
            targets: relation_targets,
            filter: None,
            alias: None,
        }
    } else {
        // Simple field aggregate mode
        match agg_type {
            AggregateType::Count => {
                // _count can work without a field argument (counts all docs)
                if let Some(field_name) = target_field {
                    Aggregate::count().with_target(AggregateTarget::with_field("", field_name))
                } else {
                    Aggregate::count()
                }
            }
            AggregateType::Sum => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("aggregate must be provided with a property to aggregate")
                })?;
                Aggregate::sum(AggregateTarget::with_field("", field_name))
            }
            AggregateType::Average => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("aggregate must be provided with a property to aggregate")
                })?;
                Aggregate::avg(AggregateTarget::with_field("", field_name))
            }
            AggregateType::Min => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("aggregate must be provided with a property to aggregate")
                })?;
                Aggregate::min(AggregateTarget::with_field("", field_name))
            }
            AggregateType::Max => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("aggregate must be provided with a property to aggregate")
                })?;
                Aggregate::max(AggregateTarget::with_field("", field_name))
            }
        }
    };

    Ok(aggregate)
}

/// Parse a top-level aggregate query (e.g., `{ _avg(Users: {field: Age}) }`).
///
/// Top-level aggregates are different from nested aggregates in that:
/// - The aggregate function name is the top-level field
/// - Arguments are collection names with their aggregate configuration
/// - The result is wrapped in a Select with the collection as the target
pub(super) fn parse_top_level_aggregate(
    field: &Field<'_, String>,
    agg_type: AggregateType,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Select> {
    let mut aggregate = parse_aggregate_field(field, agg_type, variables)?;

    // Top-level numeric aggregates require a field argument on each target.
    // This matches Go's GraphQL schema validation where collection args require
    // the "field" key (e.g., _avg(Users: {field: Age}) not _avg(Users: {})).
    if agg_type != AggregateType::Count {
        for target in &aggregate.targets {
            if target.field_name.is_none() {
                let type_name = format!("{}NumericFieldsArg!", capitalize_first(&target.host_name));
                return Err(QueryError::parse(format!(
                    "Argument \"{}\" has invalid value {{}}.\nIn field \"field\": Expected \"{}\", found null.",
                    target.host_name,
                    type_name
                )));
            }
        }
    }

    // Top-level aggregates require at least one target (collection argument).
    // query { COUNT } with no arguments should return this error.
    if aggregate.targets.is_empty() {
        return Err(QueryError::parse(
            "aggregate must be provided with a property to aggregate",
        ));
    }

    // Set alias if provided
    if let Some(ref a) = field.alias {
        aggregate = aggregate.with_alias(a.clone());
    }

    // Get the collection name from the first target
    let collection_name = aggregate
        .targets
        .first()
        .map(|t| t.host_name.clone())
        .unwrap_or_default();

    // Create a Select that wraps this aggregate
    // The field name should be the aggregate name (e.g., "AVG") so the response
    // key is correct (e.g., {"AVG": 29} not {"Users": 29})
    let mut select = Select::new(&collection_name);
    let agg_name = agg_type.as_str();
    if let Some(ref a) = field.alias {
        // If aliased, use alias as the output name: { average: AVG(...) } -> {"average": ...}
        select.field = SelectField::with_alias(agg_name, a.clone());
    } else {
        // Otherwise use the aggregate name: { AVG(...) } -> {"AVG": ...}
        select.field = SelectField::new(agg_name);
    }

    // Add to document mapping
    let index = select.document_mapping.next_index();
    select.document_mapping.add(index, agg_name);
    select
        .document_mapping
        .add_render_key(index, aggregate.output_name());

    select.fields.push(Requestable::Aggregate(aggregate));

    Ok(select)
}

/// Capitalize the first character of a string.
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}
