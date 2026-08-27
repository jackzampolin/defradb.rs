//! Alias-based filter condition evaluation

use serde_json::{Map, Value as JsonValue};

use super::eval::eval_op;
use super::op::FilterOp;
use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};

use super::Filter;

impl Filter {
    /// Evaluate alias-based filter conditions.
    /// Alias filters allow filtering by render key names instead of field names.
    /// Supports logical operators (_and, _or, _not) within the alias block.
    pub(crate) fn eval_alias_conditions_at_depth(
        &self,
        conditions: &Map<String, JsonValue>,
        fields: &[Option<JsonValue>],
        mapping: &DocumentMapping,
        depth: usize,
    ) -> Result<bool> {
        self.check_depth(depth)?;

        for (key, value) in conditions {
            // Check for logical operators within alias block
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And => {
                        let arr = value.as_array().ok_or_else(|| {
                            QueryError::invalid_filter("_and requires array in _alias")
                        })?;
                        for item in arr {
                            let sub_conditions = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_and items must be objects in _alias")
                            })?;
                            if !self.eval_alias_conditions_at_depth(
                                sub_conditions,
                                fields,
                                mapping,
                                depth + 1,
                            )? {
                                return Ok(false);
                            }
                        }
                        continue;
                    }
                    FilterOp::Or => {
                        let arr = value.as_array().ok_or_else(|| {
                            QueryError::invalid_filter("_or requires array in _alias")
                        })?;
                        let mut any_match = false;
                        for item in arr {
                            let sub_conditions = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_or items must be objects in _alias")
                            })?;
                            if self.eval_alias_conditions_at_depth(
                                sub_conditions,
                                fields,
                                mapping,
                                depth + 1,
                            )? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                        continue;
                    }
                    FilterOp::Not => {
                        let sub_conditions = value.as_object().ok_or_else(|| {
                            QueryError::invalid_filter("_not requires object in _alias")
                        })?;
                        if self.eval_alias_conditions_at_depth(
                            sub_conditions,
                            fields,
                            mapping,
                            depth + 1,
                        )? {
                            return Ok(false);
                        }
                        continue;
                    }
                    _ => {} // Non-logical ops are handled as alias field conditions below
                }
            }

            // Look up by render_key (alias) instead of field name
            let field_index = mapping.try_find_index_from_render_key(key).ok_or_else(|| {
                QueryError::unknown_field(format!("field or alias not found. Name: {}", key))
            })?;

            let field_value = fields
                .get(field_index)
                .and_then(|v| v.as_ref())
                .cloned()
                .unwrap_or(JsonValue::Null);

            let ops = value.as_object().ok_or_else(|| {
                QueryError::invalid_filter("alias field condition must be object")
            })?;

            // Check if this is a relation filter (nested field conditions) or operator conditions
            let is_relation_filter = ops.keys().any(|k| FilterOp::parse(k).is_none());

            if is_relation_filter {
                // Relation-style alias: {_alias: {books: {rating: {_gt: 4.8}}}}
                // The alias points to a relation field (array or object)
                if field_value.is_null() {
                    // Null relation -> no match
                    return Ok(false);
                } else if let Some(arr) = field_value.as_array() {
                    // One-to-many: existential semantics (any element matches)
                    let mut any_match = false;
                    for elem in arr {
                        if let Some(obj) = elem.as_object() {
                            if self.eval_relation_conditions(ops, obj)? {
                                any_match = true;
                                break;
                            }
                        }
                    }
                    if !any_match {
                        return Ok(false);
                    }
                } else if let Some(obj) = field_value.as_object() {
                    // One-to-one: direct match
                    if !self.eval_relation_conditions(ops, obj)? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            } else {
                // Scalar-style alias: {_alias: {myAge: {_gt: 20}}}
                for (op_key, op_value) in ops {
                    if let Some(op) = FilterOp::parse(op_key) {
                        if !eval_op(&field_value, op, op_value)? {
                            return Ok(false);
                        }
                    } else {
                        return Err(QueryError::invalid_filter(format!(
                            "unknown operator '{}' in _alias filter",
                            op_key
                        )));
                    }
                }
            }
        }
        Ok(true)
    }
}
