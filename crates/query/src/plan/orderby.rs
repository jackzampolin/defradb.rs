//! OrderByNode for sorting query results

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::cmp::Ordering;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{OrderBy, OrderCondition, OrderDirection};
use crate::planner::{Doc, PlanNode};

/// OrderByNode sorts documents based on ORDER BY conditions.
///
/// This node buffers all documents from its source during `start()`,
/// sorts them according to the specified conditions, then yields
/// sorted documents one at a time during `next()`.
///
/// Follows Go DefraDB semantics:
/// - Null values sort before non-null values (nulls first)
/// - Multi-field ordering with proper precedence
/// - Stable sort preserves original order for equal elements
pub struct OrderByNode {
    source: Box<dyn PlanNode>,
    order_by: OrderBy,
    document_mapping: DocumentMapping,
    buffer: Vec<Doc>,
    position: usize,
    current_doc: Doc,
}

impl OrderByNode {
    /// Create a new OrderByNode wrapping a source
    pub fn new(
        source: Box<dyn PlanNode>,
        order_by: OrderBy,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            source,
            order_by,
            document_mapping,
            buffer: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
        }
    }

    /// Compare two JSON values, returning Ordering.
    ///
    /// Follows Go DefraDB semantics:
    /// - Null < any non-null value
    /// - Same-type comparisons use natural ordering
    /// - Cross-type comparisons fall back to type name ordering
    fn compare_values(a: Option<&JsonValue>, b: Option<&JsonValue>) -> Ordering {
        match (a, b) {
            // Both null or missing - equal
            (None, None) => Ordering::Equal,
            (Some(JsonValue::Null), Some(JsonValue::Null)) => Ordering::Equal,
            (None, Some(JsonValue::Null)) => Ordering::Equal,
            (Some(JsonValue::Null), None) => Ordering::Equal,

            // Null/missing before any value
            (None, Some(_)) => Ordering::Less,
            (Some(JsonValue::Null), Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(_), Some(JsonValue::Null)) => Ordering::Greater,

            // Both have values
            (Some(a_val), Some(b_val)) => Self::compare_non_null(a_val, b_val),
        }
    }

    /// Compare two non-null JSON values
    fn compare_non_null(a: &JsonValue, b: &JsonValue) -> Ordering {
        match (a, b) {
            // Numbers: Convert to f64 for comparison. Large integers beyond ~2^53 may
            // lose precision. NaN values are treated as equal to avoid non-determinism.
            (JsonValue::Number(a_num), JsonValue::Number(b_num)) => {
                let a_f = a_num.as_f64().unwrap_or(0.0);
                let b_f = b_num.as_f64().unwrap_or(0.0);
                a_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
            }

            // Strings
            (JsonValue::String(a_str), JsonValue::String(b_str)) => a_str.cmp(b_str),

            // Booleans: false < true
            (JsonValue::Bool(a_bool), JsonValue::Bool(b_bool)) => a_bool.cmp(b_bool),

            // Arrays: compare element by element
            (JsonValue::Array(a_arr), JsonValue::Array(b_arr)) => {
                for (a_elem, b_elem) in a_arr.iter().zip(b_arr.iter()) {
                    let cmp = Self::compare_non_null(a_elem, b_elem);
                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                }
                a_arr.len().cmp(&b_arr.len())
            }

            // Objects: compare by JSON string representation (last resort)
            (JsonValue::Object(_), JsonValue::Object(_)) => {
                let a_str = a.to_string();
                let b_str = b.to_string();
                a_str.cmp(&b_str)
            }

            // Different types: compare by type name for deterministic ordering
            _ => Self::type_order(a).cmp(&Self::type_order(b)),
        }
    }

    /// Get type ordering priority (for cross-type comparisons)
    fn type_order(v: &JsonValue) -> u8 {
        match v {
            JsonValue::Null => 0,
            JsonValue::Bool(_) => 1,
            JsonValue::Number(_) => 2,
            JsonValue::String(_) => 3,
            JsonValue::Array(_) => 4,
            JsonValue::Object(_) => 5,
        }
    }

    /// Static version of compare_docs that doesn't require &self
    fn compare_docs_static(
        a: &Doc,
        b: &Doc,
        order_by: &OrderBy,
        mapping: &DocumentMapping,
    ) -> Ordering {
        for condition in &order_by.conditions {
            let cmp = Self::compare_by_condition_static(a, b, condition, mapping);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }

    /// Static version of compare_by_condition
    fn compare_by_condition_static(
        a: &Doc,
        b: &Doc,
        condition: &OrderCondition,
        mapping: &DocumentMapping,
    ) -> Ordering {
        let a_val = Self::get_field_value_static(a, &condition.fields, mapping);
        let b_val = Self::get_field_value_static(b, &condition.fields, mapping);

        let cmp = Self::compare_values(a_val, b_val);

        match condition.direction {
            OrderDirection::Asc => cmp,
            OrderDirection::Desc => cmp.reverse(),
        }
    }

    /// Static version of get_field_value.
    ///
    /// Handles both simple field paths (["age"]) and nested relation paths (["author", "age"]).
    /// For nested paths, traverses the JSON object hierarchy to find the target value.
    fn get_field_value_static<'a>(
        doc: &'a Doc,
        fields: &[String],
        mapping: &DocumentMapping,
    ) -> Option<&'a JsonValue> {
        if fields.is_empty() {
            return None;
        }

        let field_name = &fields[0];
        let index = mapping.first_index_of_name(field_name)?;
        let value = doc.get(index)?;

        // If there are more path segments, traverse the nested object
        if fields.len() > 1 {
            Self::traverse_nested_value(value, &fields[1..])
        } else {
            Some(value)
        }
    }

    /// Traverse a nested JSON value to find a field at the given path.
    fn traverse_nested_value<'a>(value: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
        if path.is_empty() {
            return Some(value);
        }

        match value {
            JsonValue::Object(obj) => {
                let field_value = obj.get(&path[0])?;
                if path.len() > 1 {
                    Self::traverse_nested_value(field_value, &path[1..])
                } else {
                    Some(field_value)
                }
            }
            JsonValue::Null => None,
            _ => None, // Can't traverse into non-object values
        }
    }
}

#[async_trait]
impl PlanNode for OrderByNode {
    async fn init(&mut self) -> Result<()> {
        self.buffer.clear();
        self.position = 0;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        // Validate that the first field of all ORDER BY paths exists in the document mapping.
        // For nested paths like ["author", "age"], we only validate "author" exists here;
        // the nested field traversal handles further validation during comparison.
        for condition in &self.order_by.conditions {
            if !condition.fields.is_empty() {
                let first_field = &condition.fields[0];
                if self
                    .document_mapping
                    .first_index_of_name(first_field)
                    .is_none()
                {
                    return Err(QueryError::execution(format!(
                        "ORDER BY field '{}' does not exist in the document schema",
                        first_field
                    )));
                }
            }
        }

        self.source.start().await?;

        // Buffer all documents from source
        while self.source.next().await? {
            self.buffer.push(self.source.value().deep_clone());
        }

        // Extract references for use in closure to avoid borrowing issues
        let order_by = &self.order_by;
        let mapping = &self.document_mapping;

        // Sort the buffer using stable sort (preserves order of equal elements)
        self.buffer
            .sort_by(|a, b| Self::compare_docs_static(a, b, order_by, mapping));

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if self.position >= self.buffer.len() {
            return Ok(false);
        }

        self.current_doc = self.buffer[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.buffer.clear();
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "orderNode"
    }

    fn explain_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        // Go DefraDB format: "orderings" array with { "fields": [...], "direction": "ASC/DESC" }
        let orderings: Vec<JsonValue> = self
            .order_by
            .conditions
            .iter()
            .map(|c| {
                let dir = match c.direction {
                    OrderDirection::Asc => "ASC",
                    OrderDirection::Desc => "DESC",
                };
                serde_json::json!({
                    "fields": c.fields,
                    "direction": dir
                })
            })
            .collect();
        obj.insert("orderings".to_string(), JsonValue::Array(orderings));

        // Recursively explain child node - merge their wrapped structure
        let child_explain = self.source.explain();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        JsonValue::Object(obj)
    }
}
