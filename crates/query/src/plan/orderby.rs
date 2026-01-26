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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ScanNode;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use serde_json::json;

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "age");
        m
    }

    fn make_doc(doc_id: &str, name: &str, age: i64) -> Doc {
        Doc::with_fields(vec![
            Some(json!(doc_id)),
            Some(json!(name)),
            Some(json!(age)),
        ])
    }

    fn make_doc_with_null_age(doc_id: &str, name: &str) -> Doc {
        Doc::with_fields(vec![
            Some(json!(doc_id)),
            Some(json!(name)),
            None, // null age
        ])
    }

    #[tokio::test]
    async fn test_orderby_single_field_asc() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![
            make_doc("doc1", "Charlie", 30),
            make_doc("doc2", "Alice", 25),
            make_doc("doc3", "Bob", 35),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("name", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push(orderby.value().get(1).cloned());
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(json!("Alice")));
        assert_eq!(results[1], Some(json!("Bob")));
        assert_eq!(results[2], Some(json!("Charlie")));
    }

    #[tokio::test]
    async fn test_orderby_single_field_desc() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![
            make_doc("doc1", "Alice", 25),
            make_doc("doc2", "Charlie", 30),
            make_doc("doc3", "Bob", 35),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("name", OrderDirection::Desc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push(orderby.value().get(1).cloned());
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(json!("Charlie")));
        assert_eq!(results[1], Some(json!("Bob")));
        assert_eq!(results[2], Some(json!("Alice")));
    }

    #[tokio::test]
    async fn test_orderby_numeric_field() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![
            make_doc("doc1", "Alice", 30),
            make_doc("doc2", "Bob", 25),
            make_doc("doc3", "Charlie", 35),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("age", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push(orderby.value().get(2).cloned());
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(json!(25)));
        assert_eq!(results[1], Some(json!(30)));
        assert_eq!(results[2], Some(json!(35)));
    }

    #[tokio::test]
    async fn test_orderby_multi_field() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // Same age, different names
        let docs = vec![
            make_doc("doc1", "Charlie", 30),
            make_doc("doc2", "Alice", 30),
            make_doc("doc3", "Bob", 25),
            make_doc("doc4", "Diana", 25),
        ];

        // Order by age ASC, then name ASC
        let order_by = OrderBy::new()
            .with_condition(OrderCondition::new("age", OrderDirection::Asc))
            .with_condition(OrderCondition::new("name", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results: Vec<(Option<JsonValue>, Option<JsonValue>)> = Vec::new();
        while orderby.next().await.unwrap() {
            results.push((
                orderby.value().get(1).cloned(),
                orderby.value().get(2).cloned(),
            ));
        }

        assert_eq!(results.len(), 4);
        // Age 25: Bob, Diana (alphabetical)
        assert_eq!(results[0], (Some(json!("Bob")), Some(json!(25))));
        assert_eq!(results[1], (Some(json!("Diana")), Some(json!(25))));
        // Age 30: Alice, Charlie (alphabetical)
        assert_eq!(results[2], (Some(json!("Alice")), Some(json!(30))));
        assert_eq!(results[3], (Some(json!("Charlie")), Some(json!(30))));
    }

    #[tokio::test]
    async fn test_orderby_null_values_first() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![
            make_doc("doc1", "Alice", 30),
            make_doc_with_null_age("doc2", "Bob"),
            make_doc("doc3", "Charlie", 25),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("age", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push((
                orderby.value().get(1).cloned(),
                orderby.value().get(2).cloned(),
            ));
        }

        assert_eq!(results.len(), 3);
        // Null age should come first
        assert_eq!(results[0], (Some(json!("Bob")), None));
        assert_eq!(results[1], (Some(json!("Charlie")), Some(json!(25))));
        assert_eq!(results[2], (Some(json!("Alice")), Some(json!(30))));
    }

    #[tokio::test]
    async fn test_orderby_null_values_desc() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![
            make_doc("doc1", "Alice", 30),
            make_doc_with_null_age("doc2", "Bob"),
            make_doc("doc3", "Charlie", 25),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("age", OrderDirection::Desc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push((
                orderby.value().get(1).cloned(),
                orderby.value().get(2).cloned(),
            ));
        }

        assert_eq!(results.len(), 3);
        // DESC: 30, 25, null (null is last since we reverse the comparison)
        assert_eq!(results[0], (Some(json!("Alice")), Some(json!(30))));
        assert_eq!(results[1], (Some(json!("Charlie")), Some(json!(25))));
        assert_eq!(results[2], (Some(json!("Bob")), None));
    }

    #[tokio::test]
    async fn test_orderby_empty_source() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("name", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        assert!(!orderby.next().await.unwrap());
    }

    #[tokio::test]
    async fn test_orderby_single_doc() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![make_doc("doc1", "Alice", 30)];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("name", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        assert!(orderby.next().await.unwrap());
        assert_eq!(orderby.value().get(1), Some(&json!("Alice")));
        assert!(!orderby.next().await.unwrap());
    }

    #[tokio::test]
    async fn test_orderby_already_sorted() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // Already sorted by name
        let docs = vec![
            make_doc("doc1", "Alice", 30),
            make_doc("doc2", "Bob", 25),
            make_doc("doc3", "Charlie", 35),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("name", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push(orderby.value().get(1).cloned());
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(json!("Alice")));
        assert_eq!(results[1], Some(json!("Bob")));
        assert_eq!(results[2], Some(json!("Charlie")));
    }

    #[tokio::test]
    async fn test_orderby_reverse_sorted() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // Reverse sorted by name
        let docs = vec![
            make_doc("doc1", "Charlie", 35),
            make_doc("doc2", "Bob", 25),
            make_doc("doc3", "Alice", 30),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("name", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push(orderby.value().get(1).cloned());
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(json!("Alice")));
        assert_eq!(results[1], Some(json!("Bob")));
        assert_eq!(results[2], Some(json!("Charlie")));
    }

    #[test]
    fn test_compare_values_nulls() {
        // Both null
        assert_eq!(OrderByNode::compare_values(None, None), Ordering::Equal);
        assert_eq!(
            OrderByNode::compare_values(Some(&JsonValue::Null), Some(&JsonValue::Null)),
            Ordering::Equal
        );

        // Null vs non-null
        assert_eq!(
            OrderByNode::compare_values(None, Some(&json!(42))),
            Ordering::Less
        );
        assert_eq!(
            OrderByNode::compare_values(Some(&json!(42)), None),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_values_numbers() {
        assert_eq!(
            OrderByNode::compare_values(Some(&json!(10)), Some(&json!(20))),
            Ordering::Less
        );
        assert_eq!(
            OrderByNode::compare_values(Some(&json!(20)), Some(&json!(10))),
            Ordering::Greater
        );
        assert_eq!(
            OrderByNode::compare_values(Some(&json!(10)), Some(&json!(10))),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_values_strings() {
        assert_eq!(
            OrderByNode::compare_values(Some(&json!("alice")), Some(&json!("bob"))),
            Ordering::Less
        );
        assert_eq!(
            OrderByNode::compare_values(Some(&json!("bob")), Some(&json!("alice"))),
            Ordering::Greater
        );
        assert_eq!(
            OrderByNode::compare_values(Some(&json!("alice")), Some(&json!("alice"))),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_values_booleans() {
        assert_eq!(
            OrderByNode::compare_values(Some(&json!(false)), Some(&json!(true))),
            Ordering::Less
        );
        assert_eq!(
            OrderByNode::compare_values(Some(&json!(true)), Some(&json!(false))),
            Ordering::Greater
        );
    }

    #[test]
    fn test_kind() {
        let mapping = make_test_mapping();
        let collection = make_test_collection();
        let order_by = OrderBy::new();
        let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
        let orderby = OrderByNode::new(Box::new(scan), order_by, mapping);
        assert_eq!(orderby.kind(), "orderNode"); // Go DefraDB compatible name
    }

    #[tokio::test]
    async fn test_orderby_nonexistent_field_returns_error() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![make_doc("doc1", "Alice", 30), make_doc("doc2", "Bob", 25)];

        // Order by a field that doesn't exist in the mapping
        let order_by = OrderBy::new().with_condition(OrderCondition::new(
            "nonexistent_field",
            OrderDirection::Asc,
        ));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        let result = orderby.start().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("ORDER BY field 'nonexistent_field' does not exist"),
            "Expected error about nonexistent field, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_orderby_stable_sort_preserves_insertion_order() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // All documents have the same age (30) but different names and doc IDs
        // The stable sort should preserve their original insertion order
        let docs = vec![
            make_doc("doc1", "First", 30),
            make_doc("doc2", "Second", 30),
            make_doc("doc3", "Third", 30),
            make_doc("doc4", "Fourth", 30),
        ];

        // Order by age - all ages are equal, so stable sort should preserve order
        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("age", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push(orderby.value().get(0).cloned()); // Get doc_id
        }

        assert_eq!(results.len(), 4);
        // Verify original insertion order is preserved for equal sort keys
        assert_eq!(results[0], Some(json!("doc1")));
        assert_eq!(results[1], Some(json!("doc2")));
        assert_eq!(results[2], Some(json!("doc3")));
        assert_eq!(results[3], Some(json!("doc4")));
    }

    #[tokio::test]
    async fn test_orderby_multiple_null_values() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        // Multiple documents with null ages
        let docs = vec![
            make_doc_with_null_age("doc1", "Alice"),
            make_doc("doc2", "Bob", 30),
            make_doc_with_null_age("doc3", "Charlie"),
            make_doc("doc4", "Diana", 25),
        ];

        let order_by =
            OrderBy::new().with_condition(OrderCondition::new("age", OrderDirection::Asc));

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let mut orderby = OrderByNode::new(Box::new(scan), order_by, mapping);

        orderby.init().await.unwrap();
        orderby.start().await.unwrap();

        let mut results = Vec::new();
        while orderby.next().await.unwrap() {
            results.push((
                orderby.value().get(0).cloned(), // doc_id
                orderby.value().get(2).cloned(), // age
            ));
        }

        assert_eq!(results.len(), 4);
        // Nulls first (in original order due to stable sort), then sorted by age
        assert_eq!(results[0], (Some(json!("doc1")), None)); // null
        assert_eq!(results[1], (Some(json!("doc3")), None)); // null
        assert_eq!(results[2], (Some(json!("doc4")), Some(json!(25)))); // 25
        assert_eq!(results[3], (Some(json!("doc2")), Some(json!(30)))); // 30
    }
}
