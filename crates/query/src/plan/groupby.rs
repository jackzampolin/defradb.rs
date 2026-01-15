//! GroupByNode for grouping query results

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::GroupBy;
use crate::planner::{Doc, PlanNode};

/// A group of documents with the same group key
#[derive(Debug)]
pub struct DocumentGroup {
    /// The documents in this group
    pub docs: Vec<Doc>,
    /// The representative document (first doc) for this group
    pub representative: Doc,
}

impl DocumentGroup {
    fn new(first_doc: Doc) -> Self {
        Self {
            representative: first_doc.deep_clone(),
            docs: vec![first_doc],
        }
    }

    fn add(&mut self, doc: Doc) {
        self.docs.push(doc);
    }
}

/// GroupByNode groups documents by specified fields.
///
/// This node buffers all documents from its source during `start()`,
/// groups them by the specified fields, then yields one document per group.
/// Each yielded document is the representative (first) document from each group.
///
/// Follows Go DefraDB pattern:
/// - Group key is generated from field values (format: `{index}_{value}_`)
/// - Groups are stored in insertion order (first group created is first yielded)
/// - Hidden documents are included in grouping
pub struct GroupByNode {
    source: Box<dyn PlanNode>,
    group_by: GroupBy,
    document_mapping: DocumentMapping,
    /// Groups keyed by their group key string
    groups: Vec<(String, DocumentGroup)>,
    /// Current position in groups
    position: usize,
    /// Current document
    current_doc: Doc,
    /// Whether start() has been called
    started: bool,
}

impl GroupByNode {
    /// Create a new GroupByNode
    pub fn new(
        source: Box<dyn PlanNode>,
        group_by: GroupBy,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            source,
            group_by,
            document_mapping,
            groups: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            started: false,
        }
    }

    /// Get the groups (for aggregation nodes to access)
    pub fn groups(&self) -> &[(String, DocumentGroup)] {
        &self.groups
    }

    /// Generate a group key from document field values
    /// Format: `{field_index}_{field_value}_` for each GROUP BY field
    fn generate_key(&self, doc: &Doc) -> String {
        let mut key = String::new();
        for field_name in &self.group_by.fields {
            if let Some(index) = self.document_mapping.first_index_of_name(field_name) {
                key.push_str(&format!("{}_", index));
                let value = doc.get(index);
                key.push_str(&format!("{}_", Self::value_to_key(value)));
            }
        }
        key
    }

    /// Convert a JSON value to a string key component
    fn value_to_key(value: Option<&JsonValue>) -> String {
        match value {
            None | Some(JsonValue::Null) => "null".to_string(),
            Some(JsonValue::Bool(b)) => b.to_string(),
            Some(JsonValue::Number(n)) => n.to_string(),
            Some(JsonValue::String(s)) => s.clone(),
            Some(JsonValue::Array(arr)) => {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(|v| Self::value_to_key(Some(v)))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Some(JsonValue::Object(obj)) => {
                format!(
                    "{{{}}}",
                    obj.iter()
                        .map(|(k, v)| format!("{}:{}", k, Self::value_to_key(Some(v))))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }
}

#[async_trait]
impl PlanNode for GroupByNode {
    async fn init(&mut self) -> Result<()> {
        self.groups.clear();
        self.position = 0;
        self.started = false;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;
        self.started = true;

        // Buffer all documents and group them
        let mut group_map: HashMap<String, usize> = HashMap::new();

        while self.source.next().await? {
            let doc = self.source.value().deep_clone();
            let key = self.generate_key(&doc);

            if let Some(&idx) = group_map.get(&key) {
                self.groups[idx].1.add(doc);
            } else {
                let idx = self.groups.len();
                group_map.insert(key.clone(), idx);
                self.groups.push((key, DocumentGroup::new(doc)));
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.started {
            self.start().await?;
        }

        if self.position >= self.groups.len() {
            return Ok(false);
        }

        // Return the representative document for the current group
        self.current_doc = self.groups[self.position].1.representative.deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "groupByNode"
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        // Position is incremented after next(), so position-1 is the current group
        if self.position > 0 && self.position <= self.groups.len() {
            Some(&self.groups[self.position - 1].1.docs)
        } else {
            None
        }
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
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "department", FieldKind::string()),
                FieldDescription::new("4", "age", FieldKind::int()),
            ],
        )
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "department");
        mapping.add(3, "age");
        mapping.add_render_key(0, "_docID");
        mapping.add_render_key(1, "name");
        mapping.add_render_key(2, "department");
        mapping.add_render_key(3, "age");
        mapping
    }

    fn make_test_docs() -> Vec<Doc> {
        vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Alice")),
                Some(json!("Engineering")),
                Some(json!(30)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Bob")),
                Some(json!("Sales")),
                Some(json!(25)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Charlie")),
                Some(json!("Engineering")),
                Some(json!(35)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc4")),
                Some(json!("Diana")),
                Some(json!("Sales")),
                Some(json!(28)),
            ]),
        ]
    }

    #[tokio::test]
    async fn test_group_by_single_field() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let group_by = GroupBy::new(vec!["department".to_string()]);
        let mut node = GroupByNode::new(Box::new(scan), group_by, mapping);

        node.init().await.unwrap();

        // Should get 2 groups (Engineering and Sales)
        assert!(node.next().await.unwrap());
        let doc1 = node.value().deep_clone();

        assert!(node.next().await.unwrap());
        let doc2 = node.value().deep_clone();

        // No more groups
        assert!(!node.next().await.unwrap());

        // Get department values
        let dept1 = doc1.get(2).and_then(|v| v.as_str()).unwrap();
        let dept2 = doc2.get(2).and_then(|v| v.as_str()).unwrap();

        // Both Engineering and Sales should be present
        let depts: Vec<&str> = vec![dept1, dept2];
        assert!(depts.contains(&"Engineering"));
        assert!(depts.contains(&"Sales"));

        node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_group_by_preserves_groups() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        let docs = make_test_docs();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let group_by = GroupBy::new(vec!["department".to_string()]);
        let mut node = GroupByNode::new(Box::new(scan), group_by, mapping);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // Check that groups have correct document counts
        assert_eq!(node.groups().len(), 2);

        // Find Engineering group
        let eng_group = node
            .groups()
            .iter()
            .find(|(_, g)| g.representative.get(2).and_then(|v| v.as_str()) == Some("Engineering"));
        assert!(eng_group.is_some());
        assert_eq!(eng_group.unwrap().1.docs.len(), 2); // Alice and Charlie

        // Find Sales group
        let sales_group = node
            .groups()
            .iter()
            .find(|(_, g)| g.representative.get(2).and_then(|v| v.as_str()) == Some("Sales"));
        assert!(sales_group.is_some());
        assert_eq!(sales_group.unwrap().1.docs.len(), 2); // Bob and Diana

        node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_group_by_empty_source() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
        let group_by = GroupBy::new(vec!["department".to_string()]);
        let mut node = GroupByNode::new(Box::new(scan), group_by, mapping);

        node.init().await.unwrap();

        // No groups from empty source
        assert!(!node.next().await.unwrap());

        node.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_group_by_handles_null_values() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let docs = vec![
            Doc::with_fields(vec![
                Some(json!("doc1")),
                Some(json!("Alice")),
                Some(json!("Engineering")),
                Some(json!(30)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc2")),
                Some(json!("Bob")),
                None, // null department
                Some(json!(25)),
            ]),
            Doc::with_fields(vec![
                Some(json!("doc3")),
                Some(json!("Charlie")),
                None, // null department
                Some(json!(35)),
            ]),
        ];

        let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
        let group_by = GroupBy::new(vec!["department".to_string()]);
        let mut node = GroupByNode::new(Box::new(scan), group_by, mapping);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // Should have 2 groups: Engineering and null
        assert_eq!(node.groups().len(), 2);

        node.close().await.unwrap();
    }
}
