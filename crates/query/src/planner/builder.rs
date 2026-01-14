//! Query planner implementation
//!
//! Converts Select operations into executable plan trees.

use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::{LimitNode, ScanNode, SelectNode};
use crate::planner::PlanNode;

/// Query planner that builds execution plans from Select operations.
pub struct Planner {
    /// Available collection schemas by name
    collections: HashMap<String, Arc<CollectionVersion>>,
}

impl Planner {
    /// Create a new planner with the given collection schemas.
    pub fn new(collections: Vec<CollectionVersion>) -> Self {
        let collections = collections
            .into_iter()
            .map(|c| (c.name.clone(), Arc::new(c)))
            .collect();
        Self { collections }
    }

    /// Build an execution plan from a Select operation.
    pub fn plan(&self, select: &Select) -> Result<Box<dyn PlanNode>> {
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?
            .clone();

        // Build the document mapping for this query
        let mapping = self.build_mapping(select, &collection)?;

        // Build the plan tree bottom-up:
        // ScanNode -> SelectNode -> LimitNode

        // 1. ScanNode - reads from storage
        let mut plan: Box<dyn PlanNode> = Box::new(
            ScanNode::new((*collection).clone(), mapping.clone())
                .with_show_deleted(select.show_deleted),
        );

        // 2. Apply filter if present
        if select.filter.is_some() || !select.fields.is_empty() {
            let mut select_node = SelectNode::new(plan, mapping.clone());
            if let Some(ref filter) = select.filter {
                select_node = select_node.with_filter(filter.clone());
            }
            plan = Box::new(select_node);
        }

        // 3. Apply limit/offset if present
        if let Some(ref limit) = select.limit {
            plan = Box::new(LimitNode::new(plan, limit.limit, limit.offset));
        }

        Ok(plan)
    }

    /// Build the document mapping for a Select operation.
    fn build_mapping(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Add all requested fields
        for requestable in &select.fields {
            match requestable {
                Requestable::Field(field) => {
                    // Look up field in schema (validates it exists)
                    let _field_def = collection.field_by_name(&field.name);
                    let index = mapping.next_index();

                    mapping.add(index, &field.name);
                    mapping.add_render_key(index, field.output_name());
                }
                Requestable::Select(nested_select) => {
                    // Nested select (relation) - add placeholder for now
                    let index = mapping.next_index();
                    mapping.add(index, &nested_select.field.name);
                    mapping.add_render_key(index, nested_select.field.output_name());
                }
                Requestable::Aggregate(_agg) => {
                    // Aggregates handled separately
                }
            }
        }

        // If no fields specified, add all collection fields
        if mapping.next_index() == 0 {
            for (i, field) in collection.fields.iter().enumerate() {
                mapping.add(i, &field.name);
                mapping.add_render_key(i, &field.name);
            }
        }

        Ok(mapping)
    }

    /// Get a collection schema by name.
    pub fn collection(&self, name: &str) -> Option<&Arc<CollectionVersion>> {
        self.collections.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapper::{Field, Filter};
    use schema::{FieldDescription, FieldKind};

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    #[test]
    fn test_planner_new() {
        let planner = Planner::new(vec![make_test_collection()]);
        assert!(planner.collection("Users").is_some());
        assert!(planner.collection("Posts").is_none());
    }

    #[tokio::test]
    async fn test_plan_simple_select() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Users")
            .with_field(Field::new("_docID"))
            .with_field(Field::new("name"));

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_with_limit() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_limit(10);

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "limitNode");
    }

    #[tokio::test]
    async fn test_plan_unknown_collection() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Posts").with_field(Field::new("title"));

        let result = planner.plan(&select);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plan_with_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "selectNode");
    }

    #[test]
    fn test_build_mapping() {
        let planner = Planner::new(vec![make_test_collection()]);
        let collection = planner.collection("Users").unwrap();

        let select = Select::new("Users")
            .with_field(Field::new("_docID"))
            .with_field(Field::new("name"));

        let mapping = planner.build_mapping(&select, collection).unwrap();

        assert!(mapping.has_field("_docID"));
        assert!(mapping.has_field("name"));
        assert!(!mapping.has_field("age"));
    }

    #[test]
    fn test_build_mapping_with_alias() {
        let planner = Planner::new(vec![make_test_collection()]);
        let collection = planner.collection("Users").unwrap();

        let select = Select::new("Users").with_field(Field::with_alias("name", "userName"));

        let mapping = planner.build_mapping(&select, collection).unwrap();

        assert!(mapping.has_field("name"));
        // Should have render key "userName"
        assert_eq!(mapping.render_keys.len(), 1);
        assert_eq!(mapping.render_keys[0].key, "userName");
    }
}
