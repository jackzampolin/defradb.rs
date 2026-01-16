//! Query planner implementation
//!
//! Converts Select operations into executable plan trees.

use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::{IndexScanNode, LimitNode, ScanNode, SelectNode};
use crate::planner::index_selection::{filter_to_index_scan, select_best_index, IndexScanParams};
use crate::planner::PlanNode;

/// Result of planning a query, containing both the plan and optional index scan info.
pub struct PlanResult {
    /// The execution plan
    pub plan: Box<dyn PlanNode>,
    /// Index scan parameters if an index will be used
    pub index_scan: Option<IndexScanParams>,
}

impl PlanResult {
    /// Check if this plan uses an index scan
    pub fn uses_index(&self) -> bool {
        self.index_scan.is_some()
    }
}

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
    ///
    /// This method returns only the plan for backwards compatibility.
    /// Use `plan_with_index_info` to also get index scan information.
    pub fn plan(&self, select: &Select) -> Result<Box<dyn PlanNode>> {
        Ok(self.plan_with_index_info(select)?.plan)
    }

    /// Build an execution plan with index scan information.
    ///
    /// Returns a `PlanResult` containing both the plan and optional `IndexScanParams`
    /// when an index can be used to optimize the query.
    pub fn plan_with_index_info(&self, select: &Select) -> Result<PlanResult> {
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?
            .clone();

        // Build the document mapping for this query
        let mapping = self.build_mapping(select, &collection)?;

        // Check if an index can be used for the filter
        let index_scan = self.try_select_index(select, &collection);

        // Build the plan tree bottom-up:
        // ScanNode/IndexScanNode -> SelectNode -> LimitNode

        // 1. Choose between IndexScanNode and ScanNode based on index availability
        let mut plan: Box<dyn PlanNode> = if let Some(ref params) = index_scan {
            Box::new(
                IndexScanNode::new((*collection).clone(), mapping.clone(), params.clone())
                    .with_show_deleted(select.show_deleted),
            )
        } else {
            Box::new(
                ScanNode::new((*collection).clone(), mapping.clone())
                    .with_show_deleted(select.show_deleted),
            )
        };

        // 2. Apply filter if present (for ScanNode) or residual filter (for IndexScanNode)
        // Note: Even with IndexScanNode, we may need a SelectNode for:
        //   - Field projection
        //   - Conditions not covered by the index
        if select.filter.is_some() || !select.fields.is_empty() {
            let mut select_node = SelectNode::new(plan, mapping.clone());
            if let Some(ref filter) = select.filter {
                // If using index scan, the index handles the primary filter condition
                // but we still apply the full filter in SelectNode for conditions
                // not covered by the index (composite indexes, etc.)
                select_node = select_node.with_filter(filter.clone());
            }
            plan = Box::new(select_node);
        }

        // 3. Apply limit/offset if present
        if let Some(ref limit) = select.limit {
            plan = Box::new(LimitNode::new(plan, limit.limit, limit.offset));
        }

        Ok(PlanResult { plan, index_scan })
    }

    /// Try to select an index for the given query.
    ///
    /// Returns `Some(IndexScanParams)` if an index can be used, `None` otherwise.
    fn try_select_index(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Option<IndexScanParams> {
        // Only try index selection if there's a filter
        let filter = select.filter.as_ref()?;

        // Get available indexes for this collection
        if collection.indexes.is_empty() {
            return None;
        }

        // Select the best index for this filter
        let best_index = select_best_index(filter, &collection.indexes)?;

        // Convert filter to index scan parameters
        filter_to_index_scan(filter, best_index)
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
                    // Validate field exists in schema (skip _docID which is always valid)
                    if field.name != "_docID" && collection.field_by_name(&field.name).is_none() {
                        return Err(QueryError::unknown_field(&field.name));
                    }
                    let index = mapping.next_index();

                    mapping.add(index, &field.name);
                    mapping.add_render_key(index, field.output_name());
                }
                Requestable::Select(nested_select) => {
                    // Nested select (relation)
                    let index = mapping.next_index();
                    mapping.add(index, &nested_select.field.name);
                    mapping.add_render_key(index, nested_select.field.output_name());
                }
                Requestable::Aggregate(agg) => {
                    return Err(QueryError::execution(format!(
                        "aggregate '{:?}' not yet implemented",
                        agg.aggregate_type
                    )));
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
    use crate::planner::index_selection::IndexScanType;
    use schema::{FieldDescription, FieldKind, IndexDescription, IndexedFieldDescription};

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

    fn make_test_collection_with_index() -> CollectionVersion {
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
        .with_index(IndexDescription {
            id: 1,
            name: "name_idx".to_string(),
            unique: false,
            fields: vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }],
        })
        .with_index(IndexDescription {
            id: 2,
            name: "age_idx".to_string(),
            unique: false,
            fields: vec![IndexedFieldDescription {
                name: "age".to_string(),
                descending: false,
            }],
        })
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

    // === Index-Aware Planning Tests ===

    #[tokio::test]
    async fn test_plan_uses_index_for_eq_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // Should use index
        assert!(result.uses_index());
        assert_eq!(result.index_scan.as_ref().unwrap().index_name, "name_idx");

        // Plan should have indexScanNode at the leaf
        // (wrapped by selectNode for field projection)
        assert_eq!(result.plan.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_uses_index_for_range_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "age".to_string(),
            serde_json::json!({"_gte": 18, "_lt": 65}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("age"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // Should use age index
        assert!(result.uses_index());
        assert_eq!(result.index_scan.as_ref().unwrap().index_name, "age_idx");

        // Verify it's a range scan
        match &result.index_scan.as_ref().unwrap().scan_type {
            IndexScanType::RangeScan { .. } => {}
            _ => panic!("expected RangeScan"),
        }
    }

    #[tokio::test]
    async fn test_plan_no_index_without_filter() {
        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let select = Select::new("Users").with_field(Field::new("name"));

        let result = planner.plan_with_index_info(&select).unwrap();

        // No filter, so no index should be used
        assert!(!result.uses_index());
    }

    #[tokio::test]
    async fn test_plan_no_index_for_non_indexed_field() {
        use std::collections::HashMap;

        // Collection without indexes
        let planner = Planner::new(vec![make_test_collection()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // No indexes available, so shouldn't use index
        assert!(!result.uses_index());
    }

    #[tokio::test]
    async fn test_plan_no_index_for_ne_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        // _ne is not index-friendly
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_ne": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // _ne cannot use index efficiently
        assert!(!result.uses_index());
    }

    #[tokio::test]
    async fn test_plan_uses_index_for_in_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_in": ["Alice", "Bob"]}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // _in can use index
        assert!(result.uses_index());
        assert_eq!(result.index_scan.as_ref().unwrap().index_name, "name_idx");

        match &result.index_scan.as_ref().unwrap().scan_type {
            IndexScanType::InScan { values } => {
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected InScan"),
        }
    }

    #[tokio::test]
    async fn test_plan_result_uses_index_method() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        // With index
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));
        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);
        let result = planner.plan_with_index_info(&select).unwrap();
        assert!(result.uses_index());

        // Without index (no filter)
        let select_no_filter = Select::new("Users").with_field(Field::new("name"));
        let result_no_filter = planner.plan_with_index_info(&select_no_filter).unwrap();
        assert!(!result_no_filter.uses_index());
    }
}
