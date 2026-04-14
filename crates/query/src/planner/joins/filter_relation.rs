//! Single filter-relation join helper.
//!
//! Creates a TypeJoinOne/TypeJoinMany for a relation field that's in the filter
//! but not in the selection set, so that complex filters can evaluate conditions
//! on the relation.

use std::sync::Arc;

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::plan::{JoinSide, ScanNode, TypeJoinMany, TypeJoinOne};
use crate::planner::PlanNode;

use super::super::builder::Planner;

impl Planner {
    /// Apply a join for a relation field that's in the filter but not in the selection.
    ///
    /// This creates a TypeJoinOne that brings in the child documents so that complex
    /// filters can evaluate conditions on the relation. The child documents are merged
    /// into the parent but won't appear in the final output (no render_key).
    ///
    /// Returns both the updated plan and mapping. The mapping is updated with child mappings
    /// for the relation field so that post-join filter evaluation can traverse into the
    /// merged child document.
    pub(in crate::planner) fn apply_filter_relation_join(
        &self,
        plan: Box<dyn PlanNode>,
        parent_collection: &CollectionVersion,
        relation_field: &schema::FieldDescription,
        relation_field_name: &str,
        mut mapping: DocumentMapping,
    ) -> Result<(Box<dyn PlanNode>, DocumentMapping)> {
        // Get the target collection for this relation
        let target_collection_id =
            relation_field
                .kind
                .relation_collection_id()
                .ok_or_else(|| {
                    QueryError::internal(format!(
                        "relation field '{}' has no target collection",
                        relation_field_name
                    ))
                })?;

        let target_collection = if target_collection_id.is_empty() {
            Arc::new(parent_collection.clone())
        } else {
            self.get_collection(target_collection_id)
                .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
        };

        // Build a child scan mapping with all fields for filter evaluation.
        // We MUST include render_keys for all fields so that when the child doc
        // is merged via render_doc_to_json(), the fields are present in the JSON.
        // The filter will then be able to evaluate conditions on those fields.
        // The relation field won't appear in the final output because the parent
        // mapping doesn't have a render_key for it.
        let child_scan_mapping = {
            let mut m = DocumentMapping::new();
            for (i, field) in target_collection.fields.iter().enumerate() {
                m.add(i, &field.name);
                // Add render_key so field appears in merged JSON for filter evaluation
                m.add_render_key(i, &field.name);
            }
            m
        };

        // Get the relation field index in the parent mapping
        let relation_field_index = mapping
            .first_index_of_name(relation_field_name)
            .ok_or_else(|| QueryError::internal("relation field not in parent mapping"))?;

        // Set up child mapping in parent for TypeJoin
        mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

        // Create the child scan plan
        let mut child_scan =
            ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
        if let Some(ref fetcher) = self.fetcher {
            child_scan = child_scan.with_fetcher(fetcher.clone());
        }
        let mut child_plan: Box<dyn PlanNode> = Box::new(child_scan);

        // Insert ACP permission filter for the child collection (if ACP-protected).
        child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection, None);

        // Find the other side of the relation
        let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
            target_collection.field_by_relation(
                rel_name,
                &parent_collection.name,
                relation_field_name,
            )
        } else {
            None
        };

        let child_relation_index = target_relation_field
            .and_then(|f| {
                target_collection
                    .fields
                    .iter()
                    .position(|tf| tf.name == f.name)
            })
            .unwrap_or(0);

        // Create join sides
        let parent_side = JoinSide::new(
            parent_collection.clone(),
            relation_field.clone(),
            relation_field_index,
        )?;

        let child_side = JoinSide::new(
            (*target_collection).clone(),
            target_relation_field
                .cloned()
                .unwrap_or_else(|| relation_field.clone()),
            child_relation_index,
        )?;

        // Create the appropriate join type based on the relation cardinality.
        // One-to-many relations need TypeJoinMany to collect all children into an array,
        // one-to-one relations use TypeJoinOne.
        if relation_field.kind.is_array() {
            let join_many =
                TypeJoinMany::new(plan, child_plan, parent_side, child_side, mapping.clone())?;
            Ok((Box::new(join_many), mapping))
        } else {
            let join = TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone());
            Ok((Box::new(join), mapping))
        }
    }
}
