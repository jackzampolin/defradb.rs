//! Multi-level filter path join helpers.
//!
//! Handles building join chains for deep filter paths like
//! `{author: {published: {rating: {_eq: 4.9}}}}`.

use std::sync::Arc;

use schema::CollectionVersion;

use crate::plan::{JoinSide, ScanNode, TypeJoinMany, TypeJoinOne};
use crate::planner::PlanNode;
use query_types::document::DocumentMapping;
use query_types::error::{QueryError, Result};

use super::super::builder::Planner;

impl Planner {
    /// Apply sub-joins for remaining elements of a multi-level filter path.
    ///
    /// This is called when processing a relation that's the start of a multi-level filter path.
    /// For example, when processing the "author" relation and the filter has path ["author", "published"],
    /// this method adds a sub-join for "published" within the author's child plan.
    ///
    /// Returns both the updated plan and the updated mapping, since the mapping must be modified
    /// to include the child mappings for the nested relations.
    pub(super) fn apply_multi_level_sub_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        remaining_path: &[String],
        parent_collection: &CollectionVersion,
        mut mapping: DocumentMapping,
    ) -> Result<(Box<dyn PlanNode>, DocumentMapping)> {
        if remaining_path.is_empty() {
            return Ok((plan, mapping));
        }

        let mut current_collection = parent_collection.clone();

        // Build sub-joins for each remaining element in the path
        for relation_field_name in remaining_path {
            // Find the relation field in the current collection
            let relation_field = current_collection
                .field_by_name(relation_field_name)
                .ok_or_else(|| QueryError::unknown_field(relation_field_name))?;

            if !relation_field.kind.is_relation() {
                return Err(QueryError::execution(format!(
                    "field '{}' on collection '{}' is not a relation",
                    relation_field_name, current_collection.name
                )));
            }

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
                Arc::new(current_collection.clone())
            } else {
                self.get_collection(target_collection_id)
                    .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
            };

            // Build a child scan mapping with all fields for filter evaluation
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
                .ok_or_else(|| {
                    QueryError::internal(format!(
                        "relation field '{}' not in mapping",
                        relation_field_name
                    ))
                })?;

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
            child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

            // Find the other side of the relation
            let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                target_collection.field_by_relation(
                    rel_name,
                    &current_collection.name,
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
                current_collection.clone(),
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

            // Create TypeJoinOne for the sub-join
            let join = TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone());

            plan = Box::new(join);

            // Update current collection for next iteration
            current_collection = (*target_collection).clone();
        }

        Ok((plan, mapping))
    }

    /// Apply joins for a multi-level relation filter path.
    ///
    /// For a path like ["author", "published"] with filter {author: {published: {rating: {_eq: 4.9}}}},
    /// this builds a chain of TypeJoin nodes:
    /// 1. Join parent (Book) → first relation (Author) via "author"
    /// 2. Join first relation (Author) → second relation (Book) via "published"
    /// 3. Apply the scalar filter (rating == 4.9) at the innermost level
    ///
    /// The filter is extracted at each level of the path and applied to the appropriate join.
    pub(in crate::planner) fn apply_multi_level_filter_joins(
        &self,
        plan: Box<dyn PlanNode>,
        path: &[String],
        start_collection: &CollectionVersion,
        _filter: &query_types::mapper::Filter,
        mut mapping: DocumentMapping,
    ) -> Result<(Box<dyn PlanNode>, DocumentMapping)> {
        if path.is_empty() {
            return Ok((plan, mapping));
        }

        // Handle only the first level here; remaining levels are nested sub-joins
        // within the first level's child plan.
        let first_name = &path[0];
        let first_field = start_collection
            .field_by_name(first_name)
            .ok_or_else(|| QueryError::unknown_field(first_name))?;

        if !first_field.kind.is_relation() {
            return Err(QueryError::execution(format!(
                "field '{}' on collection '{}' is not a relation",
                first_name, start_collection.name
            )));
        }

        let target_collection_id = first_field.kind.relation_collection_id().ok_or_else(|| {
            QueryError::internal(format!(
                "relation field '{}' has no target collection",
                first_name
            ))
        })?;

        let target_collection = if target_collection_id.is_empty() {
            Arc::new(start_collection.clone())
        } else {
            self.get_collection(target_collection_id)
                .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
        };

        // Build child scan mapping with all fields for filter evaluation
        let mut child_scan_mapping = {
            let mut m = DocumentMapping::new();
            for (i, field) in target_collection.fields.iter().enumerate() {
                m.add(i, &field.name);
                m.add_render_key(i, &field.name);
            }
            m
        };

        let relation_field_index = mapping.first_index_of_name(first_name).ok_or_else(|| {
            QueryError::internal(format!(
                "relation field '{}' not in parent mapping",
                first_name
            ))
        })?;

        // Build child scan
        let mut child_scan =
            ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
        if let Some(ref fetcher) = self.fetcher {
            child_scan = child_scan.with_fetcher(fetcher.clone());
        }
        let mut child_plan: Box<dyn PlanNode> = Box::new(child_scan);

        // Insert ACP permission filter for the child collection (if ACP-protected).
        child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

        // Add sub-joins for remaining path levels within the child plan
        if path.len() > 1 {
            let remaining = &path[1..];
            let (new_plan, new_mapping) = self.apply_multi_level_sub_joins(
                child_plan,
                remaining,
                &target_collection,
                child_scan_mapping,
            )?;
            child_plan = new_plan;
            child_scan_mapping = new_mapping;
        }

        // Set child mapping in parent
        mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

        // Find the other side of the relation
        let target_relation_field = if let Some(rel_name) = &first_field.relation_name {
            target_collection.field_by_relation(rel_name, &start_collection.name, first_name)
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

        let parent_side = JoinSide::new(
            start_collection.clone(),
            first_field.clone(),
            relation_field_index,
        )?;

        let child_side = JoinSide::new(
            (*target_collection).clone(),
            target_relation_field
                .cloned()
                .unwrap_or_else(|| first_field.clone()),
            child_relation_index,
        )?;

        // Create appropriate join based on relation cardinality
        let new_plan = if first_field.kind.is_array() {
            Box::new(TypeJoinMany::new(
                plan,
                child_plan,
                parent_side,
                child_side,
                mapping.clone(),
            )?) as Box<dyn PlanNode>
        } else {
            Box::new(TypeJoinOne::new(
                plan,
                child_plan,
                parent_side,
                child_side,
                mapping.clone(),
            )) as Box<dyn PlanNode>
        };

        Ok((new_plan, mapping))
    }
}
