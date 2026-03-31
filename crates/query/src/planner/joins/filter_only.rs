//! Filter-only relation joins.
//!
//! Handles relation filters without corresponding selections — creates
//! TypeJoinOne/TypeJoinMany with RelationFilter when a filter references
//! a relation that isn't selected.

use std::sync::Arc;

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::plan::{IndexScanNode, JoinSide, RelationFilter, ScanNode, TypeJoinMany, TypeJoinOne};
use crate::planner::PlanNode;

use super::super::builder::Planner;

impl Planner {
    /// Handle relation filters without corresponding selections.
    ///
    /// When a filter references a relation (e.g., `Author(filter: {published: {_docID: ...}})`),
    /// but the relation is not selected, we still need to create a join to filter the parent.
    pub(super) fn apply_filter_only_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        mapping: &mut DocumentMapping,
        select: &Select,
        parent_collection: &CollectionVersion,
        parent_filter: Option<&crate::mapper::Filter>,
    ) -> Result<Box<dyn PlanNode>> {
        let filter = match parent_filter {
            Some(f) => f,
            None => return Ok(plan),
        };

        // Get relations referenced by the filter
        for (relation_name, nested_conditions) in filter.relation_conditions() {
            // Skip if already joined via selection
            let already_joined = select
                .fields
                .iter()
                .any(|f| matches!(f, Requestable::Select(s) if s.field.name == relation_name));
            if already_joined {
                continue;
            }

            // Find the relation field in the parent collection
            let relation_field = match parent_collection.field_by_name(&relation_name) {
                Some(f) if f.kind.is_relation() => f,
                _ => continue, // Not a relation field
            };

            // Get the target collection
            let target_collection_id = match relation_field.kind.relation_collection_id() {
                Some(id) => id,
                None => continue,
            };

            let target_collection = if target_collection_id.is_empty() {
                Arc::new(parent_collection.clone())
            } else {
                match self.get_collection(target_collection_id) {
                    Some(c) => c,
                    None => continue,
                }
            };

            // Find the target relation field (the other side of the relation)
            let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                target_collection.field_by_relation(
                    rel_name,
                    &parent_collection.name,
                    &relation_name,
                )
            } else {
                None
            };

            // Build child mapping for filter-only join
            // Include _docID and the FK field for the join to work correctly
            let mut child_mapping = DocumentMapping::new();
            child_mapping.add(0, "_docID");
            child_mapping.add_render_key(0, "_docID");

            // Add the FK field (e.g., _authorID) - needed for TypeJoinMany cache indexing
            let fk_field_name = if let Some(target_rel) = target_relation_field {
                schema::CollectionVersion::relation_id_field_name(&target_rel.name)
            } else {
                schema::CollectionVersion::relation_id_field_name(&relation_name)
            };
            if let Some(fk_idx) = target_collection
                .fields
                .iter()
                .position(|f| f.name == fk_field_name)
            {
                child_mapping.add(fk_idx, &fk_field_name);
                child_mapping.add_render_key(fk_idx, &fk_field_name);
            }

            // Add any fields referenced by the filter
            for field_name in nested_conditions.referenced_fields() {
                if field_name != "_docID" && field_name != fk_field_name {
                    if let Some(idx) = target_collection
                        .fields
                        .iter()
                        .position(|f| f.name == field_name)
                    {
                        child_mapping.add(idx, &field_name);
                        child_mapping.add_render_key(idx, &field_name);
                    }
                }
            }

            // Build child scan, using an index if the filter is index-eligible.
            let child_index_result =
                self.try_select_child_index(Some(&nested_conditions), None, &target_collection);
            let child_has_index = child_index_result.is_some();
            let child_plan: Box<dyn PlanNode> = if let Some((params, _)) = child_index_result {
                let mut index_scan =
                    IndexScanNode::new((*target_collection).clone(), child_mapping.clone(), params);
                if let Some(ref fetcher) = self.fetcher {
                    index_scan = index_scan.with_fetcher(fetcher.clone());
                }
                Box::new(index_scan)
            } else {
                let mut child_scan =
                    ScanNode::new((*target_collection).clone(), child_mapping.clone());
                if let Some(ref fetcher) = self.fetcher {
                    child_scan = child_scan.with_fetcher(fetcher.clone());
                }
                Box::new(child_scan)
            };

            // Get field indices
            let relation_field_index =
                mapping
                    .first_index_of_name(&relation_name)
                    .unwrap_or_else(|| {
                        // Add to mapping if not present
                        let idx = mapping.next_index();
                        mapping.add(idx, &relation_name);
                        idx
                    });

            mapping.set_child_at(relation_field_index, child_mapping.clone());

            // Find child relation index
            let child_relation_index = target_relation_field
                .as_ref()
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

            // Create the relation filter
            let rel_filter = RelationFilter {
                relation_field: relation_name.clone(),
                conditions: nested_conditions.clone(),
            };

            // Create the appropriate join node based on cardinality
            if relation_field.kind.is_array() {
                // One-to-many: TypeJoinMany with filter
                let join_many =
                    TypeJoinMany::new(plan, child_plan, parent_side, child_side, mapping.clone())?
                        .with_relation_filter(rel_filter);
                plan = Box::new(join_many);
            } else {
                // One-to-one (or many-to-one): TypeJoinOne with filter
                // Check if we should use inverted index join:
                // child has index on filtered field AND parent has index on FK field.
                let parent_fk_field_name =
                    schema::CollectionVersion::relation_id_field_name(&relation_field.name);
                let parent_fk_index = if child_has_index {
                    parent_collection.indexes.iter().find(|idx| {
                        idx.fields
                            .first()
                            .is_some_and(|f| f.name == parent_fk_field_name)
                    })
                } else {
                    None
                };

                if let Some(fk_index) = parent_fk_index {
                    // Inverted index join: child scanned with index, parent looked up per-child
                    let fk_index_name = fk_index.name.clone();
                    let parent_scan_mapping = plan.document_map().clone();
                    let parent_col = parent_collection.clone();
                    let fk_field_index = parent_scan_mapping
                        .first_index_of_name(&parent_fk_field_name)
                        .unwrap_or(0);
                    let fetcher = self.fetcher.clone().unwrap();

                    // Extract scalar filter from the parent filter so it can be applied
                    // as a residual filter on parent docs fetched via FK index lookup.
                    // Without this, queries like `Book(filter: {genre: "thriller", author: {name: "X"}})`
                    // would skip the scalar conditions (genre) when using the inverted index path.
                    let scalar_filter = filter.split_by_relation().0;

                    let mut join = TypeJoinOne::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    )
                    .with_relation_filter(rel_filter)
                    .with_inverted_index(
                        fk_index_name,
                        fk_field_index,
                        parent_col,
                        parent_scan_mapping,
                        fetcher,
                    );
                    if let Some(sf) = scalar_filter {
                        join = join.with_parent_residual_filter(sf);
                    }
                    plan = Box::new(join);
                } else {
                    let join = TypeJoinOne::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    )
                    .with_relation_filter(rel_filter);
                    plan = Box::new(join);
                }
            }
        }

        Ok(plan)
    }
}
