//! Aggregate relation joins.
//!
//! Handles relation-based and inline-array aggregates — creates TypeJoinMany
//! for aggregate data fetch, manages internal keys for collision handling.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use schema::CollectionVersion;

use crate::plan::{JoinSide, ScanNode, TypeJoinMany};
use crate::planner::PlanNode;
use query_types::document::DocumentMapping;
use query_types::error::Result;
use query_types::mapper::{AggregateType, Filter, Requestable, Select};

use super::super::builder::Planner;
use super::child_mapping::add_rendered_field;
use super::SelectionJoinInfo;

impl Planner {
    /// Handle relation-based and inline-array aggregates.
    ///
    /// Relation aggregates (e.g., `_count(books: {})`) need joins to fetch data.
    /// Inline array aggregates (e.g., `_count(favouriteIntegers: {})`) need the
    /// array field added to the render mapping so the data appears in output.
    pub(super) fn apply_aggregate_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        mapping: &mut DocumentMapping,
        aggregate_internal_keys: &mut HashMap<String, (String, String)>,
        select: &Select,
        parent_collection: &CollectionVersion,
        selection_join_info: &HashMap<String, SelectionJoinInfo>,
    ) -> Result<Box<dyn PlanNode>> {
        let mut aggregate_joined_relations: HashSet<String> = HashSet::new();
        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                for target in &agg.targets {
                    // Only handle aggregates with a named target (non-empty host_name)
                    if target.host_name.is_empty() {
                        continue;
                    }

                    let relation_field_name = &target.host_name;

                    // Check if this relation is already joined by a prior aggregate
                    // or by a selection. Multiple aggregates on the same relation share
                    // one TypeJoinMany; compute_relation_aggregates() handles per-aggregate
                    // limit/offset/order in post-processing.
                    // Go also shares joins between selections and aggregates targeting
                    // the same relation (e.g., books(filter: X) + _count(books: {filter: X})).
                    let already_joined = aggregate_joined_relations
                        .contains(relation_field_name.as_str())
                        || selection_join_info
                            .get(relation_field_name.as_str())
                            .is_some_and(|info| {
                                if info.has_limit {
                                    return false;
                                }
                                // If aggregate has no filter but specifies a field_name, it's a
                                // field-level operation (e.g. _avg(books: {field: rating})) that
                                // piggybacks on the selection's join. Share unconditionally.
                                if target.filter.is_none() {
                                    return target.field_name.is_some();
                                }
                                // If aggregate has a filter, share only if it matches the
                                // selection's filter exactly.
                                let agg_filter_json = target.filter.as_ref().map(|f| {
                                    serde_json::to_string(f.conditions()).unwrap_or_default()
                                });
                                info.filter_json == agg_filter_json
                            });

                    // Find the field in the parent collection
                    let relation_field = match parent_collection.field_by_name(relation_field_name)
                    {
                        Some(f) => f,
                        None => continue,
                    };

                    // Inline array fields are handled by scan_mapping setup
                    // in build_plan() — no join needed.
                    if !relation_field.kind.is_relation() {
                        continue;
                    }

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
                            None => {
                                // CID lookup failed - try to find target by matching relation_name.
                                // This handles cases where the relation's collection_id CID differs
                                // from the target collection's current collection_id/version_id
                                // (e.g., circular schema definitions with set-based versioning).
                                let parent_rel_name =
                                    relation_field.relation_name.as_deref().unwrap_or("");
                                let mut found_by_relation = None;
                                if !parent_rel_name.is_empty() {
                                    for coll in self.collections.values() {
                                        if coll.name == parent_collection.name {
                                            continue;
                                        }
                                        for f in &coll.fields {
                                            if f.relation_name.as_deref() == Some(parent_rel_name) {
                                                found_by_relation = Some(coll.clone());
                                                break;
                                            }
                                        }
                                        if found_by_relation.is_some() {
                                            break;
                                        }
                                    }
                                }
                                match found_by_relation {
                                    Some(c) => c,
                                    None => continue,
                                }
                            }
                        }
                    };

                    // Get relation field index in parent collection (needed in both paths)
                    let relation_field_index = parent_collection
                        .fields
                        .iter()
                        .position(|f| f.name == *relation_field_name)
                        .unwrap_or(0);

                    // If the relation is already joined via a selection, we still need to
                    // ensure the filter fields are available in the output for post-processing.
                    // Add filter fields to the existing child mapping.
                    if already_joined {
                        // Add filter fields from aggregate target to existing child mapping.
                        // Always ensure render_key exists even when the field is already
                        // in the mapping (build_scan_mapping_for_join adds ALL fields but
                        // only adds render_keys for explicitly selected ones).
                        if let Some(ref filter) = target.filter {
                            for filter_field in filter.referenced_fields() {
                                if filter_field.starts_with('_') {
                                    continue;
                                }
                                if let Some(idx) = target_collection
                                    .fields
                                    .iter()
                                    .position(|f| f.name == filter_field)
                                {
                                    if let Some(child_mapping) =
                                        mapping.child_at_mut(relation_field_index)
                                    {
                                        if child_mapping
                                            .first_index_of_name(&filter_field)
                                            .is_none()
                                        {
                                            child_mapping.add(idx, &filter_field);
                                        }
                                        if !child_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == filter_field)
                                        {
                                            child_mapping.add_render_key(idx, &filter_field);
                                        }
                                    }
                                }
                            }
                        }
                        // Add order fields from aggregate target to existing child mapping.
                        // Always ensure render_key exists even when the field is already
                        // in the mapping (build_scan_mapping_for_join adds ALL fields but
                        // only adds render_keys for explicitly selected ones).
                        if let Some(ref order) = target.order {
                            for condition in &order.conditions {
                                if condition.fields.is_empty()
                                    || condition.fields[0].starts_with('_')
                                {
                                    continue;
                                }
                                let order_field = &condition.fields[0];
                                if let Some(idx) = target_collection
                                    .fields
                                    .iter()
                                    .position(|f| f.name == *order_field)
                                {
                                    if let Some(child_mapping) =
                                        mapping.child_at_mut(relation_field_index)
                                    {
                                        if child_mapping.first_index_of_name(order_field).is_none()
                                        {
                                            child_mapping.add(idx, order_field);
                                        }
                                        if !child_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == *order_field)
                                        {
                                            child_mapping.add_render_key(idx, order_field);
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(ref group_by) = target.group_by {
                            if let Some(child_mapping) = mapping.child_at_mut(relation_field_index)
                            {
                                for field_name in &group_by.fields {
                                    add_rendered_field(
                                        child_mapping,
                                        &target_collection,
                                        field_name,
                                    );
                                }
                            }
                        }
                        continue;
                    }

                    // Build a minimal child mapping for the aggregate
                    // For count, we just need to fetch the documents
                    // For sum/avg, we need the specific field
                    // For any aggregate with filter, we need the filter fields
                    let mut child_mapping = DocumentMapping::new();
                    child_mapping.add(0, "_docID");

                    // If there's a field to aggregate, add it with render_key
                    if let Some(ref field_name) = target.field_name {
                        if let Some(idx) = target_collection
                            .fields
                            .iter()
                            .position(|f| f.name == *field_name)
                        {
                            child_mapping.add(idx, field_name);
                            child_mapping.add_render_key(idx, field_name);
                        }
                    }
                    if let Some(ref group_by) = target.group_by {
                        for field_name in &group_by.fields {
                            add_rendered_field(&mut child_mapping, &target_collection, field_name);
                        }
                    }

                    // Add fields referenced by the filter so they appear in the output
                    // for post-processing filter evaluation
                    if let Some(ref filter) = target.filter {
                        for filter_field in filter.referenced_fields() {
                            // Skip special fields
                            if filter_field.starts_with('_') {
                                continue;
                            }
                            // Find the field in the target collection
                            if let Some(idx) = target_collection
                                .fields
                                .iter()
                                .position(|f| f.name == filter_field)
                            {
                                // Add to child mapping if not already present
                                if child_mapping.first_index_of_name(&filter_field).is_none() {
                                    child_mapping.add(idx, &filter_field);
                                    child_mapping.add_render_key(idx, &filter_field);
                                }
                            }
                        }
                    }

                    // Add fields referenced by the order so they appear in the output
                    // for post-processing sort before limit/offset
                    if let Some(ref order) = target.order {
                        for condition in &order.conditions {
                            if let Some(order_field) = condition.fields.first() {
                                if order_field.starts_with('_') {
                                    continue;
                                }
                                if let Some(idx) = target_collection
                                    .fields
                                    .iter()
                                    .position(|f| f.name == *order_field)
                                {
                                    if child_mapping.first_index_of_name(order_field).is_none() {
                                        child_mapping.add(idx, order_field);
                                        child_mapping.add_render_key(idx, order_field);
                                    }
                                }
                            }
                        }
                    }

                    // Build scan mapping for the child
                    let mut child_scan_mapping =
                        self.build_scan_mapping_for_join(&target_collection, &child_mapping);

                    // Determine the mapping index for this aggregate's relation data.
                    // When a selection already uses this relation (e.g., `books2020: book(...)`),
                    // we need a separate index so the aggregate gets independent, unfiltered data.
                    // We also need a unique internal key to avoid collision with the selection's
                    // limited/filtered data.
                    let selection_has_relation = select.fields.iter().any(|f| {
                        if let Requestable::Select(s) = f {
                            s.field.name == *relation_field_name
                        } else {
                            false
                        }
                    });
                    let effective_relation_index = if selection_has_relation {
                        // Selection already uses relation_field_index with its own filter/limit.
                        // Use a new index and a unique internal key for the aggregate's data
                        // to avoid collision with the selection's data in rendered JSON.
                        let idx = mapping.next_index();
                        let internal_key =
                            format!("__agg_{}_{}", relation_field_name, agg.output_name());
                        mapping.add(idx, relation_field_name);
                        mapping.add_render_key(idx, &internal_key);
                        // Store the mapping so the runner can look up data using the internal key
                        aggregate_internal_keys.insert(
                            agg.output_name().to_string(),
                            (relation_field_name.clone(), internal_key),
                        );
                        idx
                    } else {
                        if mapping.first_index_of_name(relation_field_name).is_none() {
                            mapping.add(relation_field_index, relation_field_name);
                        }
                        mapping.add_render_key(relation_field_index, relation_field_name);
                        relation_field_index
                    };

                    // Build child plan (simple scan with fetcher)
                    let mut child_scan =
                        ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
                    if let Some(ref fetcher) = self.fetcher {
                        child_scan = child_scan.with_fetcher(fetcher.clone());
                    }
                    // Apply aggregate target filter to the scan node.
                    // Go places these filters on the scanNode in explain output.
                    // Also synthesize {field: {_neq: null}} for Average aggregates
                    // to exclude null values (matching Go behavior).
                    let mut scan_filter = target.filter.clone();
                    if agg.aggregate_type == AggregateType::Average {
                        if let Some(ref field_name) = target.field_name {
                            let neq_null_filter = Filter::from_conditions({
                                let mut c = serde_json::Map::new();
                                c.insert(
                                    field_name.clone(),
                                    serde_json::json!({"_neq": serde_json::Value::Null}),
                                );
                                c
                            });
                            scan_filter = Some(match scan_filter {
                                Some(existing) => {
                                    // Merge {field: {_neq: null}} into existing conditions
                                    let mut merged = existing.conditions().clone();
                                    merged
                                        .entry(field_name.clone())
                                        .and_modify(|v| {
                                            if let serde_json::Value::Object(ref mut ops) = v {
                                                ops.insert(
                                                    "_neq".to_string(),
                                                    serde_json::Value::Null,
                                                );
                                            }
                                        })
                                        .or_insert(
                                            serde_json::json!({"_neq": serde_json::Value::Null}),
                                        );
                                    Filter::from_conditions_with_max_depth(
                                        merged,
                                        existing.max_depth(),
                                    )
                                }
                                None => neq_null_filter,
                            });
                        }
                    }
                    if let Some(ref filter) = scan_filter {
                        if !filter.has_relation_filters() {
                            child_scan = child_scan.with_filter(filter.clone());
                        }
                    }
                    let mut child_plan: Box<dyn PlanNode> = Box::new(child_scan);

                    // Add sub-joins for deep order relation fields (e.g., publisher.yearOpened).
                    // When the aggregate order references a nested relation, we need a TypeJoinOne
                    // inside the child plan so the relation data is available for sorting.
                    if let Some(ref order) = target.order {
                        for condition in &order.conditions {
                            if condition.fields.len() >= 2 {
                                let order_relation_name = &condition.fields[0];
                                if let Some(order_rel_field) =
                                    target_collection.field_by_name(order_relation_name)
                                {
                                    if order_rel_field.kind.is_relation() {
                                        let (new_plan, new_mapping) = self
                                            .apply_filter_relation_join(
                                                child_plan,
                                                &target_collection,
                                                order_rel_field,
                                                order_relation_name,
                                                child_scan_mapping.clone(),
                                            )?;
                                        child_plan = new_plan;
                                        child_scan_mapping = new_mapping;
                                        // Ensure render_key for the relation so it appears
                                        // in rendered JSON for post-processing sort
                                        if let Some(rel_idx) = child_scan_mapping
                                            .first_index_of_name(order_relation_name)
                                        {
                                            if !child_scan_mapping
                                                .render_keys
                                                .iter()
                                                .any(|rk| rk.key == *order_relation_name)
                                            {
                                                child_scan_mapping
                                                    .add_render_key(rel_idx, order_relation_name);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Add sub-joins for deep filter relation fields (e.g., publisher.yearOpened).
                    // When the aggregate filter references a relation, we need a TypeJoinOne
                    // inside the child plan so the relation data is available for filter evaluation.
                    if let Some(ref filter) = target.filter {
                        for filter_field in filter.referenced_fields() {
                            if filter_field.starts_with('_') {
                                continue;
                            }
                            if let Some(filter_rel_field) =
                                target_collection.field_by_name(&filter_field)
                            {
                                if filter_rel_field.kind.is_relation() {
                                    // Skip if a sub-join already exists (from order handling)
                                    if let Some(rel_idx) =
                                        child_scan_mapping.first_index_of_name(&filter_field)
                                    {
                                        if child_scan_mapping.child_at(rel_idx).is_some() {
                                            continue;
                                        }
                                    }
                                    let (new_plan, new_mapping) = self.apply_filter_relation_join(
                                        child_plan,
                                        &target_collection,
                                        filter_rel_field,
                                        &filter_field,
                                        child_scan_mapping.clone(),
                                    )?;
                                    child_plan = new_plan;
                                    child_scan_mapping = new_mapping;
                                    // Ensure render_key for the relation so it appears
                                    // in rendered JSON for post-processing filter
                                    if let Some(rel_idx) =
                                        child_scan_mapping.first_index_of_name(&filter_field)
                                    {
                                        if !child_scan_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == filter_field)
                                        {
                                            child_scan_mapping
                                                .add_render_key(rel_idx, &filter_field);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Insert ACP permission filter for the aggregate child collection (if ACP-protected).
                    child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

                    // Set up child mapping in parent for TypeJoin (after sub-join modifications)
                    mapping.set_child_at(effective_relation_index, child_scan_mapping.clone());

                    // Find the back-reference field
                    let target_relation_field = target_collection.fields.iter().find(|f| {
                        if !f.kind.is_relation() {
                            return false;
                        }
                        if let Some(rel_id) = f.kind.relation_collection_id() {
                            rel_id == parent_collection.version_id
                                || rel_id == parent_collection.name
                        } else {
                            false
                        }
                    });

                    let child_relation_index = target_relation_field
                        .and_then(|f| {
                            target_collection
                                .fields
                                .iter()
                                .position(|tf| tf.name == f.name)
                        })
                        .unwrap_or(0);

                    // Create join sides - use effective_relation_index so children
                    // are stored at the correct (possibly new) index
                    let parent_side = JoinSide::new(
                        parent_collection.clone(),
                        relation_field.clone(),
                        effective_relation_index,
                    )?;

                    let child_side = JoinSide::new(
                        (*target_collection).clone(),
                        target_relation_field
                            .cloned()
                            .unwrap_or_else(|| relation_field.clone()),
                        child_relation_index,
                    )?;

                    // For aggregates, always use TypeJoinMany since we're aggregating an array
                    // The aggregate field name becomes the key in the mapping
                    let aggregate_key = agg.output_name();

                    // Add the aggregate's output name to the mapping for later processing
                    if mapping.first_index_of_name(aggregate_key).is_none() {
                        let idx = mapping.next_index();
                        mapping.add(idx, aggregate_key);
                        // Note: render_key is already added in build_mapping_for_select
                    }

                    plan = Box::new(TypeJoinMany::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    )?);

                    aggregate_joined_relations.insert(relation_field_name.to_string());
                }
            }
        }

        Ok(plan)
    }
}
