//! View plan builder for non-materialized views.
//!
//! This module contains the logic for building execution plans for views
//! (collections with a QuerySource). Instead of scanning storage, views
//! parse their stored query, build a plan for it, and remap fields.

use std::collections::HashMap;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::{lens_node::LensNode, view::ViewNode, SelectNode};
use crate::planner::{PlanNode, PlanResult, Planner};

impl Planner {
    /// Validate that all nested select fields exist in the target collection's schema.
    ///
    /// This catches invalid field references in view queries, e.g. querying
    /// `books { author { name } }` when `BookView` only defines `name`.
    /// In Go, this is caught by the GraphQL schema validator. In Rust, we
    /// do it here during plan building.
    pub(crate) fn validate_nested_select_fields(
        &self,
        select: &Select,
        collection: &schema::CollectionVersion,
    ) -> Result<()> {
        for requestable in &select.fields {
            if let Requestable::Select(nested) = requestable {
                let field_name = &nested.field.name;
                if field_name == "GROUP" || field_name == "_version" {
                    continue;
                }
                let field = collection.field_by_name(field_name).ok_or_else(|| {
                    QueryError::unknown_field(format!(
                        "Cannot query field \"{}\" on type \"{}\".",
                        field_name, collection.name
                    ))
                })?;
                // Recurse into the target collection for deeper validation
                if let Some(target_id) = field.kind.relation_collection_id() {
                    if let Some(target) = self.get_collection(target_id) {
                        self.validate_nested_select_fields(nested, &target)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Build a plan for a non-materialized view.
    ///
    /// Instead of scanning storage (which is empty for views), this parses the
    /// stored query, builds a plan for it, and wraps it in a ViewNode that
    /// remaps fields from the source query's mapping to the view's mapping.
    pub(crate) fn build_view_plan(
        &self,
        select: &Select,
        collection: &schema::CollectionVersion,
        query_source: &schema::QuerySource,
    ) -> Result<PlanResult> {
        // Validate user's query fields against the view's schema types.
        // This catches references to fields that don't exist in the view's SDL,
        // e.g. circular view references where an embedded type omits a relation.
        self.validate_nested_select_fields(select, collection)?;

        // Extract the source collection name from the stored Select JSON
        let source_name = query_source
            .query
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| QueryError::execution("view QuerySource.Query missing 'Name' field"))?;

        // Build a Select for the underlying query from the stored JSON
        let source_fields_json = query_source
            .query
            .get("Fields")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                QueryError::execution("view QuerySource.Query missing 'Fields' array")
            })?;

        let mut source_select = Select::new(source_name.to_string());
        for field_json in source_fields_json {
            let field_name = field_json
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            if let Some(inner_fields) = field_json.get("Fields").and_then(|v| v.as_array()) {
                // Nested select (relation)
                let inner_name = field_name.to_string();
                let mut inner_select = Select::new(inner_name.clone()).with_field_name(inner_name);
                // Populate inner fields from stored JSON.
                // Use original field names only (no aliases) on the source select.
                // The ViewNode's child_mapping handles renaming via render keys.
                for inner_field_json in inner_fields {
                    let inner_field_name = inner_field_json
                        .get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    if inner_field_json.get("Fields").is_some() {
                        // Deeper nested select (relation within relation)
                        let deep_name = inner_field_name.to_string();
                        let deep_select = Select::new(deep_name.clone()).with_field_name(deep_name);
                        inner_select
                            .fields
                            .push(Requestable::Select(Box::new(deep_select)));
                    } else {
                        let field = crate::mapper::Field::new(inner_field_name);
                        inner_select.fields.push(Requestable::Field(field));
                    }
                }
                source_select
                    .fields
                    .push(Requestable::Select(Box::new(inner_select)));
            } else if field_json.get("Targets").is_some() {
                // Aggregate field (e.g., _count, _sum)
                let agg_type = crate::mapper::AggregateType::parse(field_name)
                    .unwrap_or(crate::mapper::AggregateType::Count);
                let mut agg = crate::mapper::Aggregate {
                    aggregate_type: agg_type,
                    targets: Vec::new(),
                    filter: None,
                    alias: field_json
                        .get("Alias")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                };
                if let Some(targets) = field_json.get("Targets").and_then(|v| v.as_array()) {
                    for target in targets {
                        let host_name = target
                            .get("HostName")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        let field_name = target
                            .get("ChildName")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let target = if let Some(fname) = field_name {
                            crate::mapper::AggregateTarget::with_field(host_name, fname)
                        } else {
                            crate::mapper::AggregateTarget::new(host_name)
                        };
                        agg.targets.push(target);
                    }
                }
                source_select.fields.push(Requestable::Aggregate(agg));
            } else {
                // Simple field
                let alias = field_json
                    .get("Alias")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let field = if let Some(a) = alias {
                    crate::mapper::Field::with_alias(field_name, a)
                } else {
                    crate::mapper::Field::new(field_name)
                };
                source_select.fields.push(Requestable::Field(field));
            }
        }

        // Reconstruct filter from stored JSON if present
        if let Some(filter_json) = query_source.query.get("Filter") {
            if !filter_json.is_null() {
                if let Some(conditions) = filter_json.get("Conditions").and_then(|c| c.as_object())
                {
                    if !conditions.is_empty() {
                        source_select.filter =
                            Some(crate::mapper::Filter::from_conditions(conditions.clone()));
                    }
                }
            }
        }

        // Build the source plan
        let source_plan_result = self.plan_with_index_info(&source_select)?;
        let source_mapping = source_plan_result.plan.document_map().clone();

        // Build the target (view) mapping
        let mut target_mapping = self.build_mapping(select, collection)?;

        // Build child mappings for nested selects (relations) in the view.
        // We use the stored Select JSON inner fields because they preserve
        // the original source field names (Name) and aliases (Alias), which
        // are needed for correct field renaming during JSON filtering.
        for field_json in source_fields_json {
            if let Some(inner_fields) = field_json.get("Fields").and_then(|v| v.as_array()) {
                let field_name = field_json
                    .get("Name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if let Some(field_index) = target_mapping.first_index_of_name(field_name) {
                    let mut child_mapping = DocumentMapping::new();
                    for inner_field in inner_fields {
                        let name = inner_field
                            .get("Name")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        let alias = inner_field.get("Alias").and_then(|v| v.as_str());
                        let output_name = alias.unwrap_or(name);
                        let idx = child_mapping.next_index();
                        child_mapping.add(idx, name);
                        child_mapping.add_render_key(idx, output_name);
                    }
                    target_mapping.set_child_at(field_index, child_mapping);
                }
            }
        }

        // Build the view plan. When a lens transform is present, the LensNode
        // converts source docs to JSON, applies the transform, and converts back
        // using target_mapping. The ViewNode then does field filtering (identity
        // mapping for scalars, nested JSON filtering for relations).
        let has_lens = query_source.transform.is_some() && self.lens_store.is_some();
        let _ = has_lens; // suppress unused warning

        let (effective_source, view_source_mapping): (Box<dyn PlanNode>, DocumentMapping) =
            if let Some(ref transform_cid) = query_source.transform {
                if let Some(ref lens_store) = self.lens_store {
                    let transform_id = lens::TransformId::new(transform_cid.as_str());
                    let lens_node = Box::new(LensNode::new(
                        source_plan_result.plan,
                        source_mapping,
                        target_mapping.clone(),
                        lens_store.clone(),
                        transform_id,
                    ));
                    // LensNode output is in target format, so ViewNode does
                    // identity mapping (but still filters nested JSON)
                    (lens_node, target_mapping.clone())
                } else {
                    (source_plan_result.plan, source_mapping)
                }
            } else {
                (source_plan_result.plan, source_mapping)
            };

        let view_plan: Box<dyn PlanNode> = Box::new(ViewNode::new(
            effective_source,
            view_source_mapping,
            target_mapping.clone(),
        ));

        // Always wrap viewNode in SelectNode (Go always has selectNode → viewNode).
        // Apply user's query-level filter if present.
        let plan: Box<dyn PlanNode> = if let Some(ref filter) = select.filter {
            Box::new(SelectNode::new(view_plan, target_mapping).with_filter(filter.clone()))
        } else {
            Box::new(SelectNode::new(view_plan, target_mapping))
        };

        Ok(PlanResult {
            plan,
            index_scan: None,
            ordering_only_fields: Vec::new(),
            aggregate_internal_keys: HashMap::new(),
        })
    }
}
