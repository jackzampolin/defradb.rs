//! View planning utilities
//!
//! Contains `build_view_plan()` for handling view/materialized queries.

use std::collections::HashMap;

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::SelectNode;
use crate::planner::PlanNode;

use super::builder::{PlanResult, Planner};

impl Planner {
    /// Build an execution plan for a view query.
    ///
    /// Views are backed by a stored query. This method parses the stored query,
    /// builds a plan for it, and wraps it in a ViewNode that remaps fields
    /// from the source query's mapping to the view's mapping.
    pub(super) fn build_view_plan(
        &self,
        select: &Select,
        collection: &CollectionVersion,
        query_source: &schema::QuerySource,
    ) -> Result<PlanResult> {
        self.validate_nested_select_fields(select, collection)?;

        let source_name = query_source
            .query
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| QueryError::execution("view QuerySource.Query missing 'Name' field"))?;

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
                let inner_name = field_name.to_string();
                let mut inner_select = Select::new(inner_name.clone()).with_field_name(inner_name);
                for inner_field_json in inner_fields {
                    let inner_field_name = inner_field_json
                        .get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    if inner_field_json.get("Fields").is_some() {
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

        if let Some(filter_json) = query_source.query.get("Filter") {
            if !filter_json.is_null() {
                if let Some(conditions) = filter_json.get("Conditions").and_then(|c| c.as_object())
                {
                    let conditions_map: std::collections::HashMap<String, serde_json::Value> =
                        conditions
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                    if !conditions_map.is_empty() {
                        source_select.filter =
                            Some(crate::mapper::Filter::from_conditions(conditions_map));
                    }
                }
            }
        }

        let source_plan_result = self.plan_with_index_info(&source_select)?;
        let source_mapping = source_plan_result.plan.document_map().clone();

        let mut target_mapping = self.build_mapping(select, collection)?;

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

        let _has_lens = query_source.transform.is_some() && self.lens_store.is_some();

        let (effective_source, view_source_mapping): (Box<dyn PlanNode>, DocumentMapping) =
            if let Some(ref transform_cid) = query_source.transform {
                if let Some(ref lens_store) = self.lens_store {
                    let transform_id = lens::TransformId::new(transform_cid.as_str());
                    let lens_node = Box::new(crate::plan::lens_node::LensNode::new(
                        source_plan_result.plan,
                        source_mapping,
                        target_mapping.clone(),
                        lens_store.clone(),
                        transform_id,
                    ));
                    (lens_node, target_mapping.clone())
                } else {
                    (source_plan_result.plan, source_mapping)
                }
            } else {
                (source_plan_result.plan, source_mapping)
            };

        let view_plan: Box<dyn PlanNode> = Box::new(crate::plan::view::ViewNode::new(
            effective_source,
            view_source_mapping,
            target_mapping.clone(),
        ));

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
