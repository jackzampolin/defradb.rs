//! Top-level aggregate execution.

use acp::{DocumentPermission, Identity};
use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::document::{documents_to_plan_docs, DocumentMapping};
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::planner::{Doc, Planner};
use crate::txn::TransactionRegistry;

use super::super::fetcher::FetcherWrapper;
use super::super::plan;
use super::super::plan_drive;
use super::super::{DocFetcher, QueryRunner};

/// Number of distinct `group_by` keys across `docs`.
///
/// A document missing a grouped field is keyed as null, so all such documents
/// share one group.
fn distinct_group_count<'a>(
    docs: impl Iterator<Item = &'a Doc>,
    mapping: &DocumentMapping,
    group_fields: &[String],
) -> i64 {
    let indexes: Vec<Option<usize>> = group_fields
        .iter()
        .map(|name| mapping.first_index_of_name(name))
        .collect();
    docs.map(|doc| {
        indexes
            .iter()
            .map(|index| {
                index
                    .and_then(|i| doc.get(i))
                    .unwrap_or(&JsonValue::Null)
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\u{1}")
    })
    .collect::<std::collections::HashSet<_>>()
    .len() as i64
}

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Top-level aggregates compute a single value over all documents in a collection.
    /// Unlike regular collection queries that return an array, top-level aggregates
    /// return a single value (the computed aggregate).
    ///
    /// Returns 0 for empty collections (Go DefraDB semantics).
    pub(crate) async fn execute_top_level_aggregate(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        collection: &Arc<CollectionVersion>,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Fetch all documents from the collection
        let docs = fetcher.get_all(&select.collection_name).await?;

        // Build document mapping for field access
        let mapping = plan::build_mapping(select, collection)?;

        // Convert storage documents to values for aggregation
        let mut plan_docs = documents_to_plan_docs(&docs, &mapping)?;

        // Apply ACP filtering when the collection is policy-backed.
        if let Some(ref policy) = collection.policy {
            let acp_identity = Identity::from(identity);
            let mut filtered = Vec::with_capacity(plan_docs.len());
            for doc in plan_docs {
                if let Some(doc_id_val) = doc.get(0) {
                    if let Some(doc_id) = doc_id_val.as_str() {
                        let has_access = crate::txn::check_doc_access_with_overlay(
                            self.acp.as_ref(),
                            &acp_identity,
                            DocumentPermission::Read,
                            &policy.id,
                            &policy.resource_name,
                            doc_id,
                            None,
                        )
                        .await
                        .unwrap_or(false);
                        if has_access {
                            filtered.push(doc);
                        }
                    }
                }
            }
            plan_docs = filtered;
        }

        // For each aggregate in the select, compute its value
        // For top-level aggregates, we return a single object with aggregate results
        let mut result = serde_json::Map::new();

        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                let output_name = agg.output_name().to_string();

                // Get the target info
                let target = agg.targets.first();
                let field_name = target.and_then(|t| t.field_name.as_ref());
                let target_filter = target.and_then(|t| t.filter.as_ref());

                // Find field index in mapping
                let field_index = field_name.and_then(|name| mapping.first_index_of_name(name));

                // Apply filter if present
                let filtered_docs: Vec<&crate::planner::Doc> = if let Some(filter) = target_filter {
                    plan_docs
                        .iter()
                        .filter(|doc| {
                            // Convert Doc to fields Vec for filter evaluation
                            let fields: Vec<Option<JsonValue>> = (0..mapping.next_index())
                                .map(|i| doc.get(i).cloned())
                                .collect();
                            filter.matches(&fields, &mapping).unwrap_or(false)
                        })
                        .collect()
                } else {
                    plan_docs.iter().collect()
                };

                // Compute the aggregate value
                let value = match agg.aggregate_type {
                    crate::mapper::AggregateType::Count => {
                        // Count documents (optionally filtered), or distinct
                        // groups when the target is grouped.
                        let count = match target.and_then(|t| t.group_by.as_ref()) {
                            Some(group_by) => distinct_group_count(
                                filtered_docs.iter().copied(),
                                &mapping,
                                &group_by.resolved_fields(collection)?,
                            ),
                            None => filtered_docs.len() as i64,
                        };
                        JsonValue::Number(count.into())
                    }
                    crate::mapper::AggregateType::Sum => {
                        if let Some(idx) = field_index {
                            let sum: f64 = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .sum();
                            if sum == sum.floor() {
                                JsonValue::Number((sum as i64).into())
                            } else {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into()),
                                )
                            }
                        } else {
                            JsonValue::Number(0.into())
                        }
                    }
                    crate::mapper::AggregateType::Average => {
                        if let Some(idx) = field_index {
                            let values: Vec<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .collect();
                            if values.is_empty() {
                                // Go DefraDB returns 0 for AVG of empty set
                                JsonValue::Number(0.into())
                            } else {
                                let avg = values.iter().sum::<f64>() / values.len() as f64;
                                JsonValue::Number(
                                    serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into()),
                                )
                            }
                        } else {
                            JsonValue::Number(0.into())
                        }
                    }
                    crate::mapper::AggregateType::Min => {
                        if let Some(idx) = field_index {
                            let min: Option<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .min_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                            match min {
                                Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                                Some(v) => JsonValue::Number(
                                    serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                                ),
                                None => JsonValue::Null,
                            }
                        } else {
                            JsonValue::Null
                        }
                    }
                    crate::mapper::AggregateType::Max => {
                        if let Some(idx) = field_index {
                            let max: Option<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                            match max {
                                Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                                Some(v) => JsonValue::Number(
                                    serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                                ),
                                None => JsonValue::Null,
                            }
                        } else {
                            JsonValue::Null
                        }
                    }
                };

                result.insert(output_name, value);
            }
        }

        // Return the result object directly (not wrapped in an array)
        // The caller will insert this into the response with the aggregate name as key
        // But for top-level aggregates, we need the caller to extract the value
        // Actually, looking at execute_query_internal, it inserts result with select.field.output_name()
        // For { AVG(...) }, output_name is "AVG", and we're returning {"AVG": value}
        // So we'd get {"AVG": {"AVG": value}} which is wrong.
        // We need to return just the value.

        // For top-level aggregates, return the single aggregate value
        // (assumes single aggregate in top-level query)
        if let Some((_, value)) = result.into_iter().next() {
            Ok(value)
        } else {
            Ok(JsonValue::Null)
        }
    }

    /// Execute a top-level aggregate with filters that reference relations.
    ///
    /// This function uses the planner to build a join plan so relation data is
    /// available for filter evaluation, then counts the filtered results.
    /// Unlike `execute_top_level_aggregate`, this handles filters like:
    /// `_count(Book: {filter: {author: {age: {_gt: 30}}}})`
    pub(crate) async fn execute_top_level_aggregate_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        use crate::mapper::AggregateType;

        // Get the aggregate info
        let agg = match select.fields.first() {
            Some(Requestable::Aggregate(a)) => a,
            _ => return Ok(JsonValue::Null),
        };
        let output_name = agg.output_name().to_string();
        let target = agg.targets.first();
        let target_filter = target.and_then(|t| t.filter.as_ref());
        let field_name = target.and_then(|t| t.field_name.as_ref());
        let group_by = target.and_then(|t| t.group_by.as_ref());

        // Create a modified select that fetches the collection with the filter applied.
        // This select returns documents (not aggregates), so the planner will build
        // a proper join plan with the relation filter.
        let collection_name = target
            .map(|t| t.host_name.clone())
            .unwrap_or_else(|| select.collection_name.clone());

        let collections_map = self.collections_map().await?;
        let group_fields = match group_by {
            Some(gb) => gb.resolved_fields(
                collections_map
                    .get(&collection_name)
                    .ok_or_else(|| QueryError::collection_not_found(&collection_name))?,
            )?,
            None => Vec::new(),
        };

        let mut select_fields = if let Some(fname) = field_name {
            // For sum/avg/etc., we need the field value
            vec![Requestable::Field(crate::mapper::Field::new(fname.clone()))]
        } else {
            // For count, we just need any field to count docs
            vec![Requestable::Field(crate::mapper::Field::new(
                "_docID".to_string(),
            ))]
        };
        // Grouped targets need the grouped fields available to build group keys.
        for name in &group_fields {
            let already_selected = select_fields
                .iter()
                .any(|f| matches!(f, Requestable::Field(existing) if existing.name == *name));
            if !already_selected {
                select_fields.push(Requestable::Field(crate::mapper::Field::new(name.clone())));
            }
        }

        let filter_select = Select {
            collection_name: collection_name.clone(),
            field: crate::mapper::Field::new(collection_name.clone()),
            fields: select_fields,
            filter: target_filter.cloned(),
            order_by: None,
            limit: None,
            group_by: None,
            doc_ids: None,
            cid: None,
            depth: None,
            show_deleted: false,
            is_encrypted: false,
            exhaustive: false,
            selection_type: crate::mapper::SelectionType::Object,
            document_mapping: crate::document::DocumentMapping::default(),
            is_cursor: false,
            cursor_params: None,
            cursor_page_info: crate::mapper::CursorPageInfoFields::default(),
            cursor_aliases: crate::mapper::CursorAliases::default(),
        };

        // Execute with the planner to get filtered documents
        let fetcher_arc = FetcherWrapper::new(fetcher);
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        let mut planner = Planner::new(collections)
            .with_query_limits(self.query_limits)
            .with_fetcher(Arc::new(fetcher_arc))
            .with_acp(self.acp.clone(), identity);
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(&filter_select)?;
        let mut plan = plan_result.plan;
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        let outcome = async {
            plan.init().await?;
            plan.start().await?;

            let mut docs = Vec::new();
            while plan.next().await? {
                let doc = plan.value().deep_clone();
                docs.push(doc);
            }
            Ok(docs)
        }
        .await;

        let docs = plan_drive::close_after(plan.as_mut(), outcome).await?;

        // Compute the aggregate based on type
        let value = match agg.aggregate_type {
            AggregateType::Count => {
                let count = if group_by.is_some() {
                    distinct_group_count(docs.iter(), &mapping, &group_fields)
                } else {
                    docs.len() as i64
                };
                JsonValue::Number(count.into())
            }
            AggregateType::Sum => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let sum: f64 = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .sum();
                        if sum == sum.floor() {
                            JsonValue::Number((sum as i64).into())
                        } else {
                            JsonValue::Number(
                                serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into()),
                            )
                        }
                    } else {
                        JsonValue::Number(0.into())
                    }
                } else {
                    JsonValue::Number(0.into())
                }
            }
            AggregateType::Average => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let values: Vec<f64> = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if values.is_empty() {
                            JsonValue::Number(0.into())
                        } else {
                            let avg = values.iter().sum::<f64>() / values.len() as f64;
                            JsonValue::Number(
                                serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into()),
                            )
                        }
                    } else {
                        JsonValue::Number(0.into())
                    }
                } else {
                    JsonValue::Number(0.into())
                }
            }
            AggregateType::Min => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let min: Option<f64> = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        match min {
                            Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                            Some(v) => JsonValue::Number(
                                serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                            ),
                            None => JsonValue::Null,
                        }
                    } else {
                        JsonValue::Null
                    }
                } else {
                    JsonValue::Null
                }
            }
            AggregateType::Max => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let max: Option<f64> = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        match max {
                            Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                            Some(v) => JsonValue::Number(
                                serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                            ),
                            None => JsonValue::Null,
                        }
                    } else {
                        JsonValue::Null
                    }
                } else {
                    JsonValue::Null
                }
            }
        };

        // Return just the value (not wrapped in an object with output_name)
        // The caller will insert this with the correct key
        let _ = output_name; // suppress unused warning
        Ok(value)
    }
}
