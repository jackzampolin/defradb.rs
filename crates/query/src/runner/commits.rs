//! Commits query execution
//!
//! Contains methods for executing _commits system collection queries:
//! - `execute_commits_query()` - Main commits query handler
//! - `render_commit()` / `render_document_fields()` / `fetch_version_data()`
//! - `json_item_matches_filter()` / `check_filter_op()`
//! - `generate_commit_group_key()` / `json_value_to_key()` / `build_commits_mapping()`
//! - `commit_to_fields()` / `compare_json_values()`

use identity::Did;
use serde_json::Value as JsonValue;
use std::collections::HashSet;

use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::txn::TransactionRegistry;

use super::commits_height::{extract_commits_height_range, HeightRangeExtraction};
use super::{DocFetcher, QueryRunner};

#[cfg(test)]
use super::commits_numeric::{
    max_commit_numeric_values, min_commit_numeric_values, sum_commit_numeric_values,
    CommitNumericValue,
};

mod aggregate;
mod grouping;
mod nested_filter;
mod rendering;

use aggregate::is_commit_aggregate_only_selection;

// Every formerly private helper moved into a child uses `pub(super)`. This
// preserves its original effective visibility to the `commits` module and all
// descendants without exposing it outside that module tree.

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a _commits system collection query.
    ///
    /// This handles queries to the special _commits collection which fetches
    /// commit history from the headstore and blockstore.
    ///
    /// ACP enforcement: after fetching, commits are filtered per-document using
    /// the same `check_doc_access` gate as regular queries. Commits for documents
    /// the caller lacks read permission on are silently excluded (Go semantics).
    pub(crate) async fn execute_commits_query(
        &self,
        select: &Select,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        use crate::fetcher::CommitsQueryOptions;
        use crate::mapper::OrderDirection;

        // Validate docIDs: Go v1.0.0-rc1 accepts a list but only supports one element.
        if let Some(ref ids) = select.doc_ids {
            if ids.is_empty() {
                return Ok(JsonValue::Array(vec![]));
            }
        }
        if let Some(ref cids) = select.cid {
            if cids.is_empty() {
                return Ok(JsonValue::Array(vec![]));
            }
        }

        let height_range = select
            .filter
            .as_ref()
            .map(extract_commits_height_range)
            .unwrap_or(HeightRangeExtraction::None);

        if matches!(height_range, HeightRangeExtraction::Empty) {
            return Ok(JsonValue::Array(vec![]));
        }

        // Build options from the select
        let base_options = CommitsQueryOptions {
            doc_id: select.doc_ids.as_ref().and_then(|ids| ids.first().cloned()),
            cid: None,
            depth: select.depth,
            height_start: match &height_range {
                HeightRangeExtraction::Range(range) => Some(range.start),
                _ => None,
            },
            height_end: match &height_range {
                HeightRangeExtraction::Range(range) => range.end,
                _ => None,
            },
            field_name: None,
        };

        // Fetch commits using the fetcher
        let mut commits = Vec::new();
        if let Some(ref cids) = select.cid {
            let mut seen_commit_cids = HashSet::new();
            let mut seen_input_cids = HashSet::new();

            for cid in cids {
                if !seen_input_cids.insert(cid.clone()) {
                    continue;
                }

                let options = CommitsQueryOptions {
                    cid: Some(cid.clone()),
                    ..base_options.clone()
                };
                for commit in self.fetcher.get_commits(&options).await? {
                    let Some(commit_cid) = commit.get("cid").and_then(|value| value.as_str())
                    else {
                        continue;
                    };
                    let doc_id = commit
                        .get("docID")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    if seen_commit_cids.insert((commit_cid.to_string(), doc_id.to_string())) {
                        commits.push(commit);
                    }
                }
            }
        } else {
            commits = self.fetcher.get_commits(&base_options).await?;
        }

        // ACP filtering: check read permission for each commit's document or
        // collection-level DAG. _commits must enforce the same ACP rules as
        // regular document queries.
        {
            use acp::Identity;

            let identity = Identity::from(caller_identity.clone());
            let node_did = self.node_did().cloned();

            // Active-version fast path. Historical commits authored under a
            // now-inactive version (after a schema migration) are NOT in this
            // map and are resolved on demand below — never left ungated.
            let collections = self.collections_map().await?;
            let by_version: std::collections::HashMap<
                String,
                std::sync::Arc<schema::CollectionVersion>,
            > = collections
                .values()
                .map(|c| (c.version_id.clone(), c.clone()))
                .collect();
            let provider = self.effective_provider();
            let mut resolved_versions: std::collections::HashMap<
                String,
                Option<std::sync::Arc<schema::CollectionVersion>>,
            > = std::collections::HashMap::new();

            let mut keep = Vec::with_capacity(commits.len());
            for commit in &mut commits {
                let version_id = commit.get("collectionVersionId").and_then(|v| v.as_str());
                let doc_id = commit.get("docID").and_then(|v| v.as_str()).unwrap_or("");

                // Resolve the commit's collection by its version id, INCLUDING
                // inactive versions. Mirrors Go's dagScanNode (GetInactive=true)
                // + fail-closed on an unresolvable version.
                let collection = match version_id {
                    None => None,
                    Some(vid) => match by_version.get(vid) {
                        Some(c) => Some(c.clone()),
                        None => {
                            if !resolved_versions.contains_key(vid) {
                                let looked_up = provider
                                    .get_collection_by_version_id(vid)
                                    .await
                                    .unwrap_or(None);
                                resolved_versions.insert(vid.to_string(), looked_up);
                            }
                            resolved_versions.get(vid).and_then(|c| c.clone())
                        }
                    },
                };

                let allowed = match &collection {
                    // Fail closed: a commit whose collection version cannot be
                    // resolved is denied (Go errors the query here). Never allow
                    // an unresolved version through unchecked.
                    None => false,
                    Some(collection) => match &collection.policy {
                        None => true,
                        Some(policy) => {
                            let checker = crate::txn::OverlayChecker {
                                acp: self.acp.as_ref(),
                                identity: &identity,
                                node_did: node_did.as_ref(),
                            };
                            acp::read_access::check_doc_read_access(
                                &checker,
                                &policy.id,
                                &policy.resource_name,
                                &collection.collection_id,
                                collection.is_branchable,
                                doc_id,
                            )
                            .await
                            .unwrap_or_else(|e| {
                                tracing::debug!(
                                    target: "acp::audit",
                                    event = "commits_acp_check_error",
                                    doc_id = %doc_id,
                                    collection_version_id = ?version_id,
                                    error = %e,
                                    "ACP check error during _commits query, denying access"
                                );
                                false
                            })
                        }
                    },
                };
                if let Some(collection) = collection {
                    commit.set("collectionID", collection.collection_id.clone());
                }
                keep.push(allowed);
            }
            let mut keep = keep.into_iter();
            commits.retain(|_| keep.next().unwrap_or(false));
        }

        // Build a mapping for commit fields (needed for filter evaluation)
        let mapping = Self::build_commits_mapping();

        // Apply filter if present
        if let Some(ref filter) = select.filter {
            commits.retain(|commit| {
                let fields = Self::commit_to_fields(commit, &mapping);
                filter.matches(&fields, &mapping).unwrap_or(true)
            });
        }

        // Apply groupBy if present - this changes how we process commits
        // Each entry is (representative_commit, all_commits_in_group)
        let grouped: Option<Vec<(document::Document, Vec<document::Document>)>> =
            if let Some(ref group_by) = select.group_by {
                if !group_by.fields.is_empty() {
                    let mut groups: Vec<(String, document::Document, Vec<document::Document>)> =
                        Vec::new();
                    let mut group_map: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();

                    for commit in commits.drain(..) {
                        let key = Self::generate_commit_group_key(&commit, &group_by.fields);
                        if let Some(&idx) = group_map.get(&key) {
                            groups[idx].2.push(commit);
                        } else {
                            let idx = groups.len();
                            group_map.insert(key.clone(), idx);
                            groups.push((key, commit.clone(), vec![commit]));
                        }
                    }

                    Some(
                        groups
                            .into_iter()
                            .map(|(_, rep, docs)| (rep, docs))
                            .collect(),
                    )
                } else {
                    None
                }
            } else {
                None
            };

        // Get the list of commits to process (either grouped representatives or all commits)
        let mut work_items: Vec<(document::Document, Option<Vec<document::Document>>)> =
            if let Some(grouped) = grouped {
                grouped
                    .into_iter()
                    .map(|(rep, all)| (rep, Some(all)))
                    .collect()
            } else {
                commits.into_iter().map(|c| (c, None)).collect()
            };

        // Apply ordering if present
        if let Some(ref order_by) = select.order_by {
            for condition in order_by.conditions.iter().rev() {
                if let Some(field_name) = condition.fields.first() {
                    let desc = matches!(condition.direction, OrderDirection::Desc);
                    work_items.sort_by(|(a, _), (b, _)| {
                        let val_a = a.get(field_name);
                        let val_b = b.get(field_name);
                        let cmp = Self::compare_json_values(val_a, val_b);
                        if desc {
                            cmp.reverse()
                        } else {
                            cmp
                        }
                    });
                }
            }
        }

        // Apply limit and offset if present
        if let Some(ref limit_spec) = select.limit {
            let offset = limit_spec.offset as usize;
            if offset > 0 && offset < work_items.len() {
                work_items = work_items.split_off(offset);
            } else if offset >= work_items.len() {
                work_items.clear();
            }
            if let Some(limit) = limit_spec.limit {
                work_items.truncate(limit as usize);
            }
        }

        if select.group_by.is_none() && is_commit_aggregate_only_selection(select) {
            let aggregate_docs: Vec<document::Document> =
                work_items.into_iter().map(|(commit, _)| commit).collect();
            work_items = vec![(document::Document::new(), Some(aggregate_docs))];
        }

        // Build results
        let mut results = Vec::new();
        for (commit, group_docs) in &work_items {
            let mut obj = serde_json::Map::new();

            // Map requested fields from the commit document
            for field in &select.fields {
                match field {
                    Requestable::Field(f) => {
                        let field_name = &f.name;
                        let output_name = f.output_name();

                        // Handle __typename specially for commits (Go returns "Commit")
                        if field_name == "__typename" {
                            obj.insert(
                                output_name.to_string(),
                                JsonValue::String("Commit".to_string()),
                            );
                        } else if let Some(value) = commit.get(field_name) {
                            let json_value = crate::json_convert::normal_value_to_json(value)
                                .unwrap_or(JsonValue::Null);
                            obj.insert(output_name.to_string(), json_value);
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Null);
                        }
                    }
                    Requestable::Aggregate(agg) => {
                        let output_name = agg.output_name();
                        obj.insert(
                            output_name.to_string(),
                            self.compute_commit_aggregate(agg, commit, group_docs.as_deref()),
                        );
                    }
                    Requestable::Select(nested) => {
                        let field_name = &nested.field.name;
                        let output_name = nested.field.output_name();

                        // Handle _group special field for grouped results
                        if field_name == "GROUP" {
                            if let Some(docs) = group_docs {
                                // Build array of group documents with requested fields
                                let group_array: Vec<JsonValue> = docs
                                    .iter()
                                    .map(|doc: &document::Document| {
                                        let mut nested_obj = serde_json::Map::new();
                                        for nested_field in &nested.fields {
                                            if let Requestable::Field(nf) = nested_field {
                                                let nf_name = &nf.name;
                                                let nf_output = nf.output_name();
                                                if let Some(val) = doc.get(nf_name) {
                                                    let json_val =
                                                        crate::json_convert::normal_value_to_json(
                                                            val,
                                                        )
                                                        .unwrap_or(JsonValue::Null);
                                                    nested_obj
                                                        .insert(nf_output.to_string(), json_val);
                                                } else {
                                                    nested_obj.insert(
                                                        nf_output.to_string(),
                                                        JsonValue::Null,
                                                    );
                                                }
                                            }
                                        }
                                        JsonValue::Object(nested_obj)
                                    })
                                    .collect();
                                obj.insert(output_name.to_string(), JsonValue::Array(group_array));
                            } else {
                                // Not grouped, _group is empty
                                obj.insert(output_name.to_string(), JsonValue::Array(vec![]));
                            }
                        } else if let Some(value) = commit.get(field_name) {
                            // Handle nested selects (e.g., links { cid })
                            if let Ok(json_val) = crate::json_convert::normal_value_to_json(value) {
                                if let Some(arr) = json_val.as_array() {
                                    // Handle array nested selects (e.g., links { cid }, heads { cid })
                                    // Apply filter if present (e.g., links(filter: {fieldName: {_eq: "age"}}))
                                    let nested_results: Vec<JsonValue> = arr
                                        .iter()
                                        .filter(|item| {
                                            // Apply nested filter if present
                                            if let Some(ref filter) = nested.filter {
                                                Self::matches_json_filter(item, filter)
                                            } else {
                                                true
                                            }
                                        })
                                        .map(|item: &JsonValue| {
                                            let mut nested_obj = serde_json::Map::new();
                                            for nested_field in &nested.fields {
                                                if let Requestable::Field(nf) = nested_field {
                                                    let nf_name = &nf.name;
                                                    let nf_output = nf.output_name();
                                                    if let Some(nv) = item.get(nf_name) {
                                                        nested_obj.insert(
                                                            nf_output.to_string(),
                                                            nv.clone(),
                                                        );
                                                    } else {
                                                        nested_obj.insert(
                                                            nf_output.to_string(),
                                                            JsonValue::Null,
                                                        );
                                                    }
                                                } else if let Requestable::Select(inner_nested) =
                                                    nested_field
                                                {
                                                    // Handle double-nested selects (e.g., heads { links { cid } })
                                                    let inner_name = &inner_nested.field.name;
                                                    let inner_output =
                                                        inner_nested.field.output_name();
                                                    if let Some(inner_val) = item.get(inner_name) {
                                                        if let Some(inner_arr) = inner_val.as_array()
                                                        {
                                                            // Apply filter and extract requested fields
                                                            let inner_results: Vec<JsonValue> =
                                                                inner_arr
                                                                    .iter()
                                                                    .filter(|inner_item| {
                                                                        if let Some(ref filter) =
                                                                            inner_nested.filter
                                                                        {
                                                                            Self::matches_json_filter(
                                                                                inner_item, filter,
                                                                            )
                                                                        } else {
                                                                            true
                                                                        }
                                                                    })
                                                                    .map(|inner_item| {
                                                                        let mut inner_obj =
                                                                            serde_json::Map::new();
                                                                        for inner_field in
                                                                            &inner_nested.fields
                                                                        {
                                                                            if let Requestable::Field(
                                                                                inf,
                                                                            ) = inner_field
                                                                            {
                                                                                let inf_name =
                                                                                    &inf.name;
                                                                                let inf_output =
                                                                                    inf.output_name(
                                                                                    );
                                                                                if let Some(inv) =
                                                                                    inner_item
                                                                                        .get(inf_name)
                                                                                {
                                                                                    inner_obj.insert(
                                                                                        inf_output
                                                                                            .to_string(
                                                                                            ),
                                                                                        inv.clone(),
                                                                                    );
                                                                                } else {
                                                                                    inner_obj.insert(
                                                                                        inf_output
                                                                                            .to_string(
                                                                                            ),
                                                                                        JsonValue::Null,
                                                                                    );
                                                                                }
                                                                            }
                                                                        }
                                                                        JsonValue::Object(inner_obj)
                                                                    })
                                                                    .collect();
                                                            nested_obj.insert(
                                                                inner_output.to_string(),
                                                                JsonValue::Array(inner_results),
                                                            );
                                                        } else {
                                                            nested_obj.insert(
                                                                inner_output.to_string(),
                                                                inner_val.clone(),
                                                            );
                                                        }
                                                    } else {
                                                        nested_obj.insert(
                                                            inner_output.to_string(),
                                                            JsonValue::Array(vec![]),
                                                        );
                                                    }
                                                }
                                            }
                                            JsonValue::Object(nested_obj)
                                        })
                                        .collect();
                                    obj.insert(
                                        output_name.to_string(),
                                        JsonValue::Array(nested_results),
                                    );
                                } else if json_val.is_object() {
                                    // Handle object nested selects (e.g., signature { type identity value })
                                    let mut nested_obj = serde_json::Map::new();
                                    for nested_field in &nested.fields {
                                        if let Requestable::Field(nf) = nested_field {
                                            let nf_name = &nf.name;
                                            let nf_output = nf.output_name();
                                            if let Some(nv) = json_val.get(nf_name) {
                                                nested_obj
                                                    .insert(nf_output.to_string(), nv.clone());
                                            } else {
                                                nested_obj
                                                    .insert(nf_output.to_string(), JsonValue::Null);
                                            }
                                        }
                                    }
                                    obj.insert(
                                        output_name.to_string(),
                                        JsonValue::Object(nested_obj),
                                    );
                                } else {
                                    obj.insert(output_name.to_string(), JsonValue::Null);
                                }
                            } else {
                                obj.insert(output_name.to_string(), JsonValue::Null);
                            }
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Array(vec![]));
                        }
                    }
                    Requestable::Similarity(_) => {
                        // Similarity is not applicable in commit context
                    }
                    Requestable::FullTextSearch(_) => {
                        // Full-text search is not applicable in commit context
                    }
                }
            }

            results.push(JsonValue::Object(obj));
        }

        Ok(JsonValue::Array(results))
    }
}

#[cfg(test)]
#[path = "commits_tests.rs"]
mod tests;
