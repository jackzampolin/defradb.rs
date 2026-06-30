//! Commits query execution
//!
//! Contains methods for executing _commits system collection queries:
//! - `execute_commits_query()` - Main commits query handler
//! - `render_commit()` / `render_document_fields()` / `fetch_version_data()`
//! - `json_item_matches_filter()` / `check_filter_op()`
//! - `generate_commit_group_key()` / `json_value_to_key()` / `build_commits_mapping()`
//! - `commit_to_fields()` / `compare_json_values()`

use document::Document;
use identity::Did;
use serde_json::Value as JsonValue;
use std::collections::HashSet;

use crate::error::Result;
use crate::mapper::{Aggregate, AggregateType, Requestable, Select};
use crate::txn::TransactionRegistry;

use super::commits_height::{extract_commits_height_range, HeightRangeExtraction};
use super::commits_numeric::{
    max_commit_numeric_values, min_commit_numeric_values, sum_commit_numeric_values,
    CommitNumericValue,
};
use super::{DocFetcher, QueryRunner};

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_filter(value: JsonValue) -> crate::mapper::Filter {
        crate::mapper::Filter::from_conditions(serde_json::from_value(value).unwrap())
    }

    #[test]
    fn test_extract_commits_height_range_simple_window() {
        let filter = make_filter(json!({
            "height": {
                "_gte": 2,
                "_lt": 5
            }
        }));

        assert_eq!(
            extract_commits_height_range(&filter),
            HeightRangeExtraction::Range(super::super::commits_height::CommitsHeightRange {
                start: 2,
                end: Some(5),
            })
        );
    }

    #[test]
    fn test_extract_commits_height_range_merges_and_conditions() {
        let filter = make_filter(json!({
            "_and": [
                { "height": { "_gte": 2 } },
                { "height": { "_lt": 4 } },
                { "fieldName": { "_eq": "_C" } }
            ]
        }));

        assert_eq!(
            extract_commits_height_range(&filter),
            HeightRangeExtraction::Range(super::super::commits_height::CommitsHeightRange {
                start: 2,
                end: Some(4),
            })
        );
    }

    #[test]
    fn test_extract_commits_height_range_ignores_non_height_or_clauses() {
        let filter = make_filter(json!({
            "height": { "_gte": 2 },
            "_or": [
                { "fieldName": { "_eq": "_C" } },
                { "fieldName": { "_eq": "age" } }
            ]
        }));

        assert_eq!(
            extract_commits_height_range(&filter),
            HeightRangeExtraction::Range(super::super::commits_height::CommitsHeightRange {
                start: 2,
                end: None,
            })
        );
    }

    #[test]
    fn test_extract_commits_height_range_rejects_disjunctive_height_filters() {
        let filter = make_filter(json!({
            "_or": [
                { "height": { "_eq": 1 } },
                { "height": { "_eq": 3 } }
            ]
        }));

        assert_eq!(
            extract_commits_height_range(&filter),
            HeightRangeExtraction::Unsupported
        );
    }

    #[test]
    fn test_extract_commits_height_range_detects_empty_window() {
        let filter = make_filter(json!({
            "height": {
                "_gt": 10,
                "_lt": 5
            }
        }));

        assert_eq!(
            extract_commits_height_range(&filter),
            HeightRangeExtraction::Empty
        );
    }

    #[test]
    fn test_commit_sum_preserves_large_int_precision() {
        let values = [
            CommitNumericValue::Int(9_007_199_254_740_992),
            CommitNumericValue::Int(1),
        ];
        let sum = sum_commit_numeric_values(&values);
        assert_eq!(sum.as_i64(), Some(9_007_199_254_740_993));
    }

    #[test]
    fn test_commit_min_max_preserve_large_int_precision() {
        let values = [
            CommitNumericValue::Int(9_007_199_254_740_993),
            CommitNumericValue::Int(9_007_199_254_740_992),
        ];
        assert_eq!(
            min_commit_numeric_values(&values).as_i64(),
            Some(9_007_199_254_740_992)
        );
        assert_eq!(
            max_commit_numeric_values(&values).as_i64(),
            Some(9_007_199_254_740_993)
        );
    }
}

fn is_commit_aggregate_only_selection(select: &Select) -> bool {
    !select.fields.is_empty()
        && select
            .fields
            .iter()
            .all(|field| matches!(field, Requestable::Aggregate(_)))
}

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    fn commit_aggregate_target_field<'a>(&self, agg: &'a Aggregate) -> Option<&'a str> {
        agg.targets.first().and_then(|target| {
            target
                .field_name
                .as_deref()
                .filter(|field_name| !field_name.is_empty())
                .or_else(|| {
                    Some(target.host_name.as_str()).filter(|host_name| !host_name.is_empty())
                })
        })
    }

    fn normal_value_to_commit_numeric(
        &self,
        value: &document::NormalValue,
    ) -> Option<CommitNumericValue> {
        value.as_int().map(CommitNumericValue::Int).or_else(|| {
            value
                .as_float64()
                .map(CommitNumericValue::Float)
                .or_else(|| {
                    value
                        .as_float32()
                        .map(|value| CommitNumericValue::Float(value as f64))
                })
        })
    }

    fn decode_commit_delta_numeric(&self, commit: &Document) -> Option<CommitNumericValue> {
        use base64::Engine;

        let delta_base64 = commit.get("delta")?.as_str()?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(delta_base64)
            .ok()?;
        let value = ciborium::from_reader::<document::NormalValue, _>(&bytes[..]).ok()?;
        self.normal_value_to_commit_numeric(&value)
    }

    fn commit_numeric_value(
        &self,
        commit: &Document,
        field_name: &str,
    ) -> Option<CommitNumericValue> {
        if field_name == "delta" {
            self.decode_commit_delta_numeric(commit)
        } else {
            commit
                .get(field_name)
                .and_then(|value| self.normal_value_to_commit_numeric(value))
        }
    }

    fn collect_commit_numeric_values(
        &self,
        commit: &Document,
        group_docs: Option<&[Document]>,
        field_name: &str,
    ) -> Vec<CommitNumericValue> {
        let mut values = Vec::new();

        if let Some(group_docs) = group_docs {
            for doc in group_docs {
                if let Some(value) = self.commit_numeric_value(doc, field_name) {
                    values.push(value);
                }
            }
        } else if let Some(value) = self.commit_numeric_value(commit, field_name) {
            values.push(value);
        }

        values
    }

    fn count_commit_aggregate_values(
        &self,
        commit: &Document,
        group_docs: Option<&[Document]>,
        field_name: Option<&str>,
    ) -> i64 {
        match field_name {
            None => group_docs.map(|docs| docs.len()).unwrap_or(1) as i64,
            Some(field_name) => {
                let mut count = 0i64;

                let mut count_doc = |doc: &Document| {
                    if field_name == "delta" {
                        if self.decode_commit_delta_numeric(doc).is_some() {
                            count += 1;
                        }
                        return;
                    }

                    if let Some(value) = doc.get(field_name) {
                        if let Ok(json_value) = crate::json_convert::normal_value_to_json(value) {
                            if let Some(array) = json_value.as_array() {
                                count += array.len() as i64;
                            } else if !json_value.is_null() {
                                count += 1;
                            }
                        }
                    }
                };

                if let Some(group_docs) = group_docs {
                    for doc in group_docs {
                        count_doc(doc);
                    }
                } else {
                    count_doc(commit);
                }

                count
            }
        }
    }

    fn compute_commit_aggregate(
        &self,
        agg: &Aggregate,
        commit: &Document,
        group_docs: Option<&[Document]>,
    ) -> JsonValue {
        match agg.aggregate_type {
            AggregateType::Count => {
                let count = self.count_commit_aggregate_values(
                    commit,
                    group_docs,
                    self.commit_aggregate_target_field(agg),
                );
                JsonValue::Number(count.into())
            }
            AggregateType::Sum => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Number(0.into());
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                sum_commit_numeric_values(&values)
            }
            AggregateType::Average => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Number(0.into());
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().map(|value| value.as_f64()).sum::<f64>() / values.len() as f64
                };
                serde_json::Number::from_f64(avg)
                    .map(JsonValue::Number)
                    .unwrap_or_else(|| JsonValue::Number(0.into()))
            }
            AggregateType::Min => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Null;
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                min_commit_numeric_values(&values)
            }
            AggregateType::Max => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Null;
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                max_commit_numeric_values(&values)
            }
        }
    }

    /// Render a Document's fields as a JSON object using only the fields requested by a Select.
    pub(crate) fn render_document_fields(
        &self,
        doc: &Document,
        select: &Select,
    ) -> serde_json::Map<String, JsonValue> {
        let mut obj = serde_json::Map::new();
        for field in &select.fields {
            if let Requestable::Field(f) = field {
                let fname = &f.name;
                let output = f.output_name();
                if fname == "_docID" {
                    if let Some(id) = doc.id() {
                        obj.insert(output.to_string(), JsonValue::String(id.to_string()));
                    } else {
                        obj.insert(output.to_string(), JsonValue::Null);
                    }
                } else if fname == "__typename" {
                    obj.insert(
                        output.to_string(),
                        JsonValue::String(select.collection_name.clone()),
                    );
                } else if let Some(nv) = doc.get(fname) {
                    let json_val =
                        crate::json_convert::normal_value_to_json(nv).unwrap_or(JsonValue::Null);
                    obj.insert(output.to_string(), json_val);
                } else {
                    obj.insert(output.to_string(), JsonValue::Null);
                }
            }
        }
        obj
    }

    /// Fetch version (commit) data for a document.
    ///
    /// Returns an array of commit objects filtered to composite commits (fieldName = "_C")
    /// and rendered with the requested fields from the _version selection.
    pub(crate) async fn fetch_version_data(
        &self,
        fetcher: &dyn DocFetcher,
        doc_id: &str,
        version_select: &Select,
        target_cid: Option<&str>,
    ) -> Result<JsonValue> {
        use crate::fetcher::CommitsQueryOptions;

        // For CID queries, traverse deeply from the specific CID block.
        // For all other cases (mutation results, regular queries), None means
        // unlimited DAG traversal from heads - returning all versions.
        let depth = if target_cid.is_some() {
            Some(1000)
        } else {
            None
        };

        let options = CommitsQueryOptions {
            doc_id: Some(doc_id.to_string()),
            cid: target_cid.map(|s| s.to_string()),
            depth,
            height_start: None,
            height_end: None,
            field_name: None,
        };

        let commits = fetcher.get_commits(&options).await?;

        // Filter to composite commits only (fieldName = "_C")
        // and render the requested fields
        let mut version_results: Vec<JsonValue> = Vec::new();

        for commit in commits {
            // Filter to composite commits
            let field_name = commit
                .get("fieldName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if field_name != "_C" {
                continue;
            }

            let commit_json = self.render_commit(&commit, version_select)?;
            version_results.push(commit_json);
        }

        // Sort by height descending (newest first)
        version_results.sort_by(|a, b| {
            let h_a = a.get("height").and_then(|v| v.as_i64()).unwrap_or(0);
            let h_b = b.get("height").and_then(|v| v.as_i64()).unwrap_or(0);
            h_b.cmp(&h_a)
        });

        Ok(JsonValue::Array(version_results))
    }

    /// Render a commit document according to the _version selection fields.
    fn render_commit(&self, commit: &Document, version_select: &Select) -> Result<JsonValue> {
        let mut obj = serde_json::Map::new();

        for requestable in &version_select.fields {
            match requestable {
                Requestable::Field(f) => {
                    let field_name = &f.name;
                    let output_name = f.output_name();

                    if let Some(value) = commit.get(field_name) {
                        let json_value = crate::json_convert::normal_value_to_json(value)
                            .unwrap_or(JsonValue::Null);
                        obj.insert(output_name.to_string(), json_value);
                    } else {
                        obj.insert(output_name.to_string(), JsonValue::Null);
                    }
                }
                Requestable::Select(nested) => {
                    let field_name = &nested.field.name;
                    let output_name = nested.field.output_name();

                    // Handle nested selections (links, heads) with optional filter
                    if let Some(value) = commit.get(field_name) {
                        if let Ok(json_val) = crate::json_convert::normal_value_to_json(value) {
                            if let Some(arr) = json_val.as_array() {
                                // Apply filter if present on the nested selection
                                let filtered_items: Vec<&JsonValue> =
                                    if let Some(ref filter) = nested.filter {
                                        arr.iter()
                                            .filter(|item| {
                                                // Check each filter condition against the item
                                                self.json_item_matches_filter(item, filter)
                                            })
                                            .collect()
                                    } else {
                                        arr.iter().collect()
                                    };

                                let nested_results: Vec<JsonValue> = filtered_items
                                    .into_iter()
                                    .map(|item| {
                                        let mut nested_obj = serde_json::Map::new();
                                        for nested_field in &nested.fields {
                                            if let Requestable::Field(nf) = nested_field {
                                                let nf_name = &nf.name;
                                                let nf_output = nf.output_name();
                                                if let Some(nv) = item.get(nf_name) {
                                                    nested_obj
                                                        .insert(nf_output.to_string(), nv.clone());
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
                                obj.insert(
                                    output_name.to_string(),
                                    JsonValue::Array(nested_results),
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
                Requestable::Aggregate(agg) => {
                    let output_name = agg.output_name();
                    obj.insert(
                        output_name.to_string(),
                        self.compute_commit_aggregate(agg, commit, None),
                    );
                }
                Requestable::Similarity(_) => {
                    // Similarity is not applicable in commit context
                }
                Requestable::FullTextSearch(_) => {
                    // Full-text search is not applicable in commit context
                }
            }
        }

        Ok(JsonValue::Object(obj))
    }

    /// Check if a JSON object matches a filter for nested commit selections.
    ///
    /// This is a simplified filter matcher for nested selections like `links(filter: {fieldName: {_eq: "Age"}})`.
    /// The filter conditions are stored as `{field_name: {_op: value}}`.
    fn json_item_matches_filter(&self, item: &JsonValue, filter: &crate::mapper::Filter) -> bool {
        use crate::mapper::FilterOp;

        // Get the filter conditions - a map of field_name -> operator conditions
        let conditions = filter.conditions();

        for (field_name, condition_value) in conditions {
            // Check if this is a logical operator (_and, _or, _not)
            if let Some(op) = FilterOp::parse(field_name) {
                match op {
                    FilterOp::And => {
                        if let JsonValue::Array(arr) = condition_value {
                            for sub_cond in arr {
                                if let JsonValue::Object(obj) = sub_cond {
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(obj.clone());
                                    if !self.json_item_matches_filter(item, &sub_filter) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    FilterOp::Or => {
                        if let JsonValue::Array(arr) = condition_value {
                            let mut any_match = false;
                            for sub_cond in arr {
                                if let JsonValue::Object(obj) = sub_cond {
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(obj.clone());
                                    if self.json_item_matches_filter(item, &sub_filter) {
                                        any_match = true;
                                        break;
                                    }
                                }
                            }
                            if !any_match {
                                return false;
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = condition_value {
                            let sub_filter = crate::mapper::Filter::from_conditions(obj.clone());
                            if self.json_item_matches_filter(item, &sub_filter) {
                                return false;
                            }
                        }
                    }
                    _ => {}
                }
                continue;
            }

            // This is a field condition: field_name -> {_op: value}
            let item_value = item.get(field_name);

            // The condition_value should be an object like {"_eq": "Age"}
            if let JsonValue::Object(ops) = condition_value {
                for (op_name, expected_value) in ops {
                    if let Some(op) = FilterOp::parse(op_name) {
                        let matches = self.check_filter_op(item_value, op, expected_value);
                        if !matches {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }

    /// Check if an item value matches a filter operator condition.
    fn check_filter_op(
        &self,
        item_value: Option<&JsonValue>,
        op: crate::mapper::FilterOp,
        expected: &JsonValue,
    ) -> bool {
        use crate::mapper::FilterOp;

        match op {
            FilterOp::Eq => match (item_value, expected) {
                (Some(JsonValue::String(a)), JsonValue::String(b)) => a == b,
                (Some(JsonValue::Number(a)), JsonValue::Number(b)) => a == b,
                (Some(JsonValue::Bool(a)), JsonValue::Bool(b)) => a == b,
                (Some(JsonValue::Null), JsonValue::Null) => true,
                (None, JsonValue::Null) => true,
                _ => false,
            },
            FilterOp::Ne => match (item_value, expected) {
                (Some(JsonValue::String(a)), JsonValue::String(b)) => a != b,
                (Some(JsonValue::Number(a)), JsonValue::Number(b)) => a != b,
                (Some(JsonValue::Bool(a)), JsonValue::Bool(b)) => a != b,
                (Some(JsonValue::Null), JsonValue::Null) => false,
                (None, _) => true,
                _ => true,
            },
            FilterOp::Gt => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a > b,
                _ => false,
            },
            FilterOp::Gte => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a >= b,
                _ => false,
            },
            FilterOp::Lt => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a < b,
                _ => false,
            },
            FilterOp::Lte => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a <= b,
                _ => false,
            },
            FilterOp::In => {
                if let JsonValue::Array(values) = expected {
                    item_value.map(|v| values.contains(v)).unwrap_or(false)
                } else {
                    false
                }
            }
            FilterOp::Nin => {
                if let JsonValue::Array(values) = expected {
                    item_value.map(|v| !values.contains(v)).unwrap_or(true)
                } else {
                    true
                }
            }
            _ => true, // For unsupported operators, default to matching
        }
    }

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
                    if seen_commit_cids.insert(commit_cid.to_string()) {
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

            let collections = self.collections_map().await?;
            let by_version: std::collections::HashMap<
                String,
                std::sync::Arc<schema::CollectionVersion>,
            > = collections
                .values()
                .map(|c| (c.version_id.clone(), c.clone()))
                .collect();

            let mut keep = Vec::with_capacity(commits.len());
            for commit in &commits {
                let version_id = commit.get("collectionVersionId").and_then(|v| v.as_str());
                let doc_id = commit.get("docID").and_then(|v| v.as_str()).unwrap_or("");

                let allowed = match version_id.and_then(|v| by_version.get(v)) {
                    None => true,
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

    /// Generate a group key from commit field values.
    /// Format matches Go DefraDB: `{index}_{value}_` for each field.
    fn generate_commit_group_key(commit: &document::Document, fields: &[String]) -> String {
        let mut key = String::new();
        for (i, field_name) in fields.iter().enumerate() {
            key.push_str(&format!("{}_", i));
            if let Some(value) = commit.get(field_name) {
                if let Ok(json_val) = crate::json_convert::normal_value_to_json(value) {
                    key.push_str(&Self::json_value_to_key(&json_val));
                } else {
                    key.push_str("null");
                }
            } else {
                key.push_str("null");
            }
            key.push('_');
        }
        key
    }

    /// Convert a JSON value to a string for use in group key.
    fn json_value_to_key(value: &JsonValue) -> String {
        match value {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => s.clone(),
            JsonValue::Array(arr) => {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(Self::json_value_to_key)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            JsonValue::Object(obj) => {
                format!(
                    "{{{}}}",
                    obj.iter()
                        .map(|(k, v)| format!("{}:{}", k, Self::json_value_to_key(v)))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }

    /// Check if a JSON value matches a filter.
    /// Used for filtering nested arrays like links and heads in commit queries.
    fn matches_json_filter(value: &JsonValue, filter: &crate::Filter) -> bool {
        filter.matches_json_object(value).unwrap_or(false)
    }

    /// Build a DocumentMapping for commit fields.
    fn build_commits_mapping() -> crate::document::DocumentMapping {
        let mut mapping = crate::document::DocumentMapping::new();
        mapping.add(0, "cid");
        mapping.add(1, "height");
        mapping.add(2, "fieldName");
        mapping.add(3, "docID");
        mapping.add(4, "delta");
        mapping.add(5, "collectionVersionId");
        mapping.add(6, "links");
        mapping.add(7, "heads");
        mapping.add(8, "signature");
        mapping
    }

    /// Convert a commit document to a fields array for filter evaluation.
    fn commit_to_fields(
        commit: &document::Document,
        _mapping: &crate::document::DocumentMapping,
    ) -> Vec<Option<JsonValue>> {
        let field_names = [
            "cid",
            "height",
            "fieldName",
            "docID",
            "delta",
            "collectionVersionId",
            "links",
            "heads",
            "signature",
        ];
        field_names
            .iter()
            .map(|name| {
                commit
                    .get(name)
                    .and_then(|v| crate::json_convert::normal_value_to_json(v).ok())
            })
            .collect()
    }

    /// Compare two JSON values for ordering.
    fn compare_json_values(
        a: Option<&document::NormalValue>,
        b: Option<&document::NormalValue>,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(va), Some(vb)) => {
                // Convert to JSON for comparison
                let ja = crate::json_convert::normal_value_to_json(va).ok();
                let jb = crate::json_convert::normal_value_to_json(vb).ok();

                match (ja, jb) {
                    (Some(JsonValue::Number(na)), Some(JsonValue::Number(nb))) => {
                        let fa = na.as_f64().unwrap_or(0.0);
                        let fb = nb.as_f64().unwrap_or(0.0);
                        fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
                    }
                    (Some(JsonValue::String(sa)), Some(JsonValue::String(sb))) => sa.cmp(&sb),
                    (Some(a), Some(b)) => a.to_string().cmp(&b.to_string()),
                    _ => Ordering::Equal,
                }
            }
        }
    }
}
