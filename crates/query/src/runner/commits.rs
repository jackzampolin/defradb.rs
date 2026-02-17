//! Commits query execution
//!
//! Contains methods for executing _commits system collection queries:
//! - `execute_commits_query()` - Main commits query handler
//! - `render_commit()` / `render_document_fields()` / `fetch_version_data()`
//! - `json_item_matches_filter()` / `check_filter_op()`
//! - `generate_commit_group_key()` / `json_value_to_key()` / `build_commits_mapping()`
//! - `commit_to_fields()` / `compare_json_values()`

use document::Document;
use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::txn::TransactionRegistry;

use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
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
        heads_only: bool,
    ) -> Result<JsonValue> {
        use crate::fetcher::CommitsQueryOptions;

        // For CID queries, traverse deeply from the specific CID block.
        // For heads_only (mutation results), depth=1 returns only current heads.
        // For regular queries, None means unlimited DAG traversal from heads.
        let depth = if target_cid.is_some() {
            Some(1000)
        } else if heads_only {
            Some(1)
        } else {
            None
        };

        let options = CommitsQueryOptions {
            doc_id: Some(doc_id.to_string()),
            cid: target_cid.map(|s| s.to_string()),
            depth,
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
                    // Handle aggregates on commit fields (e.g., _count(links: {}))
                    let output_name = agg.output_name();
                    if let Some(target) = agg.targets.first() {
                        let target_field = target
                            .field_name
                            .as_deref()
                            .filter(|s| !s.is_empty())
                            .or_else(|| Some(target.host_name.as_str()).filter(|s| !s.is_empty()));

                        if let Some(field) = target_field {
                            if let Some(val) = commit.get(field) {
                                if let Ok(json_val) = crate::json_convert::normal_value_to_json(val)
                                {
                                    if let Some(arr) = json_val.as_array() {
                                        obj.insert(
                                            output_name.to_string(),
                                            JsonValue::Number((arr.len() as i64).into()),
                                        );
                                    } else {
                                        obj.insert(
                                            output_name.to_string(),
                                            JsonValue::Number(0.into()),
                                        );
                                    }
                                } else {
                                    obj.insert(
                                        output_name.to_string(),
                                        JsonValue::Number(0.into()),
                                    );
                                }
                            } else {
                                obj.insert(output_name.to_string(), JsonValue::Number(0.into()));
                            }
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Number(1.into()));
                        }
                    } else {
                        obj.insert(output_name.to_string(), JsonValue::Number(1.into()));
                    }
                }
                Requestable::Similarity(_) => {
                    // Similarity is not applicable in commit context
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
                                    let sub_map: std::collections::HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(sub_map);
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
                                    let sub_map: std::collections::HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(sub_map);
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
                            let sub_map: std::collections::HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            let sub_filter = crate::mapper::Filter::from_conditions(sub_map);
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
    pub(crate) async fn execute_commits_query(&self, select: &Select) -> Result<JsonValue> {
        use crate::fetcher::CommitsQueryOptions;
        use crate::mapper::{AggregateType, OrderDirection};

        // Build options from the select
        let options = CommitsQueryOptions {
            doc_id: select.doc_ids.as_ref().and_then(|ids| ids.first().cloned()),
            cid: select.cid.clone(),
            depth: select.depth,
            field_name: None,
        };

        // Fetch commits using the fetcher
        let mut commits = self.fetcher.get_commits(&options).await?;

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
                        // Handle aggregates like _count on links/heads
                        let output_name = agg.output_name();
                        let count = match agg.aggregate_type {
                            AggregateType::Count => {
                                // Count on a target field (e.g., links, heads)
                                if let Some(target) = agg.targets.first() {
                                    // Get the target field name - check field_name first (from
                                    // `field:` arg), then host_name (from relation syntax)
                                    let target_field = target
                                        .field_name
                                        .as_deref()
                                        .filter(|s| !s.is_empty())
                                        .or_else(|| {
                                            Some(target.host_name.as_str())
                                                .filter(|s| !s.is_empty())
                                        });

                                    if let Some(field) = target_field {
                                        if let Some(val) = commit.get(field) {
                                            // Convert NormalValue to JSON to check array
                                            if let Ok(json_val) =
                                                crate::json_convert::normal_value_to_json(val)
                                            {
                                                if let Some(arr) = json_val.as_array() {
                                                    arr.len() as i64
                                                } else {
                                                    0
                                                }
                                            } else {
                                                0
                                            }
                                        } else {
                                            0
                                        }
                                    } else {
                                        1 // Count without target = count this commit
                                    }
                                } else {
                                    1 // Count without target = count this commit
                                }
                            }
                            _ => 0, // Other aggregates not supported for commits
                        };
                        obj.insert(output_name.to_string(), JsonValue::Number(count.into()));
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
