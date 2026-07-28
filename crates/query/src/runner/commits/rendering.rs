use document::Document;
use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::txn::TransactionRegistry;

use super::super::{DocFetcher, QueryRunner};

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
        collection_id: &str,
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

        for mut commit in commits {
            // Filter to composite commits
            let field_name = commit
                .get("fieldName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if field_name != "_C" {
                continue;
            }

            commit.set("collectionID", collection_id.to_string());
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
    pub(super) fn render_commit(
        &self,
        commit: &Document,
        version_select: &Select,
    ) -> Result<JsonValue> {
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
}
