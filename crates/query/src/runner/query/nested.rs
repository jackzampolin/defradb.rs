//! Planner orchestration and post-processing for nested queries.

use identity::Did;
use schema::{CollectionVersion, FieldDescription};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;

use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::planner::Planner;
use crate::txn::TransactionRegistry;
use std::collections::HashMap;

use super::super::fetcher::FetcherWrapper;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    /// ACP permission filtering is applied per-collection via PermissionFilterNode in the plan.
    pub(crate) async fn execute_nested_select_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Create a fetcher wrapper that can be shared across plan nodes
        // We need to wrap the reference in an Arc-compatible struct
        let fetcher_arc = FetcherWrapper::new(fetcher);

        // Build the plan using the Planner with fetcher support
        // Get all collections from provider for join planning
        let collections_map = self.collections_map().await?;
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        // Pre-compute FTS scores from the inverted index before planning.
        // Supports dotted relation paths like `file.name` and `functions.content`
        // by querying the leaf collection's BM25 index and lifting scores back
        // onto the root collection through relation foreign keys.
        let fts_scores = self
            .precompute_fulltext_scores(select, fetcher, &collections_map)
            .await?;

        let mut planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
        if !fts_scores.is_empty() {
            planner = planner.with_fts_scores(fts_scores);
        }
        if let Some(ref acp) = self.acp {
            planner = planner.with_acp(acp.clone(), identity);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        let mut plan = plan_result.plan;
        let ordering_only_fields = plan_result.ordering_only_fields;
        let aggregate_internal_keys = plan_result.aggregate_internal_keys;

        // Get the mapping from the plan
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let mut json = self.doc_to_json(doc, &mapping)?;

            // Strip ordering-only fields from nested objects.
            // These fields were added for ORDER BY but shouldn't appear in output.
            for (relation_field, nested_field) in &ordering_only_fields {
                if let Some(obj) = json.as_object_mut() {
                    if let Some(relation_value) = obj.get_mut(relation_field) {
                        if let Some(nested_obj) = relation_value.as_object_mut() {
                            nested_obj.remove(nested_field);
                        }
                    }
                }
            }

            results.push(json);
        }

        plan.close().await?;

        // Post-process relation-based aggregates
        // For aggregates like _count(books: {}), compute the value from joined data
        let results =
            self.compute_relation_aggregates(results, select, &aggregate_internal_keys)?;

        // Strip fields from relation data that were added for filter evaluation
        // but not explicitly requested in the selection set.
        let results = Self::clean_filter_only_relation_fields(results, select);

        // Apply deferred limit/offset to relation fields.
        // TypeJoinMany stores ALL children (for aggregates to count), so we apply
        // the select's limit/offset here after aggregates have been computed.
        let results = Self::apply_relation_limits(results, select);

        Ok(JsonValue::Array(results))
    }

    async fn precompute_fulltext_scores(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        collections_map: &HashMap<String, Arc<CollectionVersion>>,
    ) -> Result<HashMap<String, HashMap<String, f64>>> {
        let root_collection = collections_map
            .get(&select.collection_name)
            .cloned()
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        let mut fts_scores: HashMap<String, HashMap<String, f64>> = HashMap::new();
        let mut worklist: Vec<(&Select, Arc<CollectionVersion>, Vec<String>)> = vec![(
            select,
            root_collection,
            vec![select.field.output_name().to_string()],
        )];

        while let Some((current_select, current_collection, scope_path)) = worklist.pop() {
            for field in &current_select.fields {
                match field {
                    Requestable::FullTextSearch(fts) => {
                        let mut combined_scores: HashMap<String, f64> = HashMap::new();
                        for target_field in &fts.target_fields {
                            if let Ok(scores) = self
                                .compute_fulltext_path_scores(
                                    current_collection.clone(),
                                    target_field,
                                    &fts.query,
                                    fetcher,
                                    collections_map,
                                )
                                .await
                            {
                                for (doc_id, score) in scores {
                                    *combined_scores.entry(doc_id).or_insert(0.0) += score;
                                }
                            }
                        }

                        let score_key = Planner::fts_score_key(&scope_path, fts.output_name());
                        fts_scores.insert(score_key, combined_scores);
                    }
                    Requestable::Select(nested_select) => {
                        if nested_select.field.name == "GROUP" {
                            continue;
                        }

                        let Some(relation_field) =
                            current_collection.field_by_name(&nested_select.field.name)
                        else {
                            continue;
                        };
                        if !relation_field.kind.is_relation() {
                            continue;
                        }

                        let target_collection = Self::resolve_relation_target_collection(
                            &current_collection,
                            relation_field,
                            collections_map,
                        )
                        .ok_or_else(|| {
                            QueryError::execution(format!(
                                "Unable to resolve BM25 relation target '{}.{}'",
                                current_collection.name, nested_select.field.name
                            ))
                        })?;

                        let mut child_scope_path = scope_path.clone();
                        child_scope_path.push(nested_select.field.output_name().to_string());
                        worklist.push((nested_select, target_collection, child_scope_path));
                    }
                    _ => {}
                }
            }
        }

        Ok(fts_scores)
    }

    async fn compute_fulltext_path_scores(
        &self,
        root_collection: Arc<CollectionVersion>,
        path: &str,
        query: &str,
        fetcher: &dyn DocFetcher,
        collections_map: &HashMap<String, Arc<CollectionVersion>>,
    ) -> Result<HashMap<String, f64>> {
        let path_segments: Vec<&str> = path
            .split('.')
            .filter(|segment| !segment.is_empty())
            .collect();
        if path_segments.is_empty() {
            return Err(QueryError::execution(
                "BM25 field path must not be empty".to_string(),
            ));
        }

        let mut current_collection = root_collection;
        let mut relation_chain: Vec<(
            Arc<CollectionVersion>,
            FieldDescription,
            Arc<CollectionVersion>,
        )> = Vec::new();

        for relation_name in &path_segments[..path_segments.len() - 1] {
            let relation_field = current_collection
                .field_by_name(relation_name)
                .ok_or_else(|| QueryError::unknown_field(*relation_name))?;

            if !relation_field.kind.is_relation() {
                return Err(QueryError::execution(format!(
                    "BM25 relation path segment '{}' on collection '{}' is not a relation",
                    relation_name, current_collection.name
                )));
            }

            let target_collection = Self::resolve_relation_target_collection(
                &current_collection,
                relation_field,
                collections_map,
            )
            .ok_or_else(|| {
                QueryError::execution(format!(
                    "Unable to resolve BM25 relation target '{}.{}'",
                    current_collection.name, relation_name
                ))
            })?;

            relation_chain.push((
                current_collection.clone(),
                relation_field.clone(),
                target_collection.clone(),
            ));
            current_collection = target_collection;
        }

        let leaf_field = path_segments[path_segments.len() - 1];
        let mut scores = fetcher
            .search_fulltext_scored(&current_collection.name, leaf_field, query)
            .await?;

        for (parent_collection, relation_field, target_collection) in
            relation_chain.into_iter().rev()
        {
            scores = self
                .lift_fulltext_scores_to_parent(
                    &parent_collection,
                    &relation_field,
                    &target_collection,
                    scores,
                    fetcher,
                )
                .await?;
        }

        Ok(scores)
    }

    fn resolve_relation_target_collection(
        current_collection: &Arc<CollectionVersion>,
        relation_field: &FieldDescription,
        collections_map: &HashMap<String, Arc<CollectionVersion>>,
    ) -> Option<Arc<CollectionVersion>> {
        let target_collection_id = relation_field.kind.relation_collection_id()?;

        if target_collection_id.is_empty() {
            return Some(current_collection.clone());
        }

        collections_map
            .get(target_collection_id)
            .cloned()
            .or_else(|| {
                collections_map.values().find_map(|collection| {
                    (collection.collection_id == target_collection_id
                        || collection.version_id == target_collection_id)
                        .then(|| collection.clone())
                })
            })
            .or_else(|| {
                let relation_name = relation_field.relation_name.as_deref()?;
                collections_map.values().find_map(|collection| {
                    if collection.name == current_collection.name {
                        return None;
                    }
                    collection
                        .fields
                        .iter()
                        .any(|field| field.relation_name.as_deref() == Some(relation_name))
                        .then(|| collection.clone())
                })
            })
    }

    async fn lift_fulltext_scores_to_parent(
        &self,
        parent_collection: &Arc<CollectionVersion>,
        relation_field: &FieldDescription,
        target_collection: &Arc<CollectionVersion>,
        child_scores: HashMap<String, f64>,
        fetcher: &dyn DocFetcher,
    ) -> Result<HashMap<String, f64>> {
        if child_scores.is_empty() {
            return Ok(HashMap::new());
        }

        // Primary non-array relations hold the FK on the parent document itself.
        if !relation_field.kind.is_array() && relation_field.is_primary {
            let fk_field_name = CollectionVersion::relation_id_field_name(&relation_field.name);
            let parent_docs = fetcher.get_all(&parent_collection.name).await?;
            let mut parent_scores: HashMap<String, f64> = HashMap::new();

            for doc in parent_docs {
                let Some(parent_id) = doc.id().map(|id| id.to_string()) else {
                    continue;
                };
                let Some(target_id) = doc.get(&fk_field_name).and_then(|value| value.as_str())
                else {
                    continue;
                };
                let Some(score) = child_scores.get(target_id) else {
                    continue;
                };
                *parent_scores.entry(parent_id).or_insert(0.0) += *score;
            }

            return Ok(parent_scores);
        }

        let target_relation_field = relation_field
            .relation_name
            .as_ref()
            .and_then(|relation_name| {
                target_collection.field_by_relation(
                    relation_name,
                    &parent_collection.name,
                    &relation_field.name,
                )
            })
            .ok_or_else(|| {
                QueryError::execution(format!(
                    "Unable to resolve reverse BM25 relation '{}.{}'",
                    parent_collection.name, relation_field.name
                ))
            })?;

        if target_relation_field.kind.is_array() || !target_relation_field.is_primary {
            return Err(QueryError::execution(format!(
                "BM25 relation path '{}.{}' does not resolve to a primary foreign key holder",
                parent_collection.name, relation_field.name
            )));
        }

        let fk_field_name = CollectionVersion::relation_id_field_name(&target_relation_field.name);
        let child_doc_ids: Vec<String> = child_scores.keys().cloned().collect();
        let child_docs = fetcher
            .get_by_ids(&target_collection.name, &child_doc_ids)
            .await?
            .into_docs();

        let mut parent_scores: HashMap<String, f64> = HashMap::new();
        for doc in child_docs {
            let Some(child_id) = doc.id().map(|id| id.to_string()) else {
                continue;
            };
            let Some(parent_id) = doc.get(&fk_field_name).and_then(|value| value.as_str()) else {
                continue;
            };
            let Some(score) = child_scores.get(&child_id) else {
                continue;
            };
            *parent_scores.entry(parent_id.to_string()).or_insert(0.0) += *score;
        }

        Ok(parent_scores)
    }

    /// Strip filter-only fields from relation data in query results.
    ///
    /// When the planner adds relation joins for filter evaluation (e.g., filtering
    /// Author by book.publisher.yearOpened), those relations get render_keys so
    /// the filter can evaluate on rendered JSON. This causes the relation field to
    /// appear in output even though the user didn't request it. This function
    /// retains only the fields explicitly listed in each nested Select.
    fn clean_filter_only_relation_fields(
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
        // Build map of relation output_name → allowed sub-field names
        let mut relation_allowed_fields: Vec<(String, HashSet<String>)> = Vec::new();

        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "GROUP" {
                    continue;
                }
                let mut allowed = HashSet::new();
                // _docID is always implicit
                allowed.insert("_docID".to_string());
                for sub_field in &nested_select.fields {
                    match sub_field {
                        Requestable::Field(f) => {
                            allowed.insert(f.output_name().to_string());
                        }
                        Requestable::Select(s) => {
                            allowed.insert(s.field.output_name().to_string());
                        }
                        Requestable::Aggregate(a) => {
                            allowed.insert(a.output_name().to_string());
                        }
                        Requestable::Similarity(s) => {
                            allowed.insert(s.output_name().to_string());
                        }
                        Requestable::FullTextSearch(fts) => {
                            allowed.insert(fts.output_name().to_string());
                        }
                    }
                }
                relation_allowed_fields
                    .push((nested_select.field.output_name().to_string(), allowed));
            }
        }

        if relation_allowed_fields.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (relation_name, allowed_fields) in &relation_allowed_fields {
                    if let Some(relation_data) = obj.get_mut(relation_name.as_str()) {
                        match relation_data {
                            JsonValue::Array(items) => {
                                for item in items.iter_mut() {
                                    if let JsonValue::Object(item_obj) = item {
                                        item_obj.retain(|k, _| allowed_fields.contains(k));
                                    }
                                }
                            }
                            JsonValue::Object(item_obj) => {
                                item_obj.retain(|k, _| allowed_fields.contains(k));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        results
    }

    /// Apply deferred limit/offset to relation fields in query results.
    ///
    /// TypeJoinMany stores ALL children so that relation aggregates (e.g., _count)
    /// can see the full set. This function applies the limit/offset from the select's
    /// nested relation fields after aggregates have been computed.
    fn apply_relation_limits(mut results: Vec<JsonValue>, select: &Select) -> Vec<JsonValue> {
        // Collect relation fields with limits
        let mut relation_limits: Vec<(String, u64, u64)> = Vec::new(); // (field_name, limit, offset)
        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "GROUP" {
                    continue; // _group is handled by GroupByNode
                }
                if let Some(ref limit) = nested_select.limit {
                    let limit_val = limit.limit.unwrap_or(0); // 0 means no limit
                    let offset_val = limit.offset;
                    if limit_val > 0 || offset_val > 0 {
                        relation_limits.push((
                            nested_select.field.output_name().to_string(),
                            limit_val,
                            offset_val,
                        ));
                    }
                }
            }
        }

        if relation_limits.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (field_name, limit, offset) in &relation_limits {
                    if let Some(JsonValue::Array(items)) = obj.get_mut(field_name) {
                        let offset = *offset as usize;
                        let total = items.len();
                        if offset >= total {
                            *items = Vec::new();
                        } else {
                            let remaining: Vec<JsonValue> = items.drain(offset..).collect();
                            *items = if *limit > 0 {
                                remaining.into_iter().take(*limit as usize).collect()
                            } else {
                                remaining
                            };
                        }
                    }
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use document::Document;
    use schema::FieldKind;
    use std::sync::Mutex;

    use crate::fetcher::FetchByIdsResult;

    #[derive(Default)]
    struct FullTextTestFetcher {
        docs: Mutex<HashMap<String, Vec<Document>>>,
        scores: Mutex<HashMap<(String, String, String), HashMap<String, f64>>>,
    }

    impl FullTextTestFetcher {
        fn add_doc(&self, collection: &str, doc: Document) {
            let mut docs = self.docs.lock().unwrap();
            docs.entry(collection.to_string()).or_default().push(doc);
        }

        fn set_scores(
            &self,
            collection: &str,
            field: &str,
            query: &str,
            scores: HashMap<String, f64>,
        ) {
            self.scores.lock().unwrap().insert(
                (collection.to_string(), field.to_string(), query.to_string()),
                scores,
            );
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocFetcher for FullTextTestFetcher {
        async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>> {
            let docs = self.docs.lock().unwrap();
            Ok(docs.get(collection_name).cloned().unwrap_or_default())
        }

        async fn get_by_ids(
            &self,
            collection_name: &str,
            doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            let docs = self.docs.lock().unwrap();
            let all = docs.get(collection_name).cloned().unwrap_or_default();

            let mut found = Vec::new();
            let mut missing = Vec::new();

            for id in doc_ids {
                let doc = all.iter().find(|d| {
                    d.id()
                        .map(|doc_id| doc_id.to_string() == *id)
                        .unwrap_or(false)
                });
                match doc {
                    Some(doc) => found.push(doc.clone()),
                    None => missing.push(id.clone()),
                }
            }

            Ok(FetchByIdsResult::partial(found, missing))
        }

        async fn get_by_field_value(
            &self,
            collection_name: &str,
            field_name: &str,
            value: &str,
        ) -> Result<Vec<Document>> {
            let docs = self.docs.lock().unwrap();
            let all = docs.get(collection_name).cloned().unwrap_or_default();

            Ok(all
                .into_iter()
                .filter(|doc| {
                    doc.get(field_name)
                        .and_then(|v| v.as_str())
                        .map(|v| v == value)
                        .unwrap_or(false)
                })
                .collect())
        }

        async fn search_fulltext_scored(
            &self,
            collection_name: &str,
            field_name: &str,
            query: &str,
        ) -> Result<HashMap<String, f64>> {
            let scores = self.scores.lock().unwrap();
            Ok(scores
                .get(&(
                    collection_name.to_string(),
                    field_name.to_string(),
                    query.to_string(),
                ))
                .cloned()
                .unwrap_or_default())
        }
    }

    fn relation_collections() -> (
        CollectionVersion,
        CollectionVersion,
        HashMap<String, Arc<CollectionVersion>>,
    ) {
        let file_collection = CollectionVersion::new(
            "File",
            "v1",
            "coll-file",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "path", FieldKind::string()),
                FieldDescription::new("4", "functions", FieldKind::relation("Function", true))
                    .with_relation_name("file_functions"),
            ],
        );

        let function_collection = CollectionVersion::new(
            "Function",
            "v1",
            "coll-function",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "file", FieldKind::relation("File", false))
                    .with_relation_name("file_functions")
                    .as_primary(),
                FieldDescription::new("4", "_fileID", FieldKind::doc_id())
                    .with_relation_name("file_functions")
                    .as_primary(),
            ],
        );

        let file_collection = Arc::new(file_collection);
        let function_collection = Arc::new(function_collection);
        let collections_map = HashMap::from([
            (file_collection.name.clone(), file_collection.clone()),
            (
                function_collection.name.clone(),
                function_collection.clone(),
            ),
        ]);

        (
            (*file_collection).clone(),
            (*function_collection).clone(),
            collections_map,
        )
    }

    fn parsed_relation_collections() -> (
        CollectionVersion,
        CollectionVersion,
        HashMap<String, Arc<CollectionVersion>>,
    ) {
        let collections = crate::parse_sdl(
            r#"
            type File {
                name: String @fulltext
                path: String @fulltext
                content: String @fulltext
                functions: [Function]
            }

            type Function {
                name: String @fulltext
                content: String @fulltext
                qualifiedName: String
                startLine: Int
                file: File @primary
            }
            "#,
        )
        .unwrap();

        let file_collection = collections
            .iter()
            .find(|c| c.name == "File")
            .unwrap()
            .clone();
        let function_collection = collections
            .iter()
            .find(|c| c.name == "Function")
            .unwrap()
            .clone();
        let file_collection = Arc::new(file_collection);
        let function_collection = Arc::new(function_collection);
        let collections_map = HashMap::from([
            (file_collection.name.clone(), file_collection.clone()),
            (
                function_collection.name.clone(),
                function_collection.clone(),
            ),
        ]);

        (
            (*file_collection).clone(),
            (*function_collection).clone(),
            collections_map,
        )
    }

    fn relation_collections_resolved_by_id() -> (
        CollectionVersion,
        CollectionVersion,
        HashMap<String, Arc<CollectionVersion>>,
    ) {
        let file_collection = CollectionVersion::new(
            "File",
            "vers-file",
            "coll-file",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        );

        let function_collection = CollectionVersion::new(
            "Function",
            "vers-function",
            "coll-function",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "file", FieldKind::relation("coll-file", false))
                    .as_primary(),
                FieldDescription::new("4", "_fileID", FieldKind::doc_id()).as_primary(),
            ],
        );

        let file_collection = Arc::new(file_collection);
        let function_collection = Arc::new(function_collection);
        let collections_map = HashMap::from([
            (file_collection.name.clone(), file_collection.clone()),
            (
                function_collection.name.clone(),
                function_collection.clone(),
            ),
        ]);

        (
            (*file_collection).clone(),
            (*function_collection).clone(),
            collections_map,
        )
    }

    fn doc(json: &str) -> Document {
        Document::from_json_str(json).unwrap()
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_lifts_parent_relation_scores() {
        let (file_collection, function_collection, collections_map) = relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let file_2 = "bae-47bd7c29-69cc-5b8a-856f-caaa93d9ace0";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";
        let fn_2 = "bae-daad4cec-56aa-5b13-9502-657f29321b5d";

        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_1}","name":"auth.rs","path":"src/auth.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_2}","name":"utils.rs","path":"src/utils.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_2}","name":"handle_request","_fileID":"{file_2}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "name",
            "auth",
            HashMap::from([(file_1.to_string(), 1.5)]),
        );

        let runner = QueryRunner::new(fetcher, vec![file_collection, function_collection]);
        let scores = runner
            .compute_fulltext_path_scores(
                collections_map.get("Function").unwrap().clone(),
                "file.name",
                "auth",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(fn_1), Some(&1.5));
        assert!(!scores.contains_key(fn_2));
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_lifts_reverse_relation_scores() {
        let (file_collection, function_collection, collections_map) = relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let file_2 = "bae-47bd7c29-69cc-5b8a-856f-caaa93d9ace0";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";
        let fn_2 = "bae-daad4cec-56aa-5b13-9502-657f29321b5d";

        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_1}","name":"auth.rs","path":"src/auth.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_2}","name":"utils.rs","path":"src/utils.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"parse_token","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_2}","name":"format_output","_fileID":"{file_2}"}}"#
            )),
        );
        fetcher.set_scores(
            "Function",
            "name",
            "parse_token",
            HashMap::from([(fn_1.to_string(), 2.0)]),
        );

        let runner = QueryRunner::new(fetcher, vec![file_collection, function_collection]);
        let scores = runner
            .compute_fulltext_path_scores(
                collections_map.get("File").unwrap().clone(),
                "functions.name",
                "parse_token",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(file_1), Some(&2.0));
        assert!(!scores.contains_key(file_2));
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_with_parsed_sdl_schema() {
        let (_file_collection, _function_collection, collections_map) =
            parsed_relation_collections();
        let function_collection = collections_map.get("Function").unwrap();
        let file_field = function_collection.field_by_name("file").unwrap();

        assert!(file_field.is_primary);
        assert!(!file_field.kind.is_array());
        assert_eq!(file_field.relation_name.as_deref(), Some("file_function"));

        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let file_2 = "bae-47bd7c29-69cc-5b8a-856f-caaa93d9ace0";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";
        let fn_2 = "bae-daad4cec-56aa-5b13-9502-657f29321b5d";

        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","content":"handles inbound requests","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_2}","name":"handle_request","content":"handles inbound requests","_fileID":"{file_2}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "content",
            "auth",
            HashMap::from([(file_1.to_string(), 0.7)]),
        );

        let runner = QueryRunner::new(fetcher, vec![]);
        let scores = runner
            .compute_fulltext_path_scores(
                function_collection.clone(),
                "file.content",
                "auth",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(fn_1), Some(&0.7));
        assert!(!scores.contains_key(fn_2));
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_resolves_target_collection_by_collection_id() {
        let (file_collection, function_collection, collections_map) =
            relation_collections_resolved_by_id();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";

        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "name",
            "auth",
            HashMap::from([(file_1.to_string(), 1.25)]),
        );

        let runner = QueryRunner::new(fetcher, vec![file_collection, function_collection]);
        let scores = runner
            .compute_fulltext_path_scores(
                collections_map.get("Function").unwrap().clone(),
                "file.name",
                "auth",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(fn_1), Some(&1.25));
    }

    #[tokio::test]
    async fn precompute_fulltext_scores_scopes_nested_bm25_aliases() {
        let (_file_collection, _function_collection, collections_map) =
            parsed_relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";

        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","content":"handles inbound requests","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "name",
            "auth",
            HashMap::from([(file_1.to_string(), 1.0)]),
        );

        let select = crate::parse_query(
            r#"query {
                File {
                    score: BM25(query: "auth", fields: ["name"])
                    functions {
                        score: BM25(query: "auth", fields: ["file.name"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let runner = QueryRunner::new(fetcher, vec![]);
        let scores = runner
            .precompute_fulltext_scores(&select, runner.fetcher.as_ref(), &collections_map)
            .await
            .unwrap();

        let root_scope = vec![select.field.output_name().to_string()];
        let child_scope = vec![
            select.field.output_name().to_string(),
            "functions".to_string(),
        ];
        let root_key = Planner::fts_score_key(&root_scope, "score");
        let child_key = Planner::fts_score_key(&child_scope, "score");

        assert_ne!(root_key, child_key);
        assert_eq!(
            scores.get(&root_key).and_then(|m| m.get(file_1)),
            Some(&1.0)
        );
        assert_eq!(scores.get(&child_key).and_then(|m| m.get(fn_1)), Some(&1.0));
    }
}
