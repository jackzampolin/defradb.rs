//! Planner orchestration and post-processing for nested queries.

use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, instrument};

use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::planner::Planner;
use crate::txn::TransactionRegistry;

use super::super::fetcher::FetcherWrapper;
use super::super::{DocFetcher, QueryRunner};
use super::nested_profile::{NestedQueryProfile, ScopedFulltextProfile};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    /// ACP permission filtering is applied per-collection via PermissionFilterNode in the plan.
    #[instrument(
        name = "query.execute_nested_select",
        level = "debug",
        skip(self, select, fetcher, identity),
        fields(collection = %select.collection_name, field = %select.field.output_name())
    )]
    pub(crate) async fn execute_nested_select_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        let mut profile = NestedQueryProfile::default();

        // Build the plan using the Planner with fetcher support
        // Get all collections from provider for join planning
        let collections_map = self.collections_map().await?;

        // Validate groupBy and field references before planning
        let collection = self.get_collection(&select.collection_name).await?;
        super::super::plan::validate_select(select, &collection)?;

        // Create a fetcher wrapper that can be shared across plan nodes
        // We need to wrap the reference in an Arc-compatible struct
        let fetcher_arc = FetcherWrapper::new(fetcher);
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        // Pre-compute FTS scores from the inverted index before planning.
        // Supports dotted relation paths like `file.name` and `functions.content`
        // by querying the leaf collection's BM25 index and lifting scores back
        // onto the root collection through relation foreign keys.
        let precompute_fulltext_start = Instant::now();
        let fts_scores = self
            .precompute_fulltext_scores(select, fetcher, &collections_map)
            .await?;
        profile.precompute_fulltext_elapsed = precompute_fulltext_start.elapsed();

        let plan_build_start = Instant::now();
        let mut planner = Planner::new(collections)
            .with_query_limits(self.query_limits)
            .with_fetcher(Arc::new(fetcher_arc))
            .with_acp(self.acp.clone(), identity);
        if !fts_scores.is_empty() {
            planner = planner.with_fts_scores(fts_scores);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        profile.plan_build_elapsed = plan_build_start.elapsed();
        let mut plan = plan_result.plan;
        let ordering_only_fields = plan_result.ordering_only_fields;
        let aggregate_internal_keys = plan_result.aggregate_internal_keys;

        // Get the mapping from the plan
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        let plan_init_start = Instant::now();
        plan.init().await?;
        profile.plan_init_elapsed = plan_init_start.elapsed();
        let plan_start_start = Instant::now();
        plan.start().await?;
        profile.plan_start_elapsed = plan_start_start.elapsed();

        let mut results = Vec::new();

        loop {
            let plan_iteration_start = Instant::now();
            let has_next = plan.next().await?;
            profile.plan_iteration_elapsed += plan_iteration_start.elapsed();
            if !has_next {
                break;
            }

            let doc = plan.value();

            let doc_render_start = Instant::now();
            let mut json = self.doc_to_json(doc, &mapping)?;
            profile.doc_render_elapsed += doc_render_start.elapsed();

            // Strip ordering-only fields from nested objects.
            // These fields were added for ORDER BY but shouldn't appear in output.
            let ordering_only_strip_start = Instant::now();
            for (relation_field, nested_field) in &ordering_only_fields {
                if let Some(obj) = json.as_object_mut() {
                    if let Some(relation_value) = obj.get_mut(relation_field) {
                        if let Some(nested_obj) = relation_value.as_object_mut() {
                            nested_obj.remove(nested_field);
                        }
                    }
                }
            }
            profile.ordering_only_strip_elapsed += ordering_only_strip_start.elapsed();

            results.push(json);
        }

        let plan_exec_info = plan.exec_info();
        // Capture cursor page-info BEFORE close() releases plan resources.
        let cursor_page_info = plan.page_info();
        let plan_close_start = Instant::now();
        plan.close().await?;
        profile.plan_close_elapsed = plan_close_start.elapsed();

        // Post-process relation-based aggregates
        // For aggregates like _count(books: {}), compute the value from joined data
        let relation_aggregate_start = Instant::now();
        let results =
            self.compute_relation_aggregates(results, select, &aggregate_internal_keys)?;
        profile.relation_aggregate_elapsed = relation_aggregate_start.elapsed();

        // For nested relation-local BM25, score the already-joined relation scope instead of
        // precomputing against the full leaf collection. This preserves correct nested ordering
        // while avoiding a full-corpus BM25 pass for session-scoped child queries.
        let mut scoped_fulltext = ScopedFulltextProfile::default();
        let scoped_fulltext_start = Instant::now();
        let results = Self::apply_scoped_relation_fulltext_with_profile(
            results,
            select,
            &mut scoped_fulltext,
        );
        profile.scoped_fulltext_elapsed = scoped_fulltext_start.elapsed();
        profile.scoped_fulltext = scoped_fulltext;

        // Strip fields from relation data that were added for filter evaluation
        // but not explicitly requested in the selection set.
        let clean_filter_only_fields_start = Instant::now();
        let results = Self::clean_filter_only_relation_fields(results, select);
        profile.clean_filter_only_fields_elapsed = clean_filter_only_fields_start.elapsed();

        // Apply deferred limit/offset to relation fields.
        // TypeJoinMany stores ALL children (for aggregates to count), so we apply
        // the select's limit/offset here after aggregates have been computed.
        let relation_limits_start = Instant::now();
        let results = Self::apply_relation_limits(results, select);
        profile.relation_limits_elapsed = relation_limits_start.elapsed();
        profile.result_count = results.len();

        debug!(
            plan_docs_fetched = plan_exec_info.docs_fetched,
            plan_fields_fetched = plan_exec_info.fields_fetched,
            plan_indexes_fetched = plan_exec_info.indexes_fetched,
            plan_iterations = plan_exec_info.iterations,
            precompute_fulltext = ?profile.precompute_fulltext_elapsed,
            plan_build = ?profile.plan_build_elapsed,
            plan_init = ?profile.plan_init_elapsed,
            plan_start = ?profile.plan_start_elapsed,
            plan_iteration = ?profile.plan_iteration_elapsed,
            doc_render = ?profile.doc_render_elapsed,
            ordering_only_strip = ?profile.ordering_only_strip_elapsed,
            plan_close = ?profile.plan_close_elapsed,
            relation_aggregates = ?profile.relation_aggregate_elapsed,
            scoped_fulltext = ?profile.scoped_fulltext_elapsed,
            clean_filter_only_fields = ?profile.clean_filter_only_fields_elapsed,
            relation_limits = ?profile.relation_limits_elapsed,
            scoped_fulltext_calls = profile.scoped_fulltext.scoring_calls,
            scoped_fulltext_sort_calls = profile.scoped_fulltext.sort_calls,
            scoped_fulltext_top_k_calls = profile.scoped_fulltext.top_k_calls,
            scoped_fulltext_items_seen = profile.scoped_fulltext.items_seen,
            scoped_fulltext_target_fields_seen = profile.scoped_fulltext.target_fields_seen,
            scoped_fulltext_docs_indexed = profile.scoped_fulltext.docs_indexed,
            scoped_fulltext_scoring = ?profile.scoped_fulltext.scoring_elapsed,
            scoped_fulltext_sort = ?profile.scoped_fulltext.sort_elapsed,
            scoped_fulltext_top_k = ?profile.scoped_fulltext.top_k_elapsed,
            result_count = profile.result_count,
            "nested query profile"
        );

        // For cursor queries, wrap results in the cursor response envelope.
        if let Some(pi) = cursor_page_info {
            let inner_key = select.field.output_name().to_string();
            let mut cursor_obj = serde_json::Map::new();
            cursor_obj.insert(inner_key, JsonValue::Array(results));
            if pi.fields.any_selected() {
                let mut pageinfo = serde_json::Map::new();
                if let Some(key) = pi.fields.has_next.as_ref() {
                    pageinfo.insert(key.clone(), JsonValue::Bool(pi.has_next));
                }
                if let Some(key) = pi.fields.has_prev.as_ref() {
                    pageinfo.insert(key.clone(), JsonValue::Bool(pi.has_prev));
                }
                if let Some(key) = pi.fields.start_cursor.as_ref() {
                    pageinfo.insert(
                        key.clone(),
                        pi.start_cursor
                            .map(JsonValue::String)
                            .unwrap_or(JsonValue::Null),
                    );
                }
                if let Some(key) = pi.fields.end_cursor.as_ref() {
                    pageinfo.insert(
                        key.clone(),
                        pi.end_cursor
                            .map(JsonValue::String)
                            .unwrap_or(JsonValue::Null),
                    );
                }
                let page_info_key = select
                    .cursor_aliases
                    .page_info_alias
                    .as_deref()
                    .unwrap_or("_pageInfo")
                    .to_string();
                cursor_obj.insert(page_info_key, JsonValue::Object(pageinfo));
            }
            return Ok(JsonValue::Object(cursor_obj));
        }

        Ok(JsonValue::Array(results))
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
        // Build map of relation output_name -> allowed sub-field names
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
    pub(crate) fn apply_relation_limits(
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
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
    use acp::{DocumentACP, DocumentPermission, Identity};
    use async_trait::async_trait;
    use bm25::{Document as Bm25Document, Language, SearchEngineBuilder};
    use document::Document;
    use identity::Did;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::super::nested_profile::ScopedFulltextProfile;
    use crate::fetcher::FetchByIdsResult;
    use crate::planner::Planner;
    use schema::PolicyDescription;

    type ScoreMap = HashMap<(String, String, String), HashMap<String, f64>>;

    #[derive(Default)]
    struct FullTextTestFetcher {
        docs: Mutex<HashMap<String, Vec<Document>>>,
        scores: Mutex<ScoreMap>,
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

    struct MockDocumentAcp {
        private_docs: HashMap<String, Did>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocumentACP for MockDocumentAcp {
        async fn register_doc_object(
            &self,
            _identity: &Did,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<()> {
            Ok(())
        }

        async fn is_doc_registered(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            doc_id: &str,
        ) -> acp::Result<bool> {
            Ok(self.private_docs.contains_key(doc_id))
        }

        async fn get_doc_owner(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            doc_id: &str,
        ) -> acp::Result<Option<Did>> {
            Ok(self.private_docs.get(doc_id).cloned())
        }

        async fn check_doc_access(
            &self,
            identity: &Identity,
            _permission: DocumentPermission,
            _policy_id: &str,
            _resource_name: &str,
            doc_id: &str,
        ) -> acp::Result<bool> {
            Ok(match self.private_docs.get(doc_id) {
                None => true,
                Some(owner) => identity.did().map(|did| did == owner).unwrap_or(false),
            })
        }

        async fn add_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> acp::Result<bool> {
            Ok(false)
        }

        async fn delete_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> acp::Result<bool> {
            Ok(false)
        }

        async fn unregister_doc_object(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<()> {
            Ok(())
        }
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
    async fn nested_acp_relations_keep_public_join_targets() {
        let employee_owner =
            Did::new("did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw").unwrap();

        let mut company_collection = CollectionVersion::new(
            "Company",
            "v1",
            "coll-company",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "employees", FieldKind::relation("Employee", true))
                    .with_relation_name("employee_company"),
            ],
        );
        company_collection.policy = Some(PolicyDescription::new("policy", "companies"));

        let mut employee_collection = CollectionVersion::new(
            "Employee",
            "v1",
            "coll-employee",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "company", FieldKind::relation("Company", false))
                    .with_relation_name("employee_company")
                    .as_primary(),
                FieldDescription::new("4", "_companyID", FieldKind::doc_id())
                    .with_relation_name("employee_company")
                    .as_primary(),
            ],
        );
        employee_collection.policy = Some(PolicyDescription::new("policy", "employees"));

        let fetcher = crate::test_utils::MockFetcher::new();
        let company_public = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let company_private = "bae-47bd7c29-69cc-5b8a-856f-caaa93d9ace0";
        let emp_public_public = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";
        let emp_public_private = "bae-daad4cec-56aa-5b13-9502-657f29321b5d";
        let emp_private_public = "bae-0c6127be-2c8f-5984-b5ca-a7f4343a5123";
        let emp_private_private = "bae-7aa1c1b0-7546-5b1d-81f8-6f9d972b8e38";

        fetcher.add_doc(
            "Company",
            doc(&format!(
                r#"{{"_docID":"{company_public}","name":"Public Company"}}"#
            )),
        );
        fetcher.add_doc(
            "Company",
            doc(&format!(
                r#"{{"_docID":"{company_private}","name":"Private Company"}}"#
            )),
        );
        fetcher.add_doc(
            "Employee",
            doc(&format!(
                r#"{{"_docID":"{emp_public_public}","name":"PubEmp in PubCompany","_companyID":"{company_public}"}}"#,
            )),
        );
        fetcher.add_doc(
            "Employee",
            doc(&format!(
                r#"{{"_docID":"{emp_public_private}","name":"PubEmp in PrivateCompany","_companyID":"{company_private}"}}"#,
            )),
        );
        fetcher.add_doc(
            "Employee",
            doc(&format!(
                r#"{{"_docID":"{emp_private_public}","name":"PrivateEmp in PubCompany","_companyID":"{company_public}"}}"#,
            )),
        );
        fetcher.add_doc(
            "Employee",
            doc(&format!(
                r#"{{"_docID":"{emp_private_private}","name":"PrivateEmp in PrivateCompany","_companyID":"{company_private}"}}"#,
            )),
        );

        let acp = Arc::new(MockDocumentAcp {
            private_docs: HashMap::from([
                (company_private.to_string(), employee_owner.clone()),
                (emp_private_public.to_string(), employee_owner.clone()),
                (emp_private_private.to_string(), employee_owner),
            ]),
        });

        let runner =
            QueryRunner::new(fetcher, vec![company_collection, employee_collection]).with_acp(acp);

        let result = runner
            .execute_query(
                r#"
                query {
                    Employee {
                        name
                        company {
                            name
                        }
                    }
                }
                "#,
            )
            .await
            .unwrap();

        let JsonValue::Object(obj) = result else {
            panic!("expected object result");
        };
        let JsonValue::Array(employees) = obj.get("Employee").cloned().unwrap() else {
            panic!("expected employee array");
        };

        assert_eq!(employees.len(), 2);

        let by_name: HashMap<_, _> = employees
            .iter()
            .map(|employee| {
                (
                    employee["name"].as_str().unwrap().to_string(),
                    employee["company"].clone(),
                )
            })
            .collect();

        assert!(by_name["PubEmp in PrivateCompany"].is_null());
        assert_eq!(by_name["PubEmp in PubCompany"]["name"], "Public Company");
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

    #[tokio::test]
    async fn precompute_fulltext_scores_skips_nested_local_bm25_fields() {
        let (_file_collection, _function_collection, collections_map) =
            parsed_relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";

        fetcher.set_scores(
            "Function",
            "name",
            "handle",
            HashMap::from([(fn_1.to_string(), 1.75)]),
        );

        let select = crate::parse_query(
            r#"query {
                File {
                    functions(order: {_alias: {score: DESC}}) {
                        score: BM25(query: "handle", fields: ["name"])
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

        let child_scope = vec![
            select.field.output_name().to_string(),
            "functions".to_string(),
        ];
        let child_key = Planner::fts_score_key(&child_scope, "score");

        assert!(!scores.contains_key(&child_key));
    }

    #[test]
    fn apply_scoped_relation_fulltext_scores_and_orders_nested_items() {
        let select = crate::parse_query(
            r#"query {
                Session {
                    messages(order: {_alias: {score: DESC}}) {
                        _docID
                        score: BM25(query: "rust", fields: ["content"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let results = vec![serde_json::json!({
            "_docID": "session-1",
            "messages": [
                {"_docID": "msg-1", "content": "rust search"},
                {"_docID": "msg-2", "content": "rust rust rust rust rust"},
                {"_docID": "msg-3", "content": "database tuning"}
            ]
        })];

        let scored =
            QueryRunner::<FullTextTestFetcher>::apply_scoped_relation_fulltext(results, &select);
        let messages = scored[0]["messages"].as_array().unwrap();

        assert_eq!(messages[0]["_docID"], "msg-2");
        assert_eq!(messages[2]["_docID"], "msg-3");
        assert!(messages[0]["score"].as_f64().unwrap() > messages[1]["score"].as_f64().unwrap());
        assert_eq!(messages[2]["score"].as_f64(), Some(0.0));
    }

    #[test]
    fn compute_scoped_fulltext_scores_matches_bm25_crate_scores() {
        let select = crate::parse_query(
            r#"query {
                Session {
                    messages {
                        _docID
                        score: BM25(query: "cargo cargo bm25", fields: ["content"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let items = vec![
            serde_json::json!({
                "_docID": "msg-1",
                "content": "cargo test keeps bm25 search relevant for rust queries"
            }),
            serde_json::json!({
                "_docID": "msg-2",
                "content": "cargo cargo fmt and cargo bench help benchmark bm25 tuning"
            }),
            serde_json::json!({
                "_docID": "msg-3",
                "content": "graph joins and filters dominate this query path"
            }),
        ];

        let nested_select = select
            .fields
            .iter()
            .find_map(|requestable| match requestable {
                Requestable::Select(nested_select) => Some(nested_select),
                _ => None,
            })
            .expect("messages selection should be present");
        let fts = nested_select
            .fields
            .iter()
            .find_map(|requestable| match requestable {
                Requestable::FullTextSearch(fts) => Some(fts),
                _ => None,
            })
            .expect("BM25 selection should be present");

        let mut profile = ScopedFulltextProfile::default();
        let scoped_scores = QueryRunner::<FullTextTestFetcher>::compute_scoped_fulltext_scores(
            &items,
            fts,
            &mut profile,
            None,
        );

        let documents = items
            .iter()
            .map(|item| {
                Bm25Document::new(
                    item["_docID"].as_str().unwrap().to_string(),
                    item["content"].as_str().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let engine =
            SearchEngineBuilder::<String>::with_documents(Language::English, documents).build();
        let expected_scores = engine
            .search("cargo cargo bm25", None)
            .into_iter()
            .map(|result| (result.document.id, result.score as f64))
            .collect::<HashMap<_, _>>();

        assert_eq!(scoped_scores.len(), items.len());
        for (doc_id, expected_score) in expected_scores {
            let item_index = items
                .iter()
                .position(|item| item["_docID"].as_str() == Some(doc_id.as_str()))
                .unwrap_or_else(|| panic!("missing item for {doc_id}"));
            let actual_score = scoped_scores[item_index];
            assert!(
                (actual_score - expected_score).abs() < 1e-6,
                "score mismatch for {doc_id}: expected {expected_score}, got {actual_score}"
            );
        }

        let zero_score_index = items
            .iter()
            .position(|item| item["_docID"].as_str() == Some("msg-3"))
            .unwrap();
        assert_eq!(scoped_scores[zero_score_index], 0.0);
    }

    #[test]
    fn apply_scoped_relation_fulltext_top_k_preserves_offset_window() {
        let select = crate::parse_query(
            r#"query {
                Session {
                    messages(limit: 1, offset: 1, order: {_alias: {score: DESC}}) {
                        _docID
                        score: BM25(query: "rust", fields: ["content"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let results = vec![serde_json::json!({
            "_docID": "session-1",
            "messages": [
                {"_docID": "msg-1", "content": "rust search"},
                {"_docID": "msg-2", "content": "rust rust rust rust rust"},
                {"_docID": "msg-3", "content": "database tuning"},
                {"_docID": "msg-4", "content": "distributed systems"}
            ]
        })];

        let scored =
            QueryRunner::<FullTextTestFetcher>::apply_scoped_relation_fulltext(results, &select);
        let prelimited_messages = scored[0]["messages"].as_array().unwrap();

        assert_eq!(prelimited_messages.len(), 2);
        assert_eq!(prelimited_messages[0]["_docID"], "msg-2");
        assert_eq!(prelimited_messages[1]["_docID"], "msg-1");

        let limited = QueryRunner::<FullTextTestFetcher>::apply_relation_limits(scored, &select);
        let messages = limited[0]["messages"].as_array().unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["_docID"], "msg-1");
    }

    #[test]
    fn apply_scoped_relation_fulltext_top_k_preserves_original_zero_score_order() {
        let select = crate::parse_query(
            r#"query {
                Session {
                    messages(limit: 2, order: {_alias: {score: DESC}}) {
                        _docID
                        score: BM25(query: "missing", fields: ["content"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let results = vec![serde_json::json!({
            "_docID": "session-1",
            "messages": [
                {"_docID": "msg-1", "content": "rust search"},
                {"_docID": "msg-2", "content": "rust rust rust rust rust"},
                {"_docID": "msg-3", "content": "database tuning"}
            ]
        })];

        let scored =
            QueryRunner::<FullTextTestFetcher>::apply_scoped_relation_fulltext(results, &select);
        let messages = scored[0]["messages"].as_array().unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["_docID"], "msg-1");
        assert_eq!(messages[1]["_docID"], "msg-2");
        assert_eq!(messages[0]["score"].as_f64(), Some(0.0));
        assert_eq!(messages[1]["score"].as_f64(), Some(0.0));
    }
}
