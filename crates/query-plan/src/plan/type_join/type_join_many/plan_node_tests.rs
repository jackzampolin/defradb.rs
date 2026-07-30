use async_trait::async_trait;
use document::{Document, NormalValue};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use serde_json::json;
use std::sync::Arc;

use crate::fetcher::{DocFetcher, FetchByIdsResult, IndexScanResult};
use crate::planner::{Doc, IndexScanParams, IndexScanType, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::Result;
use query_types::mapper::GroupBy;

use super::super::JoinSide;
use super::node::TypeJoinMany;

#[derive(Debug)]
struct MockPlanNode {
    docs: Vec<Doc>,
    mapping: DocumentMapping,
    current: Doc,
    position: usize,
}

impl MockPlanNode {
    fn new(docs: Vec<Doc>, mapping: DocumentMapping) -> Self {
        Self {
            docs,
            mapping,
            current: Doc::default(),
            position: 0,
        }
    }
}

#[derive(Default)]
struct IndexedMockFetcher {
    docs: Vec<Document>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocFetcher for IndexedMockFetcher {
    async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
        Ok(self.docs.clone())
    }

    async fn get_by_ids(
        &self,
        _collection_name: &str,
        doc_ids: &[String],
    ) -> Result<FetchByIdsResult> {
        let mut found = Vec::new();
        let mut missing = Vec::new();

        for doc_id in doc_ids {
            match self
                .docs
                .iter()
                .find(|doc| doc.id().is_some_and(|id| id.to_string() == *doc_id))
            {
                Some(doc) => found.push(doc.clone()),
                None => missing.push(doc_id.clone()),
            }
        }

        Ok(FetchByIdsResult::partial(found, missing))
    }

    async fn get_by_field_value(
        &self,
        _collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>> {
        Ok(self
            .docs
            .iter()
            .filter(|doc| doc.get(field_name).and_then(|v| v.as_str()) == Some(value))
            .cloned()
            .collect())
    }

    async fn get_by_index_scan(
        &self,
        _collection_name: &str,
        params: &IndexScanParams,
    ) -> Result<IndexScanResult> {
        let values: Vec<&str> = match &params.scan_type {
            IndexScanType::ExactMatch { values } => values
                .iter()
                .filter_map(|value| match value {
                    NormalValue::String(value) => Some(value.as_str()),
                    _ => None,
                })
                .collect(),
            IndexScanType::InScan { values, .. } => values
                .iter()
                .filter_map(|value| match value {
                    NormalValue::String(value) => Some(value.as_str()),
                    _ => None,
                })
                .collect(),
            other => panic!("unexpected index scan type in test: {other:?}"),
        };

        let mut doc_ids = Vec::new();
        let mut raw_fetches = 0u64;
        for value in values {
            for doc in &self.docs {
                if doc.get("_authorID").and_then(|v| v.as_str()) == Some(value) {
                    raw_fetches += 1;
                    if let Some(doc_id) = doc.id() {
                        doc_ids.push(doc_id.to_string());
                    }
                }
            }
        }

        Ok(IndexScanResult::with_raw_count(doc_ids, raw_fetches))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for MockPlanNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.current = Doc::default();
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if self.position >= self.docs.len() {
            return Ok(false);
        }

        self.current = self.docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.mapping
    }

    fn kind(&self) -> &'static str {
        "mockPlan"
    }
}

fn make_parent_collection() -> CollectionVersion {
    CollectionVersion::new(
        "users",
        "v1",
        "users-v1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("author_posts"),
        ],
    )
}

fn make_child_collection() -> CollectionVersion {
    CollectionVersion::new(
        "posts",
        "v1",
        "posts-v1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            FieldDescription::new("4", "_authorID", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
        ],
    )
}

fn make_parent_mapping() -> DocumentMapping {
    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "posts");
    mapping
}

fn make_child_mapping() -> DocumentMapping {
    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "title");
    mapping.add(2, "author");
    mapping.add(3, "_authorID");
    mapping.add_render_key(0, "_docID");
    mapping.add_render_key(1, "title");
    mapping
}

fn make_parent_doc(doc_id: &str) -> Doc {
    let mut doc = Doc::new(2);
    doc.set_doc_id(doc_id);
    doc.stored_field_count = 1;
    doc
}

fn make_child_doc(doc_id: &str, title: &str, parent_id: &str) -> Doc {
    let mut doc = Doc::new(4);
    doc.set_doc_id(doc_id);
    doc.set(1, json!(title));
    doc.set(3, json!(parent_id));
    doc.stored_field_count = 3;
    doc
}

fn make_child_storage_doc(doc_id: &str, title: &str, parent_id: &str) -> Document {
    Document::from_json_str(&format!(
        r#"{{"_docID":"{doc_id}","title":"{title}","_authorID":"{parent_id}"}}"#
    ))
    .unwrap()
}

async fn build_join(parent_scoped: bool) -> TypeJoinMany {
    let parent_collection = make_parent_collection();
    let child_collection = make_child_collection();

    let parent_mapping = make_parent_mapping();
    let child_mapping = make_child_mapping();

    let parent_docs = vec![make_parent_doc("user-1"), make_parent_doc("user-3")];
    let child_docs = vec![
        make_child_doc("post-1", "hello", "user-1"),
        make_child_doc("post-2", "skip", "user-2"),
        make_child_doc("post-3", "world", "user-3"),
    ];

    let parent_plan: Box<dyn PlanNode> = Box::new(MockPlanNode::new(parent_docs, parent_mapping));
    let child_plan: Box<dyn PlanNode> = Box::new(MockPlanNode::new(child_docs, child_mapping));

    let mut join_mapping = make_parent_mapping();
    join_mapping.set_child_at(1, make_child_mapping());

    let parent_side = JoinSide::new(
        parent_collection.clone(),
        parent_collection.field_by_name("posts").unwrap().clone(),
        1,
    )
    .unwrap();
    let child_side = JoinSide::new(
        child_collection.clone(),
        child_collection.field_by_name("author").unwrap().clone(),
        2,
    )
    .unwrap();

    let join = TypeJoinMany::new(
        parent_plan,
        child_plan,
        parent_side,
        child_side,
        join_mapping,
    )
    .unwrap();

    if parent_scoped {
        join.with_parent_scoped_child_cache()
    } else {
        join
    }
}

#[tokio::test]
async fn init_restricts_child_cache_to_parent_scope_when_enabled() {
    let mut join = build_join(true).await;

    join.init().await.unwrap();

    assert_eq!(join.child_cache.len(), 2);
    assert!(join.child_cache.contains_key("user-1"));
    assert!(join.child_cache.contains_key("user-3"));
    assert!(!join.child_cache.contains_key("user-2"));
    assert_eq!(join.total_children_in_cache, 2);
}

#[tokio::test]
async fn init_keeps_all_children_without_parent_scope_restriction() {
    let mut join = build_join(false).await;

    join.init().await.unwrap();

    assert_eq!(join.child_cache.len(), 3);
    assert!(join.child_cache.contains_key("user-1"));
    assert!(join.child_cache.contains_key("user-2"));
    assert!(join.child_cache.contains_key("user-3"));
    assert_eq!(join.total_children_in_cache, 3);
}

#[tokio::test]
async fn child_limit_iterations_include_terminal_call_per_parent() {
    for per_parent in [false, true] {
        let join = build_join(false).await.with_limit(1);
        let mut join = if per_parent {
            join.with_per_parent_child_scan()
        } else {
            join
        };

        join.init().await.unwrap();
        join.start().await.unwrap();
        while join.next().await.unwrap() {}

        assert_eq!(join.child_limit_iterations, 4);
    }
}

#[tokio::test]
async fn grouped_join_debug_explain_includes_group_node() {
    let join = build_join(false)
        .await
        .with_group_by(GroupBy::new(vec!["title".to_string()]));

    assert!(join
        .explain_debug()
        .pointer("/typeIndexJoin/typeJoinMany/subType/selectTopNode/groupNode/selectNode/pipeNode/mockPlan")
        .is_some());
}

#[tokio::test]
async fn init_builds_child_cache_from_indexed_fetch() {
    let parent_collection = make_parent_collection();
    let child_collection = make_child_collection();

    let parent_plan: Box<dyn PlanNode> = Box::new(MockPlanNode::new(
        vec![make_parent_doc("user-1"), make_parent_doc("user-3")],
        make_parent_mapping(),
    ));
    let child_plan: Box<dyn PlanNode> =
        Box::new(MockPlanNode::new(Vec::new(), make_child_mapping()));

    let mut join_mapping = make_parent_mapping();
    join_mapping.set_child_at(1, make_child_mapping());

    let parent_side = JoinSide::new(
        parent_collection.clone(),
        parent_collection.field_by_name("posts").unwrap().clone(),
        1,
    )
    .unwrap();
    let child_side = JoinSide::new(
        child_collection.clone(),
        child_collection.field_by_name("author").unwrap().clone(),
        2,
    )
    .unwrap();

    let fetcher = Arc::new(IndexedMockFetcher {
        docs: vec![
            make_child_storage_doc(
                "bae-7b649bba-3168-5c05-827c-514c0f8d56fd",
                "hello",
                "user-1",
            ),
            make_child_storage_doc("bae-4500bd3f-c7d3-5254-b4c2-54b7b0b23f30", "skip", "user-2"),
            make_child_storage_doc(
                "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9",
                "world",
                "user-3",
            ),
        ],
    });

    let mut join = TypeJoinMany::new(
        parent_plan,
        child_plan,
        parent_side,
        child_side,
        join_mapping,
    )
    .unwrap()
    .with_indexed_child_fetch(fetcher, "posts", "_authorID", "posts__authorID_ASC");

    join.init().await.unwrap();

    assert_eq!(join.child_cache.len(), 2);
    assert!(join.child_cache.contains_key("user-1"));
    assert!(join.child_cache.contains_key("user-3"));
    assert!(!join.child_cache.contains_key("user-2"));
    assert_eq!(join.total_children_in_cache, 2);
    assert_eq!(join.child_exec_info.indexes_fetched, 2);
    assert_eq!(join.child_exec_info.fields_fetched, 4);
}
