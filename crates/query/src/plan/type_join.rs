//! Type join nodes for resolving relations
//!
//! TypeJoinOne and TypeJoinMany implement the join logic for one-to-one
//! and one-to-many relations respectively. These nodes wrap a parent plan
//! and perform lookups to resolve related documents.

use async_trait::async_trait;
use schema::{CollectionVersion, FieldDescription};
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::planner::{Doc, PlanNode};

/// Represents one side of a join operation
#[derive(Clone)]
pub struct JoinSide {
    /// The collection schema for this side
    pub collection: CollectionVersion,
    /// The relation field description
    pub relation_field: FieldDescription,
    /// Index of the relation field in the document
    pub relation_field_index: usize,
    /// Index of the `_id` field (e.g., author_id) if this side has the FK
    pub relation_id_field_index: Option<usize>,
    /// Whether this is the parent (outer) side of the join
    pub is_parent: bool,
}

impl JoinSide {
    /// Create a new join side
    pub fn new(
        collection: CollectionVersion,
        relation_field: FieldDescription,
        relation_field_index: usize,
    ) -> Self {
        // Find the _id field index for non-array relations
        let relation_id_field_index = if !relation_field.kind.is_array() {
            let id_field_name = CollectionVersion::relation_id_field_name(&relation_field.name);
            collection
                .fields
                .iter()
                .position(|f| f.name == id_field_name)
        } else {
            None
        };

        Self {
            collection,
            relation_field,
            relation_field_index,
            relation_id_field_index,
            is_parent: false,
        }
    }

    /// Mark this side as the parent (outer) side
    pub fn as_parent(mut self) -> Self {
        self.is_parent = true;
        self
    }
}

/// TypeJoinOne implements one-to-one relation joins.
///
/// The join flow for the primary side:
/// 1. Parent plan yields a document (e.g., Book with author_id: "bae-123")
/// 2. Extract the FK from the `_id` field (author_id)
/// 3. Perform a point-lookup on the child collection where _docID == FK
/// 4. Merge the child document into the parent under the relation field key
///
/// For the secondary side (no FK on this side):
/// 1. Parent plan yields a document (e.g., Author)
/// 2. Scan child collection looking for docs where their FK matches parent's _docID
/// 3. Merge the first matching child document
pub struct TypeJoinOne {
    /// Parent side of the join (outer loop)
    parent_side: JoinSide,
    /// Child side of the join (lookup)
    child_side: JoinSide,
    /// The parent plan node
    parent_plan: Box<dyn PlanNode>,
    /// The child plan node (for lookups)
    child_plan: Box<dyn PlanNode>,
    /// Document mapping for this join
    document_mapping: DocumentMapping,
    /// Current document (merged parent + child)
    current_doc: Doc,
    /// Whether this join is inverted (secondary side is parent)
    is_inverted: bool,
    /// Whether initialized
    initialized: bool,
}

impl TypeJoinOne {
    /// Create a new TypeJoinOne node
    ///
    /// # Arguments
    /// * `parent_plan` - The outer plan that yields parent documents
    /// * `child_plan` - The inner plan used for lookups
    /// * `parent_side` - Configuration for the parent side
    /// * `child_side` - Configuration for the child side
    /// * `document_mapping` - The output document mapping
    pub fn new(
        parent_plan: Box<dyn PlanNode>,
        child_plan: Box<dyn PlanNode>,
        parent_side: JoinSide,
        child_side: JoinSide,
        document_mapping: DocumentMapping,
    ) -> Self {
        // Determine if inverted: inverted when parent side doesn't have the FK
        let is_inverted = parent_side.relation_id_field_index.is_none();

        Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            is_inverted,
            initialized: false,
        }
    }

    /// Set the child plan (for re-initialization during scanning)
    pub fn with_child_plan(mut self, child_plan: Box<dyn PlanNode>) -> Self {
        self.child_plan = child_plan;
        self
    }

    /// Extract the foreign key value from the parent document
    fn extract_fk(&self, parent_doc: &Doc) -> Option<String> {
        if self.is_inverted {
            // Secondary side: use parent's _docID as the lookup key
            parent_doc.doc_id().map(String::from)
        } else {
            // Primary side: extract from the _id field (e.g., author_id)
            self.parent_side
                .relation_id_field_index
                .and_then(|idx| parent_doc.get(idx))
                .and_then(|v| v.as_str())
                .map(String::from)
        }
    }

    /// Find child document by FK lookup
    async fn find_child_doc(&mut self, fk: &str) -> Result<Option<Doc>> {
        // Re-initialize the child plan for this lookup
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value();

            if self.is_inverted {
                // Looking for child doc where child's FK == parent's _docID
                if let Some(child_fk_idx) = self.child_side.relation_id_field_index {
                    if let Some(child_fk) = child_doc.get(child_fk_idx).and_then(|v| v.as_str()) {
                        if child_fk == fk {
                            return Ok(Some(child_doc.deep_clone()));
                        }
                    }
                }
            } else {
                // Looking for child doc where _docID == fk
                if child_doc.doc_id() == Some(fk) {
                    return Ok(Some(child_doc.deep_clone()));
                }
            }
        }

        Ok(None)
    }

    /// Merge child document into parent at the relation field index
    fn merge_child(&self, parent_doc: &mut Doc, child_doc: Option<Doc>) {
        let child_value = match child_doc {
            Some(doc) => {
                // Convert child doc fields to a JSON object
                let child_mapping = self
                    .document_mapping
                    .child_at(self.parent_side.relation_field_index)
                    .unwrap_or(self.child_plan.document_map());

                let mut obj = serde_json::Map::new();
                for render_key in &child_mapping.render_keys {
                    if let Some(value) = doc.get(render_key.index) {
                        obj.insert(render_key.key.clone(), value.clone());
                    }
                }
                JsonValue::Object(obj)
            }
            None => JsonValue::Null,
        };

        parent_doc.set(self.parent_side.relation_field_index, child_value);
    }
}

#[async_trait]
impl PlanNode for TypeJoinOne {
    async fn init(&mut self) -> Result<()> {
        self.parent_plan.init().await?;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        self.parent_plan.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "TypeJoinOne.next() called before init()",
            ));
        }

        if !self.parent_plan.next().await? {
            return Ok(false);
        }

        let mut parent_doc = self.parent_plan.value().deep_clone();

        // Extract FK and lookup child
        let child_doc = if let Some(fk) = self.extract_fk(&parent_doc) {
            self.find_child_doc(&fk).await?
        } else {
            None
        };

        // Merge child into parent
        self.merge_child(&mut parent_doc, child_doc);
        self.current_doc = parent_doc;

        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.parent_plan.close().await?;
        self.child_plan.close().await?;
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.parent_plan.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "typeJoinOne"
    }
}

/// TypeJoinMany implements one-to-many relation joins.
///
/// The join flow:
/// 1. Parent plan yields a document (e.g., Author)
/// 2. Scan child collection for all docs where their FK matches parent's _docID
/// 3. Collect all matching child documents into an array
/// 4. Set the array on the parent document under the relation field key
pub struct TypeJoinMany {
    /// Parent side of the join (the "one" side)
    parent_side: JoinSide,
    /// Child side of the join (the "many" side)
    child_side: JoinSide,
    /// The parent plan node
    parent_plan: Box<dyn PlanNode>,
    /// The child plan node (for lookups)
    child_plan: Box<dyn PlanNode>,
    /// Document mapping for this join
    document_mapping: DocumentMapping,
    /// Current document (merged parent + children array)
    current_doc: Doc,
    /// Whether initialized
    initialized: bool,
}

impl TypeJoinMany {
    /// Create a new TypeJoinMany node
    pub fn new(
        parent_plan: Box<dyn PlanNode>,
        child_plan: Box<dyn PlanNode>,
        parent_side: JoinSide,
        child_side: JoinSide,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            initialized: false,
        }
    }

    /// Find all child documents that match the parent's _docID
    async fn find_child_docs(&mut self, parent_doc_id: &str) -> Result<Vec<Doc>> {
        let mut children = Vec::new();

        // Re-initialize the child plan
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value();

            // Check if child's FK matches parent's _docID
            if let Some(child_fk_idx) = self.child_side.relation_id_field_index {
                if let Some(child_fk) = child_doc.get(child_fk_idx).and_then(|v| v.as_str()) {
                    if child_fk == parent_doc_id {
                        children.push(child_doc.deep_clone());
                    }
                }
            }
        }

        Ok(children)
    }

    /// Merge child documents into parent as an array
    fn merge_children(&self, parent_doc: &mut Doc, children: Vec<Doc>) {
        let child_mapping = self
            .document_mapping
            .child_at(self.parent_side.relation_field_index)
            .unwrap_or(self.child_plan.document_map());

        let array: Vec<JsonValue> = children
            .into_iter()
            .map(|doc| {
                let mut obj = serde_json::Map::new();
                for render_key in &child_mapping.render_keys {
                    if let Some(value) = doc.get(render_key.index) {
                        obj.insert(render_key.key.clone(), value.clone());
                    }
                }
                JsonValue::Object(obj)
            })
            .collect();

        parent_doc.set(
            self.parent_side.relation_field_index,
            JsonValue::Array(array),
        );
    }
}

#[async_trait]
impl PlanNode for TypeJoinMany {
    async fn init(&mut self) -> Result<()> {
        self.parent_plan.init().await?;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        self.parent_plan.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "TypeJoinMany.next() called before init()",
            ));
        }

        if !self.parent_plan.next().await? {
            return Ok(false);
        }

        let mut parent_doc = self.parent_plan.value().deep_clone();

        // Get parent's _docID for the lookup
        let children = if let Some(parent_id) = parent_doc.doc_id() {
            self.find_child_docs(parent_id).await?
        } else {
            Vec::new()
        };

        // Merge children array into parent
        self.merge_children(&mut parent_doc, children);
        self.current_doc = parent_doc;

        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.parent_plan.close().await?;
        self.child_plan.close().await?;
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.parent_plan.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "typeJoinMany"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ScanNode;
    use schema::{FieldDescription, FieldKind};
    use serde_json::json;

    // Helper to create a Users collection (the "one" side)
    fn make_users_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                // One-to-many relation to posts (array)
                FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                    .with_relation_name("author_posts"),
            ],
        )
    }

    // Helper to create a Posts collection (the "many" side)
    fn make_posts_collection() -> CollectionVersion {
        CollectionVersion::new(
            "posts",
            "v1",
            "coll-posts",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "title", FieldKind::string()),
                // Many-to-one relation to users (singular)
                FieldDescription::new("3", "author", FieldKind::relation("users", false))
                    .with_relation_name("author_posts")
                    .as_primary(),
                // Auto-generated FK field
                FieldDescription::new("4", "author_id", FieldKind::doc_id())
                    .with_relation_name("author_posts")
                    .as_primary(),
            ],
        )
    }

    fn make_users_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "posts");
        m.add_render_key(0, "_docID");
        m.add_render_key(1, "name");
        m.add_render_key(2, "posts");
        m
    }

    fn make_posts_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "title");
        m.add(2, "author");
        m.add(3, "author_id");
        m.add_render_key(0, "_docID");
        m.add_render_key(1, "title");
        m
    }

    fn make_posts_child_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "title");
        m.add_render_key(0, "_docID");
        m.add_render_key(1, "title");
        m
    }

    fn make_user_docs() -> Vec<Doc> {
        vec![
            Doc::with_fields(vec![
                Some(json!("user-1")),
                Some(json!("Alice")),
                None, // posts will be filled by join
            ]),
            Doc::with_fields(vec![Some(json!("user-2")), Some(json!("Bob")), None]),
        ]
    }

    fn make_post_docs() -> Vec<Doc> {
        vec![
            Doc::with_fields(vec![
                Some(json!("post-1")),
                Some(json!("Alice's First Post")),
                None,                  // author object (filled by join)
                Some(json!("user-1")), // author_id FK
            ]),
            Doc::with_fields(vec![
                Some(json!("post-2")),
                Some(json!("Alice's Second Post")),
                None,
                Some(json!("user-1")),
            ]),
            Doc::with_fields(vec![
                Some(json!("post-3")),
                Some(json!("Bob's Post")),
                None,
                Some(json!("user-2")),
            ]),
        ]
    }

    #[test]
    fn test_join_side_new() {
        let posts = make_posts_collection();
        let relation_field = posts.field_by_name("author").unwrap().clone();

        let side = JoinSide::new(posts, relation_field, 2);

        // Should find the author_id field at index 3
        assert_eq!(side.relation_id_field_index, Some(3));
        assert!(!side.is_parent);
    }

    #[test]
    fn test_join_side_array_no_fk_index() {
        let users = make_users_collection();
        let relation_field = users.field_by_name("posts").unwrap().clone();

        let side = JoinSide::new(users, relation_field, 2);

        // Array relations don't have an _id field
        assert_eq!(side.relation_id_field_index, None);
    }

    #[tokio::test]
    async fn test_type_join_one_primary_side() {
        // Query: Posts { author { name } }
        // Post.author is the primary side (has author_id FK)

        let posts_collection = make_posts_collection();
        let users_collection = make_users_collection();

        let posts_mapping = make_posts_mapping();
        let users_mapping = make_users_mapping();

        // Parent: Posts scan
        let post_docs = make_post_docs();
        let parent_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(post_docs);

        // Child: Users scan (for lookups)
        let user_docs = make_user_docs();
        let child_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(user_docs);

        let parent_relation = posts_collection.field_by_name("author").unwrap().clone();
        let child_relation = users_collection.field_by_name("posts").unwrap().clone();

        let parent_side = JoinSide::new(
            posts_collection,
            parent_relation,
            2, // author field index
        )
        .as_parent();

        let child_side = JoinSide::new(
            users_collection,
            child_relation,
            2, // posts field index
        );

        // Build output mapping with child mapping for nested object
        let mut output_mapping = posts_mapping.clone();
        let mut author_child_mapping = DocumentMapping::new();
        author_child_mapping.add(0, "_docID");
        author_child_mapping.add(1, "name");
        author_child_mapping.add_render_key(0, "_docID");
        author_child_mapping.add_render_key(1, "name");
        output_mapping.set_child_at(2, author_child_mapping);

        let mut join = TypeJoinOne::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            output_mapping,
        );

        join.init().await.unwrap();
        join.start().await.unwrap();

        let mut results = Vec::new();
        while join.next().await.unwrap() {
            let doc = join.value();
            // Get the author field (index 2)
            let author_value = doc.get(2).cloned();
            results.push((doc.doc_id().map(String::from), author_value));
        }

        assert_eq!(results.len(), 3);

        // First post (post-1) should have Alice as author
        let (post_id, author) = &results[0];
        assert_eq!(post_id.as_deref(), Some("post-1"));
        assert!(author.is_some());
        let author_obj = author.as_ref().unwrap();
        assert_eq!(author_obj.get("name"), Some(&json!("Alice")));

        // Third post (post-3) should have Bob as author
        let (post_id, author) = &results[2];
        assert_eq!(post_id.as_deref(), Some("post-3"));
        assert!(author.is_some());
        let author_obj = author.as_ref().unwrap();
        assert_eq!(author_obj.get("name"), Some(&json!("Bob")));

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_many() {
        // Query: Users { posts { title } }
        // User.posts is a one-to-many relation

        let users_collection = make_users_collection();
        let posts_collection = make_posts_collection();

        let users_mapping = make_users_mapping();
        let posts_mapping = make_posts_mapping();

        // Parent: Users scan
        let user_docs = make_user_docs();
        let parent_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(user_docs);

        // Child: Posts scan (for lookups)
        let post_docs = make_post_docs();
        let child_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(post_docs);

        let parent_relation = users_collection.field_by_name("posts").unwrap().clone();
        let child_relation = posts_collection.field_by_name("author").unwrap().clone();

        let parent_side = JoinSide::new(
            users_collection,
            parent_relation,
            2, // posts field index
        )
        .as_parent();

        let child_side = JoinSide::new(
            posts_collection,
            child_relation,
            2, // author field index
        );

        // Build output mapping with child mapping for nested array
        let mut output_mapping = users_mapping.clone();
        let posts_child_mapping = make_posts_child_mapping();
        output_mapping.set_child_at(2, posts_child_mapping);

        let mut join = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            output_mapping,
        );

        join.init().await.unwrap();
        join.start().await.unwrap();

        let mut results = Vec::new();
        while join.next().await.unwrap() {
            let doc = join.value();
            let posts_value = doc.get(2).cloned();
            results.push((doc.doc_id().map(String::from), posts_value));
        }

        assert_eq!(results.len(), 2);

        // Alice (user-1) should have 2 posts
        let (user_id, posts) = &results[0];
        assert_eq!(user_id.as_deref(), Some("user-1"));
        let posts_arr = posts.as_ref().unwrap().as_array().unwrap();
        assert_eq!(posts_arr.len(), 2);
        assert_eq!(
            posts_arr[0].get("title"),
            Some(&json!("Alice's First Post"))
        );
        assert_eq!(
            posts_arr[1].get("title"),
            Some(&json!("Alice's Second Post"))
        );

        // Bob (user-2) should have 1 post
        let (user_id, posts) = &results[1];
        assert_eq!(user_id.as_deref(), Some("user-2"));
        let posts_arr = posts.as_ref().unwrap().as_array().unwrap();
        assert_eq!(posts_arr.len(), 1);
        assert_eq!(posts_arr[0].get("title"), Some(&json!("Bob's Post")));

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_one_no_match() {
        // Test case where FK points to a non-existent document

        let posts_collection = make_posts_collection();
        let users_collection = make_users_collection();

        let posts_mapping = make_posts_mapping();
        let users_mapping = make_users_mapping();

        // Post with FK to non-existent user
        let post_docs = vec![Doc::with_fields(vec![
            Some(json!("post-orphan")),
            Some(json!("Orphan Post")),
            None,
            Some(json!("user-nonexistent")), // FK to non-existent user
        ])];

        let parent_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(post_docs);

        // Empty users collection
        let child_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(vec![]);

        let parent_relation = posts_collection.field_by_name("author").unwrap().clone();
        let child_relation = users_collection.field_by_name("posts").unwrap().clone();

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).as_parent();
        let child_side = JoinSide::new(users_collection, child_relation, 2);

        let mut output_mapping = posts_mapping.clone();
        let mut author_child_mapping = DocumentMapping::new();
        author_child_mapping.add(0, "_docID");
        author_child_mapping.add(1, "name");
        author_child_mapping.add_render_key(0, "_docID");
        author_child_mapping.add_render_key(1, "name");
        output_mapping.set_child_at(2, author_child_mapping);

        let mut join = TypeJoinOne::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            output_mapping,
        );

        join.init().await.unwrap();
        join.start().await.unwrap();

        assert!(join.next().await.unwrap());
        let doc = join.value();

        // Author field should be null
        assert_eq!(doc.get(2), Some(&JsonValue::Null));

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_many_empty() {
        // Test case where parent has no matching children

        let users_collection = make_users_collection();
        let posts_collection = make_posts_collection();

        let users_mapping = make_users_mapping();
        let posts_mapping = make_posts_mapping();

        let user_docs = vec![Doc::with_fields(vec![
            Some(json!("user-lonely")),
            Some(json!("Lonely User")),
            None,
        ])];

        let parent_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(user_docs);

        // Empty posts collection
        let child_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(vec![]);

        let parent_relation = users_collection.field_by_name("posts").unwrap().clone();
        let child_relation = posts_collection.field_by_name("author").unwrap().clone();

        let parent_side = JoinSide::new(users_collection, parent_relation, 2).as_parent();
        let child_side = JoinSide::new(posts_collection, child_relation, 2);

        let mut output_mapping = users_mapping.clone();
        let posts_child_mapping = make_posts_child_mapping();
        output_mapping.set_child_at(2, posts_child_mapping);

        let mut join = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            output_mapping,
        );

        join.init().await.unwrap();
        join.start().await.unwrap();

        assert!(join.next().await.unwrap());
        let doc = join.value();

        // Posts field should be an empty array
        let posts = doc.get(2).unwrap();
        assert!(posts.is_array());
        assert_eq!(posts.as_array().unwrap().len(), 0);

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_next_before_init_errors() {
        let posts_collection = make_posts_collection();
        let users_collection = make_users_collection();

        let posts_mapping = make_posts_mapping();
        let users_mapping = make_users_mapping();

        let parent_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(vec![]);
        let child_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(vec![]);

        let parent_relation = posts_collection.field_by_name("author").unwrap().clone();
        let child_relation = users_collection.field_by_name("posts").unwrap().clone();

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).as_parent();
        let child_side = JoinSide::new(users_collection, child_relation, 2);

        let mut join = TypeJoinOne::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            posts_mapping,
        );

        // Call next without init
        let result = join.next().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("called before init"));
    }
}
