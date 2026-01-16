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

/// Represents one side of a join operation.
///
/// Encapsulates the collection schema, relation field, and field indexes needed
/// for join operations. Automatically derives the FK field index from the
/// relation field name for non-array relations.
#[derive(Clone, Debug)]
pub struct JoinSide {
    /// The collection schema for this side
    collection: CollectionVersion,
    /// The relation field description
    relation_field: FieldDescription,
    /// Index of the relation field in the document
    relation_field_index: usize,
    /// Index of the FK field (e.g., `author_id` for an "author" relation) if this side holds the foreign key.
    /// For array relations (one-to-many from this side), this is None since the FK lives on the other side.
    relation_id_field_index: Option<usize>,
}

impl JoinSide {
    /// Create a new join side.
    ///
    /// Automatically derives the FK field index for non-array relations by looking
    /// up the `{relation_field_name}_id` field in the collection schema.
    ///
    /// The `relation_field_index` is the position in the output document mapping
    /// where the joined data will be stored, not an index into `collection.fields`.
    pub fn new(
        collection: CollectionVersion,
        relation_field: FieldDescription,
        relation_field_index: usize,
    ) -> Result<Self> {
        // Auto-derive the FK field index for non-array relations
        let relation_id_field_index = if !relation_field.kind.is_array() {
            let id_field_name = CollectionVersion::relation_id_field_name(&relation_field.name);
            collection
                .fields
                .iter()
                .position(|f| f.name == id_field_name)
        } else {
            None
        };

        Ok(Self {
            collection,
            relation_field,
            relation_field_index,
            relation_id_field_index,
        })
    }

    pub fn collection(&self) -> &CollectionVersion {
        &self.collection
    }

    pub fn relation_field(&self) -> &FieldDescription {
        &self.relation_field
    }

    pub fn relation_field_index(&self) -> usize {
        self.relation_field_index
    }

    /// Get the FK field index (e.g., `author_id`) if this side holds the FK.
    /// Returns None for array relations since the FK lives on the "many" side.
    pub fn relation_id_field_index(&self) -> Option<usize> {
        self.relation_id_field_index
    }
}

/// TypeJoinOne implements one-to-one relation joins.
///
/// **Primary side join flow** (when parent has the FK, e.g., `Book.author`):
/// 1. Parent plan yields a document (e.g., Book with `author_id: "bae-123"`)
/// 2. Extract the FK value from the relation's ID field (e.g., `author_id`)
/// 3. Scan child collection for document where `_docID` matches the FK value
/// 4. Merge the child document into the parent under the relation field key
///
/// **Secondary/inverted side join flow** (when parent lacks FK, e.g., `Author.book`):
/// 1. Parent plan yields a document (e.g., Author with `_docID: "bae-123"`)
/// 2. Scan child collection for docs where their FK matches parent's `_docID`
/// 3. Merge the first matching child document
pub struct TypeJoinOne {
    /// Parent side of the join (outer loop)
    parent_side: JoinSide,
    /// Child side of the join (lookup)
    child_side: JoinSide,
    /// The parent plan node
    parent_plan: Box<dyn PlanNode>,
    /// The child plan node (re-initialized for each lookup)
    child_plan: Box<dyn PlanNode>,
    /// Document mapping for this join
    document_mapping: DocumentMapping,
    /// Current document (merged parent + child)
    current_doc: Doc,
    /// Whether this is an inverted join. Inverted joins occur when querying from the
    /// secondary side of a relation (the side without the FK). This changes lookup
    /// direction: instead of looking up child by FK value, we scan children to find
    /// those pointing to parent's `_docID`.
    is_inverted: bool,
    /// Whether initialized
    initialized: bool,
}

impl TypeJoinOne {
    /// Create a new TypeJoinOne node.
    pub fn new(
        parent_plan: Box<dyn PlanNode>,
        child_plan: Box<dyn PlanNode>,
        parent_side: JoinSide,
        child_side: JoinSide,
        document_mapping: DocumentMapping,
    ) -> Self {
        // Inverted when parent side doesn't have the FK (secondary side query)
        let is_inverted = parent_side.relation_id_field_index().is_none();

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

    /// Extract the foreign key value from the parent document
    fn extract_fk(&self, parent_doc: &Doc) -> Option<String> {
        if self.is_inverted {
            // Secondary side: use parent's _docID as the lookup key
            parent_doc.doc_id().map(String::from)
        } else {
            // Primary side: extract from the _id field (e.g., author_id)
            self.parent_side
                .relation_id_field_index()
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
                if let Some(child_fk_idx) = self.child_side.relation_id_field_index() {
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

    /// Merge child document into parent at the relation field index.
    fn merge_child(&self, parent_doc: &mut Doc, child_doc: Option<Doc>) {
        let child_value = match child_doc {
            Some(doc) => {
                // Get child mapping. Falls back to child plan's mapping if not explicitly
                // set in parent mapping - this happens for simple queries where child
                // mapping was not pre-configured during planning.
                let child_mapping = self
                    .document_mapping
                    .child_at(self.parent_side.relation_field_index())
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

        parent_doc.set(self.parent_side.relation_field_index(), child_value);
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

impl std::fmt::Debug for TypeJoinMany {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeJoinMany")
            .field("parent_side", &self.parent_side)
            .field("child_side", &self.child_side)
            .field("parent_plan", &format_args!("<PlanNode: {}>", self.parent_plan.kind()))
            .field("child_plan", &format_args!("<PlanNode: {}>", self.child_plan.kind()))
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl TypeJoinMany {
    /// Create a new TypeJoinMany node.
    ///
    /// # Errors
    /// Returns an error if `child_side` does not have a `relation_id_field_index` (FK field).
    /// One-to-many joins require the child to have an FK field pointing to the parent.
    pub fn new(
        parent_plan: Box<dyn PlanNode>,
        child_plan: Box<dyn PlanNode>,
        parent_side: JoinSide,
        child_side: JoinSide,
        document_mapping: DocumentMapping,
    ) -> Result<Self> {
        // Validate that child side has FK field - required for one-to-many joins
        if child_side.relation_id_field_index().is_none() {
            return Err(QueryError::internal(format!(
                "TypeJoinMany requires child side to have FK field. \
                 Child collection '{}' relation field '{}' has no FK field.",
                child_side.collection().name,
                child_side.relation_field().name
            )));
        }

        Ok(Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            initialized: false,
        })
    }

    /// Find all child documents that match the parent's _docID.
    async fn find_child_docs(&mut self, parent_doc_id: &str) -> Result<Vec<Doc>> {
        let mut children = Vec::new();

        // Re-initialize the child plan for this lookup
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        // Safe: constructor validates that child_side has FK field index
        let child_fk_idx = self
            .child_side
            .relation_id_field_index()
            .expect("TypeJoinMany child_side FK index validated in constructor");

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value();

            // Check if child's FK matches parent's _docID
            if let Some(child_fk) = child_doc.get(child_fk_idx).and_then(|v| v.as_str()) {
                if child_fk == parent_doc_id {
                    children.push(child_doc.deep_clone());
                }
            }
        }

        Ok(children)
    }

    /// Merge child documents into parent as an array.
    fn merge_children(&self, parent_doc: &mut Doc, children: Vec<Doc>) {
        // Get child mapping. Falls back to child plan's mapping if not explicitly
        // set in parent mapping - this happens for simple queries where child
        // mapping was not pre-configured during planning.
        let child_mapping = self
            .document_mapping
            .child_at(self.parent_side.relation_field_index())
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
            self.parent_side.relation_field_index(),
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

        let side = JoinSide::new(posts, relation_field, 2).unwrap();

        // Should find the author_id field at index 3
        assert_eq!(side.relation_id_field_index(), Some(3));
    }

    #[test]
    fn test_join_side_array_no_fk_index() {
        let users = make_users_collection();
        let relation_field = users.field_by_name("posts").unwrap().clone();

        let side = JoinSide::new(users, relation_field, 2).unwrap();

        // Array relations don't have an _id field
        assert_eq!(side.relation_id_field_index(), None);
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

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).unwrap();

        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

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

        let parent_side = JoinSide::new(users_collection, parent_relation, 2).unwrap();

        let child_side = JoinSide::new(posts_collection, child_relation, 2).unwrap();

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
        )
        .unwrap();

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

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

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

        let parent_side = JoinSide::new(users_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(posts_collection, child_relation, 2).unwrap();

        let mut output_mapping = users_mapping.clone();
        let posts_child_mapping = make_posts_child_mapping();
        output_mapping.set_child_at(2, posts_child_mapping);

        let mut join = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            output_mapping,
        )
        .unwrap();

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

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

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

    // Helper for inverted one-to-one test - Authors collection (secondary side, no FK)
    fn make_authors_collection() -> CollectionVersion {
        CollectionVersion::new(
            "authors",
            "v1",
            "coll-authors",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                // One-to-one relation to book (singular, secondary side - no FK)
                FieldDescription::new("3", "book", FieldKind::relation("books", false))
                    .with_relation_name("author_book"),
            ],
        )
    }

    // Helper for inverted one-to-one test - Books collection (primary side, has FK)
    fn make_books_collection() -> CollectionVersion {
        CollectionVersion::new(
            "books",
            "v1",
            "coll-books",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "title", FieldKind::string()),
                // One-to-one relation to author (singular, primary side - has FK)
                FieldDescription::new("3", "author", FieldKind::relation("authors", false))
                    .with_relation_name("author_book")
                    .as_primary(),
                // Auto-generated FK field
                FieldDescription::new("4", "author_id", FieldKind::doc_id())
                    .with_relation_name("author_book")
                    .as_primary(),
            ],
        )
    }

    fn make_authors_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "book");
        m.add_render_key(0, "_docID");
        m.add_render_key(1, "name");
        m.add_render_key(2, "book");
        m
    }

    fn make_books_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "title");
        m.add(2, "author");
        m.add(3, "author_id");
        m.add_render_key(0, "_docID");
        m.add_render_key(1, "title");
        m
    }

    #[tokio::test]
    async fn test_type_join_one_inverted_secondary_side() {
        // Query: Authors { book { title } }
        // Author.book is the SECONDARY side (no FK - inverted join)
        // Book has author_id FK pointing to Author

        let authors_collection = make_authors_collection();
        let books_collection = make_books_collection();

        let authors_mapping = make_authors_mapping();
        let books_mapping = make_books_mapping();

        // Parent: Authors scan
        let author_docs = vec![
            Doc::with_fields(vec![
                Some(json!("author-1")),
                Some(json!("J.K. Rowling")),
                None, // book will be filled by join
            ]),
            Doc::with_fields(vec![
                Some(json!("author-2")),
                Some(json!("George Orwell")),
                None,
            ]),
        ];
        let parent_scan =
            ScanNode::new(authors_collection.clone(), authors_mapping.clone()).with_docs(author_docs);

        // Child: Books scan (for lookups)
        let book_docs = vec![
            Doc::with_fields(vec![
                Some(json!("book-1")),
                Some(json!("Harry Potter")),
                None,                     // author object
                Some(json!("author-1")),  // author_id FK
            ]),
            Doc::with_fields(vec![
                Some(json!("book-2")),
                Some(json!("1984")),
                None,
                Some(json!("author-2")),
            ]),
        ];
        let child_scan =
            ScanNode::new(books_collection.clone(), books_mapping.clone()).with_docs(book_docs);

        // Parent side: Author.book (secondary, no FK)
        let parent_relation = authors_collection.field_by_name("book").unwrap().clone();
        // Child side: Book.author (primary, has FK)
        let child_relation = books_collection.field_by_name("author").unwrap().clone();

        let parent_side = JoinSide::new(
            authors_collection,
            parent_relation,
            2, // book field index
        )
        .unwrap();

        let child_side = JoinSide::new(
            books_collection,
            child_relation,
            2, // author field index
        )
        .unwrap();

        // Verify this is an inverted join (parent has no FK)
        assert!(parent_side.relation_id_field_index().is_none());
        // Child should have FK
        assert!(child_side.relation_id_field_index().is_some());

        // Build output mapping with child mapping for nested object
        let mut output_mapping = authors_mapping.clone();
        let mut book_child_mapping = DocumentMapping::new();
        book_child_mapping.add(0, "_docID");
        book_child_mapping.add(1, "title");
        book_child_mapping.add_render_key(0, "_docID");
        book_child_mapping.add_render_key(1, "title");
        output_mapping.set_child_at(2, book_child_mapping);

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
            let book_value = doc.get(2).cloned();
            results.push((doc.doc_id().map(String::from), book_value));
        }

        assert_eq!(results.len(), 2);

        // J.K. Rowling (author-1) should have Harry Potter
        let (author_id, book) = &results[0];
        assert_eq!(author_id.as_deref(), Some("author-1"));
        assert!(book.is_some());
        let book_obj = book.as_ref().unwrap();
        assert_eq!(book_obj.get("title"), Some(&json!("Harry Potter")));

        // George Orwell (author-2) should have 1984
        let (author_id, book) = &results[1];
        assert_eq!(author_id.as_deref(), Some("author-2"));
        assert!(book.is_some());
        let book_obj = book.as_ref().unwrap();
        assert_eq!(book_obj.get("title"), Some(&json!("1984")));

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_many_next_before_init_errors() {
        // Test that TypeJoinMany also errors when next() is called before init()

        let users_collection = make_users_collection();
        let posts_collection = make_posts_collection();

        let users_mapping = make_users_mapping();
        let posts_mapping = make_posts_mapping();

        let parent_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(vec![]);
        let child_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(vec![]);

        let parent_relation = users_collection.field_by_name("posts").unwrap().clone();
        let child_relation = posts_collection.field_by_name("author").unwrap().clone();

        let parent_side = JoinSide::new(users_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(posts_collection, child_relation, 2).unwrap();

        let mut join = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            users_mapping,
        )
        .unwrap();

        // Call next without init
        let result = join.next().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("called before init"));
    }

    #[tokio::test]
    async fn test_type_join_one_null_fk() {
        // Test case where FK field has explicit null value (not just missing)

        let posts_collection = make_posts_collection();
        let users_collection = make_users_collection();

        let posts_mapping = make_posts_mapping();
        let users_mapping = make_users_mapping();

        // Post with explicit null FK
        let post_docs = vec![Doc::with_fields(vec![
            Some(json!("post-null-fk")),
            Some(json!("Post with null author")),
            None,
            Some(JsonValue::Null), // Explicit null FK
        ])];

        let parent_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(post_docs);

        let user_docs = make_user_docs();
        let child_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(user_docs);

        let parent_relation = posts_collection.field_by_name("author").unwrap().clone();
        let child_relation = users_collection.field_by_name("posts").unwrap().clone();

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

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

        // Author field should be null (explicit null FK means no author)
        assert_eq!(doc.get(2), Some(&JsonValue::Null));

        join.close().await.unwrap();
    }

    // Helper for self-referential test - Employees collection
    fn make_employees_collection() -> CollectionVersion {
        CollectionVersion::new(
            "employees",
            "v1",
            "coll-employees",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                // Self-referential: manager is an Employee
                FieldDescription::new("3", "manager", FieldKind::relation("employees", false))
                    .with_relation_name("employee_manager")
                    .as_primary(),
                // FK field for manager
                FieldDescription::new("4", "manager_id", FieldKind::doc_id())
                    .with_relation_name("employee_manager")
                    .as_primary(),
            ],
        )
    }

    fn make_employees_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "manager");
        m.add(3, "manager_id");
        m.add_render_key(0, "_docID");
        m.add_render_key(1, "name");
        m.add_render_key(2, "manager");
        m
    }

    #[tokio::test]
    async fn test_type_join_one_self_referential() {
        // Query: Employee { manager { name } }
        // Self-referential relation where employees point to their manager

        let employees_collection = make_employees_collection();
        let employees_mapping = make_employees_mapping();

        // Alice is the CEO (no manager), Bob reports to Alice
        let employee_docs = vec![
            Doc::with_fields(vec![
                Some(json!("emp-alice")),
                Some(json!("Alice")),
                None,           // manager object
                JsonValue::Null.into(), // manager_id (no manager)
            ]),
            Doc::with_fields(vec![
                Some(json!("emp-bob")),
                Some(json!("Bob")),
                None,
                Some(json!("emp-alice")), // Bob reports to Alice
            ]),
            Doc::with_fields(vec![
                Some(json!("emp-charlie")),
                Some(json!("Charlie")),
                None,
                Some(json!("emp-bob")), // Charlie reports to Bob
            ]),
        ];

        // Parent: Employee scan
        let parent_scan = ScanNode::new(employees_collection.clone(), employees_mapping.clone())
            .with_docs(employee_docs.clone());

        // Child: Same collection (self-referential)
        let child_scan = ScanNode::new(employees_collection.clone(), employees_mapping.clone())
            .with_docs(employee_docs);

        let manager_relation = employees_collection.field_by_name("manager").unwrap().clone();

        let parent_side = JoinSide::new(
            employees_collection.clone(),
            manager_relation.clone(),
            2, // manager field index
        )
        .unwrap();

        // For self-referential, child side uses the same relation
        let child_side = JoinSide::new(
            employees_collection,
            manager_relation,
            2, // manager field index
        )
        .unwrap();

        // Build output mapping
        let mut output_mapping = employees_mapping.clone();
        let mut manager_child_mapping = DocumentMapping::new();
        manager_child_mapping.add(0, "_docID");
        manager_child_mapping.add(1, "name");
        manager_child_mapping.add_render_key(0, "_docID");
        manager_child_mapping.add_render_key(1, "name");
        output_mapping.set_child_at(2, manager_child_mapping);

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
            let manager_value = doc.get(2).cloned();
            results.push((doc.doc_id().map(String::from), manager_value));
        }

        assert_eq!(results.len(), 3);

        // Alice has no manager
        let (emp_id, manager) = &results[0];
        assert_eq!(emp_id.as_deref(), Some("emp-alice"));
        assert_eq!(manager, &Some(JsonValue::Null));

        // Bob's manager is Alice
        let (emp_id, manager) = &results[1];
        assert_eq!(emp_id.as_deref(), Some("emp-bob"));
        assert!(manager.is_some());
        let manager_obj = manager.as_ref().unwrap();
        assert_eq!(manager_obj.get("name"), Some(&json!("Alice")));

        // Charlie's manager is Bob
        let (emp_id, manager) = &results[2];
        assert_eq!(emp_id.as_deref(), Some("emp-charlie"));
        assert!(manager.is_some());
        let manager_obj = manager.as_ref().unwrap();
        assert_eq!(manager_obj.get("name"), Some(&json!("Bob")));

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_one_close_without_init() {
        // Test that close() works even if init() was never called

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

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

        let mut join = TypeJoinOne::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            posts_mapping,
        );

        // close() without init() should not panic
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_one_double_close() {
        // Test that calling close() twice doesn't cause issues

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

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

        let mut join = TypeJoinOne::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            posts_mapping,
        );

        join.init().await.unwrap();
        join.close().await.unwrap();
        // Second close should also succeed
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_one_next_after_close() {
        // Test that next() after close() returns error (since close resets initialized)

        let posts_collection = make_posts_collection();
        let users_collection = make_users_collection();

        let posts_mapping = make_posts_mapping();
        let users_mapping = make_users_mapping();

        let post_docs = make_post_docs();
        let parent_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(post_docs);
        let user_docs = make_user_docs();
        let child_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(user_docs);

        let parent_relation = posts_collection.field_by_name("author").unwrap().clone();
        let child_relation = users_collection.field_by_name("posts").unwrap().clone();

        let parent_side = JoinSide::new(posts_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

        let mut join = TypeJoinOne::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            posts_mapping,
        );

        join.init().await.unwrap();
        join.start().await.unwrap();
        join.close().await.unwrap();

        // next() after close() should error since initialized was reset
        let result = join.next().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("called before init"));
    }

    #[tokio::test]
    async fn test_type_join_many_close_without_init() {
        // Test that TypeJoinMany close() works even if init() was never called

        let users_collection = make_users_collection();
        let posts_collection = make_posts_collection();

        let users_mapping = make_users_mapping();
        let posts_mapping = make_posts_mapping();

        let parent_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(vec![]);
        let child_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(vec![]);

        let parent_relation = users_collection.field_by_name("posts").unwrap().clone();
        let child_relation = posts_collection.field_by_name("author").unwrap().clone();

        let parent_side = JoinSide::new(users_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(posts_collection, child_relation, 2).unwrap();

        let mut join = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            users_mapping,
        )
        .unwrap();

        // close() without init() should not panic
        join.close().await.unwrap();
    }

    #[test]
    fn test_type_join_many_requires_child_fk() {
        // Test that TypeJoinMany returns error when child side has no FK field
        let users_collection = make_users_collection();
        let posts_collection = make_posts_collection();

        let users_mapping = make_users_mapping();
        let posts_mapping = make_posts_mapping();

        let parent_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(vec![]);
        let child_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(vec![]);

        // Parent side: users.posts (array relation)
        let parent_relation = users_collection.field_by_name("posts").unwrap().clone();
        // Child side: users.posts (array relation - no FK field)
        // Using the array relation from users, which has no FK field
        let child_relation = users_collection.field_by_name("posts").unwrap().clone();

        let parent_side = JoinSide::new(users_collection.clone(), parent_relation, 2).unwrap();
        let child_side = JoinSide::new(users_collection, child_relation, 2).unwrap();

        // This should fail because child_side has no FK field
        let result = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            users_mapping,
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires child side to have FK field"));
    }

    #[tokio::test]
    async fn test_type_join_many_child_null_fk() {
        // Test that children with null FK values are correctly skipped
        let users_collection = make_users_collection();
        let posts_collection = make_posts_collection();

        let users_mapping = make_users_mapping();
        let posts_mapping = make_posts_mapping();

        // User to query
        let user_docs = vec![Doc::with_fields(vec![
            Some(json!("user-1")),
            Some(json!("Alice")),
            None,
        ])];

        // Posts - one with valid FK, one with null FK
        let post_docs = vec![
            Doc::with_fields(vec![
                Some(json!("post-1")),
                Some(json!("Valid Post")),
                None,
                Some(json!("user-1")), // Valid FK
            ]),
            Doc::with_fields(vec![
                Some(json!("post-2")),
                Some(json!("Orphan Post")),
                None,
                Some(JsonValue::Null), // Null FK - should be skipped
            ]),
        ];

        let parent_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(user_docs);
        let child_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(post_docs);

        let parent_relation = users_collection.field_by_name("posts").unwrap().clone();
        let child_relation = posts_collection.field_by_name("author").unwrap().clone();

        let parent_side = JoinSide::new(users_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(posts_collection, child_relation, 2).unwrap();

        let mut output_mapping = users_mapping.clone();
        let posts_child_mapping = make_posts_child_mapping();
        output_mapping.set_child_at(2, posts_child_mapping);

        let mut join = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            output_mapping,
        )
        .unwrap();

        join.init().await.unwrap();
        join.start().await.unwrap();

        assert!(join.next().await.unwrap());
        let doc = join.value();

        // Should only have 1 post (the one with valid FK)
        let posts = doc.get(2).unwrap().as_array().unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].get("title"), Some(&json!("Valid Post")));

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_type_join_many_parent_without_doc_id() {
        // Test that parent without _docID returns empty children array
        let users_collection = make_users_collection();
        let posts_collection = make_posts_collection();

        let users_mapping = make_users_mapping();
        let posts_mapping = make_posts_mapping();

        // User without _docID (malformed data)
        let user_docs = vec![Doc::with_fields(vec![
            Some(JsonValue::Null), // Null _docID
            Some(json!("Ghost User")),
            None,
        ])];

        let post_docs = make_post_docs();

        let parent_scan =
            ScanNode::new(users_collection.clone(), users_mapping.clone()).with_docs(user_docs);
        let child_scan =
            ScanNode::new(posts_collection.clone(), posts_mapping.clone()).with_docs(post_docs);

        let parent_relation = users_collection.field_by_name("posts").unwrap().clone();
        let child_relation = posts_collection.field_by_name("author").unwrap().clone();

        let parent_side = JoinSide::new(users_collection, parent_relation, 2).unwrap();
        let child_side = JoinSide::new(posts_collection, child_relation, 2).unwrap();

        let mut output_mapping = users_mapping.clone();
        let posts_child_mapping = make_posts_child_mapping();
        output_mapping.set_child_at(2, posts_child_mapping);

        let mut join = TypeJoinMany::new(
            Box::new(parent_scan),
            Box::new(child_scan),
            parent_side,
            child_side,
            output_mapping,
        )
        .unwrap();

        join.init().await.unwrap();
        join.start().await.unwrap();

        assert!(join.next().await.unwrap());
        let doc = join.value();

        // Should have empty posts array (parent has no _docID to match against)
        let posts = doc.get(2).unwrap().as_array().unwrap();
        assert_eq!(posts.len(), 0);

        join.close().await.unwrap();
    }
}
