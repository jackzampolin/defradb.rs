//! TypeJoinOne - one-to-one relation joins

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tracing::warn;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::planner::{Doc, PlanNode};

use super::{JoinDirection, JoinSide};

/// TypeJoinOne implements one-to-one relation joins.
///
/// **Primary side join flow** (when parent has the FK, e.g., `Book.author`):
/// 1. Parent plan yields a document (e.g., Book with `author_id: "bae-123"`)
/// 2. Extract the FK value from the relation's ID field (e.g., `author_id`)
/// 3. Lookup child document where `_docID` matches the FK value
/// 4. Merge the child document into the parent under the relation field key
///
/// **Secondary/inverted side join flow** (when parent lacks FK, e.g., `Author.book`):
/// 1. Parent plan yields a document (e.g., Author with `_docID: "bae-123"`)
/// 2. Lookup child document where their FK matches parent's `_docID`
/// 3. Merge the first matching child document
///
/// # Optimization
///
/// Child documents are pre-loaded and indexed during `init()` to avoid
/// O(N * M) nested loop scans. Lookups are O(1) via HashMap.
pub struct TypeJoinOne {
    /// Parent side of the join (outer loop)
    parent_side: JoinSide,
    /// Child side of the join (lookup)
    child_side: JoinSide,
    /// The parent plan node
    parent_plan: Box<dyn PlanNode>,
    /// The child plan node (scanned once during init)
    child_plan: Box<dyn PlanNode>,
    /// Document mapping for this join
    document_mapping: DocumentMapping,
    /// Current document (merged parent + child)
    current_doc: Doc,
    /// The direction of this join, determined by which side holds the FK.
    pub(crate) direction: JoinDirection,
    /// Whether initialized
    initialized: bool,
    /// Cached child documents indexed by lookup key.
    /// For Primary joins: key is child's _docID
    /// For Inverted joins: key is child's FK field value
    child_cache: HashMap<String, Doc>,
}

impl std::fmt::Debug for TypeJoinOne {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeJoinOne")
            .field("parent_side", &self.parent_side)
            .field("child_side", &self.child_side)
            .field(
                "parent_plan",
                &format_args!("<PlanNode: {}>", self.parent_plan.kind()),
            )
            .field(
                "child_plan",
                &format_args!("<PlanNode: {}>", self.child_plan.kind()),
            )
            .field("direction", &self.direction)
            .field("initialized", &self.initialized)
            .finish()
    }
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
        // Determine join direction based on which side holds the FK
        let direction = match parent_side.relation_id_field_index() {
            Some(idx) => JoinDirection::Primary {
                parent_fk_index: idx,
            },
            None => JoinDirection::Inverted,
        };

        Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            direction,
            initialized: false,
            child_cache: HashMap::new(),
        }
    }

    /// Extract the foreign key value from the parent document.
    ///
    /// For primary joins, extracts the FK field value.
    /// For inverted joins, extracts the parent's `_docID`.
    ///
    /// Logs a warning if the FK field exists but has an unexpected type.
    fn extract_fk(&self, parent_doc: &Doc) -> Option<String> {
        match &self.direction {
            JoinDirection::Inverted => {
                // Secondary side: use parent's _docID as the lookup key
                parent_doc.doc_id().map(String::from)
            }
            JoinDirection::Primary { parent_fk_index } => {
                // Primary side: extract from the FK field (e.g., author_id)
                let value = parent_doc.get(*parent_fk_index)?;

                // Check for type mismatch (FK should be string or null)
                if !value.is_null() && !value.is_string() {
                    warn!(
                        parent_collection = %self.parent_side.collection().name,
                        relation_field = %self.parent_side.relation_field().name,
                        fk_index = parent_fk_index,
                        actual_type = ?value,
                        "FK field has unexpected type, expected string or null"
                    );
                }

                value.as_str().map(String::from)
            }
        }
    }

    /// Find child document by FK lookup using the pre-built cache.
    fn find_child_doc(&self, fk: &str) -> Option<Doc> {
        self.child_cache.get(fk).map(|doc| doc.deep_clone())
    }

    /// Build the child cache by scanning child_plan once.
    /// For Primary joins: index by child's _docID
    /// For Inverted joins: index by child's FK field value
    async fn build_child_cache(&mut self) -> Result<()> {
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        let child_fk_idx = self.child_side.relation_id_field_index();

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value().deep_clone();

            let key = match &self.direction {
                JoinDirection::Primary { .. } => {
                    // Index by child's _docID for FK → doc lookup
                    child_doc.doc_id().map(String::from)
                }
                JoinDirection::Inverted => {
                    // Index by child's FK field value for reverse lookup
                    child_fk_idx.and_then(|idx| {
                        child_doc.get(idx).and_then(|v| v.as_str().map(String::from))
                    })
                }
            };

            if let Some(k) = key {
                // For one-to-one, we only keep the first match
                self.child_cache.entry(k).or_insert(child_doc);
            } else {
                warn!(
                    child_collection = %self.child_side.collection().name,
                    doc_id = ?child_doc.doc_id(),
                    direction = ?self.direction,
                    "Child document skipped during cache building - no valid lookup key"
                );
            }
        }

        self.child_plan.close().await?;
        Ok(())
    }

    /// Merge child document into parent at the relation field index.
    fn merge_child(&self, parent_doc: &mut Doc, child_doc: Option<Doc>) {
        // Get child mapping. Falls back to child plan's mapping if not explicitly
        // set in parent mapping - this happens for simple queries where child
        // mapping was not pre-configured during planning.
        let child_value = match child_doc {
            Some(doc) => {
                let child_mapping = self
                    .document_mapping
                    .child_at(self.parent_side.relation_field_index())
                    .unwrap_or(self.child_plan.document_map());
                child_mapping.render_doc_to_json(&doc)
            }
            None => JsonValue::Null,
        };

        parent_doc.set(self.parent_side.relation_field_index(), child_value);
    }
}

#[async_trait]
impl PlanNode for TypeJoinOne {
    async fn init(&mut self) -> Result<()> {
        // Build child cache first (scans child_plan once)
        self.build_child_cache().await?;
        // Then init parent plan
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

        // Extract FK and lookup child in cache (O(1) lookup)
        let child_doc = self
            .extract_fk(&parent_doc)
            .and_then(|fk| self.find_child_doc(&fk));

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
        // child_plan was already closed in build_child_cache()
        self.child_cache.clear();
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
