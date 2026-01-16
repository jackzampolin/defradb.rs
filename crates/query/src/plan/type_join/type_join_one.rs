//! TypeJoinOne - one-to-one relation joins

use async_trait::async_trait;
use serde_json::Value as JsonValue;
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
    /// The direction of this join, determined by which side holds the FK.
    pub(crate) direction: JoinDirection,
    /// Whether initialized
    initialized: bool,
}

impl std::fmt::Debug for TypeJoinOne {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeJoinOne")
            .field("parent_side", &self.parent_side)
            .field("child_side", &self.child_side)
            .field("parent_plan", &format_args!("<PlanNode: {}>", self.parent_plan.kind()))
            .field("child_plan", &format_args!("<PlanNode: {}>", self.child_plan.kind()))
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
            Some(idx) => JoinDirection::Primary { parent_fk_index: idx },
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

    /// Find child document by FK lookup.
    async fn find_child_doc(&mut self, fk: &str) -> Result<Option<Doc>> {
        // Re-initialize the child plan for this lookup
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value();

            match &self.direction {
                JoinDirection::Inverted => {
                    // Looking for child doc where child's FK == parent's _docID
                    if let Some(child_fk_idx) = self.child_side.relation_id_field_index() {
                        let child_fk_value = child_doc.get(child_fk_idx);

                        // Log type mismatch for non-null, non-string FK values
                        if let Some(v) = child_fk_value {
                            if !v.is_null() && !v.is_string() {
                                warn!(
                                    child_collection = %self.child_side.collection().name,
                                    relation_field = %self.child_side.relation_field().name,
                                    fk_index = child_fk_idx,
                                    actual_type = ?v,
                                    "Child FK field has unexpected type, expected string or null"
                                );
                            }
                        }

                        if let Some(child_fk) = child_fk_value.and_then(|v| v.as_str()) {
                            if child_fk == fk {
                                return Ok(Some(child_doc.deep_clone()));
                            }
                        }
                    }
                }
                JoinDirection::Primary { .. } => {
                    // Looking for child doc where _docID == fk
                    if child_doc.doc_id() == Some(fk) {
                        return Ok(Some(child_doc.deep_clone()));
                    }
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
