//! TypeJoinMany - one-to-many relation joins

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tracing::warn;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::planner::{Doc, PlanNode};

use super::JoinSide;

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
    /// The FK field index on the child side (validated at construction).
    /// Stored directly to avoid runtime option unwrapping.
    child_fk_index: usize,
    /// Whether initialized
    initialized: bool,
}

impl std::fmt::Debug for TypeJoinMany {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeJoinMany")
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
            .field("child_fk_index", &self.child_fk_index)
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
        // Validate and extract child FK field index - required for one-to-many joins
        let child_fk_index = child_side.relation_id_field_index().ok_or_else(|| {
            QueryError::internal(format!(
                "TypeJoinMany requires child side to have FK field. \
                 Child collection '{}' relation field '{}' has no FK field.",
                child_side.collection().name,
                child_side.relation_field().name
            ))
        })?;

        Ok(Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            child_fk_index,
            initialized: false,
        })
    }

    /// Find all child documents that match the parent's _docID.
    async fn find_child_docs(&mut self, parent_doc_id: &str) -> Result<Vec<Doc>> {
        let mut children = Vec::new();

        // Re-initialize the child plan for this lookup
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value();
            let child_fk_value = child_doc.get(self.child_fk_index);

            // Log type mismatch for non-null, non-string FK values
            if let Some(v) = child_fk_value {
                if !v.is_null() && !v.is_string() {
                    warn!(
                        child_collection = %self.child_side.collection().name,
                        relation_field = %self.child_side.relation_field().name,
                        fk_index = self.child_fk_index,
                        actual_type = ?v,
                        "Child FK field has unexpected type, expected string or null"
                    );
                }
            }

            // Check if child's FK matches parent's _docID
            if let Some(child_fk) = child_fk_value.and_then(|v| v.as_str()) {
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
            .iter()
            .map(|doc| child_mapping.render_doc_to_json(doc))
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
