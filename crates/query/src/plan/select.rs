//! SelectNode for selecting fields from documents

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::Filter;
use crate::planner::{Doc, ExecInfo, PlanNode};

/// SelectNode selects specific fields from documents.
///
/// This node wraps another plan node and applies field selection,
/// optional filtering, and prepares documents for rendering.
pub struct SelectNode {
    /// Source plan node
    source: Box<dyn PlanNode>,
    /// Document mapping for this select
    document_mapping: DocumentMapping,
    /// Optional additional filter
    filter: Option<Filter>,
    /// Optional document IDs for filtering (used in explain output)
    doc_ids: Option<Vec<String>>,
    /// Current document
    current_doc: Doc,
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
    /// Count of documents that matched the filter
    filter_matches: u64,
}

impl SelectNode {
    /// Create a new select node wrapping a source
    pub fn new(source: Box<dyn PlanNode>, document_mapping: DocumentMapping) -> Self {
        Self {
            source,
            document_mapping,
            filter: None,
            doc_ids: None,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
            filter_matches: 0,
        }
    }

    /// Set an additional filter
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set document IDs for explain output
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        if !doc_ids.is_empty() {
            self.doc_ids = Some(doc_ids);
        }
        self
    }

    /// Extract the join type key (typeJoinOne or typeJoinMany) and its content
    /// from a typeIndexJoin explain object.
    fn get_join_type_content(
        join_obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<(&str, &serde_json::Map<String, serde_json::Value>)> {
        // The structure is: { typeJoinOne|typeJoinMany: { root: ..., subType: ... } }
        for key in &["typeJoinOne", "typeJoinMany"] {
            if let Some(content) = join_obj.get(*key).and_then(|v| v.as_object()) {
                return Some((*key, content));
            }
        }
        // Flat structure: { joinType: ..., root: ..., subType: ... }
        if join_obj.contains_key("root") {
            return None; // Signal caller to use join_obj directly
        }
        None
    }

    /// Get the root value from a typeIndexJoin explain object, navigating
    /// through the join type wrapper (typeJoinOne/typeJoinMany) if present.
    fn get_join_root(
        join_obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<&serde_json::Value> {
        if let Some((_, content)) = Self::get_join_type_content(join_obj) {
            content.get("root")
        } else {
            join_obj.get("root")
        }
    }

    /// Set the root value in a typeIndexJoin explain object, navigating
    /// through the join type wrapper (typeJoinOne/typeJoinMany) if present.
    fn set_join_root(
        join_obj: &mut serde_json::Map<String, serde_json::Value>,
        new_root: serde_json::Value,
    ) {
        for key in &["typeJoinOne", "typeJoinMany"] {
            if let Some(content) = join_obj.get_mut(*key).and_then(|v| v.as_object_mut()) {
                content.insert("root".to_string(), new_root);
                return;
            }
        }
        // Flat structure fallback
        join_obj.insert("root".to_string(), new_root);
    }

    /// Detect chained typeIndexJoin nodes and flatten into a parallelNode.
    ///
    /// In Rust, multiple joins are chained: outer.root = inner join.
    /// In Go, multiple joins are siblings in a parallelNode array.
    ///
    /// `wrap_multi_scan`: if true, wrap shared root in multiScanNode (Debug mode).
    /// If false, use shared root directly (Default/Simple mode).
    fn flatten_join_chain(
        explain: &serde_json::Value,
        wrap_multi_scan: bool,
    ) -> Option<serde_json::Value> {
        let obj = explain.as_object()?;
        let join_content = obj.get("typeIndexJoin")?.as_object()?;
        let root = Self::get_join_root(join_content)?;

        // Check if root contains another typeIndexJoin (indicating a chain)
        let root_obj = root.as_object()?;
        if root_obj.get("typeIndexJoin").is_none() {
            return None; // Single join, no parallelNode needed
        }

        // Walk the chain collecting all joins and finding the innermost root
        let mut joins_data: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        let mut current = join_content.clone();

        loop {
            if let Some(current_root) =
                Self::get_join_root(&current).and_then(|r| r.as_object())
            {
                if let Some(inner_join) = current_root.get("typeIndexJoin") {
                    joins_data.push(current.clone());
                    current = inner_join.as_object()?.clone();
                    continue;
                }
            }
            // This is the innermost join - its root is the actual scanNode
            joins_data.push(current);
            break;
        }

        // The innermost join's root is the shared scanNode
        let innermost = joins_data.last()?;
        let shared_root = Self::get_join_root(innermost)?.clone();

        let new_root = if wrap_multi_scan {
            serde_json::json!({ "multiScanNode": shared_root })
        } else {
            shared_root
        };

        // Build the parallel array in reverse order (innermost first = Go convention)
        let mut parallel_items: Vec<serde_json::Value> = Vec::new();
        for join_data in joins_data.iter().rev() {
            let mut join_copy = join_data.clone();
            Self::set_join_root(&mut join_copy, new_root.clone());
            parallel_items.push(serde_json::json!({
                "typeIndexJoin": serde_json::Value::Object(join_copy)
            }));
        }

        Some(serde_json::json!({
            "parallelNode": parallel_items
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for SelectNode {
    async fn init(&mut self) -> Result<()> {
        // Reset execution stats
        self.exec_info = ExecInfo::default();
        self.filter_matches = 0;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        // Track iteration (Go counts each call to next, including final false)
        self.exec_info.iterations += 1;

        loop {
            if !self.source.next().await? {
                return Ok(false);
            }

            let doc = self.source.value();

            // Apply filter if present
            if let Some(ref filter) = self.filter {
                if !filter.matches(doc.fields(), &self.document_mapping)? {
                    continue;
                }
            }

            // Track filter match
            self.filter_matches += 1;

            // Copy the document (field projection happens at render time)
            self.current_doc = doc.deep_clone();
            return Ok(true);
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "selectNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Go DefraDB format: docID is array of IDs if filtering, null otherwise
        obj.insert(
            "docID".to_string(),
            match &self.doc_ids {
                Some(ids) => serde_json::json!(ids),
                None => serde_json::Value::Null,
            },
        );

        // Go DefraDB format: always include filter (null if none)
        // Strip _docID conditions - Go handles doc_ids separately and doesn't show them as filters
        if let Some(ref filter) = self.filter {
            let conditions = filter.conditions();
            let stripped = super::strip_docid_from_conditions(conditions);
            obj.insert("filter".to_string(), stripped);
        } else {
            obj.insert("filter".to_string(), serde_json::Value::Null);
        }

        // Recursively explain child node - merge their wrapped structure.
        // Go uses parallelNode when there are multiple joins. Detect chained
        // typeIndexJoin nodes and flatten them into a parallelNode.
        // Default mode: no multiScanNode wrapper
        let child_explain = self.source.explain();
        let flattened = Self::flatten_join_chain(&child_explain, false);
        let explain_to_merge = flattened.as_ref().unwrap_or(&child_explain);
        if let Some(child_obj) = explain_to_merge.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }

    fn explain_debug_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Debug mode: wrap shared root in multiScanNode
        let child_explain = self.source.explain_debug();
        let flattened = Self::flatten_join_chain(&child_explain, true);
        let explain_to_merge = flattened.as_ref().unwrap_or(&child_explain);
        if let Some(child_obj) = explain_to_merge.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations as u64),
        );
        obj.insert(
            "filterMatches".to_string(),
            serde_json::json!(self.filter_matches as u64),
        );

        // Recursively explain child node with execution info.
        // Execute mode: no multiScanNode wrapper
        let child_explain = self.source.explain_execute();
        let flattened = Self::flatten_join_chain(&child_explain, false);
        let explain_to_merge = flattened.as_ref().unwrap_or(&child_explain);
        if let Some(child_obj) = explain_to_merge.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
