//! OrphanNode scans for documents without a matching relation (orphans)

use std::collections::HashSet;

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, ExecInfo, PlanNode};

enum Inner {
    /// Parent stores FK: wraps a scan with FK IS NULL filter, just delegates.
    PrimarySide { scan: Box<dyn PlanNode> },
    /// Parent doesn't store FK: wraps a parent scan, skips docs already yielded by the join.
    SecondarySide {
        parent_scan: Box<dyn PlanNode>,
        yielded_ids: HashSet<String>,
    },
}

/// Scans for documents that have no matching relation (orphans).
pub struct OrphanNode {
    inner: Inner,
    document_mapping: DocumentMapping,
    current_doc: Doc,
    exec_info: ExecInfo,
}

impl OrphanNode {
    /// Create an orphan node for the primary side (parent stores FK).
    ///
    /// The scan is expected to have an FK IS NULL filter already applied.
    pub fn primary_side(scan: Box<dyn PlanNode>, document_mapping: DocumentMapping) -> Self {
        Self {
            inner: Inner::PrimarySide { scan },
            document_mapping,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }

    /// Create an orphan node for the secondary side (parent doesn't store FK).
    ///
    /// Iterates the parent scan, skipping any document whose docID appears in `yielded_ids`.
    pub fn secondary_side(
        parent_scan: Box<dyn PlanNode>,
        yielded_ids: HashSet<String>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            inner: Inner::SecondarySide {
                parent_scan,
                yielded_ids,
            },
            document_mapping,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }

    fn source_node(&self) -> &dyn PlanNode {
        match &self.inner {
            Inner::PrimarySide { scan } => scan.as_ref(),
            Inner::SecondarySide { parent_scan, .. } => parent_scan.as_ref(),
        }
    }

    fn source_node_mut(&mut self) -> &mut Box<dyn PlanNode> {
        match &mut self.inner {
            Inner::PrimarySide { scan } => scan,
            Inner::SecondarySide { parent_scan, .. } => parent_scan,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for OrphanNode {
    async fn init(&mut self) -> Result<()> {
        self.exec_info = ExecInfo::default();
        self.source_node_mut().init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source_node_mut().start().await
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;

        match &mut self.inner {
            Inner::PrimarySide { scan } => {
                if !scan.next().await? {
                    return Ok(false);
                }
                self.current_doc = scan.value().deep_clone();
                Ok(true)
            }
            Inner::SecondarySide {
                parent_scan,
                yielded_ids,
            } => loop {
                if !parent_scan.next().await? {
                    return Ok(false);
                }
                let doc = parent_scan.value();
                if let Some(doc_id) = doc.doc_id() {
                    if yielded_ids.contains(doc_id) {
                        continue;
                    }
                }
                self.current_doc = doc.deep_clone();
                return Ok(true);
            },
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.source_node_mut().close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source_node())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "orphanNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        let child_explain = self.source_node().explain();
        if let Some(child_obj) = child_explain.as_object() {
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
            serde_json::json!(self.exec_info.iterations),
        );

        let child_explain = self.source_node().explain_execute();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
