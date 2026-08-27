//! OrphanNode scans for documents without a matching relation (orphans)

use std::collections::HashSet;
use std::sync::Arc;

use async_lock::RwLock;
use async_trait::async_trait;
use document::NormalValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::fetcher::DocFetcher;
use crate::planner::{Doc, ExecInfo, PlanNode};
use crate::planner::{IndexScanParams, IndexScanType};

/// Shared set of parent docIDs yielded by the main join.
/// TypeJoinOne writes to this during iteration; OrphanNode reads it to skip non-orphans.
pub type SharedYieldedIds = Arc<RwLock<HashSet<String>>>;

enum Inner {
    /// Parent stores FK: wraps a scan with FK IS NULL filter, just delegates.
    PrimarySide { scan: Box<dyn PlanNode> },
    /// Parent doesn't store FK: wraps a parent scan, skips docs already yielded by the join.
    SecondarySide {
        parent_scan: Box<dyn PlanNode>,
        yielded_ids: SharedYieldedIds,
        fetcher: Option<Arc<dyn DocFetcher>>,
        child_collection_name: String,
        child_fk_index_name: String,
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
        yielded_ids: SharedYieldedIds,
        fetcher: Option<Arc<dyn DocFetcher>>,
        child_collection_name: String,
        child_fk_index_name: String,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            inner: Inner::SecondarySide {
                parent_scan,
                yielded_ids,
                fetcher,
                child_collection_name,
                child_fk_index_name,
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

    fn scan_metrics_from_explain(value: &serde_json::Value) -> Option<ExecInfo> {
        let obj = value.as_object()?;

        obj.values()
            .find_map(Self::scan_metrics_from_explain)
            .or_else(|| {
                if obj.contains_key("docFetches")
                    && obj.contains_key("fieldFetches")
                    && obj.contains_key("indexFetches")
                {
                    Some(ExecInfo {
                        iterations: obj.get("iterations")?.as_u64()?,
                        docs_fetched: obj.get("docFetches")?.as_u64()?,
                        fields_fetched: obj.get("fieldFetches")?.as_u64()?,
                        indexes_fetched: obj.get("indexFetches")?.as_u64()?,
                    })
                } else {
                    None
                }
            })
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
                fetcher,
                child_collection_name,
                child_fk_index_name,
            } => loop {
                if !parent_scan.next().await? {
                    return Ok(false);
                }
                let doc = parent_scan.value();
                self.exec_info.docs_fetched += 1;
                self.exec_info.fields_fetched += 1;
                if let Some(doc_id) = doc.doc_id() {
                    let ids = yielded_ids.read().await;
                    if ids.contains(doc_id) {
                        continue;
                    }

                    if let Some(fetcher) = fetcher {
                        let scan_result = fetcher
                            .get_by_index_scan(
                                child_collection_name,
                                &IndexScanParams {
                                    index_name: child_fk_index_name.clone(),
                                    scan_type: IndexScanType::ExactMatch {
                                        values: vec![NormalValue::String(doc_id.to_string())],
                                    },
                                    limit: Some(1),
                                    offset: 0,
                                    value_filter: None,
                                    cursor_seek: None,
                                },
                            )
                            .await?;
                        self.exec_info.indexes_fetched += 1;
                        if !scan_result.doc_ids().is_empty() {
                            continue;
                        }
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
        // Go's orphanNode/orphanPointLookupNode reports flat metrics:
        // {iterations, docFetches, fieldFetches, indexFetches}
        // NOT nested in a child scanNode.
        let is_secondary_side = matches!(&self.inner, Inner::SecondarySide { .. });
        let child_info = match &self.inner {
            Inner::SecondarySide { .. } => self.exec_info.clone(),
            Inner::PrimarySide { .. } => {
                Self::scan_metrics_from_explain(&self.source_node().explain_execute())
                    .unwrap_or_else(|| self.source_node().exec_info())
            }
        };
        let matched_docs = child_info.iterations.saturating_sub(1);
        let mut obj = serde_json::Map::new();
        obj.insert(
            "iterations".to_string(),
            serde_json::json!(if is_secondary_side {
                child_info.docs_fetched + 1
            } else {
                child_info.iterations
            }),
        );
        obj.insert(
            "docFetches".to_string(),
            serde_json::json!(if is_secondary_side {
                child_info.docs_fetched
            } else {
                matched_docs
            }),
        );
        obj.insert(
            "fieldFetches".to_string(),
            serde_json::json!(if is_secondary_side {
                child_info.fields_fetched
            } else {
                matched_docs
            }),
        );
        obj.insert(
            "indexFetches".to_string(),
            serde_json::json!(if is_secondary_side {
                child_info.indexes_fetched
            } else {
                matched_docs
            }),
        );
        serde_json::Value::Object(obj)
    }
}
