//! CursorNode — wraps a child plan with cursor pagination semantics.
//!
//! Sits at the top of a cursor query's plan tree, above the existing scan/
//! filter/order stack. Owns per-row cursor logic: skip-until-after,
//! collect, probe-for-hasNext, encode startCursor/endCursor.

use async_trait::async_trait;
use cursor::Cursor;
use query_types::doc::Doc;
use query_types::document::DocumentMapping;
use query_types::error::Result;
use query_types::mapper::{CursorPageInfoFields, OrderCondition};
use std::collections::BTreeMap;

use crate::planner::{ExecInfo, PlanNode};

/// Direction of cursor pagination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    /// Actively collecting rows for the page.
    Collecting,
    /// Scanning rows until we pass the `after` boundary (no index seek).
    SkippingUntilAfter,
    /// All rows have been emitted; subsequent calls return false.
    Drained,
}

/// Plan node that implements cursor pagination above any child plan.
pub struct CursorNode {
    inner: Box<dyn PlanNode>,
    direction: CursorDirection,
    page_size: u64,
    after: Option<Cursor>,
    #[allow(dead_code)] // Task 9 (backward) will use before
    before: Option<Cursor>,
    page_info_fields: CursorPageInfoFields,
    order_fields: Vec<OrderCondition>,

    state: CursorState,
    current_doc: Doc,
    first_doc: Option<Doc>,
    last_doc: Option<Doc>,
    has_next: bool,
    has_prev: bool,
    index_seek_active: bool,
    emitted: u64,
    start_cursor: Option<String>,
    end_cursor: Option<String>,
    exec_info: ExecInfo,
}

impl CursorNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inner: Box<dyn PlanNode>,
        direction: CursorDirection,
        page_size: u64,
        after: Option<Cursor>,
        before: Option<Cursor>,
        page_info_fields: CursorPageInfoFields,
        order_fields: Vec<OrderCondition>,
        index_seek_active: bool,
    ) -> Self {
        // Forward: if the index already positions us past `after`, or there is
        // no `after` token, start collecting immediately.
        // Backward: Task 9 will populate the buffer on the first next() call.
        let initial_state = match direction {
            CursorDirection::Forward => {
                if index_seek_active || after.is_none() {
                    CursorState::Collecting
                } else {
                    CursorState::SkippingUntilAfter
                }
            }
            CursorDirection::Backward => CursorState::Collecting,
        };
        Self {
            inner,
            direction,
            page_size,
            after,
            before,
            page_info_fields,
            order_fields,
            state: initial_state,
            current_doc: Doc::default(),
            first_doc: None,
            last_doc: None,
            has_next: false,
            has_prev: false,
            index_seek_active,
            emitted: 0,
            start_cursor: None,
            end_cursor: None,
            exec_info: ExecInfo::default(),
        }
    }

    /// Return page-info computed after the last `next()` call returned false.
    pub fn page_info(&self) -> CursorPageInfo {
        CursorPageInfo {
            has_next: self.has_next,
            has_prev: self.has_prev,
            start_cursor: self.start_cursor.clone(),
            end_cursor: self.end_cursor.clone(),
            fields: self.page_info_fields,
        }
    }

    /// Build a `Cursor` from the current doc using the configured order fields.
    ///
    /// Order field values are looked up by name through the document mapping.
    fn build_cursor_from_doc(&self, doc: &Doc) -> Cursor {
        let doc_id = doc.doc_id().unwrap_or("").to_string();
        let mapping = self.inner.document_map();
        let mut keys = BTreeMap::new();
        for cond in &self.order_fields {
            if let Some(field_name) = cond.fields.first() {
                if let Some(idx) = mapping.first_index_of_name(field_name) {
                    if let Some(value) = doc.get(idx) {
                        keys.insert(field_name.to_string(), value.clone());
                    }
                }
            }
        }
        Cursor { doc_id, keys }
    }

    fn finalize_page_info(&mut self) {
        if self.page_info_fields.start_cursor {
            if let Some(doc) = self.first_doc.take() {
                self.start_cursor = Some(self.build_cursor_from_doc(&doc).encode());
                self.first_doc = Some(doc);
            }
        }
        if self.page_info_fields.end_cursor {
            if let Some(doc) = self.last_doc.take() {
                self.end_cursor = Some(self.build_cursor_from_doc(&doc).encode());
                self.last_doc = Some(doc);
            }
        }
    }

    async fn next_forward(&mut self) -> Result<bool> {
        loop {
            match self.state {
                CursorState::Collecting => {
                    if self.emitted >= self.page_size {
                        // Probe one extra row to determine hasNextPage.
                        self.has_next = self.inner.next().await?;
                        self.has_prev = self.after.is_some();
                        self.state = CursorState::Drained;
                        self.finalize_page_info();
                        return Ok(false);
                    }
                    if self.inner.next().await? {
                        let doc = self.inner.value().deep_clone();
                        if self.first_doc.is_none() {
                            self.first_doc = Some(doc.deep_clone());
                        }
                        self.last_doc = Some(doc.deep_clone());
                        self.current_doc = doc;
                        self.emitted += 1;
                        return Ok(true);
                    } else {
                        self.has_next = false;
                        self.has_prev = self.after.is_some();
                        self.state = CursorState::Drained;
                        self.finalize_page_info();
                        return Ok(false);
                    }
                }
                CursorState::SkippingUntilAfter => {
                    // Slow path: no index seek. Pull rows from the child until
                    // we find the first row whose docID is strictly greater
                    // than the `after` cursor's docID.
                    let after_doc_id = self.after.as_ref().map(|c| c.doc_id.clone());
                    if self.inner.next().await? {
                        let doc = self.inner.value().deep_clone();
                        let row_id = doc.doc_id().map(|s| s.to_string());
                        match (after_doc_id.as_deref(), row_id.as_deref()) {
                            (Some(after), Some(row)) if row > after => {
                                // Found the first row past the boundary.
                                self.state = CursorState::Collecting;
                                self.first_doc = Some(doc.deep_clone());
                                self.last_doc = Some(doc.deep_clone());
                                self.current_doc = doc;
                                self.emitted += 1;
                                return Ok(true);
                            }
                            _ => continue, // still skipping
                        }
                    } else {
                        // Exhausted before finding the boundary.
                        self.state = CursorState::Drained;
                        self.finalize_page_info();
                        return Ok(false);
                    }
                }
                CursorState::Drained => return Ok(false),
            }
        }
    }

    async fn next_backward(&mut self) -> Result<bool> {
        // Task 9 will implement backward cursor pagination.
        Err(query_types::error::QueryError::execution(
            "backward cursor pagination not yet implemented",
        ))
    }
}

/// Page-info emitted once the node is drained.
pub struct CursorPageInfo {
    pub has_next: bool,
    pub has_prev: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub fields: CursorPageInfoFields,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for CursorNode {
    async fn init(&mut self) -> Result<()> {
        self.emitted = 0;
        self.first_doc = None;
        self.last_doc = None;
        self.has_next = false;
        self.has_prev = false;
        self.start_cursor = None;
        self.end_cursor = None;
        self.state = match self.direction {
            CursorDirection::Forward => {
                if self.index_seek_active || self.after.is_none() {
                    CursorState::Collecting
                } else {
                    CursorState::SkippingUntilAfter
                }
            }
            CursorDirection::Backward => CursorState::Collecting,
        };
        self.exec_info = ExecInfo::default();
        self.inner.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.inner.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;
        match self.direction {
            CursorDirection::Forward => self.next_forward().await,
            CursorDirection::Backward => self.next_backward().await,
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.inner.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        self.inner.document_map()
    }

    fn kind(&self) -> &'static str {
        "cursorNode"
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "pageSize".to_string(),
            serde_json::Value::Number(self.page_size.into()),
        );
        obj.insert(
            "direction".to_string(),
            serde_json::Value::String(
                match self.direction {
                    CursorDirection::Forward => "forward",
                    CursorDirection::Backward => "backward",
                }
                .to_string(),
            ),
        );
        let child_explain = self.inner.explain();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }
        serde_json::Value::Object(obj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use query_types::doc::Doc;
    use query_types::document::DocumentMapping;
    use query_types::error::Result;
    use query_types::mapper::CursorPageInfoFields;
    use std::collections::VecDeque;

    // --- FakePlan: a PlanNode that yields a preset sequence of Docs ---

    struct FakePlan {
        rows: VecDeque<Doc>,
        current: Doc,
        mapping: DocumentMapping,
    }

    impl FakePlan {
        fn new(rows: Vec<Doc>) -> Self {
            Self {
                rows: rows.into(),
                current: Doc::default(),
                mapping: DocumentMapping::new(),
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl PlanNode for FakePlan {
        async fn init(&mut self) -> Result<()> {
            Ok(())
        }
        async fn start(&mut self) -> Result<()> {
            Ok(())
        }
        async fn next(&mut self) -> Result<bool> {
            match self.rows.pop_front() {
                Some(doc) => {
                    self.current = doc;
                    Ok(true)
                }
                None => Ok(false),
            }
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
            "fakePlan"
        }
    }

    /// Build a Doc with `_docID` set to `id`.
    fn doc_with_id(id: &str) -> Doc {
        let mut doc = Doc::new(1);
        doc.set_doc_id(id);
        doc
    }

    /// Extract the doc_id from the CursorNode's current value.
    fn current_id(node: &CursorNode) -> &str {
        node.value().doc_id().unwrap_or("")
    }

    #[tokio::test]
    async fn forward_first_only_emits_n_rows() {
        let inner = FakePlan::new(vec![
            doc_with_id("a"),
            doc_with_id("b"),
            doc_with_id("c"),
            doc_with_id("d"),
        ]);
        let mut node = CursorNode::new(
            Box::new(inner),
            CursorDirection::Forward,
            2,
            None, // no after token
            None,
            CursorPageInfoFields {
                has_next: true,
                ..Default::default()
            },
            vec![],
            false,
        );

        assert!(node.next().await.unwrap(), "first row");
        assert_eq!(current_id(&node), "a");

        assert!(node.next().await.unwrap(), "second row");
        assert_eq!(current_id(&node), "b");

        // Third call should drain and return false.
        assert!(!node.next().await.unwrap(), "should be done after page_size");

        let info = node.page_info();
        assert!(info.has_next, "has_next should be true: c and d remain");
    }

    #[tokio::test]
    async fn forward_first_after_skips_to_boundary() {
        let inner = FakePlan::new(vec![
            doc_with_id("a"),
            doc_with_id("b"),
            doc_with_id("c"),
            doc_with_id("d"),
        ]);
        let after = Cursor::from_doc_id("b");
        let mut node = CursorNode::new(
            Box::new(inner),
            CursorDirection::Forward,
            2,
            Some(after),
            None,
            CursorPageInfoFields {
                has_next: true,
                has_prev: true,
                ..Default::default()
            },
            vec![],
            false, // no index seek — slow path
        );

        // Should skip "a" and "b", then emit "c" and "d".
        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "c");

        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "d");

        assert!(!node.next().await.unwrap());

        let info = node.page_info();
        assert!(info.has_prev, "has_prev because after token was set");
        assert!(!info.has_next, "has_next false: no rows remain after d");
    }
}
