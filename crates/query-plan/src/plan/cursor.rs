//! CursorNode — wraps a child plan with cursor pagination semantics.
//!
//! Sits at the top of a cursor query's plan tree, above the existing scan/
//! filter/order stack. Owns per-row cursor logic: skip-until-after,
//! collect, probe-for-hasNext, encode startCursor/endCursor.

use async_trait::async_trait;
use cursor::Cursor;
use query_types::doc::Doc;
use query_types::document::DocumentMapping;
use query_types::error::{QueryError, Result};
use query_types::mapper::{CursorPageInfoFields, OrderCondition, OrderDirection};
use std::collections::{BTreeMap, VecDeque};

use crate::planner::{ExecInfo, PlanNode};

/// Returns true when the order is either empty or only orders by `_docID`.
///
/// The slow-path comparisons in `SkippingUntilAfter` and `populate_backward_buffer_slow`
/// compare rows against cursor boundaries using only the doc_id string. This is only
/// correct when the ORDER BY is `_docID` (or absent, which also sorts by doc_id).
/// For any other field ordering the comparison must account for the full order tuple,
/// which requires knowing the field values of each row — not just the doc_id.
///
/// When this returns false and the slow path is taken, the caller must return an error
/// rather than silently returning wrong results.
fn is_doc_id_or_empty_order(order_fields: &[OrderCondition]) -> bool {
    order_fields.is_empty()
        || order_fields
            .iter()
            .all(|c| c.fields.first().map(String::as_str) == Some("_docID"))
}

/// Returns the iteration direction implied by the first `_docID` order condition.
///
/// Returns `Asc` when `order_fields` is empty (natural doc_id order is ascending).
/// Caller must have already verified order is `_docID`-only or empty via
/// `is_doc_id_or_empty_order` before calling this.
fn doc_id_order_direction(order_fields: &[OrderCondition]) -> OrderDirection {
    order_fields
        .first()
        .filter(|c| c.fields.first().map(String::as_str) == Some("_docID"))
        .map(|c| c.direction)
        .unwrap_or(OrderDirection::Asc)
}

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

/// Cursor keys extracted from a document at emit time (doc_id + order field values).
///
/// Storing only what's needed for cursor encoding avoids keeping a full Doc clone alive
/// solely for page_info computation.
struct CursorSnapshot {
    doc_id: String,
    keys: BTreeMap<String, serde_json::Value>,
}

/// Plan node that implements cursor pagination above any child plan.
pub struct CursorNode {
    inner: Box<dyn PlanNode>,
    direction: CursorDirection,
    page_size: u64,
    after: Option<Cursor>,
    before: Option<Cursor>,
    page_info_fields: CursorPageInfoFields,
    order_fields: Vec<OrderCondition>,

    state: CursorState,
    /// True once `populate_backward_buffer` has run for the backward path.
    buffer_populated: bool,
    /// Buffer used by the backward path: populated on the first `next()` call.
    buffer: VecDeque<Doc>,
    current_doc: Doc,
    first_snapshot: Option<CursorSnapshot>,
    last_snapshot: Option<CursorSnapshot>,
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
        // Backward: always start in Collecting; `buffer_populated` tracks whether
        // the first-call buffer fill has happened.
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
            buffer_populated: false,
            buffer: VecDeque::new(),
            current_doc: Doc::default(),
            first_snapshot: None,
            last_snapshot: None,
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
    pub fn page_info_data(&self) -> CursorPageInfo {
        CursorPageInfo {
            has_next: self.has_next,
            has_prev: self.has_prev,
            start_cursor: self.start_cursor.clone(),
            end_cursor: self.end_cursor.clone(),
            fields: self.page_info_fields,
        }
    }

    /// Extract the cursor snapshot from a doc using the configured order fields.
    ///
    /// Captures only the doc_id and order-field values needed to encode a cursor,
    /// avoiding a full Doc clone for the page_info path.
    fn snapshot_from_doc(
        doc: &Doc,
        document_map: &DocumentMapping,
        order_fields: &[OrderCondition],
    ) -> CursorSnapshot {
        let doc_id = doc.doc_id().unwrap_or("").to_string();
        let mut keys = BTreeMap::new();
        for cond in order_fields {
            if let Some(field_name) = cond.fields.first() {
                if let Some(idx) = document_map.first_index_of_name(field_name) {
                    if let Some(value) = doc.get(idx) {
                        keys.insert(field_name.to_string(), value.clone());
                    }
                }
            }
        }
        CursorSnapshot { doc_id, keys }
    }

    /// Build a `Cursor` from a previously captured snapshot.
    fn build_cursor_from_snapshot(snapshot: &CursorSnapshot) -> Cursor {
        Cursor {
            doc_id: snapshot.doc_id.clone(),
            keys: snapshot.keys.clone(),
        }
    }

    fn finalize_page_info(&mut self) {
        if self.page_info_fields.start_cursor {
            if let Some(snap) = self.first_snapshot.as_ref() {
                self.start_cursor = Some(Self::build_cursor_from_snapshot(snap).encode());
            }
        }
        if self.page_info_fields.end_cursor {
            if let Some(snap) = self.last_snapshot.as_ref() {
                self.end_cursor = Some(Self::build_cursor_from_snapshot(snap).encode());
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
                        let doc = self.inner.value().deep_clone(); // ONE clone per row
                        let map = self.inner.document_map();
                        if self.first_snapshot.is_none() {
                            self.first_snapshot =
                                Some(Self::snapshot_from_doc(&doc, map, &self.order_fields));
                        }
                        self.last_snapshot =
                            Some(Self::snapshot_from_doc(&doc, map, &self.order_fields));
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
                    //
                    // This comparison is only correct for doc_id or empty ordering.
                    // For other field orderings, the cursor boundary is determined by
                    // the full (field_values, doc_id) tuple, not just doc_id — and
                    // extracting named field values from a Doc requires index-to-name
                    // mapping that is not available here. Return an error so callers
                    // notice the limitation rather than silently returning wrong pages.
                    if !is_doc_id_or_empty_order(&self.order_fields) {
                        return Err(QueryError::execution(
                            "cursor slow path (no index seek) does not support non-docID ordering; \
                             ensure the collection has an index covering the ORDER BY fields",
                        ));
                    }
                    let after_doc_id = self.after.as_ref().map(|c| c.doc_id.clone());
                    let dir = doc_id_order_direction(&self.order_fields);
                    if self.inner.next().await? {
                        let doc = self.inner.value().deep_clone(); // ONE clone per row
                        let row_id = doc.doc_id().map(|s| s.to_string());
                        let past_boundary = match (after_doc_id.as_deref(), row_id.as_deref()) {
                            (Some(after), Some(row)) => match dir {
                                OrderDirection::Asc => row > after,
                                OrderDirection::Desc => row < after,
                            },
                            _ => false,
                        };
                        if past_boundary {
                            // Found the first row past the boundary.
                            self.state = CursorState::Collecting;
                            let map = self.inner.document_map();
                            let snap = Self::snapshot_from_doc(&doc, map, &self.order_fields);
                            // First collected row is both the start and end of the page so far.
                            self.last_snapshot = Some(CursorSnapshot {
                                doc_id: snap.doc_id.clone(),
                                keys: snap.keys.clone(),
                            });
                            self.first_snapshot = Some(snap);
                            self.current_doc = doc;
                            self.emitted += 1;
                            return Ok(true);
                        }
                        // still skipping
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
        // On the very first call, drain the inner plan into self.buffer.
        if !self.buffer_populated {
            self.populate_backward_buffer().await?;
            self.buffer_populated = true;
        }

        match self.state {
            CursorState::Collecting => match self.buffer.pop_front() {
                Some(doc) => {
                    self.current_doc = doc;
                    Ok(true)
                }
                None => {
                    self.state = CursorState::Drained;
                    Ok(false)
                }
            },
            CursorState::Drained => Ok(false),
            CursorState::SkippingUntilAfter => unreachable!("SkippingUntilAfter is forward-only"),
        }
    }

    /// Drain the inner plan into `self.buffer` according to backward semantics,
    /// then compute snapshots and page-info flags.
    async fn populate_backward_buffer(&mut self) -> Result<()> {
        if self.index_seek_active {
            self.populate_backward_buffer_fast().await?;
        } else {
            self.populate_backward_buffer_slow().await?;
        }
        self.populate_snapshots_after_buffer();
        Ok(())
    }

    /// Fast path: the inner plan already iterates in reverse from the `before`
    /// boundary. Collect up to `page_size + 1` docs and reverse them so callers
    /// receive docs in logical (ascending) order.
    async fn populate_backward_buffer_fast(&mut self) -> Result<()> {
        let window_size = self.page_size as usize + 1; // +1 to detect has_prev
        let mut collected: Vec<Doc> = Vec::with_capacity(window_size);
        while self.inner.next().await? {
            let doc = self.inner.value().deep_clone();
            collected.push(doc);
            if collected.len() >= window_size {
                break;
            }
        }
        // Reverse so the buffer holds docs in ascending (logical) order.
        collected.reverse();
        if collected.len() > self.page_size as usize {
            self.has_prev = true;
            collected.remove(0); // drop the extra doc at the front
        }
        for doc in collected {
            self.buffer.push_back(doc);
        }
        Ok(())
    }

    /// Slow path: scan forward; keep a sliding window of the last `page_size + 1`
    /// docs, stopping at (but not including) the `before` boundary doc_id when set.
    ///
    /// The boundary comparison uses only doc_id, which is only correct for doc_id
    /// or empty ordering. When a `before` cursor is present and the order is not
    /// doc_id-only, return an error instead of silently returning wrong results
    /// (see `is_doc_id_or_empty_order`). When there is no `before` cursor, no
    /// boundary comparison happens — the slow path just slides a window — so any
    /// ordering is safe.
    async fn populate_backward_buffer_slow(&mut self) -> Result<()> {
        let before_doc_id = self.before.as_ref().map(|c| c.doc_id.clone());
        if before_doc_id.is_some() && !is_doc_id_or_empty_order(&self.order_fields) {
            return Err(QueryError::execution(
                "cursor slow path (no index seek) does not support non-docID ordering; \
                 ensure the collection has an index covering the ORDER BY fields",
            ));
        }
        let window_size = self.page_size as usize + 1; // +1 to detect has_prev
        let dir = doc_id_order_direction(&self.order_fields);
        while self.inner.next().await? {
            let value = self.inner.value();
            if let Some(boundary) = before_doc_id.as_deref() {
                // Stop at (not past) the boundary: the `before` doc itself is excluded
                // from output, matching Go's semantics.
                //
                // For ASC order the iterator sees rows in ascending doc_id order;
                // stop when we reach or pass the boundary: `>=`.
                // For DESC order the iterator sees rows in descending doc_id order
                // (larger IDs first); stop when we reach or go below the boundary: `<=`.
                let stop = match dir {
                    OrderDirection::Asc => value.doc_id().unwrap_or("") >= boundary,
                    OrderDirection::Desc => value.doc_id().unwrap_or("") <= boundary,
                };
                if stop {
                    break;
                }
            }
            let doc = value.deep_clone();
            self.buffer.push_back(doc);
            if self.buffer.len() > window_size {
                self.buffer.pop_front();
            }
        }
        if self.buffer.len() > self.page_size as usize {
            self.has_prev = true;
            self.buffer.pop_front();
        }
        Ok(())
    }

    /// Capture first/last snapshots from the buffer and set `has_next`.
    /// Called after either fast or slow population.
    fn populate_snapshots_after_buffer(&mut self) {
        let map = self.inner.document_map();
        if let Some(first) = self.buffer.front() {
            self.first_snapshot = Some(Self::snapshot_from_doc(first, map, &self.order_fields));
        }
        if let Some(last) = self.buffer.back() {
            self.last_snapshot = Some(Self::snapshot_from_doc(last, map, &self.order_fields));
        }
        // has_next is always true when a `before` token was provided, because the
        // caller of this page knows there are rows on the next (forward) page.
        self.has_next = self.before.is_some();
        self.finalize_page_info();
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
        self.first_snapshot = None;
        self.last_snapshot = None;
        self.has_next = false;
        self.has_prev = false;
        self.start_cursor = None;
        self.end_cursor = None;
        self.buffer.clear();
        self.buffer_populated = false;
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

    fn page_info(&self) -> Option<crate::plan::CursorPageInfo> {
        Some(self.page_info_data())
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
        assert!(
            !node.next().await.unwrap(),
            "should be done after page_size"
        );

        let info = node.page_info_data();
        assert!(info.has_next, "has_next should be true: c and d remain");
    }

    #[tokio::test]
    async fn backward_last_only_emits_last_n_in_order() {
        // No `before` cursor; iterate forward through all, keep last 2.
        let inner = FakePlan::new(vec![
            doc_with_id("a"),
            doc_with_id("b"),
            doc_with_id("c"),
            doc_with_id("d"),
        ]);
        let mut node = CursorNode::new(
            Box::new(inner),
            CursorDirection::Backward,
            2,
            None,
            None,
            CursorPageInfoFields {
                has_next: true,
                has_prev: true,
                ..Default::default()
            },
            vec![],
            false,
        );
        node.init().await.unwrap();

        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "c");
        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "d");
        assert!(!node.next().await.unwrap());

        let info = node.page_info_data();
        assert!(!info.has_next, "before is None => has_next=false");
        assert!(
            info.has_prev,
            "we dropped rows from the front => has_prev=true"
        );
    }

    #[tokio::test]
    async fn backward_last_before_stops_at_boundary() {
        // `before` = "c"; collect a, b (window of 2); stop before "c".
        let inner = FakePlan::new(vec![
            doc_with_id("a"),
            doc_with_id("b"),
            doc_with_id("c"),
            doc_with_id("d"),
        ]);
        let before = Cursor::from_doc_id("c");
        let mut node = CursorNode::new(
            Box::new(inner),
            CursorDirection::Backward,
            2,
            None,
            Some(before),
            CursorPageInfoFields {
                has_next: true,
                has_prev: true,
                ..Default::default()
            },
            vec![],
            false,
        );
        node.init().await.unwrap();

        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "a");
        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "b");
        assert!(!node.next().await.unwrap());

        let info = node.page_info_data();
        assert!(info.has_next, "before.is_some() => has_next=true");
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

        let info = node.page_info_data();
        assert!(info.has_prev, "has_prev because after token was set");
        assert!(!info.has_next, "has_next false: no rows remain after d");
    }

    #[tokio::test]
    async fn backward_last_index_seek_fast_path_emits_in_order() {
        // index_seek_active = true means the inner plan already iterates in reverse.
        // FakePlan with [d, c, b, a] simulates that (inner = reverse logical order).
        let inner = FakePlan::new(vec![
            doc_with_id("d"),
            doc_with_id("c"),
            doc_with_id("b"),
            doc_with_id("a"),
        ]);
        let mut node = CursorNode::new(
            Box::new(inner),
            CursorDirection::Backward,
            2,
            None,
            None,
            CursorPageInfoFields {
                has_next: true,
                has_prev: true,
                ..Default::default()
            },
            vec![],
            true, // index_seek_active — fast path
        );
        node.init().await.unwrap();
        // Buffer: collected reverse = [d, c, b]; len=3 == window_size → break.
        // Reverse for logical order = [b, c, d]; len=3 > page_size=2 → has_prev=true, drop b.
        // Final buffer = [c, d]. Output: c, d.
        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "c");
        assert!(node.next().await.unwrap());
        assert_eq!(current_id(&node), "d");
        assert!(!node.next().await.unwrap());
        let info = node.page_info_data();
        assert!(info.has_prev, "page_size+1 < total => has_prev=true");
        assert!(!info.has_next, "before is None => has_next=false");
    }
}
