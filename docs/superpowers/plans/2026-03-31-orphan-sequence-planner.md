# OrphanNode & SequenceNode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `OrphanNode` and `SequenceNode` plan nodes to fix 47 FFI test failures for relation-ordered queries with orphan document handling.

**Architecture:** Three new plan nodes (`OrphanNode::PrimarySide`, `OrphanNode::SecondarySide`, `SequenceNode`) that implement `PlanNode`. The planner wires them around `TypeJoinOne` when `@exhaustive` + relation ordering is detected. Orphan logic is removed from `TypeJoinOne`.

**Tech Stack:** Rust, async_trait, existing PlanNode trait at `crates/query/src/planner/traits.rs`

---

### Task 1: Create SequenceNode

**Files:**
- Create: `crates/query/src/plan/sequence.rs`
- Modify: `crates/query/src/plan/mod.rs`

- [ ] **Step 1: Create `sequence.rs` with SequenceNode struct**

```rust
// crates/query/src/plan/sequence.rs
//! SequenceNode — chains two plan nodes sequentially.
//!
//! Exhausts the first child completely, then the second.
//! Used to concatenate orphan results with join results in the
//! correct order (ASC = orphans first, DESC = orphans last).

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::traits::{Doc, ExecInfo, PlanNode};

pub struct SequenceNode {
    children: [Box<dyn PlanNode>; 2],
    active_child: usize,
    current_doc: Doc,
    document_mapping: DocumentMapping,
    exec_info: ExecInfo,
}

impl SequenceNode {
    pub fn new(
        first: Box<dyn PlanNode>,
        second: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            children: [first, second],
            active_child: 0,
            current_doc: Doc::default(),
            document_mapping,
            exec_info: ExecInfo::default(),
        }
    }
}

#[async_trait]
impl PlanNode for SequenceNode {
    async fn init(&mut self) -> Result<()> {
        self.active_child = 0;
        self.exec_info = ExecInfo::default();
        self.children[0].init().await?;
        self.children[1].init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.children[0].start().await?;
        self.children[1].start().await
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;

        loop {
            if self.active_child >= 2 {
                return Ok(false);
            }

            if self.children[self.active_child].next().await? {
                self.current_doc = self.children[self.active_child].value().deep_clone();
                return Ok(true);
            }

            // Current child exhausted, move to next
            self.active_child += 1;
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.children[0].close().await?;
        self.children[1].close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.children[0].as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "sequenceNode"
    }

    fn explain_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();
        obj.insert("first".to_string(), self.children[0].explain());
        obj.insert("second".to_string(), self.children[1].explain());
        JsonValue::Object(obj)
    }

    fn explain_execute_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();
        obj.insert("iterations".to_string(), serde_json::json!(self.exec_info.iterations));
        obj.insert("first".to_string(), self.children[0].explain_execute());
        obj.insert("second".to_string(), self.children[1].explain_execute());
        JsonValue::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }
}
```

- [ ] **Step 2: Register module in `plan/mod.rs`**

Add to `crates/query/src/plan/mod.rs`:
```rust
mod sequence;
```

And add the re-export:
```rust
pub use sequence::SequenceNode;
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build -p query`
Expected: Compiles with no errors

- [ ] **Step 4: Commit**

```bash
git add crates/query/src/plan/sequence.rs crates/query/src/plan/mod.rs
git commit -m "feat(query): add SequenceNode plan node"
```

---

### Task 2: Create OrphanNode — PrimarySide variant

**Files:**
- Create: `crates/query/src/plan/orphan.rs`
- Modify: `crates/query/src/plan/mod.rs`

The PrimarySide variant handles the case where the **parent stores the FK**. It wraps a `ScanNode` with a `FK IS NULL` filter to find parents that have no relation.

- [ ] **Step 1: Create `orphan.rs` with PrimarySide**

```rust
// crates/query/src/plan/orphan.rs
//! OrphanNode — scans for documents without a matching relation.
//!
//! Two variants:
//! - PrimarySide: parent stores FK, scan with FK IS NULL filter
//! - SecondarySide: parent doesn't store FK, point-lookup child FK index

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::traits::{Doc, ExecInfo, PlanNode};

/// OrphanNode variant — determines how orphans are detected.
enum OrphanVariant {
    /// Parent stores FK. Inner plan is a ScanNode with FK IS NULL filter.
    PrimarySide {
        scan: Box<dyn PlanNode>,
    },
    /// Parent doesn't store FK. Scan all parents, point-lookup child FK index.
    SecondarySide {
        parent_scan: Box<dyn PlanNode>,
        /// Set of parent docIDs already yielded by the main join.
        /// Parents in this set are NOT orphans.
        yielded_ids: std::collections::HashSet<String>,
        /// Fetcher for checking child FK index existence.
        fetcher: Option<std::sync::Arc<dyn crate::runner::DocFetcher>>,
        /// Child FK index name for point lookups.
        child_fk_index_name: String,
    },
}

pub struct OrphanNode {
    variant: OrphanVariant,
    document_mapping: DocumentMapping,
    current_doc: Doc,
    exec_info: ExecInfo,
}

impl OrphanNode {
    /// Create a PrimarySide orphan node.
    /// `scan` should be a ScanNode configured with FK IS NULL filter.
    pub fn primary_side(
        scan: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            variant: OrphanVariant::PrimarySide { scan },
            document_mapping,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }

    /// Create a SecondarySide orphan node.
    /// `parent_scan` iterates all parents; orphans are those not in `yielded_ids`.
    pub fn secondary_side(
        parent_scan: Box<dyn PlanNode>,
        yielded_ids: std::collections::HashSet<String>,
        fetcher: std::sync::Arc<dyn crate::runner::DocFetcher>,
        child_fk_index_name: String,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            variant: OrphanVariant::SecondarySide {
                parent_scan,
                yielded_ids,
                fetcher: Some(fetcher),
                child_fk_index_name,
            },
            document_mapping,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }
}

#[async_trait]
impl PlanNode for OrphanNode {
    async fn init(&mut self) -> Result<()> {
        self.exec_info = ExecInfo::default();
        match &mut self.variant {
            OrphanVariant::PrimarySide { scan } => scan.init().await,
            OrphanVariant::SecondarySide { parent_scan, .. } => parent_scan.init().await,
        }
    }

    async fn start(&mut self) -> Result<()> {
        match &mut self.variant {
            OrphanVariant::PrimarySide { scan } => scan.start().await,
            OrphanVariant::SecondarySide { parent_scan, .. } => parent_scan.start().await,
        }
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;
        match &mut self.variant {
            OrphanVariant::PrimarySide { scan } => {
                if scan.next().await? {
                    self.current_doc = scan.value().deep_clone();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            OrphanVariant::SecondarySide {
                parent_scan,
                yielded_ids,
                ..
            } => {
                while parent_scan.next().await? {
                    let doc = parent_scan.value();
                    let doc_id = match doc.doc_id() {
                        Some(id) => id.to_string(),
                        None => continue,
                    };
                    // Skip parents already yielded by the main join
                    if yielded_ids.contains(&doc_id) {
                        continue;
                    }
                    self.current_doc = doc.deep_clone();
                    return Ok(true);
                }
                Ok(false)
            }
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        match &mut self.variant {
            OrphanVariant::PrimarySide { scan } => scan.close().await,
            OrphanVariant::SecondarySide { parent_scan, .. } => parent_scan.close().await,
        }
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        match &self.variant {
            OrphanVariant::PrimarySide { scan } => Some(scan.as_ref()),
            OrphanVariant::SecondarySide { parent_scan, .. } => Some(parent_scan.as_ref()),
        }
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "orphanNode"
    }

    fn explain_inner(&self) -> JsonValue {
        match &self.variant {
            OrphanVariant::PrimarySide { scan } => scan.explain(),
            OrphanVariant::SecondarySide { parent_scan, .. } => parent_scan.explain(),
        }
    }

    fn explain_execute_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();
        obj.insert("iterations".to_string(), serde_json::json!(self.exec_info.iterations));
        match &self.variant {
            OrphanVariant::PrimarySide { scan } => {
                let child = scan.explain_execute();
                if let Some(child_obj) = child.as_object() {
                    for (k, v) in child_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
            OrphanVariant::SecondarySide { parent_scan, .. } => {
                let child = parent_scan.explain_execute();
                if let Some(child_obj) = child.as_object() {
                    for (k, v) in child_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        JsonValue::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }
}
```

- [ ] **Step 2: Register in `plan/mod.rs`**

Add to `crates/query/src/plan/mod.rs`:
```rust
mod orphan;
```

And add the re-export:
```rust
pub use orphan::OrphanNode;
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build -p query`
Expected: Compiles (OrphanNode may have unused import warnings — that's fine)

- [ ] **Step 4: Commit**

```bash
git add crates/query/src/plan/orphan.rs crates/query/src/plan/mod.rs
git commit -m "feat(query): add OrphanNode plan node with PrimarySide and SecondarySide variants"
```

---

### Task 3: Remove orphan logic from TypeJoinOne

**Files:**
- Modify: `crates/query/src/plan/type_join/type_join_one.rs`

- [ ] **Step 1: Remove orphan fields from struct**

In `TypeJoinOne` struct definition (~line 45-94), remove:
```rust
    include_orphans: bool,
    yielded_parent_ids: HashSet<String>,
    orphan_phase: bool,
```

- [ ] **Step 2: Remove `with_include_orphans()` builder method**

Remove the method (~line 217-221):
```rust
pub fn with_include_orphans(mut self) -> Self {
    self.include_orphans = true;
    self
}
```

- [ ] **Step 3: Remove `next_orphan()` method**

Remove the entire method (~line 543-568).

- [ ] **Step 4: Update `next_inverted_index()` and `next_ordered_primary()`**

In both methods, remove the orphan fallback at the end:
```rust
// REMOVE these lines:
if self.include_orphans {
    return self.next_orphan().await;
}
```

Also remove `yielded_parent_ids` tracking:
```rust
// REMOVE these lines in next_inverted_index():
if let Some(pid) = parent_doc.doc_id() {
    self.yielded_parent_ids.insert(pid.to_string());
}

// REMOVE these lines in next_ordered_primary():
if let Some(pid) = parent_doc.doc_id() {
    self.yielded_parent_ids.insert(pid.to_string());
}
```

But **ADD** a public method to track yielded IDs for the external OrphanNode:
```rust
/// Get the set of parent docIDs yielded during the join.
/// Used by OrphanNode::SecondarySide to exclude already-yielded parents.
pub fn yielded_parent_ids(&self) -> &HashSet<String> {
    &self.yielded_parent_ids
}
```

Wait — we still need `yielded_parent_ids` for the SecondarySide OrphanNode to know which parents were already yielded. Keep the field and tracking, just remove the orphan phase logic. The field becomes output-only (populated during join, read by OrphanNode after).

Revised: Keep `yielded_parent_ids: HashSet<String>` field and the tracking inserts. Remove `include_orphans`, `orphan_phase`, `with_include_orphans()`, and `next_orphan()`.

- [ ] **Step 5: Update `init()` to remove orphan state reset**

Remove from `init()`:
```rust
self.yielded_parent_ids.clear();
self.orphan_phase = false;
```

Keep `self.yielded_parent_ids.clear();` since it resets for re-init.

- [ ] **Step 6: Verify compilation**

Run: `cargo build -p query`
Expected: Compiles. The planner code that called `with_include_orphans()` will now fail — that's expected and fixed in Task 4.

- [ ] **Step 7: Commit**

```bash
git add crates/query/src/plan/type_join/type_join_one.rs
git commit -m "refactor(query): remove orphan phase from TypeJoinOne"
```

---

### Task 4: Wire SequenceNode and OrphanNode in the planner

**Files:**
- Modify: `crates/query/src/planner/joins/mod.rs`

This is the key task — replace `join.with_include_orphans()` calls with proper `SequenceNode(OrphanNode, TypeJoinOne)` wrapping.

- [ ] **Step 1: Add imports at top of file**

```rust
use crate::plan::sequence::SequenceNode;
use crate::plan::orphan::OrphanNode;
```

- [ ] **Step 2: Replace `with_include_orphans()` for OrderedInvertedPrimary mode**

Find the section (~line 1270-1280) where `select.exhaustive` is checked for `OrderedInvertedPrimary`:

```rust
// BEFORE:
if select.exhaustive {
    join = join.with_include_orphans();
}
plan = Box::new(join);
```

Replace with:
```rust
let join_box: Box<dyn PlanNode> = Box::new(join);
if select.exhaustive {
    // OrderedInvertedPrimary: child has FK, parent is secondary side.
    // Use SecondarySide orphan detection — scan parents, skip yielded.
    // For now, create a parent scan clone for the orphan node.
    let orphan_scan = create_parent_scan_for_orphans(
        &parent_collection,
        &parent_scan_mapping,
        &fetcher,
    );
    let orphan = OrphanNode::secondary_side(
        orphan_scan,
        std::collections::HashSet::new(), // Will be populated at runtime
        fetcher.clone(),
        child_fk_field_name.clone(),
        mapping.clone(),
    );
    let orphan_box: Box<dyn PlanNode> = Box::new(orphan);
    // ASC: join first (sorted), orphans last (NULLs at end)
    // DESC: orphans first, join last
    let is_asc = /* extract from order_by */;
    plan = if is_asc {
        Box::new(SequenceNode::new(join_box, orphan_box, mapping.clone()))
    } else {
        Box::new(SequenceNode::new(orphan_box, join_box, mapping.clone()))
    };
} else {
    plan = join_box;
}
```

Note: The exact wiring depends on how the parent scan can be cloned/recreated. The agent implementing this should read the surrounding context to understand what `parent_collection`, `parent_scan_mapping`, and `fetcher` are, and create a fresh `ScanNode` for the orphan.

- [ ] **Step 3: Replace `with_include_orphans()` for InvertedIndex mode**

Find the section (~line 1310-1316) for `InvertedIndex`:

Apply the same pattern but for PrimarySide (parent stores FK):
```rust
if select.exhaustive {
    // InvertedIndex: parent has FK field. Use PrimarySide orphan detection.
    // Create a ScanNode with FK IS NULL filter.
    let null_filter = create_fk_null_filter(&parent_fk_field_name);
    let orphan_scan = ScanNode::new(parent_collection.clone(), parent_scan_mapping.clone())
        .with_filter(null_filter)
        .with_fetcher(fetcher.clone());
    let orphan = OrphanNode::primary_side(
        Box::new(orphan_scan),
        mapping.clone(),
    );
    let orphan_box: Box<dyn PlanNode> = Box::new(orphan);
    let is_asc = /* extract from order_by */;
    plan = if is_asc {
        Box::new(SequenceNode::new(orphan_box, join_box, mapping.clone()))
    } else {
        Box::new(SequenceNode::new(join_box, orphan_box, mapping.clone()))
    };
} else {
    plan = join_box;
}
```

- [ ] **Step 4: Add helper functions**

Add near the top of the file or in a helpers section:
```rust
/// Create a FK IS NULL filter for orphan detection.
fn create_fk_null_filter(fk_field_name: &str) -> Filter {
    // Build: {fk_field_name: {_eq: null}}
    Filter::from_condition(fk_field_name, FilterOp::Eq, JsonValue::Null)
}

/// Create a fresh parent scan for orphan detection.
fn create_parent_scan_for_orphans(
    collection: &CollectionVersion,
    mapping: &DocumentMapping,
    fetcher: &Arc<dyn DocFetcher>,
) -> Box<dyn PlanNode> {
    Box::new(
        ScanNode::new(collection.clone(), mapping.clone())
            .with_fetcher(fetcher.clone())
    )
}
```

The exact API for creating filters and scans depends on the existing code patterns. The implementing agent should check how `ScanNode::with_filter()` and `Filter` are constructed elsewhere in the planner.

- [ ] **Step 5: Verify compilation**

Run: `cargo build -p query`
Expected: Compiles with no errors

- [ ] **Step 6: Run unit tests**

Run: `cargo test -p query`
Expected: All existing tests pass

- [ ] **Step 7: Commit**

```bash
git add crates/query/src/planner/joins/mod.rs
git commit -m "feat(query): wire OrphanNode and SequenceNode in planner for @exhaustive"
```

---

### Task 5: Build, copy FFI library, and run FFI tests

**Files:**
- No code changes — validation only

- [ ] **Step 1: Run clippy**

Run: `cargo clippy --all -- -D warnings`
Expected: Clean

- [ ] **Step 2: Build release FFI**

Run: `PROTOC=$(which protoc) cargo build --release -p ffi`
Expected: Compiles

- [ ] **Step 3: Copy FFI library to Go worktree**

Run: `cp target/release/libffi.dylib /Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/tests/clients/rustffi/libdefra_ffi.dylib`

- [ ] **Step 4: Run index FFI tests**

Run: `ffi-test run index --skip-build`
Expected: Improvement from 26 failures — target under 10

- [ ] **Step 5: Run explain FFI tests**

Run: `ffi-test run explain --skip-build`
Expected: Improvement from 21 failures — target under 10

- [ ] **Step 6: Run full FFI sweep**

Run all packages to check for regressions.

- [ ] **Step 7: Commit and push**

```bash
git push origin feat/ffi-update
```

---

## Implementation Notes

**Key challenges for the implementing agent:**

1. **Filter construction:** The `create_fk_null_filter` helper needs to build a `Filter` with `{fk_field: {_eq: null}}`. Check how filters are constructed in `crates/query/src/mapper/filter/` — there should be factory methods or a builder.

2. **Sort direction extraction:** The planner needs to know if ordering is ASC or DESC to decide orphan placement in the SequenceNode. Check how `order_by` is accessed in the planner context where joins are wired.

3. **SecondarySide yielded_ids sharing:** The `OrphanNode::SecondarySide` needs the set of parent IDs yielded by `TypeJoinOne`. Since both are separate plan nodes in a SequenceNode, the OrphanNode can't read from TypeJoinOne at runtime. Two options:
   - **Option A:** OrphanNode scans ALL parents and the SequenceNode is smart enough to deduplicate (simpler but O(N))
   - **Option B:** Use a shared `Arc<RwLock<HashSet<String>>>` between TypeJoinOne and OrphanNode (complex but efficient)
   - **Option C:** Have OrphanNode do its own orphan detection via point lookups (matches Go's `orphanPointLookupNode` for secondary side)

   For PrimarySide, this isn't an issue — the FK IS NULL filter handles it directly.

4. **ScanNode cloning:** Creating a fresh ScanNode for orphan detection requires access to the collection, mapping, and fetcher. These are available in the planner context where the join is wired.
