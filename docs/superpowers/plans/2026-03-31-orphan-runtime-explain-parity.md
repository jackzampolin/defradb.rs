# Orphan Runtime & Explain Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move orphan handling inside TypeJoinOne so explain output matches Go's `typeIndexJoin > sequenceNode > [orphanNode, typeJoinOne]` structure.

**Architecture:** Add `OrphanConfig` to `TypeJoinOne` containing an `OrphanNode` and sort direction. When set, `TypeJoinOne` manages two-phase iteration (orphan phase + join phase or vice versa) and reports the sequenceNode/orphanNode structure in explain output. The external SequenceNode wrapping in the planner is reverted.

**Tech Stack:** Rust, async_trait, existing PlanNode trait

---

### Task 1: Add OrphanConfig to TypeJoinOne and implement two-phase iteration

**Files:**
- Modify: `crates/query/src/plan/type_join/type_join_one.rs`

This is the core change. TypeJoinOne gets internal orphan handling that produces Go-compatible explain output.

- [ ] **Step 1: Add OrphanConfig struct and field**

Add to `type_join_one.rs` after the imports:

```rust
use crate::plan::OrphanNode;
use crate::mapper::OrderDirection;

/// Configuration for orphan handling inside TypeJoinOne.
/// When present, the join manages two phases:
/// - ASC: orphan phase first, then join phase
/// - DESC: join phase first, then orphan phase
pub struct OrphanConfig {
    pub orphan_node: OrphanNode,
    pub direction: OrderDirection,
}
```

Add field to `TypeJoinOne` struct:
```rust
    orphan_config: Option<OrphanConfig>,
```

Initialize as `None` in `TypeJoinOne::new()`.

Add builder method:
```rust
pub fn with_orphan_config(mut self, orphan_node: OrphanNode, direction: OrderDirection) -> Self {
    self.orphan_config = Some(OrphanConfig { orphan_node, direction });
    self
}
```

- [ ] **Step 2: Implement two-phase next() logic**

Add a new field to track phase:
```rust
    orphan_phase_active: bool,
    orphan_phase_done: bool,
```

Modify the `next()` dispatch for `InvertedIndex` and `OrderedInvertedPrimary` modes. After the existing child-driven iteration returns `Ok(false)` (exhausted), check orphan config:

For ASC (orphans first): At the START of iteration, before the child-driven scan, yield from orphan_node. Once orphan_node exhausted, fall through to normal join.

For DESC (orphans last): After child-driven scan exhausted, yield from orphan_node.

The key change in `next_inverted_index()` and `next_ordered_primary()`:

```rust
// At the end, where it currently returns Ok(false):
if let Some(ref mut config) = self.orphan_config {
    if config.direction == OrderDirection::Desc && !self.orphan_phase_done {
        // DESC: orphans come after join results
        self.orphan_phase_active = true;
    }
}
if self.orphan_phase_active && !self.orphan_phase_done {
    return self.next_orphan_phase().await;
}
Ok(false)
```

For ASC, the orphan phase needs to run BEFORE the child-driven scan. This requires checking at the start of `next()`:
```rust
if let Some(ref config) = self.orphan_config {
    if config.direction == OrderDirection::Asc && !self.orphan_phase_done {
        if !self.orphan_phase_active {
            self.orphan_phase_active = true;
            // Init and start the orphan node
            self.orphan_config.as_mut().unwrap().orphan_node.init().await?;
            self.orphan_config.as_mut().unwrap().orphan_node.start().await?;
        }
        if self.orphan_config.as_mut().unwrap().orphan_node.next().await? {
            // Merge orphan doc: parent doc with null child
            let orphan_doc = self.orphan_config.as_ref().unwrap().orphan_node.value().deep_clone();
            self.current_doc = orphan_doc;
            // Set null for the relation field
            self.current_doc.set(self.parent_side.relation_field_index(), JsonValue::Null);
            return Ok(true);
        }
        self.orphan_phase_done = true;
        // Fall through to normal join iteration
    }
}
```

Add `next_orphan_phase()` method for DESC:
```rust
async fn next_orphan_phase(&mut self) -> Result<bool> {
    let config = self.orphan_config.as_mut().unwrap();
    if !self.orphan_phase_active {
        self.orphan_phase_active = true;
        config.orphan_node.init().await?;
        config.orphan_node.start().await?;
    }
    if config.orphan_node.next().await? {
        let mut orphan_doc = config.orphan_node.value().deep_clone();
        orphan_doc.set(self.parent_side.relation_field_index(), JsonValue::Null);
        self.current_doc = orphan_doc;
        return Ok(true);
    }
    self.orphan_phase_done = true;
    Ok(false)
}
```

- [ ] **Step 3: Update init() and close()**

In `init()`, reset orphan state:
```rust
self.orphan_phase_active = false;
self.orphan_phase_done = false;
```

In `close()`, close orphan node if present:
```rust
if let Some(ref mut config) = self.orphan_config {
    config.orphan_node.close().await?;
}
```

- [ ] **Step 4: Update explain_inner() for Go-compatible output**

When `orphan_config` is set, `explain_inner()` wraps the normal typeJoinOne explain in a `sequenceNode` array:

```rust
fn explain_inner(&self) -> JsonValue {
    let join_explain = self.explain_join_inner(); // existing explain logic, renamed

    if let Some(ref config) = self.orphan_config {
        let orphan_explain = config.orphan_node.explain();
        let orphan_entry = serde_json::json!({ "orphanNode": orphan_explain });
        let join_entry = serde_json::json!({ "typeJoinOne": join_explain });

        let sequence = if config.direction == OrderDirection::Asc {
            serde_json::json!([orphan_entry, join_entry])
        } else {
            serde_json::json!([join_entry, orphan_entry])
        };

        serde_json::json!({ "sequenceNode": sequence })
    } else {
        serde_json::json!({ "typeJoinOne": join_explain })
    }
}
```

Rename the existing `explain_inner()` body to `explain_join_inner()` (private method) so the orphan wrapper can call it.

- [ ] **Step 5: Verify compilation**

Run: `cargo build -p query`
Expected: Compiles

- [ ] **Step 6: Run unit tests**

Run: `cargo test -p query`
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add crates/query/src/plan/type_join/type_join_one.rs
git commit -m "feat(query): add OrphanConfig to TypeJoinOne for internal orphan handling"
```

---

### Task 2: Revert external SequenceNode wrapping in planner

**Files:**
- Modify: `crates/query/src/planner/joins/mod.rs`

Replace the external `SequenceNode(OrphanNode, TypeJoinOne)` wrapping with `TypeJoinOne.with_orphan_config()`.

- [ ] **Step 1: Replace OrderedInvertedPrimary wrapping (Case 1)**

Find the block at ~line 1280 that creates external SequenceNode for OrderedInvertedPrimary. Replace with:

```rust
if select.exhaustive {
    let orphan_scan = ScanNode::new(orphan_col, orphan_mapping)
        .with_fetcher(orphan_fetcher);
    let orphan = OrphanNode::secondary_side(
        Box::new(orphan_scan),
        std::collections::HashSet::new(),
        mapping.clone(),
    );
    let direction = parent_order_for_child
        .as_ref()
        .and_then(|o| o.conditions.first())
        .map(|c| c.direction)
        .unwrap_or(OrderDirection::Asc);
    let join = join.with_orphan_config(orphan, direction);
    plan = Box::new(join);
} else {
    plan = Box::new(join);
}
```

- [ ] **Step 2: Replace InvertedIndex wrapping (Case 2)**

Find the block at ~line 1349 that creates external SequenceNode for InvertedIndex. Replace with:

```rust
if select.exhaustive {
    let null_filter = Filter::from_conditions(
        serde_json::Map::from_iter([(
            parent_fk_field_name.clone(),
            serde_json::json!({"_eq": null}),
        )]),
    );
    let orphan_scan = ScanNode::new(orphan_col, orphan_mapping)
        .with_filter(null_filter)
        .with_fetcher(orphan_fetcher);
    let orphan = OrphanNode::primary_side(
        Box::new(orphan_scan),
        mapping.clone(),
    );
    let direction = parent_order_for_child
        .as_ref()
        .and_then(|o| o.conditions.first())
        .map(|c| c.direction)
        .unwrap_or(OrderDirection::Asc);
    let join = join.with_orphan_config(orphan, direction);
    plan = Box::new(join);
} else {
    plan = Box::new(join);
}
```

- [ ] **Step 3: Remove unused SequenceNode import if no longer needed**

Check if `SequenceNode` is used elsewhere in the file. If not, remove from the import.

- [ ] **Step 4: Verify compilation and tests**

Run: `cargo build -p query && cargo test -p query && cargo clippy --all -- -D warnings`
Expected: All pass, clippy clean

- [ ] **Step 5: Commit**

```bash
git add crates/query/src/planner/joins/mod.rs
git commit -m "refactor(query): move orphan handling inside TypeJoinOne via OrphanConfig"
```

---

### Task 3: Build FFI and validate against tests

**Files:**
- No code changes — validation only

- [ ] **Step 1: Build release FFI**

```bash
PROTOC=$(which protoc) cargo build --release -p ffi
cp target/release/libffi.dylib /Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/tests/clients/rustffi/libdefra_ffi.dylib
```

- [ ] **Step 2: Run the key orphan test to verify data correctness**

```bash
cd /Users/johnzampolin/go/src/github.com/sourcenetwork/defradb && \
CGO_CFLAGS="-I/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/ffi" \
CGO_LDFLAGS="-L/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/target/release" \
CGO_ENABLED=1 DEFRA_CLIENT_RUST_FFI=true DEFRA_BADGER_FILE=true \
go test -count=1 -tags=rust_ffi \
-run "TestQueryWithOrderByRelationField_ExhaustiveWithParentPrimaryASC_ShouldIncludeOrphans" \
-v -timeout 60s ./tests/integration/index/ 2>&1 | tail -15
```

Expected: PASS (data correct AND explain asserter passes since sequenceNode is now inside typeIndexJoin)

- [ ] **Step 3: Run full index FFI tests**

```bash
ffi-test run index --skip-build
```

Expected: Improvement from 27 failures — target under 15

- [ ] **Step 4: Run full explain FFI tests**

```bash
ffi-test run explain --skip-build
```

Expected: explain/default stays at 99%, no regression

- [ ] **Step 5: Push**

```bash
git push origin feat/ffi-update
```
