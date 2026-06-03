# SsiRange — SSI range/scan carve-out soundness (`is_document_collection_scan_prefix`)

## What this slice proves

**Property (GREEN invariant).** The document-collection scan-prefix carve-out plus
range-read conflict handling in `ConflictTracker` eliminate only **false-positive** range
conflicts — they never drop a real **range write-skew**. Concretely: with the *correct*
carve-out (suppress only `d/d/`,`/d/`,`d/del/`,`/del/` document scans, while fully tracking
`d/i/` FK-index range reads), every schedule the tracker *accepts* is still
conflict-serializable: the MVSG over the committed txns, computed from each txn's **true**
read footprint, is acyclic.

**Teeth (RED).** A *too-aggressive* carve-out — one that also suppresses index-range reads
(`d/i/`) — accepts a genuine range write-skew: two txns range-scan the same FK-index range,
each insert a key inside the other's scan, and both commit. The independent MVSG oracle
finds the anti-dependency 2-cycle. A real counterexample (`EXIT 12`,
`INV_Serializable violated`).

This is the SI-vs-SSI distinction localized to *range* (predicate) reads. The carve-out is a
performance heuristic: full-collection document scans return false positives against unrelated
inserts (a new doc lands in the same `/d/<coll>/` prefix but is irrelevant to the scanner's
decision), so suppressing them avoids spurious aborts. Soundness = the suppression must never
overlap a real anti-dependency.

## The independent oracle (cannot fake green)

`INV_Serializable == ~HasCycle` over the MVSG, with every edge built from `TrueReads(t) ==
PointReads[t] \cup RangeKeys[t]` — the ground-truth set of keys the txn actually observed,
**including range keys the carve-out hid from the gate**. The carve-out (the mechanism under
test) only feeds `RecordedReads(t)` (what `check_and_record` stores/consults); it never touches
the oracle. So if the mechanism accepts a non-serializable schedule, the oracle still sees the
real reads and reports the cycle. This mirrors the committed `Ssi` slice's MVSG oracle exactly;
the only change is `Reads[x]` → `TrueReads(x)` so a carved-away range still contributes its
anti-dependency edges.

## Source anchors (the real code this abstracts)

All paths under `crates/storage/src/`.

| Spec symbol | Real code | Anchor |
|---|---|---|
| `RecordedReads(t)` range branch | `ReadSet::record_iter_options` — push range UNLESS carved | `backends/shared.rs:195-207` |
| `IsCarved(kind)` (the carve-out) | `is_document_collection_scan_prefix(prefix)` | `backends/shared.rs:214-223` |
| carved prefixes `DocScan` | `"d/d/"`, `"/d/"`, `"d/del/"`, `"/del/"` | `backends/shared.rs:219-222` |
| `RangeKind = "IndexRange"` (NOT carved) | FK index range reads under `d/i/` (the doc comment's "FK index range reads") | `backends/shared.rs:216-217`; index key prefix `keys/datastore/index_key.rs:80-87` |
| `PointReads[t]` | `ReadSet::record_key` (get/has/get_size) | `backends/shared.rs:191-193`; `backends/memory/transaction.rs:90,103,116` |
| `RangeKeys[t]` membership = `ReadRange::contains` | `ReadRange::contains` (prefix / start..end) | `backends/shared.rs:225-236` |
| recorded range consulted at commit (rw_B) | `read_set.conflicts_key(committed_write)` | `backends/shared.rs:301-303`; `conflicts_key` `shared.rs:209-211` |
| recorded range hit by my write (rw_A) | `committed_reads.conflicts_key(write_key)` | `backends/shared.rs:295` |
| ww conflict | `committed_writes.contains(write_key)` | `backends/shared.rs:294` |
| snapshot filter `rec.ver > snap[t]` | `if *commit_ver > read_version` | `backends/shared.rs:292` |
| `record_iter_options` call site | `MemoryTxn::iterator` / `RedbTxn::iterator` | `backends/memory/transaction.rs:125`; `backends/redb/transaction.rs:203` |
| existing carve-out unit tests (oracle of intent) | `ignores_document_collection_scan_prefixes`, `detects_write_to_committed_read_prefix` | `backends/shared.rs:352-367`, `331-349` |

The two storage unit tests are the *intent* the model formalizes: `d/i/books/` range reads DO
conflict with a `d/i/books/...` insert (`shared.rs:331-349` — the GREEN_Correct shape), while
`d/d/books/` document scans do NOT conflict with a `d/d/books/...` insert
(`shared.rs:352-367` — the GREEN_DocScanFalsePositive shape).

## Parity / divergence with Go

Go DefraDB has **no equivalent carve-out**. It runs on badger, whose SSI conflict detection
(`detectConflicts`) tracks read *fingerprints* over the byte keyspace with no document-scan
exception. The `is_document_collection_scan_prefix` heuristic is a **Rust-only addition** to the
hand-rolled `ConflictTracker` for the non-badger backends (memory/redb/fjall/rocksdb), added
because the Rust full-collection scan recorded a broader prefix range than badger's
fingerprinting did and produced spurious aborts in the ACP relation tests
(`shared.rs:216-218`). So:

- **Parity claim:** the carve-out must close the *false-positive gap* (match badger's
  accept decision on unrelated-insert schedules) **without** opening a *false-negative gap*
  (never accept a schedule badger would abort as non-serializable). This slice proves the
  second half: no accepted schedule is non-serializable under true reads, for the correct
  carve domain.
- **Divergence risk this model guards:** broadening the carve-out (e.g. dropping the prefix
  check and carving *all* ranges, or accidentally matching `d/i/` index keys) silently degrades
  Rust from serializable to SI-with-predicate-skew, diverging from badger. That is the RED
  config.

## Model (state machine)

`Txns` fixed finite; each `t` has constants:
- `PointReads[t]` ⊆ `Keys` — point get/has reads (always tracked).
- `RangeKeys[t]` ⊆ `Keys` — the **true** keys its single range read covers (ground truth).
- `RangeKind[t]` ∈ {`None`,`DocScan`,`IndexRange`} — the prefix class of that range read.
- `Writes[t]` ⊆ `Keys` — keys written.

Lifecycle `idle → active → committed | aborted`, identical to `Ssi`:
- `Begin(t)`: `snap[t] := version`.
- `Commit(t)`: accept iff read-only OR `~Conflicts(t)`; on accept assign `cver`, append a
  record `[w, rr]` where `rr = RecordedReads(t)`, bump `version`.

`RecordedReads(t) = PointReads[t] ∪ (IsCarved(RangeKind[t]) ? {} : RangeKeys[t])`. The
mechanism — `Conflicts(t)` (ww / rw_A / rw_B) — runs entirely over `RecordedReads`. `IsCarved`
is the `CarveMode` parameter:

| CarveMode | carves | meaning |
|---|---|---|
| `"Correct"` | `DocScan` only | the real code |
| `"TooAggressive"` | `DocScan` + `IndexRange` | the bug |
| `"NoCarve"` | nothing | maximally conservative baseline |

## Invariant

`INV_Serializable` (headline, the oracle). `INV_MonotoneCommit` + `INV_TypeOK` (sanity).
`DroppedRangeRWConflict` is an auxiliary *diagnostic* operator (not asserted as an invariant in
the cfgs) that names the unsound event explicitly: a committed `a` whose carved range key was
overwritten by a later-committing `b` that `a`'s snapshot didn't see.

## Red / green scenarios (all five differ ONLY in CarveMode and/or write-target keyspace)

| Config | CarveMode | RangeKind | Writes | Expect | Why |
|---|---|---|---|---|---|
| `MC_SsiRange_Green_Correct` | `Correct` | `IndexRange` | inside scan range | **green** | index range tracked ⇒ rw_B aborts 2nd ⇒ serializable |
| `MC_SsiRange_Red_TooAggressive` | `TooAggressive` | `IndexRange` | inside scan range | **red** | index range carved ⇒ both commit ⇒ MVSG cycle |
| `MC_SsiRange_Green_DocScanFalsePositive` | `Correct` | `DocScan` | **disjoint** keyspace | **green** | carved conflict is a true false positive (no anti-dep) |
| `MC_SsiRange_Green_NoCarveBaseline` | `NoCarve` | `IndexRange` | inside scan range | **green** | proves oracle isn't trivially cyclic; cycle is carve-caused |
| `MC_SsiRange_Probe_DocScanSkew` | `Correct` | `DocScan` | inside scan range | **red** | see below |

The headline pair is **Green_Correct (red counterpart Red_TooAggressive)** — same txn shape,
only `CarveMode` differs, so the carve-out is the *sole* variable proven to flip the verdict.

### The DocScan probe — load-bearing honesty

`MC_SsiRange_Probe_DocScanSkew` is RED: when two txns full-scan a document range AND each
writes a key **inside that same `/d/<coll>/` range**, carving the DocScan suppresses a real
anti-dependency and the oracle finds the cycle. So the carve-out is **NOT unconditionally
sound** — its soundness rests on a domain precondition the worst-case probe deliberately
violates:

> A txn's writes never land inside the `/d/<coll>/` document-data range it scanned *in a way
> the engine tracks only via that carved range.*

This holds in the real code for the reason the carve-out exists:
1. **Predicate reads that drive a write-skew are FK-index range reads (`d/i/`)**, a *different
   keyspace* from document-data scans (`d/d/`,`/d/`). `index_prefix` builds `/<coll>/<idx>/…`
   under the index store; document scans build `/<coll>/<instance>/…` under the datastore
   (`keys/datastore/index_key.rs:80` vs `data_store_key.rs:38-51`). Index ranges are
   `RangeKind="IndexRange"` → **never carved** → the genuine skew is the GREEN_Correct case,
   caught.
2. **Same-document concurrent writes** to the doc keyspace are tracked by the **point-key**
   read/write sets (`ReadSet::keys`, `record_key`, `shared.rs:191`), not the carved range — a
   writer that read doc D's value then writes D registers a point key, and ww/rw on that point
   key still fires. The carve-out only drops the *broad prefix range*, not the specific keys.
3. **Unrelated inserts** into the scanned collection (a new doc) are exactly the false positive
   the carve-out targets — the GREEN_DocScanFalsePositive case, where writes go to a disjoint
   key and there is no anti-dependency to drop.

So the probe's RED is a faithful statement of the carve-out's **boundary**, not a bug in the
shipped code: it shows the heuristic is sound only because document scans and the writes that
could skew them live in disjoint keyspaces (FK index / point keys), which the real key encoding
guarantees. If a future change made a `/d/` document scan the *sole* recorded footprint of a
predicate the same txn then writes into (no point-key, same prefix), the carve-out would become
unsound — and this probe is the regression guard for that.

## Run commands (integrator wires into run-all.sh; not edited here)

```
cd proofs/tla
./tools/tlc -metadir states/srange_green  -config MC_SsiRange_Green_Correct.cfg              MC_SsiRange_Green_Correct.tla
./tools/tlc -metadir states/srange_red    -config MC_SsiRange_Red_TooAggressive.cfg          MC_SsiRange_Red_TooAggressive.tla
./tools/tlc -metadir states/srange_fp     -config MC_SsiRange_Green_DocScanFalsePositive.cfg MC_SsiRange_Green_DocScanFalsePositive.tla
./tools/tlc -metadir states/srange_nc     -config MC_SsiRange_Green_NoCarveBaseline.cfg      MC_SsiRange_Green_NoCarveBaseline.tla
./tools/tlc -metadir states/srange_probe  -config MC_SsiRange_Probe_DocScanSkew.cfg          MC_SsiRange_Probe_DocScanSkew.tla
```

Suggested `run-all.sh` rows (integrator adds; this slice does not edit run-all.sh):
```
"MC_SsiRange_Green_Correct.cfg              MC_SsiRange_Green_Correct.tla              GREEN"
"MC_SsiRange_Red_TooAggressive.cfg          MC_SsiRange_Red_TooAggressive.tla          RED"
"MC_SsiRange_Green_DocScanFalsePositive.cfg MC_SsiRange_Green_DocScanFalsePositive.tla GREEN"
"MC_SsiRange_Green_NoCarveBaseline.cfg      MC_SsiRange_Green_NoCarveBaseline.tla      GREEN"
"MC_SsiRange_Probe_DocScanSkew.cfg          MC_SsiRange_Probe_DocScanSkew.tla          RED"
```

## Observed verdicts (this run, 2026-06-03, TLC 2.19)

| Config | Exit | Result |
|---|---|---|
| Green_Correct | 0 | No error (INV_Serializable holds) |
| Red_TooAggressive | 12 | INV_Serializable violated — 2-txn range write-skew trace (`rr={}` for both) |
| Green_DocScanFalsePositive | 0 | No error |
| Green_NoCarveBaseline | 0 | No error |
| Probe_DocScanSkew | 12 | INV_Serializable violated — documents carve-out boundary (see above) |

## Boundaries / what is abstracted away (honesty)

- **One range read per txn.** Each txn has a single `(RangeKeys, RangeKind)` range plus point
  reads. Real code accumulates a `Vec<ReadRange>` (`shared.rs:178`); multiple ranges are the
  union of single-range cases for conflict purposes — no new control flow, but not separately
  exercised at this bound.
- **Range membership is given, not byte-encoded.** `RangeKeys[t]` is the *set of keys the
  range covers* (the result of `ReadRange::contains`, `shared.rs:225-236`), abstracting the
  order-preserving byte encoding. Encoding monotonicity is a separate Lean target
  (survey item: "Order-preserving encoding monotonicity"); a wrong encoding could make
  `contains` mis-classify membership, which this slice assumes correct.
- **DocScan vs IndexRange classification is given.** The model takes `RangeKind` as a constant;
  in code it is decided by `is_document_collection_scan_prefix`'s prefix match. The model tests
  *what the carve-out does given a classification*; that the prefix check classifies real keys
  correctly (a `d/i/` key never starts with `d/d/`) is a property of the key encoding, assumed.
- **Atomic commit critical section.** `Commit` is one atomic action, faithful to
  `committed.lock()` held across check-then-append (`shared.rs:288`).
- **Pruning** (`committed.len() > 1000`, `shared.rs:318`) not modeled — never triggers at this
  bound; only drops records too old to conflict.
- **Inherited from `Ssi`:** `snap == read_version` (the TOCTOU window between
  `current_version()` and the snapshot read is separately surveyed; wider snapshot ⇒ more
  aborts, never fewer ⇒ conservative for *this* property).
```
