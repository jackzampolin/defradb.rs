---- MODULE MC_MergeQueue_Red_FailOpen ----
EXTENDS MergeQueue
\* RED: keep the per-doc mutex but use the Go fail-OPEN exhaustion policy
\* (FailMode="Open"): after MaxRetries txn conflicts, Go's Merge() falls through to
\* `return nil`, so the caller marks the block done though it was never applied.
\* A single doc with one block, plus enough adversarial user-writes to exhaust the retry
\* budget on every attempt, drives the block to Exhaust with marked=TRUE and
\* docState empty -> a marked-done-but-undelivered block: a SILENT DROP.
\* Violates INV_NoSilentDrop (and INV_NoLoss). The mutex is present, so the only thing
\* under test is the exhaustion fork.

mcBlocks  == {"b1"}
mcDocs    == {"d1"}

mcBlockDoc == [b \in mcBlocks |-> "d1"]
mcDup      == [b \in mcBlocks |-> b]
====
