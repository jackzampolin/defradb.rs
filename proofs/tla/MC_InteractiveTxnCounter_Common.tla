---- MODULE MC_InteractiveTxnCounter_Common ----
EXTENDS InteractiveTxnCounter
\* Shared model values for the InteractiveTxnCounter GREEN/RED configs. Two docs so the
\* interactive finalize and the batch (create_many) acquirer can CONTEND on a shared doc set
\* (so that with the gate Off they can grab docs in OPPOSITE orders and deadlock), plus a
\* single-doc merge on d1 so a local interactive RMW and a same-doc merge can race the
\* per-doc critical section.

\* Docs are naturals (1, 2); the interactive finalize uses `<` (MinDoc) as a defensive,
\* incidental order while the batch acquirer uses ARBITRARY order (irreducibly incremental).
mcDocs     == {1, 2}
mcITouched == {1, 2}   \* interactive txn touches both -> contends with batch on both
mcBTouched == {1, 2}   \* batch touches both -> arbitrary-order acquire can invert the order
mcMergeDoc == 1        \* merge contends on doc 1 with the interactive/batch guards
====
