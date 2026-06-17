---- MODULE MC_InteractiveTxnCounter_Common ----
EXTENDS InteractiveTxnCounter
\* Shared model values for the InteractiveTxnCounter GREEN/RED configs. Two docs so the
\* interactive finalize and the batch (create_many) acquirer can CONTEND on a shared doc
\* (forcing sorted acquisition to matter), plus a single-doc merge on d1 so a local
\* interactive RMW and a same-doc merge can race the per-doc critical section.

\* Docs are naturals (1, 2) so the sorted-acquire total order is plain `<`.
mcDocs     == {1, 2}
mcITouched == {1, 2}   \* interactive txn touches both -> contends with batch on both
mcBTouched == {1, 2}   \* create_many touches both -> sorted-acquire deadlock test
mcMergeDoc == 1        \* merge contends on doc 1 with the interactive/batch guards
====
