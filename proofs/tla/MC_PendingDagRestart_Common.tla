---- MODULE MC_PendingDagRestart_Common ----
EXTENDS PendingDagRestart
\* Shared model values for the PendingDagRestart GREEN/RED configs. Three docs against
\* a single pending slot, as in MC_PushLogAdmission_Common: overflow nacks stay
\* reachable, and one admitted-then-crashed registration is enough to witness the
\* durability question while the state space stays tiny.
mcDocs == {1, 2, 3}
mcCap  == 1
====
