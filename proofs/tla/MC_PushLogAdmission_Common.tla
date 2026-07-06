---- MODULE MC_PushLogAdmission_Common ----
EXTENDS PushLogAdmission
\* Shared model values for the PushLogAdmission GREEN/RED configs. Three docs against a
\* single pending slot so admission overflow is reachable with concurrent inflight pushes
\* (fan-in: any second push while one registration is outstanding overflows), while the
\* state space stays tiny. Pusher identity is abstracted away - see PushLogAdmission.tla.
mcDocs == {1, 2, 3}
mcCap  == 1
====
