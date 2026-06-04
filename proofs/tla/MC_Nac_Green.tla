---- MODULE MC_Nac_Green ----
EXTENDS Nac
\* GREEN: the correct NAC lifecycle mechanism.
\*  - write-block while DisabledTemporarily (operations.rs)
\*  - re_enable authorized by the PERSISTED admin check (lib.rs:248)
\*  - disable PERSISTS the _disabled flag (lifecycle.rs:88-96)
\* All four invariants must hold.

mcOwner == "owner"
mcAdmins0 == {"a1"}
mcNonAdmins == {"adv"}
mcMaxMutations == 2
====
