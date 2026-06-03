---- MODULE MC_Nac_Red_WriteWhileDisabled ----
EXTENDS Nac
\* RED: the write-block-while-disabled guard is removed (BugWriteNotBlocked).
\* While DisabledTemporarily, is_admin is live-permissive (true for everyone),
\* so a non-admin can mutate the admin set. Expected violations:
\*   INV_NoWriteWhileDisabled        (a write fires while disabled)
\*   INV_NoNonAdminMutatesAdminSet   (the writer is a non-admin)

mcOwner == "owner"
mcAdmins0 == {"a1"}
mcNonAdmins == {"adv"}
mcMaxMutations == 2
====
