---- MODULE MC_Nac_Red_NoPersist ----
EXTENDS Nac
\* RED: disable() does not persist the _disabled flag (BugNoPersist). The
\* runtime status is DisabledTemporarily but disk_disabled stays FALSE, so a
\* Restart recovers Enabled -- the temporary-disable is silently forgotten on a
\* node bounce. Expected violation:
\*   INV_DisabledPersistsAcrossRestart  (status=Disabled while disk flag clear)

mcOwner == "owner"
mcAdmins0 == {"a1"}
mcNonAdmins == {"adv"}
mcMaxMutations == 2
====
