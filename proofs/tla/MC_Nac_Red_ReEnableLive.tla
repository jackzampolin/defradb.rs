---- MODULE MC_Nac_Red_ReEnableLive ----
EXTENDS Nac
\* RED: re_enable authorized via the LIVE is_admin check (BugReEnableLive)
\* instead of the persisted check (lib.rs:248). While DisabledTemporarily the
\* live check is true for EVERYONE, so a non-admin can re-enable NAC. Once
\* Enabled, the live check collapses to the persisted one, so the non-admin who
\* re-enabled is NOT actually an admin and the admin set stays gated -- BUT the
\* re-enable itself was an unauthorized lifecycle transition. Expected:
\*   INV_ReEnableNeedsPersistedAdmin   (non-admin re-enabled)
\* The headline INV_NoNonAdminMutatesAdminSet still holds here (the writes
\* remain gated by the persisted check post-re-enable), which is exactly why a
\* dedicated re-enable-authorization invariant is required to catch this bug.

mcOwner == "owner"
mcAdmins0 == {"a1"}
mcNonAdmins == {"adv"}
mcMaxMutations == 2
====
