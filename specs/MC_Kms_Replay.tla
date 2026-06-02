---- MODULE MC_Kms_Replay ----
EXTENDS Kms
\* Revoke/replay scenario: Eve starts authorized, may create a request while
\* authorized, then can be revoked before any response envelope is released.
mcNodes == {"owner", "eve"}
mcDIDs == {"did:owner", "did:eve"}
mcDidOf ==
  [n \in mcNodes |-> IF n = "owner" THEN "did:owner" ELSE "did:eve"]
mcKeys == {"K"}
mcInitialAuthorized ==
  [k \in mcKeys |-> {"did:owner", "did:eve"}]
mcInitiallyHasKey ==
  [n \in mcNodes |-> IF n = "owner" THEN mcKeys ELSE {}]
mcRequesters == mcNodes
mcGrantable == {}
mcRevocable == {<<"eve", "K">>}
====
