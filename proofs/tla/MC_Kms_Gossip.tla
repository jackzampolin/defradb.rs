---- MODULE MC_Kms_Gossip ----
EXTENDS Kms
\* Basic pubsub scenario: owner initially holds K, Bob is authorized but missing
\* it, and Eve is on the encryption topic but is not authorized.
mcNodes == {"owner", "bob", "eve"}
mcDIDs == {"did:owner", "did:bob", "did:eve"}
mcDidOf ==
  [n \in mcNodes |->
     CASE n = "owner" -> "did:owner"
       [] n = "bob"   -> "did:bob"
       [] OTHER       -> "did:eve"]
mcKeys == {"K"}
mcInitialAuthorized ==
  [k \in mcKeys |-> {"did:owner", "did:bob"}]
mcInitiallyHasKey ==
  [n \in mcNodes |-> IF n = "owner" THEN mcKeys ELSE {}]
mcRequesters == mcNodes
mcGrantable == {}
mcRevocable == {}
====
