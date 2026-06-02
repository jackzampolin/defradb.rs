---- MODULE MC_Auth_Red_Stale ----
EXTENDS Auth
\* RED: a token verifies while valid, then the credential expires or is revoked.
\* A cached authorization model still authorizes, which is stale replay.

mcRequests == {"r_stale"}
mcActors == {"admin"}
mcPermissions == {"P2pReplicatorAdd"}
mcEntries == {"HttpP2PReplicators"}

mcEntryOf == [r \in mcRequests |-> "HttpP2PReplicators"]
mcPresentedActor == [r \in mcRequests |-> "admin"]
mcRequiredPermission == [r \in mcRequests |-> "P2pReplicatorAdd"]
mcInitialCredential == [r \in mcRequests |-> "valid"]
mcInitialGrants == {<<"admin", "P2pReplicatorAdd">>}
mcMutableGrantPairs == {}
mcGateByEntry == [e \in mcEntries |-> "ActorGate"]
mcEntryCanMutate == [e \in mcEntries |-> TRUE]
====
