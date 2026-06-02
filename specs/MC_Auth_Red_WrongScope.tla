---- MODULE MC_Auth_Red_WrongScope ----
EXTENDS Auth
\* RED: the actor has a valid token and one node permission, but not the
\* permission required by this mutation. Token-only auth is privilege escalation.

mcRequests == {"r_wrong_scope"}
mcActors == {"adv"}
mcPermissions == {"P2pCollectionAdd", "DacPolicyAdd"}
mcEntries == {"HttpDacPolicy"}

mcEntryOf == [r \in mcRequests |-> "HttpDacPolicy"]
mcPresentedActor == [r \in mcRequests |-> "adv"]
mcRequiredPermission == [r \in mcRequests |-> "DacPolicyAdd"]
mcInitialCredential == [r \in mcRequests |-> "valid"]
mcInitialGrants == {<<"adv", "P2pCollectionAdd">>}
mcMutableGrantPairs == {}
mcGateByEntry == [e \in mcEntries |-> "ActorGate"]
mcEntryCanMutate == [e \in mcEntries |-> TRUE]
====
