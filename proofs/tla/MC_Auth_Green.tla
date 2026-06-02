---- MODULE MC_Auth_Green ----
EXTENDS Auth
\* GREEN: current remote management surface. HTTP mutations use ActorGate.
\* Iroh sync and embedded-direct adapter calls are documented as non-mutating
\* remote-management entry points in this slice.

mcRequests ==
  {"r_good",
   "r_absent",
   "r_wrong_scope"}

mcActors == {"admin", "adv"}

mcPermissions ==
  {"P2pCollectionAdd",
   "P2pReplicatorAdd",
   "DacPolicyAdd",
   "NacRelationAdd"}

mcEntries ==
  {"HttpP2PCollections",
   "HttpP2PReplicators",
   "HttpDacPolicy",
   "HttpNacGrant",
   "IrohSyncStream",
   "EmbeddedDirect"}

mcEntryOf ==
  [r \in mcRequests |->
    CASE r \in {"r_good", "r_absent"} -> "HttpP2PCollections"
      [] r = "r_wrong_scope" -> "HttpDacPolicy"
      [] OTHER -> "HttpP2PCollections"]

mcPresentedActor ==
  [r \in mcRequests |->
    IF r = "r_good" THEN "admin" ELSE "adv"]

mcRequiredPermission ==
  [r \in mcRequests |->
    CASE r \in {"r_good", "r_absent"} -> "P2pCollectionAdd"
      [] r = "r_wrong_scope" -> "DacPolicyAdd"
      [] OTHER -> "P2pCollectionAdd"]

mcInitialCredential ==
  [r \in mcRequests |->
    CASE r = "r_absent" -> "absent"
      [] OTHER -> "valid"]

mcInitialGrants ==
  {<<"admin", "P2pCollectionAdd">>,
   <<"admin", "P2pReplicatorAdd">>,
   <<"admin", "DacPolicyAdd">>,
   <<"admin", "NacRelationAdd">>,
   <<"adv", "P2pCollectionAdd">>}

mcMutableGrantPairs == {}

mcGateByEntry ==
  [e \in mcEntries |->
    CASE e = "IrohSyncStream" -> "PeerGate"
      [] e = "EmbeddedDirect" -> "DidOnlyGate"
      [] OTHER -> "ActorGate"]

mcEntryCanMutate ==
  [e \in mcEntries |->
    e \in {"HttpP2PCollections", "HttpP2PReplicators", "HttpDacPolicy", "HttpNacGrant"}]
====
