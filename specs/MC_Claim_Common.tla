---- MODULE MC_Claim_Common ----
EXTENDS Claim
\* Two instances, i1 and i2, share DID X and contend for one AgentRequest owned by
\* X. fy is a foreign DID-Y instance: unfiltered replication may deliver the doc
\* to it, but it is not a contender because defra-agent's watcher only claims rows
\* where agent_did equals the local agent_did.

mcInstances == {"i1", "i2", "fy"}
mcDIDs == {"X", "Y"}
mcDidOf == [i \in mcInstances |-> IF i = "fy" THEN "Y" ELSE "X"]
mcRequestDID == "X"

\* LWW priority abstraction for claimed_at plus deterministic instance tie-break.
\* i2 wins if both concurrent claim blocks eventually merge.
mcClaimRank ==
  [i \in mcInstances |-> CASE i = "i1" -> 1
                       [] i = "i2" -> 2
                       [] OTHER -> 0]

\* Full collection replication: every instance can receive every claim block.
mcPeersUnfiltered == [i \in mcInstances |-> mcInstances]

\* DID-filtered replication with the correct partition: all DID-X contenders stay
\* in one mutual replication set; DID-Y is outside the request's claim race.
mcPeersFiltered ==
  [i \in mcInstances |-> IF mcDidOf[i] = "X" THEN {"i1", "i2"} ELSE {"fy"}]

\* Dangerous variant: a bad filter/subscription partition splits same-DID
\* instances, so their claim blocks never converge.
mcPeersSplitSameDID == [i \in mcInstances |-> {i}]
====
