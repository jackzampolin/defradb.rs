---- MODULE MC_PushBacklog_Common ----
EXTENDS PushBacklog
\* Shared model values for the PushBacklog GREEN/RED configs. Three peers - one slow
\* (sends never complete) and two healthy, one of them heavy (weight 2) so the byte cap
\* is exercised independently of the item cap. Arrivals run well past the queue cap so
\* SpawnPerItem residency growth is reachable, two workers against PerPeerCap 1 so the
\* slow peer can hold at most half the pool, while the state space stays tiny.
mcPeers == {"slow", "h1", "h2"}
mcSlowPeers == {"slow"}
mcWeight == [p \in mcPeers |-> IF p = "h2" THEN 2 ELSE 1]
mcMaxArrivals == 5
mcQueueCap == 2
mcByteCap == 3
mcWorkers == 2
mcPerPeerCap == 1
====
