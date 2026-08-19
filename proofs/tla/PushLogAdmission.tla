---- MODULE PushLogAdmission ----
\* PushLog admission at the hub's sync manager (#1088 slice 1 / W1), abstracting
\* crates/p2p/src/sync/manager/process/pushlog.rs (process_pushlog + insert_pending_dag)
\* and the reply seams in crates/p2p/src/sync/coordinator/event_handler/pushlog.rs.
\*
\* THE MECHANISM: a pushed head block whose DAG has missing links must be REGISTERED in
\* the hub's bounded pending-DAG map (capacity = SyncConfig::max_pending_dags, Go-era
\* MAX_PENDING_DAGS) before the hub can complete it via Bitswap and merge. The pusher
\* drives its persisted retry ladder off the PushLogReply: a success clears the durable
\* scope marker only after ownership was honestly transferred.
\*
\* THE BUG (fa4a84f7 regression of #592): when the pending map is at capacity the hub
\* drops the registration and returns Ok(()) — which both reply seams turn into
\* PushLogReply::success. The pusher's retry record is destroyed while the hub holds
\* neither a merge nor a registration: silent, permanent divergence (issue #1088 M1).
\*
\* THE INVARIANT (heart of the fix): a success PushLogReply implies the pushed block is
\* either MERGED or REGISTERED AS PENDING on the hub. No code path may reply success
\* after discarding state. On capacity overflow the hub must nack (RATE_LIMITED_MESSAGE),
\* which the sender hands to its persisted scope-marker ladder — so the scope stays
\* dirty, rederives its current head, and eventually merges.
\*
\* One knob:
\*   ReplyMode = "NackOnFull"    - capacity overflow replies RATE_LIMITED_MESSAGE; the
\*                                 pusher keeps its retry record and re-pushes. [GREEN]
\*             = "SuccessOnFull" - historical regression: drop registration, reply
\*                                 success; the pusher clears its marker. [RED]
\*
\* Abstractions: pusher identity is irrelevant to the invariant (any fan-in of pushers
\* contends for the same global capacity), so docs are the unit and "the pusher" is the
\* per-doc ack/retry record. HOW a push comes to have missing links (single-head-block
\* live update, M3 send-timeout truncation) is likewise abstracted: HubComplete models an
\* arrival whose DAG is already complete, HubAdmit/HubOverflow* model one that is not.
\* TTL eviction of an admitted entry is deliberately NOT modeled: it is the residual
\* divergence window of the pending map itself (issue #844 / #1088 W2-W3 follow-ups),
\* orthogonal to the reply decision this slice fixes.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Docs,        \* pushed documents (each = one head push contending for admission)
  Cap,         \* pending-DAG capacity (SyncConfig::max_pending_dags)
  ReplyMode    \* "NackOnFull" | "SuccessOnFull"

ASSUME Docs # {}
ASSUME Cap \in Nat /\ Cap >= 1
ASSUME ReplyMode \in {"NackOnFull", "SuccessOnFull"}

\* Pusher-side per-doc record:
\*   unsent       - not yet pushed
\*   inflight     - push sent, awaiting PushLogReply
\*   ackedSuccess - success reply consumed; retry record DELETED (terminal on the pusher)
\*   retryQueued  - error reply consumed; retry record persisted, will re-push
PusherStates == {"unsent", "inflight", "ackedSuccess", "retryQueued"}

VARIABLES
  pending,   \* SUBSET Docs - hub pending-DAG registrations awaiting missing links
  merged,    \* SUBSET Docs - merged on the hub
  pusher     \* [Docs -> PusherStates]

vars == <<pending, merged, pusher>>

TypeOK ==
  /\ pending \subseteq Docs
  /\ merged \subseteq Docs
  /\ pending \cap merged = {}
  /\ Cardinality(pending) <= Cap
  /\ pusher \in [Docs -> PusherStates]

Init ==
  /\ pending = {}
  /\ merged = {}
  /\ pusher = [d \in Docs |-> "unsent"]

\* Pusher sends (or re-sends from its persisted retry ladder) the doc's head push.
Send(d) ==
  /\ pusher[d] \in {"unsent", "retryQueued"}
  /\ pusher' = [pusher EXCEPT ![d] = "inflight"]
  /\ UNCHANGED <<pending, merged>>

\* The pushed DAG is complete on arrival (all links present or already merged):
\* the hub merges and replies success. Success here is honest - state is kept.
HubComplete(d) ==
  /\ pusher[d] = "inflight"
  /\ merged' = merged \cup {d}
  /\ pending' = pending \ {d}
  /\ pusher' = [pusher EXCEPT ![d] = "ackedSuccess"]
  /\ UNCHANGED <<>>

\* Missing links and a free slot (or an existing registration for the same root -
\* insert_pending_dag replaces in place at capacity): register pending, reply success.
\* Success here is honest - the registration guarantees Bitswap completion is tracked.
HubAdmit(d) ==
  /\ pusher[d] = "inflight"
  /\ d \notin merged
  /\ (Cardinality(pending) < Cap \/ d \in pending)
  /\ pending' = pending \cup {d}
  /\ pusher' = [pusher EXCEPT ![d] = "ackedSuccess"]
  /\ UNCHANGED merged

\* Capacity overflow, current-main behavior (process/pushlog.rs WARN + Ok(()) ->
\* PushLogReply::success): the registration is DROPPED but the pusher is told success,
\* so it deletes its retry record. The laundering step. [RED path]
HubOverflowSuccess(d) ==
  /\ ReplyMode = "SuccessOnFull"
  /\ pusher[d] = "inflight"
  /\ d \notin merged
  /\ d \notin pending
  /\ Cardinality(pending) >= Cap
  /\ pusher' = [pusher EXCEPT ![d] = "ackedSuccess"]
  /\ UNCHANGED <<pending, merged>>

\* Capacity overflow, fixed behavior: reply RATE_LIMITED_MESSAGE. The pusher's
\* backoff consumer keeps the doc queued for retry. [GREEN path]
HubOverflowNack(d) ==
  /\ ReplyMode = "NackOnFull"
  /\ pusher[d] = "inflight"
  /\ d \notin merged
  /\ d \notin pending
  /\ Cardinality(pending) >= Cap
  /\ pusher' = [pusher EXCEPT ![d] = "retryQueued"]
  /\ UNCHANGED <<pending, merged>>

\* Bitswap completes a registered DAG (retry_pending_dag -> DagReady -> merge).
Resolve(d) ==
  /\ d \in pending
  /\ pending' = pending \ {d}
  /\ merged' = merged \cup {d}
  /\ UNCHANGED pusher

Next ==
  \E d \in Docs :
    \/ Send(d)
    \/ HubComplete(d)
    \/ HubAdmit(d)
    \/ HubOverflowSuccess(d)
    \/ HubOverflowNack(d)
    \/ Resolve(d)

\* All docs merged: stutter so TLC does not flag deadlock on a finished schedule.
\* (Under SuccessOnFull a schedule can also WEDGE with merged # Docs - every pusher
\* record acked, nothing pending, nothing re-pushable. That dead end is the bug's
\* operational signature, but the invariant below catches it earlier and crisper.)
Done == merged = Docs
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* Weak fairness per doc: retries keep getting sent, registered DAGs keep resolving,
\* and an admissible inflight push is eventually admitted or completed.
FairSpec ==
  Spec /\ \A d \in Docs :
    /\ WF_vars(Send(d))
    /\ WF_vars(Resolve(d))
    /\ WF_vars(HubAdmit(d))
    /\ WF_vars(HubComplete(d))

INV_TypeOK == TypeOK

\* THE #1088 M1 INVARIANT: a success reply implies the hub kept state for the doc -
\* it is merged or registered as pending. SuccessOnFull violates this at the
\* overflow step; NackOnFull preserves it in every reachable state.
INV_SuccessImpliesRegisteredOrMerged ==
  \A d \in Docs : pusher[d] = "ackedSuccess" => d \in pending \cup merged

\* Liveness (GREEN, under FairSpec): with overflow nacked and retries fair, every
\* pushed doc eventually merges - the loop has a fixed point.
EventuallyAllMerged == <>(merged = Docs)
====
