---- MODULE EncryptedLwwReplay ----
\* Encrypted LWW materialization across partition, key delay, and restart (#1049).
\*
\* Encryption does not change the LWW algebra proved by
\* DefraConvergence.PriorityReconcile.lwwCM. It adds a temporal obligation:
\* an acknowledged ciphertext whose DEK is not available yet must remain
\* re-drivable across transient KMS failure and restart, and delivery filters
\* must retain the composite link to the encrypted field block that triggers
\* the key request.
\*
\* Source anchors:
\* - crates/db-merge/src/merge_handler/composite_fields.rs propagates transient
\*   KMS failures so the merge remains pending;
\* - crates/db-merge/src/push_docs{,_transport}.rs replay composite heads whose
\*   DAG completion fetches linked encrypted LWW blocks;
\* - crates/p2p/src/sync/pending_store.rs persists acknowledged pending DAGs;
\* - crates/crdt/src/lww.rs selects the greatest (priority, value) version.
EXTENDS Naturals

CONSTANTS
  LocalVersion,
  RemoteVersion,
  PendingMode,  \* "Durable" | "Volatile"
  FilterMode,   \* "PreserveEncrypted" | "DropEncrypted"
  MergeMode,    \* "Lww" | "ArrivalWins"
  KmsFailureMode \* "Retry" | "TerminalSkip"

ASSUME LocalVersion \in Nat
ASSUME RemoteVersion \in Nat
ASSUME LocalVersion > RemoteVersion
ASSUME PendingMode \in {"Durable", "Volatile"}
ASSUME FilterMode \in {"PreserveEncrypted", "DropEncrypted"}
ASSUME MergeMode \in {"Lww", "ArrivalWins"}
ASSUME KmsFailureMode \in {"Retry", "TerminalSkip"}

VARIABLES
  running,
  connected,
  partitionUsed,
  restartAvailable,
  senderPending,
  ciphertextStored,
  volatilePending,
  durablePending,
  requestPending,
  keyServiceReady,
  keyEnvelope,
  keyAvailable,
  localApplied,
  remoteApplied,
  materialized,
  acknowledged

vars ==
  << running, connected, partitionUsed, restartAvailable, senderPending,
     ciphertextStored, volatilePending, durablePending, requestPending,
     keyServiceReady, keyEnvelope, keyAvailable, localApplied, remoteApplied,
     materialized, acknowledged >>

TypeOK ==
  /\ running \in BOOLEAN
  /\ connected \in BOOLEAN
  /\ partitionUsed \in BOOLEAN
  /\ restartAvailable \in BOOLEAN
  /\ senderPending \in BOOLEAN
  /\ ciphertextStored \in BOOLEAN
  /\ volatilePending \in BOOLEAN
  /\ durablePending \in BOOLEAN
  /\ requestPending \in BOOLEAN
  /\ keyServiceReady \in BOOLEAN
  /\ keyEnvelope \in BOOLEAN
  /\ keyAvailable \in BOOLEAN
  /\ localApplied \in BOOLEAN
  /\ remoteApplied \in BOOLEAN
  /\ materialized \in 0..LocalVersion
  /\ acknowledged \in BOOLEAN

Init ==
  /\ running = TRUE
  /\ connected = FALSE
  /\ partitionUsed = FALSE
  /\ restartAvailable = TRUE
  /\ senderPending = TRUE
  /\ ciphertextStored = FALSE
  /\ volatilePending = FALSE
  /\ durablePending = FALSE
  /\ requestPending = FALSE
  /\ keyServiceReady = FALSE
  /\ keyEnvelope = FALSE
  /\ keyAvailable = FALSE
  /\ localApplied = FALSE
  /\ remoteApplied = FALSE
  /\ materialized = 0
  /\ acknowledged = FALSE

Reconnect ==
  /\ running
  /\ ~connected
  /\ connected' = TRUE
  /\ UNCHANGED <<running, partitionUsed, restartAvailable, senderPending,
                  ciphertextStored, volatilePending, durablePending,
                  requestPending, keyServiceReady, keyEnvelope, keyAvailable,
                  localApplied, remoteApplied, materialized, acknowledged>>

Partition ==
  /\ running
  /\ connected
  /\ ~partitionUsed
  /\ ~remoteApplied
  /\ connected' = FALSE
  /\ partitionUsed' = TRUE
  /\ UNCHANGED <<running, restartAvailable, senderPending, ciphertextStored,
                  volatilePending, durablePending, requestPending, keyEnvelope,
                  keyServiceReady, keyAvailable, localApplied, remoteApplied,
                  materialized, acknowledged>>

ApplyLocal ==
  /\ running
  /\ ~localApplied
  /\ localApplied' = TRUE
  /\ materialized' =
       IF materialized >= LocalVersion THEN materialized ELSE LocalVersion
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  senderPending, ciphertextStored, volatilePending,
                  durablePending, requestPending, keyEnvelope, keyAvailable,
                  keyServiceReady, remoteApplied, acknowledged>>

DeliverRemote ==
  /\ running
  /\ connected
  /\ senderPending
  /\ FilterMode = "PreserveEncrypted"
  /\ senderPending' = FALSE
  /\ ciphertextStored' = TRUE
  /\ volatilePending' = TRUE
  /\ durablePending' = (PendingMode = "Durable")
  /\ acknowledged' = TRUE
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  requestPending, keyEnvelope, keyAvailable, localApplied,
                  keyServiceReady, remoteApplied, materialized>>

DropRemote ==
  /\ running
  /\ connected
  /\ senderPending
  /\ FilterMode = "DropEncrypted"
  /\ senderPending' = FALSE
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  ciphertextStored, volatilePending, durablePending,
                  requestPending, keyServiceReady, keyEnvelope, keyAvailable,
                  localApplied, remoteApplied, materialized, acknowledged>>

RequestKey ==
  /\ running
  /\ volatilePending
  /\ ciphertextStored
  /\ ~keyAvailable
  /\ ~requestPending
  /\ ~keyEnvelope
  /\ requestPending' = TRUE
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  senderPending, ciphertextStored, volatilePending,
                  durablePending, keyEnvelope, keyAvailable, localApplied,
                  keyServiceReady, remoteApplied, materialized, acknowledged>>

KeyServiceBecomesReady ==
  /\ running
  /\ ~keyServiceReady
  /\ keyServiceReady' = TRUE
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  senderPending, ciphertextStored, volatilePending,
                  durablePending, requestPending, keyEnvelope, keyAvailable,
                  localApplied, remoteApplied, materialized, acknowledged>>

KeyUnavailable ==
  /\ running
  /\ requestPending
  /\ ~keyServiceReady
  /\ requestPending' = FALSE
  /\ volatilePending' =
       IF KmsFailureMode = "Retry" THEN volatilePending ELSE FALSE
  /\ durablePending' =
       IF KmsFailureMode = "Retry" THEN durablePending ELSE FALSE
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  senderPending, ciphertextStored, keyServiceReady, keyEnvelope,
                  keyAvailable, localApplied, remoteApplied, materialized,
                  acknowledged>>

ReleaseKey ==
  /\ running
  /\ connected
  /\ requestPending
  /\ keyServiceReady
  /\ requestPending' = FALSE
  /\ keyEnvelope' = TRUE
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  senderPending, ciphertextStored, volatilePending,
                  durablePending, keyServiceReady, keyAvailable, localApplied,
                  remoteApplied, materialized, acknowledged>>

ReceiveKey ==
  /\ running
  /\ connected
  /\ keyEnvelope
  /\ keyEnvelope' = FALSE
  /\ keyAvailable' = TRUE
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  senderPending, ciphertextStored, volatilePending,
                  durablePending, requestPending, keyServiceReady, localApplied,
                  remoteApplied, materialized, acknowledged>>

MaterializeRemote ==
  /\ running
  /\ volatilePending
  /\ ciphertextStored
  /\ keyAvailable
  /\ ~remoteApplied
  /\ remoteApplied' = TRUE
  /\ volatilePending' = FALSE
  /\ durablePending' = FALSE
  /\ materialized' =
       IF MergeMode = "Lww"
       THEN IF materialized >= RemoteVersion THEN materialized ELSE RemoteVersion
       ELSE RemoteVersion
  /\ UNCHANGED <<running, connected, partitionUsed, restartAvailable,
                  senderPending, ciphertextStored, requestPending, keyEnvelope,
                  keyServiceReady, keyAvailable, localApplied, acknowledged>>

Crash ==
  /\ running
  /\ restartAvailable
  /\ acknowledged
  /\ ~remoteApplied
  /\ running' = FALSE
  /\ connected' = FALSE
  /\ restartAvailable' = FALSE
  /\ volatilePending' = FALSE
  /\ requestPending' = FALSE
  /\ keyEnvelope' = FALSE
  /\ UNCHANGED <<partitionUsed, senderPending, ciphertextStored, durablePending,
                  keyServiceReady, keyAvailable, localApplied, remoteApplied,
                  materialized, acknowledged>>

Recover ==
  /\ ~running
  /\ running' = TRUE
  /\ volatilePending' = durablePending
  /\ UNCHANGED <<connected, partitionUsed, restartAvailable, senderPending,
                  ciphertextStored, durablePending, requestPending, keyEnvelope,
                  keyServiceReady, keyAvailable, localApplied, remoteApplied,
                  materialized, acknowledged>>

Next ==
  \/ Reconnect
  \/ Partition
  \/ ApplyLocal
  \/ DeliverRemote
  \/ DropRemote
  \/ RequestKey
  \/ KeyServiceBecomesReady
  \/ KeyUnavailable
  \/ ReleaseKey
  \/ ReceiveKey
  \/ MaterializeRemote
  \/ Crash
  \/ Recover

Fairness ==
  /\ WF_vars(Reconnect)
  /\ WF_vars(ApplyLocal)
  /\ WF_vars(DeliverRemote)
  /\ WF_vars(DropRemote)
  /\ WF_vars(RequestKey)
  /\ WF_vars(KeyServiceBecomesReady)
  /\ WF_vars(KeyUnavailable)
  /\ WF_vars(ReleaseKey)
  /\ WF_vars(ReceiveKey)
  /\ WF_vars(MaterializeRemote)
  /\ WF_vars(Recover)

Spec == Init /\ [][Next]_vars /\ Fairness

INV_TypeOK == TypeOK

\* Once the sender retires an acknowledged push, either the receiver has
\* applied it or a volatile/durable retry obligation still backs the ack.
INV_AckBacked ==
  ~acknowledged \/ remoteApplied \/ volatilePending \/ durablePending

\* A filter may reject an entire document, but it must not retire the encrypted
\* field block of a document selected for delivery.
INV_NoFilteredLoss ==
  senderPending \/ ciphertextStored \/ remoteApplied

\* The remote replay is older than the concurrent local write. Decryption and
\* arrival order must not let it overwrite the existing LWW winner.
INV_LwwWinner ==
  ~(localApplied /\ remoteApplied) \/ materialized = LocalVersion

\* Under eventual reconnect and fair key service, both writes settle on the
\* same LWW winner even if the receiver restarts before or after key delivery.
LIVE_WinnerMaterialized ==
  <>[](localApplied /\ remoteApplied /\ materialized = LocalVersion)
====
