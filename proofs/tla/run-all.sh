#!/usr/bin/env bash
# Run every TLA+ model and check its verdict against the expected red/green oracle.
# Each run gets a unique -metadir (TLC's default scratch dir is per-second; a tight
# loop would otherwise collide). Exits non-zero if any verdict does not match.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# rows: <cfg> <module> <GREEN|RED>
RUNS=(
  "M1Convergence.cfg            M1Convergence.tla            GREEN"  # B3 control (S1')
  "M1Naive.cfg                  M1Convergence.tla            RED"    # B3 #2721 (S1)
  "MC_S2.cfg                    MC_S2.tla                    GREEN"  # B3 ideal (S2)
  "MC_S3.cfg                    MC_S3.tla                    RED"    # B3 split ownership (S3)
  "MC_S3_Fixed.cfg              MC_S3.tla                    GREEN"  # B3 immutable key (S3)
  "MC_S4_Naive.cfg              MC_S4.tla                    RED"    # B3 field-grain #2721 (S4)
  "MC_S4_FullWalkA.cfg          MC_S4.tla                    RED"    # B3 Model A over-fetch (S4)
  "MC_S4_ModelB.cfg             MC_S4.tla                    GREEN"  # B3 Model B (S4)
  "MC_Conv_Eventual.cfg         MC_Conv_Eventual.tla         GREEN"  # convergence, eventual connectivity
  "MC_Conv_RestartEviction.cfg  MC_Conv_RestartEviction.tla  GREEN"  # convergence, eviction+restart
  "MC_Conv_NoHeadRediscovery.cfg MC_Conv_NoHeadRediscovery.tla RED"  # convergence fails w/o rediscovery
  "MC_Claim_Unfiltered_Eventual.cfg  MC_Claim_Common.tla     GREEN"  # claim eventual-unique
  "MC_Claim_Filtered_Eventual.cfg    MC_Claim_Common.tla     GREEN"  # claim eventual-unique (filtered)
  "MC_Claim_Unfiltered_Execution.cfg MC_Claim_Common.tla     RED"    # exec-unique fails (CAS race)
  "MC_Claim_Filtered_Execution.cfg   MC_Claim_Common.tla     RED"    # exec-unique fails (filtered)
  "MC_Claim_Split_Eventual.cfg       MC_Claim_Common.tla     RED"    # split same-DID breaks convergence
  "MC_Kms_Green.cfg             MC_Kms_Gossip.tla            GREEN"  # KMS policy-gated
  "MC_Kms_NoPolicy_Red.cfg      MC_Kms_Gossip.tla            RED"    # KMS unauthorized obtains key
  "MC_Kms_BroadcastCiphertext_Red.cfg MC_Kms_Gossip.tla     RED"    # KMS ciphertext to non-recipient
  "MC_Kms_RevokeReplay_Green.cfg MC_Kms_Replay.tla          GREEN"  # KMS revoke/replay gated
  "MC_Kms_Revoke_Red.cfg        MC_Kms_Replay.tla            RED"    # KMS revoked obtains key
  "MC_Kms_Replay_Red.cfg        MC_Kms_Replay.tla            RED"    # KMS replay grants key
  "MC_EncryptedLwwReplay_Green.cfg MC_EncryptedLwwReplay_Common.tla GREEN" # #1049: durable ciphertext/key replay converges across restart
  "MC_EncryptedLwwReplay_Red_TerminalUnavailable.cfg MC_EncryptedLwwReplay_Common.tla RED" # transient KMS miss is terminally acknowledged
  "MC_EncryptedLwwReplay_Red_VolatilePending.cfg MC_EncryptedLwwReplay_Common.tla RED" # acked ciphertext loses its retry obligation on restart
  "MC_EncryptedLwwReplay_Red_FilterDropsField.cfg MC_EncryptedLwwReplay_Common.tla RED" # replication filter drops the encrypted LWW field block
  "MC_EncryptedLwwReplay_Red_ArrivalWins.cfg MC_EncryptedLwwReplay_Common.tla RED" # decrypted stale replay overwrites the LWW winner
  "MC_Auth_Green.cfg            MC_Auth_Green.tla            GREEN"  # auth gated
  "MC_Auth_Red_PeerOnly.cfg     MC_Auth_Red_PeerOnly.tla     RED"   # auth: PeerID-only executes
  "MC_Auth_Red_Stale.cfg        MC_Auth_Red_Stale.tla        RED"   # auth: stale token authorizes
  "MC_Auth_Red_WrongScope.cfg   MC_Auth_Red_WrongScope.tla   RED"   # auth: wrong-permission executes
  "MC_Replicator_Naive_Red.cfg       MC_Replicator_Naive_Red.tla       RED"   # replicator drops doc on disconnect
  "MC_Replicator_Resumable_Green.cfg MC_Replicator_Resumable_Green.tla GREEN" # replicator resumes -> no loss
  "MC_Commits_Red_UserOnly.cfg          MC_Commits_Red_UserOnly.tla          RED"   # commits leak (only User gated)
  "MC_Commits_Red_ReplicationUngated.cfg MC_Commits_Red_ReplicationUngated.tla RED" # commit block replicated to unauth peer
  "MC_Commits_Green.cfg                 MC_Commits_Green.tla                 GREEN" # both paths + replication gated
  "MC_Integrity_Red_NoCheck.cfg       MC_Integrity_Attacks.tla     RED"   # no sig check -> forged merges
  "MC_Integrity_Red_SigOnly.cfg       MC_Integrity_Attacks.tla     RED"   # sig-only (no author bind) -> spoof merges
  "MC_Integrity_Red_ReplayNoCheck.cfg MC_Integrity_Attacks.tla     RED"   # replayed sig over new content merges
  "MC_Integrity_Green.cfg             MC_Integrity_Attacks.tla     GREEN" # VerifyThenMerge gate
  "MC_Integrity_HonestConvergence.cfg MC_Integrity_Attacks.tla     GREEN" # gate doesn't block honest convergence
  "MC_Acp_Green.cfg            MC_Acp_Green.tla            GREEN" # ACP revocation propagates
  "MC_Acp_StaleCache_Red.cfg   MC_Acp_StaleCache_Red.tla   RED"   # stale cache grants revoked permission
  "MC_Ssi_Green.cfg             MC_Ssi_Green.tla             GREEN" # SSI: full ww+rw test -> serializable
  "MC_Ssi_Red_WriteSkew.cfg     MC_Ssi_Red_WriteSkew.tla     RED"   # SSI: ww-only -> write-skew admitted (INV_Serializable)
  "MC_Ssi_Probe_NoSnapFilter.cfg MC_Ssi_Probe_NoSnapFilter.tla GREEN" # SSI probe: snapshot guard is liveness, not safety
  "MC_Capability_Green.cfg         MC_Capability_Common.tla   GREEN" # capability: only legit tokens authorized
  "MC_Capability_Red_Forge.cfg     MC_Capability_Common.tla   RED"   # capability: forged token accepted (INV_OnlyLegitAccepted)
  "MC_Capability_Red_Ttl.cfg       MC_Capability_Common.tla   RED"   # capability: over-cap TTL accepted (INV_TtlCapped)
  "MC_Capability_Red_WrongTarget.cfg MC_Capability_Common.tla RED"   # capability: wrong peer/collection accepted (INV_TargetBound)
  "MC_Capability_Red_Revoked.cfg   MC_Capability_Common.tla   RED"   # capability: revoked token accepted (INV_RevokedNeverAccepted)
  "MC_SsiRange_Green_Correct.cfg          MC_SsiRange_Green_Correct.tla          GREEN" # SSI carve-out: doc-scan carve tracks index-range skew
  "MC_SsiRange_Red_TooAggressive.cfg      MC_SsiRange_Red_TooAggressive.tla      RED"   # SSI carve-out: over-broad carve drops a real skew
  "MC_SsiRange_Green_DocScanFalsePositive.cfg MC_SsiRange_Green_DocScanFalsePositive.tla GREEN" # carve drops only a false positive
  "MC_SsiRange_Green_NoCarveBaseline.cfg  MC_SsiRange_Green_NoCarveBaseline.tla  GREEN" # anti-vacuity baseline (cycle is caused by the carve)
  "MC_SsiRange_Probe_DocScanSkew.cfg      MC_SsiRange_Probe_DocScanSkew.tla      RED"   # boundary probe: unsound IF keyspaces overlap (they don't)
  "MC_Nac_Green.cfg                MC_Nac_Green.tla                GREEN" # NAC lifecycle: no priv-esc across enable/disable
  "MC_Nac_Red_NoPersist.cfg        MC_Nac_Red_NoPersist.tla        RED"   # NAC: disabled-flag not persisted across restart
  "MC_Nac_Red_ReEnableLive.cfg     MC_Nac_Red_ReEnableLive.tla     RED"   # NAC: re-enable from stale live admin (escalation)
  "MC_Nac_Red_WriteWhileDisabled.cfg MC_Nac_Red_WriteWhileDisabled.tla RED" # NAC: admin write accepted while disabled
  "MC_TxnRegistry_Green.cfg        MC_TxnRegistry_Green.tla        GREEN" # txn cleanup never evicts a live transaction
  "MC_TxnRegistry_Red_NaiveSweep.cfg MC_TxnRegistry_Red_NaiveSweep.tla RED" # naive sweep evicts a still-live txn
  "MC_MergeQueue_Green.cfg         MC_MergeQueue_Green.tla         GREEN" # per-doc merge serialized; no loss/dup; fails closed
  "MC_MergeQueue_CrossDocParallel.cfg MC_MergeQueue_CrossDocParallel.tla RED" # per-doc-only P2P workers overlap on shared mutable index keyspaces
  "MC_MergeQueue_Red_FailOpen.cfg  MC_MergeQueue_Red_FailOpen.tla  RED"   # retry exhaustion silently drops a block
  "MC_MergeQueue_Red_NoMutex.cfg   MC_MergeQueue_Red_NoMutex.tla   RED"   # no per-doc mutex -> same-doc double-apply
  "MC_MergeQueue_Red_LocalMergeInterleave.cfg MC_MergeQueue_Red_LocalMergeInterleave.tla RED" # shared guard removed -> local write + same-doc merge interleave (INV_NoLocalMergeInterleave)
  "MC_TwoStoreCounter_Red_Split.cfg TwoStoreCounter.tla           RED"   # counter reconcile-from-blob clobbers a concurrent local increment (INV_NoLoss)
  "MC_TwoStoreCounter_Red_DoubleApply.cfg TwoStoreCounter.tla     RED"   # #4935: re-delivered delta merged twice w/o dedup -> double-apply (INV_NoDoubleApply)
  "MC_TwoStoreCounter_Green.cfg     TwoStoreCounter.tla           GREEN" # unified RMW + merged-set dedup -> exact (no loss, no double)
  "MC_MixedFieldMaterialization_Red_WholeDoc.cfg MixedFieldMaterialization.tla RED" # stale whole-doc field merge clobbers another field
  "MC_MixedFieldMaterialization_Green.cfg MixedFieldMaterialization.tla GREEN" # componentwise field materialization preserves mixed product state
  "MC_IndexReconciliation_Red_SaveOnly.cfg IndexReconciliation.tla RED" # stale index key survives CRDT winner rematerialization
  "MC_IndexReconciliation_Green.cfg IndexReconciliation.tla GREEN" # index keys equal the winning CRDT materialized value
  "MC_DocumentMaterialization_Red_Overwrite.cfg DocumentMaterialization.tla RED" # active rematerialization must not clear delete marker
  "MC_DocumentMaterialization_Green.cfg DocumentMaterialization.tla GREEN" # deletion marker is componentwise/absorbing
  "MC_InteractiveTxnCounter_Green.cfg MC_InteractiveTxnCounter_Common.tla GREEN" # #1044: gate On + commit-only finalize -> deadlock-free + gate never held across idle
  "MC_InteractiveTxnCounter_Red_NoGate.cfg MC_InteractiveTxnCounter_Common.tla RED" # gate Off -> arbitrary-order batch vs finalize circular-wait DEADLOCK (gate is load-bearing)
  "MC_InteractiveTxnCounter_Red_AcrossLifetime.cfg MC_InteractiveTxnCounter_Common.tla RED" # #1041 old path: gate held across user-controlled idle lifetime (INV_GateBoundedHold)
  "MC_PushLogAdmission_Green.cfg MC_PushLogAdmission_Common.tla GREEN" # #1088 W1: capacity overflow nacks -> success implies registered-or-merged + all docs merge
  "MC_PushLogAdmission_Red_SuccessOnFull.cfg MC_PushLogAdmission_Common.tla RED" # fa4a84f7 regression: overflow drops registration but acks success (INV_SuccessImpliesRegisteredOrMerged)
  "MC_PushBacklog_Green.cfg MC_PushBacklog_Common.tla GREEN" # #1099: bounded queue + fixed workers + per-peer cap -> all bounds hold, healthy peers progress
  "MC_PushBacklog_Red_SpawnPerItem.cfg MC_PushBacklog_Common.tla RED" # current main: task spawned per (write,peer) before the semaphore -> resident work unbounded (INV_QueueBounded)
  "MC_PushBacklog_Red_PermitLeak.cfg MC_PushBacklog_Common.tla RED" # completing job keeps its worker slot -> pool decays (INV_PermitConservation)
  "MC_PushBacklog_Red_RetainHandles.cfg MC_PushBacklog_Common.tla RED" # SyncShutdownHandle retains every completed JoinHandle (INV_HandlesBounded)
  "MC_PushBacklog_Red_NoPeerCap.cfg MC_PushBacklog_Common.tla RED" # no per-peer cap: slow peer's stuck sends hold every worker -> healthy peers starve (LIVE_HealthyProgress)
  "MC_PushCoalescing_Green.cfg MC_PushCoalescing_Common.tla GREEN" # #1102: latest head retained while queued/persisted predecessors retire
  "MC_PushCoalescing_Red_AppendEvery.cfg MC_PushCoalescing_Common.tla RED" # no live coalescing -> two heads occupy one (doc,peer) queue
  "MC_PushCoalescing_Red_StaleRetry.cfg MC_PushCoalescing_Common.tla RED" # failed superseded active head re-enters persisted retry
  "MC_PendingDagRestart_Green.cfg MC_PendingDagRestart_Common.tla GREEN" # #1099: pending-DAG registrations persisted + restored after crash -> acks stay backed, all docs merge
  "MC_PendingDagRestart_Red_ProcessLocal.cfg MC_PendingDagRestart_Common.tla RED" # process-local registrations: hub crash after success-ack -> silent permanent loss (INV_AckBacked)
  "MC_SyncOwnership_Green.cfg SyncOwnership.tla GREEN" # #1116 stage 3: one head hint transfers durable completion ownership to the receiver
  "MC_SyncOwnership_Green_IrohOrigin.cfg SyncOwnership.tla GREEN" # Iroh binds durable recovery to the signed, routable origin that owns the linked DAG
  "MC_SyncOwnership_Red_DocOnlyMarkers.cfg SyncOwnership.tla RED" # no collection marker loses a dropped collection-head obligation
  "MC_SyncOwnership_Red_PayloadLedger.cfg SyncOwnership.tla RED" # current main stores CID/version delivery state instead of scope markers
  "MC_SyncOwnership_Red_VolatileRegistration.cfg SyncOwnership.tla RED" # success ack followed by restart loses a volatile receiver obligation
  "MC_SyncOwnership_Red_DuplicateFetch.cfg SyncOwnership.tla RED" # alternate triggers claim two fetch owners for one root
  "MC_SyncOwnership_Red_StaleAckClears.cfg SyncOwnership.tla RED" # stale ack clears the marker for a newer current head
  "MC_SyncOwnership_Red_VolatileServeAuthority.cfg SyncOwnership.tla RED" # success ack survives receiver restart but sender restart loses CAR-serving authority
  "MC_SyncOwnership_Red_RelayOnlyProvider.cfg SyncOwnership.tla RED" # an unverified payload relay is not an authenticated recovery provider
  "MC_SyncOwnership_Red_UnroutableOrigin.cfg SyncOwnership.tla RED" # publisher identity without a direct-or-relayed CAR route cannot discharge ownership
  "MC_SyncOwnership_Red_UnsignedIrohOrigin.cfg SyncOwnership.tla RED" # unsigned Iroh payload SourcePeerID is not an authenticated recovery provider
  "MC_SyncOwnership_Red_RootOnlyHop.cfg SyncOwnership.tla RED" # authenticated gossip hop owns only the root, not the linked DAG promised by the hint
  "MC_SyncOwnership_Red_CancelOnProgress.cfg SyncOwnership.tla RED" # first arriving CAR block truncates the receiver-owned response before DAG completion
  "MC_SyncOwnership_Red_RecursiveFirst.cfg SyncOwnership.tla RED" # known missing frontier is delayed behind a recursive full-DAG CAR
  "MC_SyncOwnership_Red_EveryRoot.cfg SyncOwnership.tla RED" # successive heads from one sender/scope accumulate obsolete durable roots
  "MC_SyncOwnership_Red_ParallelMerge.cfg SyncOwnership.tla RED" # frontend-selected parallel writers violate the receiver's single merge-owner boundary
  "MC_SyncOwnership_Red_DuplicateTerminal.cfg SyncOwnership.tla RED" # concurrent same-root terminal cleanup violates the single durable metadata writer
  "MC_Jwt_Green.cfg                MC_Jwt_Green.tla                GREEN" # token->DID binds genuine signer DID
  "MC_Jwt_Red_NoAlgBinding.cfg     MC_Jwt_Red_NoAlgBinding.tla     RED"   # alg-confusion: header alg not bound to key type
  "MC_Jwt_Red_NoIssBinding.cfg     MC_Jwt_Red_NoIssBinding.tla     RED"   # iss not bound to did(pubkey)
  "MC_Jwt_Red_NoSig.cfg            MC_Jwt_Red_NoSig.tla            RED"   # signature not verified -> forged token
  "MC_DeferredAcp_Green.cfg        MC_DeferredAcp_Green.tla        GREEN" # txn-local ACP projection gates as committed state would
  "MC_DeferredAcp_Red_OwnerBypass.cfg MC_DeferredAcp_Red_OwnerBypass.tla RED" # projection grants what committed state denies
  "MC_DeferredAcp_Red_RollbackHooks.cfg MC_DeferredAcp_Red_RollbackHooks.tla RED" # rollback leaves hooks applied (not a no-op)
  "MC_DeferredAcp_Red_SharedOverlay.cfg MC_DeferredAcp_Red_SharedOverlay.tla RED" # one txn observes another's uncommitted projection
  "MC_PendingDagQuarantine_Green.cfg PendingDagQuarantine.tla GREEN" # #1128: deterministic rejection quarantines durably; sound docs merge, poison docs quarantine
  "MC_PendingDagQuarantine_Red_RetryForever.cfg PendingDagQuarantine.tla RED" # pre-#1128 wedge, still live for any unclassified Rejected producer: Rejected treated as retryable skip -> poison root swept forever (LIVE_PoisonQuiesces)
  "MC_PendingDagQuarantine_Red_OvereagerQuarantine.cfg PendingDagQuarantine.tla RED" # forbidden overcorrection: sound doc's transient failure also quarantines it (LIVE_SoundEventuallyMerged)
)

fails=0; n=0
for row in "${RUNS[@]}"; do
  read -r cfg mod want <<<"$row"
  n=$((n+1))
  out=$(./tools/tlc -metadir "states/run$n" -config "$cfg" "$mod" 2>&1)
  if echo "$out" | grep -q "No error has been found"; then got=GREEN
  elif echo "$out" | grep -qE "is violated|was violated|properties were violated|Deadlock reached|Deadlock"; then got=RED
  else got=ERROR; fi
  rm -rf "states/run$n"
  if [ "$got" = "$want" ]; then printf "  ok   %-6s %-34s %s\n" "$got" "$cfg" "$mod"
  else printf "  FAIL want=%s got=%s  %-30s %s\n" "$want" "$got" "$cfg" "$mod"; fails=$((fails+1)); fi
done

echo "----"
if [ "$fails" -eq 0 ]; then echo "all $n TLA+ runs matched their expected verdict"; else echo "$fails/$n MISMATCHED"; fi
exit "$fails"
