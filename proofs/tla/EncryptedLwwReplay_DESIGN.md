# Encrypted LWW restart/replay - TLA+ design

This model covers issue **#1049**. Encryption does not change the LWW merge
algebra proved by `DefraConvergence.PriorityReconcile.lwwCM`; it changes when a
received value can be materialized. The temporal obligation is that an
acknowledged encrypted field remains re-drivable until its DEK is available,
including across a receiver restart.

## Source-grounded facts

| Fact | Source | Model consequence |
|---|---|---|
| Filtered replication pushes every non-counter field block, including encrypted LWW blocks, as a head so receipt triggers DEK resolution. | `crates/db/src/merge/push_docs.rs`, `crates/db/src/merge/push_docs_transport.rs` | `FilterMode="PreserveEncrypted"` stores the ciphertext; the red mode drops it and violates `INV_NoFilteredLoss`. |
| Only `AccessDenied` is a terminal encrypted-field skip. `KeyUnavailable` and other transient KMS errors abort the merge transaction. | `crates/db/src/merge/merge_handler/composite_fields.rs`, `crates/db/src/merge/merge_handler/mod.rs` | `KmsFailureMode="Retry"` retains the pending merge; the red terminal mode retires it. |
| Push-originated pending-DAG registrations are persisted before success acknowledgement and removed only after merge or quarantine. | `crates/p2p/src/sync/pending_store.rs`, `crates/p2p/src/sync/manager/process/pending_dag.rs` | `PendingMode="Durable"` restores the retry obligation after a crash. |
| A verified remotely fetched encryption block is stored before its key is returned to the merge. | `crates/kms/src/defra_kms.rs` | `keyAvailable` survives `Crash`; an in-flight request or envelope does not. |
| LWW selects the greatest version independent of delivery order. | `crates/crdt/src/lww.rs` | `MergeMode="Lww"` preserves `LocalVersion`; arrival-wins is the red policy. |

## Properties

- `INV_AckBacked`: an acknowledged remote value is applied or still has a
  volatile/durable retry obligation.
- `INV_NoFilteredLoss`: retiring the sender's delivery record cannot lose the
  encrypted field block selected for replication.
- `INV_LwwWinner`: key timing and replay order cannot replace a newer local LWW
  value with an older remote value.
- `LIVE_WinnerMaterialized`: under fair reconnect and key service, both writes
  eventually settle on the LWW winner.

The model starts with the key service unavailable. It explores a failed lookup,
service recovery, restart before receipt, restart after the key is durably
stored, and replay against a newer local write.

## Configs

| Config | Verdict | Fault isolated |
|---|---|---|
| `MC_EncryptedLwwReplay_Green.cfg` | GREEN | durable pending state, retryable KMS miss, preserved field, LWW merge |
| `MC_EncryptedLwwReplay_Red_TerminalUnavailable.cfg` | RED | transient KMS miss is acknowledged as a terminal skip |
| `MC_EncryptedLwwReplay_Red_VolatilePending.cfg` | RED | restart destroys the only retry obligation |
| `MC_EncryptedLwwReplay_Red_FilterDropsField.cfg` | RED | selected encrypted field is omitted from delivery |
| `MC_EncryptedLwwReplay_Red_ArrivalWins.cfg` | RED | stale decrypted replay overwrites the newer local value |

## Conformance fence

`proofs/tests/behavioral/partition.rs::convergence_encrypted_lww_restart_merge`
drives two encrypted siblings across a restart-induced partition, first proves
identical commit DAGs, then asserts both nodes decrypt and materialize the LWW
winner. Because those siblings are created after the restart, this binds
`INV_LwwWinner`; it does not schedule a restart after ciphertext acknowledgement
while the DEK is unavailable. `INV_AckBacked` therefore remains a conformance
boundary.
`tools/integration-test/tests/p2p/filtered_replication.rs` asserts matching
encrypted documents decrypt on the filtered peer, proving their field heads
survive selection and trigger DEK resolution, binding `INV_NoFilteredLoss`. The
merge/KMS unit tests exercise the deterministic timeout-to-retry classification
that is hard to schedule reliably through the external harness.
