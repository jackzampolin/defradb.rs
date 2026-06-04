# Survey: `crates/kms/`

## Purpose
Key Management Service: distributes document/collection encryption keys (DEKs)
across P2P nodes. `DefraKms` composes a `KeyStore`, zero or more `KeyTransport`s,
and one `AccessPolicy`. `get_keys` serves locally-held CIDs (after a policy check)
and fans out remote misses over transports; `serve_request` answers a peer's
`FetchEncryptionKeyRequest` by ECIES-wrapping block bytes only after
`policy.check_release(actor, scope)` returns `Allow`. `NacDacPolicy` is the
dual-gate (DAC for document-scoped, NAC for collection-scoped). Tracks Go
`internal/kms/` + the NAC-aware fix from Go PR #4778.

## State machines
- **Key-distribution protocol (implicit, security):** request issuance ->
  policy-gated serve -> ECIES-wrapped reply -> recipient-only unwrap -> DEK in
  store. Adversary surface: ciphertext broadcast on pubsub, request replay,
  revocation timing.
- **`PolicyDecision` gate (explicit):** Allow/Deny, evaluated at serve time
  (not request time) per actor + scope, with NAC-not-configured / no-DAC-policy
  allow fallbacks.
- **`OnceLock` NAC install (trivial):** node_acp set-once; not model-worthy.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Authorized-eventually-has-key | TLA+ | every currently-authorized node eventually holds every key it may read (fair delivery) | yes (Kms slice, `INV_AuthorizedEventuallyHasKey`) | — |
| Only-authorized-has-key | TLA+ | no node holds usable plaintext DEK unless currently authorized; policy gate at serve time | yes (`INV_OnlyAuthorizedHasKey`, NoPolicy_Red) | — |
| Recipient-only-decrypt | TLA+ | only the envelope's intended requester can unwrap (ECIES recipient binding) | yes (`INV_OnlyIntendedRecipientDecrypts`, BroadcastCiphertext_Red) | — |
| Revoked-cannot-obtain | TLA+ | node revoked before any envelope released to it never obtains key | yes (`INV_RevokedCannotObtain`, Revoke_Red) | — |
| No-replay-grant | TLA+ | replaying an old (once-authorized) request grants nothing after revocation | yes (`INV_NoReplayGrant`, Replay_Red) | — |

## Verdict
**Not model-worthy as new work.** Every security/distribution property of this
crate is already covered by the committed KMS TLA+ slice (`Kms_DESIGN.md`,
`MC_Kms_*` runs) under the ECIES recipient-binding assumption. The dual-gate
NAC/DAC release decision is also reachable via the ACP soundness/auth slices.
Remaining code (wire CBOR, stores, transport plumbing, AAD byte-layout) is
glue/IO validated by unit + integration tests. No new TLA+ or Lean slice needed.
