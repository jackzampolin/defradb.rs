# Integrity TLA+ Design

## Outcome

This slice models the integrity gate beneath replication: an honest node must not
merge a block unless the block has a valid signature over its exact content and
the verified signer is the same identity the block claims as author.

The model separates three facts that are easy to conflate in code review:

- A signature is present.
- The signature verifies over this exact content.
- The verified signer matches the claimed block author / effective creator.

Only the conjunction of the last two is sufficient for merge.

## Source Grounding

- `crates/db/src/merge/merge_handler/signature.rs:17` defines
  `verify_block_signature`.
- `crates/db/src/merge/merge_handler/signature.rs:23` returns `Ok(None)` for an
  unsigned block; invalid or malformed signature material returns
  `SignatureVerificationFailed`.
- `crates/db/src/merge/merge_handler/signature.rs:65` serializes the block with
  the signature field removed, so verification is over the block content, not
  over the signature CID itself.
- `crates/db/src/merge/merge_handler/signature.rs:91` calls
  `pub_key.verify(&signed_bytes, &signature.value)`.
- `crates/db/src/merge/merge_handler/signature.rs:96` derives the verified
  signer's DID from the public key.
- `crates/db/src/merge/merge_handler/mod.rs:514` verifies normal P2P blocks
  before CRDT-specific merge dispatch and stores the result in
  `metadata.verified_creator`.
- `crates/db/src/merge/merge_handler/batch.rs:269` applies the same signature
  verification on the batch path.
- `crates/p2p/src/sync/merge.rs:247` makes `effective_creator()` prefer
  `verified_creator` over self-reported `creator`.
- `crates/db/src/merge/peer_identity.rs:69` maps libp2p peer keys to DIDs for
  peer identity, but peer identity is not a substitute for block author
  signature verification.

## Crypto Boundary

Signature validity is abstract. The model does not prove Ed25519, secp256k1, or
secp256r1 unforgeability. It assumes the usual EUF-CMA boundary: the adversary
can sign as identities whose keys it controls and can replay signatures it has
seen, but cannot produce a valid signature for an identity whose private key it
does not hold.

In `Integrity.tla`, `ValidSig(b)` means `b.sig.status = "valid"` and
`b.sig.signedContent = b.content`. That abstracts the concrete
`pub_key.verify(signed_bytes, signature)` call. `Authentic(b)` adds
`b.sig.signer = b.author`, matching the required verified-creator binding.

## Model Shape

Blocks are records:

```tla
[author |-> did, content |-> value, sig |-> signatureRecord]
```

Signatures are records:

```tla
[status |-> "absent" | "invalid" | "valid",
 signer |-> did,
 signedContent |-> value]
```

The adversarial scenario includes:

- an honest Alice block with a valid Alice signature over Alice content;
- an unsigned block claiming Alice;
- an invalid-signature block claiming Alice;
- a replayed Alice signature over different content;
- a valid Eve signature over content that claims Alice as author.

The merge policy is a constant:

- `NoCheck`: buggy path, merges anything in the network.
- `SigOnly`: buggy path, checks content verification but not signer/author
  binding.
- `VerifyThenMerge`: intended gate, requires `Authentic(b)`.

## Properties

- `INV_NoForgedMerge`: every block in an honest node's `merged` set is
  authentic: valid signature over exact content and signer equals claimed
  author.
- `INV_AuthorBinding`: every merged block's signer is the claimed author; this
  catches valid signatures from the wrong identity.
- `INV_NoReplayForge`: a signature replayed over different content never
  appears in an honest node's merged set.
- `HonestBlocksEventuallyMerged`: liveness sanity check; under connected fair
  delivery, the strict integrity gate still eventually merges honest signed
  blocks.

## TLC Runs

Run from `proofs/tla`.

```bash
./tools/tlc -metadir states/integrity_red_nocheck -config MC_Integrity_Red_NoCheck.cfg MC_Integrity_Attacks.tla
```

Verdict: RED. `INV_NoForgedMerge` is violated after `NoCheck` merges an
unsigned block claiming `did:alice`.

```bash
./tools/tlc -metadir states/integrity_red_sigonly -config MC_Integrity_Red_SigOnly.cfg MC_Integrity_Attacks.tla
```

Verdict: RED. `INV_AuthorBinding` is violated after `SigOnly` accepts a block
claiming `did:alice` but signed by `did:eve`.

```bash
./tools/tlc -metadir states/integrity_red_replay -config MC_Integrity_Red_ReplayNoCheck.cfg MC_Integrity_Attacks.tla
```

Verdict: RED. `INV_NoReplayForge` is violated after `NoCheck` merges a block
whose signature was valid only for different content.

```bash
./tools/tlc -metadir states/integrity_green -config MC_Integrity_Green.cfg MC_Integrity_Attacks.tla
```

Verdict: GREEN. `TypeOK`, `INV_NoForgedMerge`, `INV_AuthorBinding`, and
`INV_NoReplayForge` all hold under `VerifyThenMerge`.

```bash
./tools/tlc -metadir states/integrity_honest_conv -config MC_Integrity_HonestConvergence.cfg MC_Integrity_Attacks.tla
```

Verdict: GREEN. The strict gate does not block the honest signed block under
fair connected delivery.

## Limitations

This is a bounded TLC model. It proves the guard structure over the finite
scenario, not cryptographic unforgeability and not automated Rust/TLA
conformance. It also does not model authorization policy, document ACP/NAC, key
rotation, recovery trust, or CRDT merge algebra; those are separate slices.
