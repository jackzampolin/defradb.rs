# Survey: `crates/crypto/`

## Purpose
Cryptographic primitives library. Key management (secp256k1, secp256r1, Ed25519,
BLS12-381), digital signatures (ECDSA/EdDSA), symmetric encryption (AES-256-GCM),
asymmetric encryption (ECIES), SHA-256 hashing, DID key encode/parse (multicodec +
multibase), batch Merkle-root signing/verification, and searchable-encryption (SE)
HMAC equality tags + replication artifacts. Designed for byte-for-byte Go parity.

## State machines
None internal. The crate is stateless transformation code (sign/verify, encrypt/
decrypt, hash, encode/decode). The only enums (`ArtifactType`, `OperationType` add/
delete) are tags, not lifecycles. SE artifacts feed a replication protocol, but that
protocol lives in p2p/db, not here.

## Modelable candidates

| Name | Kind | Property | Already-modeled | Priority |
|------|------|----------|-----------------|----------|
| SE equality-tag determinism & domain separation | Lean | `tag(k,id,col,fld,v)` is a deterministic function; equal iff all 5 inputs equal (modulo the documented `:`-delimiter collision); changing any of key/identity/collection/field/value changes the tag | no | low |
| DID key round-trip | Lean | `parse_did_key(create_did_key(kt, pk)) == (kt, pk)` for all supported KeyTypes; multicodec mapping is a bijection | no | low |
| Sign/verify + AES/ECIES round-trip soundness | Lean | `verify(pk, m, sign(sk,m))` holds; `decrypt(dec_key, encrypt(enc_key,m)) == m` | no | low |
| Batch Merkle-root sign/verify | Lean | `verify_batch_signature(sign_batch(cids,cfg), cids)`; verify fails if CID set differs (root binds the whole set) | no (Integrity slice models *block* sig-before-merge, not batch Merkle root) | low |

## Verdict
Effectively **plumbing**. Correctness here rests on vetted upstream crates (RustCrypto
`hmac`/`sha2`/`aes-gcm`, `k256`/`p256`/`ed25519-dalek`) plus extensive Go-parity unit/
compat tests (`tests/go_compat_*.rs`). The SE tag and DID round-trip are genuine
algebraic laws but are low-value to formalize: they are single-function determinism/
inverse properties fully covered by existing deterministic-output and round-trip tests,
and any real adversary/consistency questions (SE artifact replication, key
distribution, sig-before-merge) live in the p2p/db slices already modeled (KMS,
Integrity, Commits, Replicator). No TLA+ candidates — no concurrency, replication, or
security state machine originates in this crate.

**model_worthy: false.**
