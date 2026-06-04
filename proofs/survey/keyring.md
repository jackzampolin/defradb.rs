# Survey: `crates/keyring/`

## Purpose

Local key-storage abstraction for DefraDB. Defines a `Keyring` trait
(`set`/`get`/`delete`/`list`, keys as raw bytes wrapped in `Zeroizing`) with three
backends: file-based JWE (PBES2-HS512-A256KW, Go-jwx compatible), OS system keyring
(macOS Keychain / Linux Secret Service / Windows Credential Manager), and
`systemd-creds`. Adds a validated `KeyName` type (path-traversal guard) and a
non-caching `KeyHandle` that re-fetches key bytes on each use to limit memory exposure.

## State machines

None. Every backend is a stateless CRUD wrapper over the filesystem / OS keyring.
No lifecycle enums, no transitions, no concurrent or distributed protocol. `KeyHandle`
deliberately holds no state (fetches on demand). Key *distribution* across nodes is
out of scope here — that lives in `crates/kms` (already modeled).

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| KeyName path-traversal soundness | Lean | validated name never resolves outside the keyring dir | no | low |
| File set/get round-trip | none | encrypt-then-decrypt returns original bytes | no (integration test) | low |
| KMS key distribution | TLA+ | (not in this crate) | yes (`Kms` slice, `crates/kms`) | n/a |

The KeyName predicate is a pure total function exhaustively covered by unit tests in
`key_name.rs`; a Lean restatement would add negligible assurance over the existing
tests. The JWE/systemd round-trip is IO correctness validated by
`tests/integration_tests.rs` and Go cross-compat — not a proof target.

## Verdict

**Plumbing.** No concurrency, no distributed/eventual-consistency behavior, no
adversary state machine, no non-trivial algebraic law. The interesting security
property (key distribution / KMS) is already covered by the `Kms` TLA+ slice in a
different crate. `model_worthy: false`.
