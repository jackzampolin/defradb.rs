# Survey: `crates/wasm/` (defra-wasm)

## Purpose
Browser client. Thin `wasm-bindgen` wrapper exposing DefraDB to JavaScript. All
substantive behavior is delegated to underlying crates (`db`, `query`, `document`,
`crypto`, `schema`). This crate is glue: JS interop, serde marshalling, error mapping.

## Modules
- `client.rs` — `DefraClient`: wraps `Arc<DB<LevelDbStore>>` + `QueryRunner`. Delegates
  `add_schema` / `query` / `mutate` / `get_collections` / `persist` / `close`.
- `verification.rs` — standalone re-exports of `crypto`/`document` fns: ed25519/secp256k1
  signature verify, sha256, `compute_document_cid`, keypair generation.
- `bindings.rs` — serde <-> `JsValue` conversion, config/info structs.
- `error.rs` — `WasmError` -> `JsValue` mapping.

## State machines
- Only one, trivial: `DefraClient.closed` is a 2-state open/closed lifecycle (every
  method guards on `closed`, `close()` is idempotent). No concurrency, no protocol,
  no distributed state. Single-threaded browser context.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| client open/closed lifecycle | none | guard-after-close idempotence | n/a (integration) | low |
| `compute_document_cid` determinism | Lean | same JSON -> same CID (content-addressing) | yes (integrity / convergence content-addressing) | low |
| signature verification | Lean/TLA | verify soundness | yes (integrity slice) | low |

All three are inherited from underlying crates. The CID determinism and signature
soundness properties are real, but they are proven where the logic lives
(`crypto`/`document`, covered by the integrity & convergence slices) — not in this
re-export shim. Verifying them "in wasm" would re-prove the same lemma.

## Verdict
**Plumbing — not model-worthy.** No new state machine, no new algebraic law, no
concurrency or adversary surface originates here. The crate forwards calls and
marshals types across the JS boundary; integration tests (`wasm-bindgen-test`) and
the existing integrity/convergence slices cover everything of consequence.
`model_worthy: false`.
