# Survey: `crates/cursor/`

## Purpose
Opaque cursor-token codec for GraphQL cursor pagination. Defines a single
`Cursor` struct (`doc_id`/`d`, `keys`/`k`, `direction`/`o`) and two operations:
`encode` (serde-JSON → base64url-no-pad) and `decode` (base64url → JSON →
validate non-empty doc_id). Serde field names/attributes mirror Go's
`internal/cursor.CursorPayload` for byte-for-byte token interoperability.
Total crate size: ~74 lines of source.

## State machines
None. No status/lifecycle enum, no transitions, no multi-component protocol.
The only enum is `CursorError` (three error variants), which is a flat error
taxonomy, not a state machine. Encode/decode are stateless pure functions.

## Modelable candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | — | — | — |

Notes on the one law-shaped behavior considered and rejected:
- Round-trip `decode(encode(c)) == c` and Go byte-exact parity are real
  properties, but they are (a) trivially established by serde's deterministic
  derive, and (b) already pinned by unit tests (`tests/codec.rs`) and
  cross-language fixtures (`tests/go_fixtures.rs`, Go-produced tokens). A Lean
  proof would restate the test oracle without adding assurance — the risk is
  drift in serde attributes, which fixtures catch directly. Not model-worthy.

## Verdict
**Plumbing.** Deterministic, stateless serialization glue with no concurrency,
no distributed/consistency behavior, no security state machine, and no
non-trivial algebraic law. Integration/unit/fixture tests fully cover it.
`model_worthy: false`, no candidates.
