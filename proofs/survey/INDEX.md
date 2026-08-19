# Formal-Modelability Survey — Coverage Index

Consolidated map of all 40 per-crate surveys in `proofs/survey/`. Three sections:
already-modeled candidates (cross-check), the proposed-new backlog (prioritized),
and out-of-scope crates (covered by integration tests).

---

## 1. ALREADY MODELED

Candidates with `already_modeled=true`, grouped by the existing proof slice they
map onto. This is a sanity cross-check that the surveys agree with what is built.

### Acp slice (Zanzibar soundness + stale-revocation cache)
- **acp** — Zanzibar check soundness / no-escalation (Lean)
- **acp** — Tuple revocation + stale positive cache (TLA+)
- **acp** — NAC management-channel auth gate (TLA+)
- **db-nac** — NAC lifecycle auth gate, live-vs-persisted admin asymmetry (TLA+)
- **sourcehub** — Access-decision cache stale-revocation (TLA+) — `MC_Acp_StaleCache_Red`
- **zanzibar** — Check soundness / no-escalation (Lean) — `Acp/Soundness.lean`
- **zanzibar** — Cycle-detection safety (Lean)

### Commits slice (dual-path ACP gating)
- **acp** — ACP-on-commits dual-path gating (TLA+)
- **db** — BlockVerifyDualPathAcp (either)
- **db-merge** — acp-register-on-merge (TLA+)
- **query** — Commits/User dual-path ACP gating (TLA+)

### Auth slice (#1012 management-channel auth)
- **http** — Management-auth request gate (TLA+)
- **http** — Route-permission table completeness (none)
- **pg-compat** — auth DID==username gate (TLA+)

### Integrity slice (signature-before-merge)
- **blockstore** — CID verification soundness (Lean)
- **db-merge** — sig-verify-before-merge (TLA+)
- **defra-core** — Block integrity before merge (TLA+)

### Convergence slice (DAG completeness / CRDT merge order)
- **blockstore** — merge-tracking lifecycle unmerged→merged (TLA+)
- **db** — FetcherConvergence (TLA+)
- **db-merge** — composite-dag-completeness (TLA+)

### Replicator slice (status lifecycle / no-loss / resume)
- **embedded** — Replicator retry/resume pass (TLA+)
- **embedded** — SE-artifact re-push convergence (TLA+)

### KMS slice (key distribution)
- **kms** — Authorized-eventually-has-key liveness (TLA+)
- **kms** — Only-authorized-has-key serve-gate (TLA+)
- **kms** — Recipient-only-decrypt ECIES binding (TLA+)
- **kms** — Revoked-cannot-obtain (TLA+)
- **kms** — No-replay-grant (TLA+)
- **db-merge** — se-artifact-roundtrip (either)

### Lean CRDT-laws slice (`DefraConvergence/LocalState.lean`)
- **crdt** — LWW merge is a join, comm/assoc/idem (Lean)
- **crdt** — Counter Int64 wrapping-add laws (Lean)
- **crdt** — Float counter non-convergence (Lean)
- **crdt** — Composite componentwise merge (Lean)
- **crdt** — Applied-set idempotency layer (Lean)
- **db-merge** — head-advance-and-priority (Lean)
- **defra-core** — CRDT delta-payload merge laws (Lean)

### Content-addressing (assumed precondition of Convergence/Integrity)
- **db** — SchemaVersionContentAddr (Lean)
- **db-backup** — docID content-addressing purity (Lean)
- **db-blocks** — CID content-addressing determinism (Lean)
- **db-blocks** — Head/priority update protocol (TLA+)
- **db-blocks** — Deterministic head ordering (none)
- **document** — DocID content-addressing determinism (Lean)
- **lens** — transform-id content-addressing/dedup (Lean)
- **schema** — Schema-def CID determinism (Lean)
- **schema** — collection-set CID order-invariance (Lean)

---

## 2. PROPOSED NEW (backlog)

Candidates with `already_modeled=false` in a `model_worthy=true` crate, sorted
high → low priority.

| Candidate | Crate | Kind | Property to prove | Priority |
|-----------|-------|------|-------------------|----------|
| SSI snapshot-isolation correctness | storage | TLA+ | Every accepted commit is serializable: ConflictTracker aborts iff a later-committed txn wrote a key/range this txn read, or read a key it wrote — no lost update or write-skew survives | high |
| explicit-replay capability gate | p2p | TLA+ | Capability tokens are unforgeable and peer+collection-bound; wrong-target/collection replay rejected; TTL ≤ MAX_CAPABILITY_TTL even on key compromise; revoked tokens never re-accepted | high |
| SSI collection-scan carve-out soundness | storage | TLA+ | The `is_document_collection_scan_prefix` carve-out only eliminates false positives — it never drops a true write-skew conflict | medium |
| Order-preserving encoding monotonicity | storage | Lean | a<b ⟹ encode_ascending(a) <lex encode_ascending(b); descending inverts; cross-type marker bytes form a total order consistent with per-type ordering | medium |
| NAC lifecycle privilege-escalation safety | acp | TLA+ | Across Enabled→DisabledTemporarily→re_enable, no non-admin mutates admin/grant set; writes while disabled rejected; persisted disabled-flag survives restart; live-vs-persisted is_admin asymmetry is sound | medium |
| TxnRegistryCleanupRace | db | TLA+ | Stale-txn cleanup sweep never evicts a still-live transaction (no lost active txn); only genuinely idle-past-max-age txns are removed and rolled back | medium |
| merge-queue-serialization | db-merge | TLA+ | Per-doc MergeQueue mutex serializes same-doc merges while parallel across docs; bounded 5x conflict-retry loses/duplicates no block; retry exhaustion fails closed | medium |
| Deferred-ACP overlay consistency | query | TLA+ | Txn-local projected_registrations gates reads as the committed ACP state would; commit applies all hooks, rollback applies none; no txn observes another's uncommitted projection; fail-closed across projected→committed | medium |
| index-maintenance-consistency | db-index | Lean | After on_document_update(old,new) the stored index-entry set equals extract(new): no stale tuples remain, none missing | medium |
| JWT issuer binding / algorithm-confusion resistance | identity | either | from_token yields DID d only when iss==did(pubkey)==d, header alg matches key_type, signature verifies; no cross-curve confusion; discharges Auth-slice assumption | medium |
| capability revocation consistency | p2p | TLA+ | Once revoked, every later verify denies; revocation monotone and consistent under concurrent verify/revoke on shared deny-list | medium |
| CID content-addressing determinism | defra-core | Lean | Equal blocks ⟹ equal canonical DAG-CBOR ⟹ same CID; distinct canonical content ⟹ distinct CID (injectivity over canonical encoding) | medium |
| Block.new canonicalization | defra-core | Lean | Block::new is idempotent and order-insensitive: sorted heads/links + empty→None yields a unique normal form, CID independent of input link ordering | medium |
| index-value-extraction-determinism | db-index | Lean | extract_index_values is pure: array-expansion × Cartesian product yields exactly the product of per-field cardinalities, row-major order, tuple arity = #index fields | low |
| cartesian-product-laws | db-index | Lean | len(prod)=∏len(set_i); empty input → [[]] (unit); every tuple has input arity; row-major/lex ordering | low |
| LWW tie-break key is a total order | crdt | Lean | (u64 priority, lex value-bytes, tombstone) comparison is a deterministic total order, justifying resolvedKey:=max abstraction | low |
| Encoding round-trip / determinism | storage | Lean | decode(encode(x))==x for all types; decode consumes exactly the encoded bytes (injective deterministic encoding) | low |
| EncryptedStore key-binding soundness | storage | either | AES-256-GCM with storage key as AAD: wrong-key or relocated value always surfaces a decrypt error, never silent plaintext/garbage | low |
| capability rate-limiter fairness/liveness | p2p | TLA+ | Rate-obeying peer never permanently starved (liveness); flooding peer bounded to capacity+refill per interval (safety) | low |
| Merkle batch-root determinism | defra-core | Lean | compute_merkle_root invariant under input permutation (CIDs sorted); distinct CID sets → distinct roots modulo SHA256 | low |
| DocID parse/format round-trip | defra-core | Lean | from_string∘to_string=id, from_bytes∘to_bytes=id; version≠V0 always rejected; uvarint encode/decode total and invertible | low |
| Token temporal validity under clock skew | identity | TLA+ | With skew S, token accepted iff nbf≤now+S and exp+S≥now; under bounded clock divergence no expired token accepted, no fresh one spuriously rejected at boundaries | low |
| DID derivation determinism | identity | Lean | did() is a pure deterministic function of the public key; equal keys ⟹ equal DIDs | low |
| Definition-update immutability soundness | schema | Lean | UPDATE_VALIDATORS sound+total over protected fields: any accepted patch preserves name/version_id/collection_id/policy/indexes/branchable/field props/order | low |
| Single-active-version / relation-primary invariant | schema | Lean | At most one active version per collection_id; each named relation has exactly one primary side across its two collections | low |
| cache↔storage coherence | blockstore | TLA+ | Block LRU and merged-CID LRU never diverge from committed storage; no stale positive survives a delete or eviction | low |

---

## 3. OUT OF SCOPE (`model_worthy=false`)

Plumbing/glue crates with no novel modelable invariant; behavior covered by
integration tests, Go FFI parity, or unit tests. One line each on why.

- **blockstore** — content-addressing + LRU caching + a clean trait surface; merge-tracking already modeled at protocol level; CID verify reduces to the assumed hash boundary.
- **cli** — clap parsing + composition-root wiring; syncers/pushers delegate to db_merge/p2p/acp already modeled by B3 + replicator slices.
- **crypto** — stateless sign/verify/encrypt/hash/encode; correctness rests on vetted RustCrypto crates; round-trips pinned by go_compat parity tests.
- **cursor** — pure base64url codec; round-trip + Go byte-parity pinned by go_fixtures cross-language tests.
- **datastore** — txn-wrapper + namespace-prefix shim; lifecycle enforced by Rust ownership; real isolation lives in storage backends.
- **db-backup** — JSON (de)serialization + deterministic 3-phase FK-remap transform; covered by `--test backup`; content-addressing belongs to document layer.
- **db-blocks** — deterministic single-txn block construction; the produce-side twin of already-modeled CRDT/convergence/content-addressing properties.
- **db-search** — linear embed→query→fuse flow; only RRF total-order determinism, a low-value v1 heuristic de-risked by a doc_id tie-break.
- **db-nac** — config-aware wrapper; the real NAC state machine lives in crates/acp; auth gate already in the Auth slice.
- **defra-node** — pure assembly/wiring; all emergent protocol behavior delegated to crates with existing slices.
- **defra-version** — compile-time version/build metadata; immutable string/JSON shapes covered by unit tests.
- **document** — pure single-threaded value handling; only content-addressing determinism, already assumed by Convergence/Integrity and recorded in db-blocks survey.
- **embedded** — node-assembly Arc-graph + spawn/shutdown/recovery; retry/resume/convergence already in Replicator + Convergence slices.
- **events** — in-process tokio-mpsc fan-out; drop-counting is a single-atomic invariant covered by tokio unit tests; end-to-end consistency in consumer slices.
- **ffi** — extern "C" marshalling over an opaque handle registry; correctness-bearing behavior delegated to crates with existing slices; covered by Go suite.
- **http** — routing + JSON serde + auth-gate enforcement boundary; the auth state machine is exactly the existing Auth slice; routes pinned by unit test.
- **keyring** — stateless CRUD over filesystem/OS keyring; KeyName path-traversal covered by unit tests; key distribution lives in kms.
- **kms** — genuine adversarial key-distribution state machine, but already fully modeled by the committed KMS TLA+ slice (no new work).
- **lens** — visited-set-guarded walk over a path-collapsed version DAG; termination evident from the guard; inverse-law lives in opaque user WASM.
- **orbis** — gRPC sign-delegation client; sync→async runtime-hop is a local Tokio idiom; all proof-worthy behavior delegated to crypto/Auth/Integrity/Acp.
- **p2p-adapter** — HTTP-facing adapter delegating to p2p/db/db-merge/acp; sync-completion loop is advisory-only; convergence proven downstream.
- **pg-compat** — stateless SQL→GraphQL transpiler + pgwire IO; auth check is a one-line DID==username guard; translation pinned by integration tests.
- **query** — deterministic single-node dataflow + trait-seam glue; every security/replication concern delegated to already-modeled crates.
- **query-types** — query-engine type vocabulary; filter eval/split are local GraphQL semantics pinned by filter_tests + Go parity.
- **schema** — pure deterministic definition logic; content-addressing core lives in defra-core; validation = "matches Go" via FFI parity.
- **sourcehub** — on-chain ACP client plumbing; the security-critical cache state machine is already in the Acp slice; light-client trust is external.
- **telemetry** — OpenTelemetry exporter glue; only a once-per-process latch + dedup string filter, covered by unit tests.
- **wasm** — wasm-bindgen JS-boundary shim; only a 2-state open/closed lifecycle; substantive logic re-exported from already-modeled crates.
- **zanzibar** — entire correctness surface already proven by the acp Lean slice (`Acp/Soundness.lean`); remaining modules are deterministic glue.
