# Survey: `crates/sourcehub/`

## Purpose
SourceHub ACP client: implements `DocumentACP`/`SourceHubProvider` against two on-chain
backends (Cosmos SDK x/acp via LCD+CometBFT RPC, and EVM "hub.rs" via Alloy). Builds and
signs txs, queries policies/relationships, and gates document access. Adds local caches
(access decisions, policy metadata), a circuit breaker, and a proof-validated light-client
cache invalidated by chain height / module-state-root advance.

## State machines
- **Circuit breaker** (`circuit_breaker.rs`): explicit Closed → Open → HalfOpen FSM over
  atomics. Trips Open after N consecutive failures; after `reset_timeout` a single HalfOpen
  probe closes (success) or re-trips (failure). Caller (`cosmos_provider.rs::with_circuit_breaker`)
  maps Open/timeout → `ProviderError::Unavailable`, which all access decisions treat as fail-closed.
- **Access-decision cache lifecycle** (`access_cache.rs` + `dac.rs::check_access`): TTL expiry +
  eager `invalidate_object` on every set/delete relationship; only *positive* decisions cached.
- **Light-client cache invalidation** (`hub_rs/provider.rs::run_light_client_observer`): on
  `module_state_root` change at a new height, `invalidate_stale` + publish events. Core proof
  logic lives in the external `acp-light-client` git dep (assumed boundary).
- EVM nonce reservation (`reserve_nonce_at_or_after`): CAS loop — plumbing.

## Candidates
| name | kind | property | already-modeled | priority |
|---|---|---|---|---|
| CircuitBreaker fail-closed FSM | TLA+ | no request passes while Open; HalfOpen admits exactly one probe; only a successful probe closes; Unavailable ⇒ fail-closed | partial — Acp/Auth models *assume* Unavailable fail-closes; the FSM transitions themselves are unmodeled | low |
| Access-cache stale-revocation | TLA+ | a revoked positive decision is never served from cache after relationship mutation | YES — `MC_Acp_Green` / `MC_Acp_StaleCache_Red` model `access_cache.rs` exactly | n/a |
| Light-client proof cache | TLA+/Lean | cache entry trusted only if validated against current module-state-root | no (logic in external `acp-light-client` dep) | low |

## Verdict
Mostly **plumbing/glue**: tx building, ABI/JSON (de)serialization, REST/RPC IO, bearer-token
construction. The one security-critical state machine, the access-decision cache under
revocation, is **already proven** by the existing Acp slice (which cites this crate's
`access_cache.rs` line-by-line). The circuit breaker is a real but small, well-unit-tested FSM
whose only load-bearing invariant (Unavailable ⇒ fail-closed) is already an assumed boundary in
the Acp/Auth models. Light-client proof validation is an external-dependency boundary.

**model_worthy: false** — no high/medium candidate that isn't already covered or external.
