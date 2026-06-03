# pg-compat — formal-modelability survey

## Purpose
Postgres wire-protocol compatibility server. Accepts `psql`/PG-driver connections,
translates SQL (via `sqlparser`) into GraphQL, executes through the existing
`QueryExecutor`, and encodes results back into the PG text wire format. Supports
simple + extended query protocols, DID+JWT startup auth, transaction control
(BEGIN/COMMIT/ROLLBACK delegating to `TransactionHandle`), DDL→SDL bridging,
joins/aggregates/set-ops/distinct/subquery lowering, and synthetic catalog queries.

## State machines
- **Auth startup handshake** (`handler/auth.rs`): `Startup → {anonymous→Authenticated}
  | {DID user → AuthenticationInProgress → (verify JWT, DID==username) → Authenticated
  | Rejected}`. A real security state machine, but it is a thin wrapper: the actual
  token parse / signature / audience / DID-match checks all delegate to the `identity`
  crate. The only pg-compat-specific rule is `token_did == username`.
- **Upsert decision** (`handler/execute.rs`): check-query → exists ? Update : Insert.
  Read-then-write, not atomic at this layer; atomicity (if any) lives in the executor/txn.
- **Txn control**: BEGIN/COMMIT/ROLLBACK store/clear a `txn_id` in connection metadata;
  semantics owned by `query`/`db` (TransactionHandle), not here.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | SQL→GraphQL translation is syntactic plumbing; correctness is per-shape and covered by `integration-test --test ...` + `bridge/tests.rs` | — | — |
| auth DID==username gate | TLA+ | only a connection whose JWT issuer DID equals the supplied username authenticates | yes (auth slice covers mgmt-channel DID/JWT auth) | low |

## Verdict
**Not model-worthy.** This crate is a stateless SQL→GraphQL transpiler plus IO glue.
No concurrency, replication, eventual consistency, content-addressing, or CRDT algebra
originates here — all such behavior is delegated to `query`, `db`, `identity`. The auth
handshake is the only state machine, and its security-relevant logic (JWT verify,
audience, DID extraction) belongs to `identity` and is already abstracted by the
existing **auth** TLA+ slice; the residual `token_did == username` check is a one-line
guard better covered by an integration test than a model. Translation correctness is
exercised exhaustively by the per-shape integration suites and `bridge/tests.rs`.
