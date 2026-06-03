# Survey: `crates/query/`

## Purpose
The GraphQL query engine. Volcano-iterator pipeline:
`GraphQL string → parse → map → plan nodes → execute → results`. Also hosts
mutation execution (`runner/mutation.rs`), `_commits` reads
(`runner/commits.rs`), REST collection doc-id pagination (`rest/`),
subscription event→query conversion (`subscription.rs`), and transaction
guards (`txn/`). Types/parsing/plan layers are split into `query-types`,
`query-parse`, `query-plan`. Most logic is plumbing over those layers.

## State machines
- **TransactionGuard** (`txn/guard.rs`): begin → execute* → commit | rollback,
  enforced as a linear type (consumed on finalize, Drop logs a leak). This is a
  compile-time discipline, not a concurrent protocol — nothing to model.
- **BroadcastStatus** (`query-plan/mutator.rs`): flat result enum
  (Success/Failed/Pending/NotAttempted), no transitions.
- **Injection seams** for security/replication: `DocumentACP`, `NacChecker`,
  `SeQueryTransport` traits are *declared* here but *implemented* in `acp`,
  `db-merge`, `p2p`. The query crate only calls them; the actual access state
  machines live elsewhere and are already modeled.

## Modelable candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Commits/User dual-path ACP gating | TLA+ | unauthorized reader gets content via neither User nor `_commits` path | yes (Commits slice, `INV_BothPathsGated`) | — |
| SE remote-query confidentiality | TLA+ | encrypted `_eq` fan-out leaks no plaintext to non-recipients | yes (KMS/Integrity boundary; SE math assumed) | — |
| Management/query NAC gate | TLA+ | no execution without fresh scope-correct actor permission | yes (Auth slice) | — |

All three are *boundary seams* in this crate; the proofs target their real
implementations. No novel candidate originates in `crates/query/`.

## Verdict
**Plumbing.** The query engine is deterministic single-node dataflow
(parse/plan/execute) plus glue that delegates every security and replication
concern through traits to already-modeled crates (`acp`, `db-merge`, `p2p`).
The transaction guard is a linear-type safety pattern, not a concurrency model;
cursor round-trip parity is covered by `cursor.md` (rejected there). Volcano
correctness and ACP-filtering wiring are exercised by the `query`/`acp`/`p2p`
integration suites. `model_worthy: false`; no new candidates.
