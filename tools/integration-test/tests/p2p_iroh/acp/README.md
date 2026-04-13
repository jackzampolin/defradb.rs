# ACP (Access Control Policy) Tests

These tests cover ACP behavior over iroh transport, including:
- local ACP replication behavior
- SourceHub-backed DAC permissioned replication
- SourceHub-backed document-actor relationship propagation
- NAC trust-boundary behavior

## Files

- `acp.rs` — Local ACP policy enforcement with iroh transport
- `dac.rs` — DAC permissioned replication, including the Go `replicator_with_doc_actor_relationship` and `subscribe_with_doc_actor_relationship` parity cases via SourceHub-backed relationship propagation
- `nac.rs` — Node Access Control via SourceHub
- `trust_boundary.rs` — Trust boundary enforcement between iroh peers

## Environment

Some `dac.rs` and `nac.rs` tests require a SourceHub test environment. In this repo that means
`sourcehubd` must be resolvable via `SOURCEHUB_BINARY`, `SOURCEHUB_WORKSPACE`, or `PATH`.

Without `sourcehubd`, those tests fail at harness startup rather than at ACP assertion time.
