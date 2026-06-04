//! TLA axis: drive the real release binary through each family's witnessing
//! scenario and assert the modeled invariant holds against the artifact.
//!
//! Each behavioral check is a submodule under `behavioral/`, bound to a registry
//! Property (`conformance::registry::PROPERTIES`). Order-preserving encoding is
//! intentionally absent here — a plain ordered query may sort in memory rather
//! than exercise the index key encoding, so it is marked `Boundary` in the
//! registry (the byte table is verified against Lean statically instead).

#[path = "support.rs"]
mod support;

#[path = "behavioral/cid.rs"]
mod cid;

#[path = "behavioral/acp.rs"]
mod acp;

#[path = "behavioral/deferred_acp.rs"]
mod deferred_acp;

#[path = "behavioral/replication.rs"]
mod replication;

#[path = "behavioral/partition.rs"]
mod partition;

#[path = "behavioral/parity.rs"]
mod parity;

#[path = "behavioral/bughunt.rs"]
mod bughunt;

#[path = "behavioral/index.rs"]
mod index;

#[path = "behavioral/nac.rs"]
mod nac;

#[path = "behavioral/nac_lifecycle.rs"]
mod nac_lifecycle;

#[path = "behavioral/replicator_lifecycle.rs"]
mod replicator_lifecycle;

#[path = "behavioral/ssi.rs"]
mod ssi;

#[path = "behavioral/kms.rs"]
mod kms;
