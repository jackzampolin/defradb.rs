//! Key Management Service for DefraDB.
//!
//! This crate is a design-time stub. The full implementation lands across
//! milestones M1–M7, each on its own branch:
//!
//! - M1: `KmsService` trait, `DefraKms`, `MemoryKeyStore`, `NoopKms`,
//!   `NacDacPolicy`, Go wire-compatible `FetchEncryptionKeyRequest`/`Reply`.
//! - M2: Iroh transport adapter, multi-transport composition.
//! - M3: `KeyringKeyStore` (FileKeyring + SystemKeyring), KEK lifecycle,
//!   `WrappingHeader` on the `Encryption` block.
//! - M4: `SourceHubAttestedPolicy`, `CombinedPolicy`, reply attestation.
//! - M5: `EnclaveKeyStore`, `EnclaveUnwrap` trait, `DefraEnclaveEcdhCallback` FFI.
//! - M6: `ThresholdKeyStore` (Orbis ring).
//! - M7: `SeKeyProvider` to subsume the FFI searchable-encryption key path.
//!
//! Tracks Go DefraDB's `internal/kms/` package and the NAC-aware fix from
//! Go PR #4778 (commit `1fab9fb3`). See the design comment on
//! defradb.rs issue #976 for the full spec.

mod error;
pub use error::{Error, Result};

mod types;
pub use types::{EncryptionCid, KeyScope, PolicyDecision};

mod context;
pub use context::RequestContext;

mod results;
pub use results::{KeyResults, ResolvedKey, ResultsReceiver, ResultsSender};

mod wire;
pub use wire::{FetchEncryptionKeyReply, FetchEncryptionKeyRequest};

mod service;
pub use service::{KmsService, PeerIdentity};

mod store;
pub use store::{KeyStore, StoredKey};

mod memory_store;
pub use memory_store::MemoryKeyStore;

mod transport;
pub use transport::{IncomingHandler, KeyTransport, SignedFetchRequest, TransportReplyStream};

mod ecies_envelope;
pub use ecies_envelope::{unwrap_with_private, wrap_for_requester};
