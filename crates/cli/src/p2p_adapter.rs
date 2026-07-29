//! Adapter to bridge P2PHostHandle to HTTP's P2POperations trait.
//!
//! Re-exports the shared libp2p implementation from `defra-p2p-adapter`.

pub use crate::p2p_doc_pusher::{DbDocPusher, DocPusher};
#[allow(unused_imports)] // retained for `crate::p2p_adapter::Foo` call sites
pub use defra_p2p_adapter::{CollectionLookup, P2PAdapter, VersionSyncer};
