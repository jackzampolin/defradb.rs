//! Zanzibar permission model — defradb-specific integration layer.
//!
//! The core Zanzibar engine (types, expressions, evaluation, store trait,
//! memory store) lives in the standalone `zanzibar` crate.
//!
//! This module provides:
//! - `ZanzibarDocumentACP`: bridges Zanzibar engine to defradb's `DocumentACP` trait
//! - `PersistentZanzibarStore`: implements `ZanzibarStore` against defradb's storage layer

mod acp;
pub mod store;

pub use acp::ZanzibarDocumentACP;
#[cfg(not(target_arch = "wasm32"))]
pub use store::PersistentZanzibarStore;
