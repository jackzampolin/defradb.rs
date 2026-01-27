//! DefraDB Browser Client
//!
//! WASM build of DefraDB for browser-based applications.
//!
//! # Features
//!
//! - **Schema Management**: Define document types using GraphQL SDL
//! - **GraphQL Queries**: Execute queries against local data
//! - **Merkle Proof Verification**: Verify data integrity from indexers
//! - **Document Sync**: Merge incoming documents with CRDT conflict resolution
//!
//! # Architecture
//!
//! This client is designed for the Shinzo wallet browser extension:
//!
//! ```text
//! Shinzo Indexer (Go DefraDB + P2P)
//!     ↓ HTTP API (Documents + Merkle Proofs)
//!     ↓
//! Browser WASM DefraDB (this crate)
//!     ├── Verify proofs
//!     ├── CRDT merge
//!     ├── Store locally (Memory or IndexedDB)
//!     └── Query locally
//! ```
//!
//! # Example
//!
//! ```javascript
//! import init, { DefraClient, verify_merkle_proof } from 'defra-wasm';
//!
//! await init();
//!
//! // Create client with in-memory storage
//! const client = await new DefraClient({ storage: 'memory' });
//!
//! // Add schema
//! await client.add_schema(`
//!   type Account {
//!     address: String
//!     balance: Int
//!   }
//! `);
//!
//! // Query data
//! const result = await client.query('{ Account { address balance } }');
//! console.log(result);
//!
//! // Verify a proof from the indexer
//! const isValid = verify_merkle_proof(proofJson);
//!
//! // Cleanup
//! await client.close();
//! ```
//!
//! # Storage Backends
//!
//! - **Memory**: Fast, ephemeral storage (data lost on page refresh)
//! - **IndexedDB**: Persistent browser storage (coming soon)

pub mod bindings;
pub mod client;
pub mod error;
pub mod sdl;
pub mod storage;
pub mod verification;

// Re-export the main client class
pub use client::DefraClient;

// Re-export standalone verification functions
pub use verification::{
    compute_document_cid, generate_ed25519_keypair, generate_secp256k1_keypair, sha256_hash,
    verify_ed25519_signature, verify_merkle_proof, verify_merkle_proof_cbor,
    verify_secp256k1_signature, verify_signed_proof,
};

use wasm_bindgen::prelude::*;

/// Initialize the WASM module.
///
/// This sets up panic hooks for better error messages in the browser console.
/// Called automatically when importing the module, but can be called explicitly.
#[wasm_bindgen(start)]
pub fn wasm_init() {
    // Set panic hook for better error messages
    console_error_panic_hook::set_once();
}

/// Get the version of the DefraDB WASM client.
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let v = version();
        assert!(!v.is_empty());
    }
}
