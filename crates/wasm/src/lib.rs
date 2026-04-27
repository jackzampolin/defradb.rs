//! DefraDB Browser Client
//!
//! WASM build of DefraDB for browser-based applications.
//!
//! # Features
//!
//! - **Schema Management**: Define document types using GraphQL SDL
//! - **GraphQL Queries**: Execute queries against local data
//! - **Merkle Proof Verification**: Verify data integrity from indexers
//!
//! # Architecture
//!
//! This client wraps the core `db` crate, providing a JavaScript-friendly
//! interface to DefraDB's full functionality:
//!
//! ```text
//! JavaScript (browser)
//!     ↓ wasm-bindgen
//! defra-wasm (this crate)
//!     ↓
//! db crate (DB, Collection, QueryRunner)
//!     ↓
//! storage crate (LevelDbStore)
//! ```
//!
//! # Example
//!
//! ```javascript
//! import init, { DefraClient, verify_merkle_proof } from 'defra-wasm';
//!
//! await init();
//!
//! // Create client
//! const client = await DefraClient.create({ storage: 'leveldb' });
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

pub mod bindings;
#[cfg(target_arch = "wasm32")]
pub mod client;
pub mod error;
#[cfg(target_arch = "wasm32")]
mod storage_tests;
pub mod verification;

// Re-export the main client class
#[cfg(target_arch = "wasm32")]
pub use client::DefraClient;

// Re-export standalone verification functions
pub use verification::{
    compute_document_cid, generate_ed25519_keypair, generate_secp256k1_keypair, sha256_hash,
    verify_ed25519_signature, verify_secp256k1_signature,
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
