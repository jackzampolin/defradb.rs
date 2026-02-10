//! Lens IPLD block types for P2P sync.
//!
//! These types match Go's `lens/host-go/store/block.go` IPLD schema.
//! Used to deserialize lens blocks fetched via Bitswap during P2P sync.

use cid::Cid;
use serde::Deserialize;

/// Top-level lens config block containing links to module blocks.
#[derive(Debug, Deserialize)]
pub struct LensConfigBlock {
    pub modules: Vec<Cid>,
}

/// Lens module block with configuration for a single WASM module.
#[derive(Debug, Deserialize)]
pub struct LensModuleBlock {
    pub inverse: bool,
    pub arguments: Vec<LensKeyValue>,
    pub lens: Cid,
}

/// Key-value pair for lens module arguments.
#[derive(Debug, Deserialize)]
pub struct LensKeyValue {
    pub key: String,
    pub value: String,
}

/// Lens WASM block containing WASM bytes or links to chunks.
///
/// Go uses a keyed union: `{"wasmBytes": <bytes>}` or `{"chunks": [<CID>...]}`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum LensWasmBlock {
    Direct {
        #[serde(rename = "wasmBytes")]
        wasm_bytes: Vec<u8>,
    },
    Chunked {
        chunks: Vec<Cid>,
    },
}

impl LensWasmBlock {
    /// Extract WASM bytes, returning None if chunked (caller must resolve chunks).
    pub fn wasm_bytes(&self) -> Option<&[u8]> {
        match self {
            LensWasmBlock::Direct { wasm_bytes } => Some(wasm_bytes),
            LensWasmBlock::Chunked { .. } => None,
        }
    }

    /// Get chunk CIDs if this is a chunked block.
    pub fn chunks(&self) -> Option<&[Cid]> {
        match self {
            LensWasmBlock::Direct { .. } => None,
            LensWasmBlock::Chunked { chunks } => Some(chunks),
        }
    }
}
