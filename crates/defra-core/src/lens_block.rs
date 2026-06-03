//! Lens IPLD block types for P2P sync.
//!
//! These types match Go's `lens/host-go/store/block.go` IPLD schema.
//! Used to serialize/deserialize lens blocks for Bitswap during P2P sync.

use cid::Cid;
use serde::{Deserialize, Serialize};

/// Top-level lens config block containing links to module blocks.
#[derive(Debug, Serialize, Deserialize)]
pub struct LensConfigBlock {
    pub modules: Vec<Cid>,
}

/// Lens module block with configuration for a single WASM module.
#[derive(Debug, Serialize, Deserialize)]
pub struct LensModuleBlock {
    pub inverse: bool,
    pub arguments: Vec<LensKeyValue>,
    pub lens: Cid,
}

/// Key-value pair for lens module arguments.
#[derive(Debug, Serialize, Deserialize)]
pub struct LensKeyValue {
    pub key: String,
    pub value: String,
}

/// Lens WASM block containing WASM bytes or links to chunks.
///
/// Go uses a keyed union: `{"wasmBytes": <bytes>}` or `{"chunks": [<CID>...]}`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum LensWasmBlock {
    Direct {
        #[serde(rename = "wasmBytes", with = "serde_bytes")]
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

/// A CID paired with its serialized bytes.
pub type CidBlock = (Cid, Vec<u8>);

/// Returns `true` if `bytes` decode as any known lens IPLD block shape —
/// the top-level config, a module block, a WASM block (direct or chunked),
/// or a raw WASM chunk byte string.
///
/// Mirrors Go DefraDB's `hasAccess` passthrough for
/// `lens` / `modules` / `wasmBytes` / `chunks` blocks
/// (`internal/db/p2p/p2p.go:335-348`). The Bitswap access filter uses this
/// to let lens schema-migration blocks propagate in `Controlled` mode
/// without per-collection replicator trust (they carry no user data).
pub fn is_lens_block(bytes: &[u8]) -> bool {
    use serde_bytes::ByteBuf;

    // Most specific structural shapes first — failures here are fast and
    // the order isn't behaviourally significant (any match → allow).
    if serde_ipld_dagcbor::from_slice::<LensModuleBlock>(bytes).is_ok() {
        return true;
    }
    if serde_ipld_dagcbor::from_slice::<LensWasmBlock>(bytes).is_ok() {
        return true;
    }
    if serde_ipld_dagcbor::from_slice::<LensConfigBlock>(bytes).is_ok() {
        return true;
    }
    // Chunked WASM stores each chunk as a bare CBOR byte string.
    serde_ipld_dagcbor::from_slice::<ByteBuf>(bytes).is_ok()
}

/// Maximum size for a single WASM block before chunking.
/// Must be well below iroh-bitswap's MAX_BUF_SIZE (2 MB) to account for
/// CBOR encoding overhead and Bitswap message framing.
const MAX_BLOCK_SIZE: usize = 256 * 1024; // 256 KB

/// Build the 3-level IPLD block hierarchy for a lens module.
///
/// For WASM files larger than MAX_BLOCK_SIZE, the bytes are chunked into
/// multiple blocks and the `LensWasmBlock::Chunked` variant is used.
///
/// Returns (config_block_cid, vec of CidBlock pairs for all blocks to store).
pub fn build_lens_ipld_blocks(
    wasm_bytes: &[u8],
    inverse: bool,
    arguments: &[(String, String)],
) -> Result<(Cid, Vec<CidBlock>), String> {
    use crate::{DAG_CBOR_CODEC, SHA2_256_CODE};
    use multihash::Multihash;
    use sha2::{Digest, Sha256};

    let compute_cid = |bytes: &[u8]| -> Result<Cid, String> {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mh = Multihash::<64>::wrap(*SHA2_256_CODE, &digest)
            .map_err(|e| format!("multihash: {}", e))?;
        Ok(Cid::new_v1(*DAG_CBOR_CODEC, mh))
    };

    let mut blocks = Vec::new();

    // Level 3: LensBlock (WASM bytes, possibly chunked)
    let lens_cid = if wasm_bytes.len() <= MAX_BLOCK_SIZE {
        // Small enough for a single block
        let lens_block = LensWasmBlock::Direct {
            wasm_bytes: wasm_bytes.to_vec(),
        };
        let lens_bytes = serde_ipld_dagcbor::to_vec(&lens_block)
            .map_err(|e| format!("encode lens block: {}", e))?;
        let cid = compute_cid(&lens_bytes)?;
        blocks.push((cid, lens_bytes));
        cid
    } else {
        // Chunk the WASM bytes into smaller blocks
        let mut chunk_cids = Vec::new();
        for chunk in wasm_bytes.chunks(MAX_BLOCK_SIZE) {
            // Each chunk is stored as a DAG-CBOR encoded byte string
            let chunk_bytes = serde_ipld_dagcbor::to_vec(&serde_bytes::ByteBuf::from(chunk))
                .map_err(|e| format!("encode chunk: {}", e))?;
            let chunk_cid = compute_cid(&chunk_bytes)?;
            blocks.push((chunk_cid, chunk_bytes));
            chunk_cids.push(chunk_cid);
        }
        // Create the chunked lens block referencing all chunks
        let lens_block = LensWasmBlock::Chunked { chunks: chunk_cids };
        let lens_bytes = serde_ipld_dagcbor::to_vec(&lens_block)
            .map_err(|e| format!("encode chunked lens block: {}", e))?;
        let cid = compute_cid(&lens_bytes)?;
        blocks.push((cid, lens_bytes));
        cid
    };

    // Level 2: ModuleBlock
    let module_block = LensModuleBlock {
        inverse,
        arguments: arguments
            .iter()
            .map(|(k, v)| LensKeyValue {
                key: k.clone(),
                value: v.clone(),
            })
            .collect(),
        lens: lens_cid,
    };
    let module_bytes = serde_ipld_dagcbor::to_vec(&module_block)
        .map_err(|e| format!("encode module block: {}", e))?;
    let module_cid = compute_cid(&module_bytes)?;
    blocks.push((module_cid, module_bytes));

    // Level 1: ConfigBlock
    let config_block = LensConfigBlock {
        modules: vec![module_cid],
    };
    let config_bytes = serde_ipld_dagcbor::to_vec(&config_block)
        .map_err(|e| format!("encode config block: {}", e))?;
    let config_cid = compute_cid(&config_bytes)?;
    blocks.push((config_cid, config_bytes));

    Ok((config_cid, blocks))
}
