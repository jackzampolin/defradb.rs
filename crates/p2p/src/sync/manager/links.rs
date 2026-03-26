//! IPLD link extraction and missing link detection.

use std::collections::{HashSet, VecDeque};

use bytes::Bytes;
use cid::Cid;
use libipld::{Block, DefaultParams};

use blockstore::Blockstore;

use crate::error::{Error, Result};

/// Find missing blocks by extracting links from the block data (one level).
///
/// This parses the block's IPLD structure and checks which linked CIDs
/// are not present in the blockstore.
pub async fn find_missing_links<B: Blockstore>(
    blockstore: &B,
    block_data: &[u8],
) -> Result<Vec<Cid>> {
    extract_references(blockstore, block_data).await
}

/// Extract IPLD references from block data and check which are missing.
async fn extract_references<B: Blockstore>(blockstore: &B, block_data: &[u8]) -> Result<Vec<Cid>> {
    let refs = extract_ipld_links(block_data)?;

    let mut missing = Vec::new();
    for link_cid in refs {
        match blockstore.has(&link_cid).await {
            Ok(true) => {}
            Ok(false) => {
                missing.push(link_cid);
            }
            Err(e) => {
                tracing::warn!(
                    cid = %link_cid,
                    error = %e,
                    "Failed to check if block exists, treating as missing"
                );
                missing.push(link_cid);
            }
        }
    }

    Ok(missing)
}

/// Parse IPLD block data and return all referenced CIDs.
fn extract_ipld_links(block_data: &[u8]) -> Result<Vec<Cid>> {
    use libipld::multihash::{Code, MultihashDigest};
    let hash = Code::Sha2_256.digest(block_data);
    let dummy_cid = Cid::new_v1(0x71, hash); // 0x71 = DAG-CBOR codec

    let mut refs = Vec::new();
    let block = Block::<DefaultParams>::new_unchecked(dummy_cid, block_data.to_vec());
    if let Err(e) = block.references(&mut refs) {
        let error_msg = e.to_string();
        if error_msg.contains("Unsupported codec") {
            return Ok(Vec::new());
        }
        return Err(Error::BlockParseError {
            reason: format!("Failed to extract references: {}. Block may be corrupt.", e),
        });
    }
    Ok(refs)
}

/// Iteratively find ALL missing blocks in the DAG rooted at `block_data`.
///
/// Walks the entire DAG using a work queue instead of recursion, so stack
/// depth is constant regardless of DAG depth. Uses `is_merged()` to
/// short-circuit traversal of already-merged subtrees.
pub async fn find_all_missing_links<B: Blockstore>(
    blockstore: &B,
    block_data: &[u8],
) -> Result<Vec<Cid>> {
    let mut missing = Vec::new();
    let mut visited = HashSet::new();
    let mut queue: VecDeque<Bytes> = VecDeque::new();
    queue.push_back(Bytes::copy_from_slice(block_data));

    while let Some(data) = queue.pop_front() {
        let refs = extract_ipld_links(&data)?;

        for link_cid in refs {
            if !visited.insert(link_cid) {
                continue;
            }

            // Early-exit: merged subtrees are complete, skip traversal
            match blockstore.is_merged(&link_cid).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        cid = %link_cid,
                        error = %e,
                        "Failed to check merge status, will traverse"
                    );
                }
            }

            match blockstore.has(&link_cid).await {
                Ok(true) => {
                    // Block exists but not merged — enqueue for further traversal
                    if let Ok(Some(child_data)) = blockstore.get(&link_cid).await {
                        queue.push_back(child_data);
                    }
                }
                Ok(false) => {
                    tracing::debug!(cid = %link_cid, "Missing block at depth in DAG");
                    missing.push(link_cid);
                }
                Err(e) => {
                    tracing::warn!(
                        cid = %link_cid,
                        error = %e,
                        "Failed to check if block exists, treating as missing"
                    );
                    missing.push(link_cid);
                }
            }
        }
    }

    Ok(missing)
}
