//! IPLD link extraction and missing link detection.

use std::collections::HashSet;

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

/// Recursively find ALL missing blocks in the DAG rooted at `block_data`.
///
/// Unlike `find_missing_links` (one level), this walks the entire DAG tree.
/// For multi-level DAGs (Collection → Composite → LWW), this discovers
/// missing blocks at any depth.
pub async fn find_all_missing_links<B: Blockstore>(
    blockstore: &B,
    block_data: &[u8],
) -> Result<Vec<Cid>> {
    let mut missing = Vec::new();
    let mut visited = HashSet::new();
    find_missing_recursive(blockstore, block_data, &mut missing, &mut visited).await?;
    Ok(missing)
}

/// Inner recursive link walker.
fn find_missing_recursive<'a, B: Blockstore + 'a>(
    blockstore: &'a B,
    block_data: &'a [u8],
    missing: &'a mut Vec<Cid>,
    visited: &'a mut HashSet<Cid>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        use libipld::multihash::{Code, MultihashDigest};
        let hash = Code::Sha2_256.digest(block_data);
        let dummy_cid = Cid::new_v1(0x71, hash);

        let mut refs = Vec::new();
        let block = Block::<DefaultParams>::new_unchecked(dummy_cid, block_data.to_vec());
        if let Err(e) = block.references(&mut refs) {
            let error_msg = e.to_string();
            if error_msg.contains("Unsupported codec") {
                return Ok(());
            }
            return Err(Error::BlockParseError {
                reason: format!("Failed to extract references: {}", e),
            });
        }

        for link_cid in refs {
            if visited.contains(&link_cid) {
                continue;
            }
            visited.insert(link_cid);

            match blockstore.has(&link_cid).await {
                Ok(true) => {
                    // Block exists — recursively check ITS links too
                    if let Ok(Some(child_data)) = blockstore.get(&link_cid).await {
                        find_missing_recursive(blockstore, &child_data, missing, visited).await?;
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
        Ok(())
    })
}
