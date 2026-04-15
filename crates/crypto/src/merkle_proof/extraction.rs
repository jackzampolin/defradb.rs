//! Merkle proof extraction from blockstores.

use cid::Cid;
use defra_core::block::Block;
use defra_core::{Error, Result};
use std::collections::{HashMap, HashSet, VecDeque};

use super::proof::MerkleProof;
use super::proof_node::ProofNode;

/// Blockstore trait for proof extraction
///
/// This trait abstracts block retrieval to allow proof extraction from
/// any block storage implementation.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait ProofBlockstore: defra_core::thread_bounds::MaybeSendSync {
    /// Get a block by CID
    async fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
}

/// Extract a Merkle proof from a blockstore
///
/// Traverses from the leaf block to the root by following the `heads` references.
/// Uses BFS to find the shortest path when multiple paths exist.
///
/// # Arguments
///
/// * `blockstore` - The block storage to retrieve blocks from
/// * `leaf_cid` - The CID of the leaf block to start from
/// * `root_cid` - The CID of the root block to reach
///
/// # Returns
///
/// * `Ok(Some(proof))` - A valid proof if a path exists
/// * `Ok(None)` - No path exists from leaf to root
/// * `Err(e)` - An error occurred during traversal
pub async fn extract_proof<B: ProofBlockstore>(
    blockstore: &B,
    leaf_cid: Cid,
    root_cid: Cid,
) -> Result<Option<MerkleProof>> {
    // Handle trivial case: leaf == root
    if leaf_cid == root_cid {
        let block_data = blockstore
            .get_block(&leaf_cid)
            .await?
            .ok_or_else(|| Error::BlockError(format!("Block not found: {}", leaf_cid)))?;

        let node = ProofNode {
            cid: leaf_cid,
            data: block_data,
        };
        return Ok(Some(MerkleProof::new(leaf_cid, root_cid, vec![node])));
    }

    // BFS to find path from leaf to root
    // We traverse "forward" through heads references
    let mut visited: HashSet<Cid> = HashSet::new();
    let mut parent_map: HashMap<Cid, Cid> = HashMap::new(); // parent -> child (who discovered this parent during BFS)
    let mut block_cache: HashMap<Cid, Vec<u8>> = HashMap::new();
    let mut queue: VecDeque<Cid> = VecDeque::new();

    // Start from leaf
    let leaf_data = blockstore
        .get_block(&leaf_cid)
        .await?
        .ok_or_else(|| Error::BlockError(format!("Leaf block not found: {}", leaf_cid)))?;

    block_cache.insert(leaf_cid, leaf_data);
    visited.insert(leaf_cid);
    queue.push_back(leaf_cid);

    // BFS traversal following heads references.
    // DoS protection: bounded by the size of the blockstore (caller's responsibility),
    // not by a hardcoded limit here. Go has no equivalent cap on chain depth.
    while let Some(current_cid) = queue.pop_front() {
        // Get the current block data
        let block_data = if let Some(data) = block_cache.get(&current_cid) {
            data.clone()
        } else {
            let data = blockstore
                .get_block(&current_cid)
                .await?
                .ok_or_else(|| Error::BlockError(format!("Block not found: {}", current_cid)))?;
            block_cache.insert(current_cid, data.clone());
            data
        };

        let block = Block::from_dag_cbor(&block_data)?;

        // In DefraDB's Merkle structure, newer blocks point to older blocks via heads.
        // Following heads from leaf toward root naturally traverses to ancestors.

        if let Some(heads) = &block.heads {
            for parent_cid in heads {
                if !visited.contains(parent_cid) {
                    // Fetch parent block - this must exist if referenced
                    let parent_data = blockstore.get_block(parent_cid).await?.ok_or_else(|| {
                        Error::BlockError(format!(
                            "Missing parent block {} referenced by {}",
                            parent_cid, current_cid
                        ))
                    })?;

                    block_cache.insert(*parent_cid, parent_data);
                    visited.insert(*parent_cid);
                    parent_map.insert(*parent_cid, current_cid);
                    queue.push_back(*parent_cid);

                    // Check if we reached the root
                    if *parent_cid == root_cid {
                        // Reconstruct path from root back to leaf
                        let path = reconstruct_path(&parent_map, &block_cache, leaf_cid, root_cid)?;
                        return Ok(Some(MerkleProof::new(leaf_cid, root_cid, path)));
                    }
                }
            }
        }
    }

    // No path found
    Ok(None)
}

/// Reconstruct the path from leaf to root using the parent map
fn reconstruct_path(
    parent_map: &HashMap<Cid, Cid>,
    block_cache: &HashMap<Cid, Vec<u8>>,
    leaf_cid: Cid,
    root_cid: Cid,
) -> Result<Vec<ProofNode>> {
    // Build path from root back to leaf following parent_map
    let mut path = Vec::new();
    let mut current = root_cid;

    // Start at root
    let root_data = block_cache
        .get(&root_cid)
        .ok_or_else(|| Error::BlockError("Root block not in cache".to_string()))?;
    path.push(ProofNode {
        cid: root_cid,
        data: root_data.clone(),
    });

    // Follow the chain back to leaf
    while current != leaf_cid {
        let child = parent_map
            .get(&current)
            .ok_or_else(|| Error::BlockError("Path reconstruction failed".to_string()))?;

        let child_data = block_cache
            .get(child)
            .ok_or_else(|| Error::BlockError("Block not in cache".to_string()))?;

        path.push(ProofNode {
            cid: *child,
            data: child_data.clone(),
        });

        current = *child;
    }

    // Reverse to get leaf -> root order
    path.reverse();
    Ok(path)
}
