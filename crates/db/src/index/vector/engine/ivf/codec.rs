//! Encoding shared by every coarse-quantized engine: centroids, inverted-list
//! keys, and the marker that a coarse build has completed.

use crate::index::error::{Error, Result};
use crate::index::vector::engine::ann::Centroids;
use crate::index::vector::store::{NodeId, VectorNodeStore};

pub const CENTROID: u8 = b'c';
pub const LIST: u8 = b'l';
pub const STATE: u8 = b's';

const STATE_VERSION: u8 = 1;

pub fn encode_vector(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

pub fn decode_vector(bytes: &[u8]) -> Result<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
        return Err(Error::Other(
            "vector index: a stored vector is ragged".into(),
        ));
    }
    Ok(bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|&c| f32::from_le_bytes(c))
        .collect())
}

pub fn encode_centroids(centroids: &Centroids) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + centroids.values.len() * 4);
    out.extend_from_slice(&(centroids.k as u32).to_le_bytes());
    out.extend_from_slice(&(centroids.dimensions as u32).to_le_bytes());
    out.extend_from_slice(&encode_vector(&centroids.values));
    out
}

pub fn decode_centroids(bytes: &[u8]) -> Result<Centroids> {
    if bytes.len() < 8 {
        return Err(Error::Other("vector index: a codebook is truncated".into()));
    }
    let k = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let dimensions = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    let values = decode_vector(&bytes[8..])?;
    if k * dimensions != values.len() {
        return Err(Error::Other(
            "vector index: a codebook's shape disagrees with its payload".into(),
        ));
    }
    Ok(Centroids {
        k,
        dimensions,
        values,
    })
}

/// `listID || nodeID`, both big-endian so a prefix scan of a list is contiguous
/// and ordered.
pub fn list_key(list: u32, node: NodeId) -> Vec<u8> {
    let mut key = Vec::with_capacity(12);
    key.extend_from_slice(&list.to_be_bytes());
    key.extend_from_slice(&node.0.to_be_bytes());
    key
}

pub fn list_prefix(list: u32) -> Vec<u8> {
    list.to_be_bytes().to_vec()
}

pub fn node_from_list_key(key: &[u8]) -> Result<NodeId> {
    if key.len() < 12 {
        return Err(Error::Other("vector index: a list key is truncated".into()));
    }
    let mut id = [0u8; 8];
    id.copy_from_slice(&key[4..12]);
    Ok(NodeId(u64::from_be_bytes(id)))
}

/// What a coarse build needs before it reads anything else: the number of
/// centroids and the vector width.
///
/// IVF-PQ's own trained state additionally carries `m`, the subquantizer
/// count, so it keeps a wider struct and its own codec in `ivfpq::codec`
/// rather than growing this one with a field that means nothing to IVF_FLAT;
/// changing this shape would also change IVF-PQ's on-disk encoding and stop an
/// index trained before this split from decoding. IVF_FLAT has nothing beyond
/// `nlist` and `dimensions`, so it uses this state directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrainedState {
    pub nlist: u32,
    pub dimensions: u32,
}

pub fn encode_state(state: &TrainedState) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(STATE_VERSION);
    out.extend_from_slice(&state.nlist.to_le_bytes());
    out.extend_from_slice(&state.dimensions.to_le_bytes());
    out
}

pub fn decode_state(bytes: &[u8]) -> Result<TrainedState> {
    if bytes.len() != 9 || bytes[0] != STATE_VERSION {
        return Err(Error::Other(
            "vector index: unsupported trained-state encoding".into(),
        ));
    }
    let read =
        |at: usize| u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);
    Ok(TrainedState {
        nlist: read(1),
        dimensions: read(5),
    })
}

/// Loads every centroid a build wrote, in order, from `state`.
pub async fn load_centroids<S: VectorNodeStore>(
    store: &S,
    nlist: u32,
    dimensions: u32,
) -> Result<Centroids> {
    let mut values = Vec::with_capacity(nlist as usize * dimensions as usize);
    for index in 0..nlist {
        let bytes = store
            .get_aux(CENTROID, &index.to_be_bytes())
            .await?
            .ok_or_else(|| Error::Other(format!("vector index: centroid {index} is missing")))?;
        values.extend_from_slice(&decode_vector(&bytes)?);
    }
    Ok(Centroids {
        k: nlist as usize,
        dimensions: dimensions as usize,
        values,
    })
}
