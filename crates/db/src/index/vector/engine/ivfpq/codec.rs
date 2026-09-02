//! IVF-PQ's own trained-state marker, on top of the coarse codec both IVF
//! engines share.

pub use crate::index::vector::engine::ivf::{
    decode_centroids, encode_centroids, encode_vector, list_key, list_prefix, node_from_list_key,
    CENTROID, LIST, STATE,
};

use crate::index::error::{Error, Result};

pub const CODEBOOK: u8 = b'b';

const STATE_VERSION: u8 = 1;

/// What a trained IVF-PQ index needs before it reads anything else.
///
/// Carries `m`, the subquantizer count, on top of the coarse `nlist` and
/// `dimensions` the shared [`ivf`](crate::index::vector::engine::ivf) module's
/// own `TrainedState` holds. IVF_FLAT has no `m`, so it uses that shared state
/// directly instead of this wider one; this stays its own type and its own
/// 13-byte encoding so an index trained before this split keeps decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrainedState {
    pub nlist: u32,
    pub m: u32,
    pub dimensions: u32,
}

pub fn encode_state(state: &TrainedState) -> Vec<u8> {
    let mut out = Vec::with_capacity(13);
    out.push(STATE_VERSION);
    out.extend_from_slice(&state.nlist.to_le_bytes());
    out.extend_from_slice(&state.m.to_le_bytes());
    out.extend_from_slice(&state.dimensions.to_le_bytes());
    out
}

pub fn decode_state(bytes: &[u8]) -> Result<TrainedState> {
    if bytes.len() != 13 || bytes[0] != STATE_VERSION {
        return Err(Error::Other(
            "vector index: unsupported trained-state encoding".into(),
        ));
    }
    let read =
        |at: usize| u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);
    Ok(TrainedState {
        nlist: read(1),
        m: read(5),
        dimensions: read(9),
    })
}
