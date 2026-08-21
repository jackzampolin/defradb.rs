//! Encoding for the SSG adjacency, beside the graph it was built from.

use crate::error::{Error, Result};
use crate::vector::store::NodeId;

pub const ADJACENCY: u8 = b'g';
pub const STATE: u8 = b's';

const STATE_VERSION: u8 = 1;

pub fn encode_neighbours(ids: &[NodeId]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        out.extend_from_slice(&id.0.to_le_bytes());
    }
    out
}

pub fn decode_neighbours(bytes: &[u8]) -> Result<Vec<NodeId>> {
    if !bytes.len().is_multiple_of(8) {
        return Err(Error::Other(
            "vector index: an SSG adjacency is ragged".into(),
        ));
    }
    Ok(bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|&c| NodeId(u64::from_le_bytes(c)))
        .collect())
}

pub fn node_key(id: NodeId) -> Vec<u8> {
    id.0.to_be_bytes().to_vec()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltState {
    pub entry_point: NodeId,
    pub nodes: u64,
}

pub fn encode_state(state: &BuiltState) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.push(STATE_VERSION);
    out.extend_from_slice(&state.entry_point.0.to_le_bytes());
    out.extend_from_slice(&state.nodes.to_le_bytes());
    out
}

pub fn decode_state(bytes: &[u8]) -> Result<BuiltState> {
    if bytes.len() != 17 || bytes[0] != STATE_VERSION {
        return Err(Error::Other(
            "vector index: unsupported SSG state encoding".into(),
        ));
    }
    let read = |at: usize| {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[at..at + 8]);
        u64::from_le_bytes(buf)
    };
    Ok(BuiltState {
        entry_point: NodeId(read(1)),
        nodes: read(9),
    })
}
