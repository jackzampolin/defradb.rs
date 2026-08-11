//! Byte layout of a stored node and of the graph's meta singleton.
//!
//! Byte-identical to Go's `hnsw.MarshalNode` / `MarshalMeta`, little-endian
//! throughout. An index is local state that never crosses the wire, so this is
//! not a protocol; it is matched anyway because the two runtimes read the same
//! on-disk file when a node switches between them, and a layout is the one
//! thing that cannot be renegotiated afterwards.
//!
//! ```text
//! Node   1  version            4  vector length n
//!        8  id                4n  vector elements, f32 bits
//!        1  deleted            4  layer count L
//!                                 per layer: 4 neighbor count, then 8 per id
//!
//! Meta   1  version    8  entry point    4  top layer (i32)
//! ```
//!
//! A stored `Meta` always describes a real graph, so there is nothing to encode
//! for "no graph yet": that is the absence of the key.

use crate::error::{Error, Result};
use crate::vector::store::{Meta, Node, NodeId};

/// Bumped when the layout changes, so an old value is rejected rather than
/// misread. Written as the first byte of both encodings.
const NODE_VERSION: u8 = 0x01;
const META_VERSION: u8 = 0x01;

/// Widths of the fixed-size components. Named so the size arithmetic and the
/// offset advances cannot drift apart from the layout above.
const VERSION_WIDTH: usize = 1;
const FLAG_WIDTH: usize = 1;
const COUNT_WIDTH: usize = 4;
const F32_WIDTH: usize = 4;
const NODE_ID_WIDTH: usize = 8;
const TOP_LAYER_WIDTH: usize = 4;

const META_LEN: usize = VERSION_WIDTH + NODE_ID_WIDTH + TOP_LAYER_WIDTH;

/// The smallest valid encoded node: version, id, deleted, vector length and
/// layer count, before any vector element or layer.
const NODE_HEADER_LEN: usize = VERSION_WIDTH + NODE_ID_WIDTH + FLAG_WIDTH + COUNT_WIDTH * 2;

/// Encodes `node` for storage as a single value.
pub fn encode_node(node: &Node) -> Vec<u8> {
    let mut size = NODE_HEADER_LEN + F32_WIDTH * node.vector.len();
    for layer in &node.layers {
        size += COUNT_WIDTH + NODE_ID_WIDTH * layer.len();
    }

    let mut buf = Vec::with_capacity(size);
    buf.push(NODE_VERSION);
    buf.extend_from_slice(&node.id.0.to_le_bytes());
    buf.push(u8::from(node.deleted));
    buf.extend_from_slice(&(node.vector.len() as u32).to_le_bytes());
    for component in &node.vector {
        buf.extend_from_slice(&component.to_bits().to_le_bytes());
    }
    buf.extend_from_slice(&(node.layers.len() as u32).to_le_bytes());
    for layer in &node.layers {
        buf.extend_from_slice(&(layer.len() as u32).to_le_bytes());
        for neighbor in layer {
            buf.extend_from_slice(&neighbor.0.to_le_bytes());
        }
    }
    buf
}

/// Decodes a value written by [`encode_node`].
///
/// Every length read from the buffer is checked against what remains before it
/// is used to allocate, so a corrupt or truncated value is an error rather than
/// a huge allocation or a panic.
pub fn decode_node(bytes: &[u8]) -> Result<Node> {
    let mut cursor = Cursor::new(bytes);

    if cursor.take_u8()? != NODE_VERSION {
        return Err(invalid("unsupported node encoding version"));
    }
    let id = NodeId(cursor.take_u64()?);
    let deleted = cursor.take_u8()? != 0;

    let dimensions = cursor.take_u32()? as usize;
    cursor.ensure(dimensions.saturating_mul(F32_WIDTH))?;
    let mut vector = Vec::with_capacity(dimensions);
    for _ in 0..dimensions {
        vector.push(f32::from_bits(cursor.take_u32()?));
    }

    let layer_count = cursor.take_u32()? as usize;
    // Each layer carries at least its count prefix, so this bounds the
    // allocation before it happens.
    cursor.ensure(layer_count.saturating_mul(COUNT_WIDTH))?;
    let mut layers = Vec::with_capacity(layer_count);
    for _ in 0..layer_count {
        let neighbor_count = cursor.take_u32()? as usize;
        cursor.ensure(neighbor_count.saturating_mul(NODE_ID_WIDTH))?;
        let mut neighbors = Vec::with_capacity(neighbor_count);
        for _ in 0..neighbor_count {
            neighbors.push(NodeId(cursor.take_u64()?));
        }
        layers.push(neighbors);
    }

    Ok(Node {
        id,
        vector,
        layers,
        deleted,
    })
}

/// Encodes `meta` for storage as a single value.
pub fn encode_meta(meta: &Meta) -> Vec<u8> {
    let mut buf = Vec::with_capacity(META_LEN);
    buf.push(META_VERSION);
    buf.extend_from_slice(&meta.entry_point.0.to_le_bytes());
    buf.extend_from_slice(&(meta.top_layer as u32).to_le_bytes());
    buf
}

/// Decodes a value written by [`encode_meta`].
pub fn decode_meta(bytes: &[u8]) -> Result<Meta> {
    if bytes.len() != META_LEN {
        return Err(invalid("meta encoding has the wrong length"));
    }
    let mut cursor = Cursor::new(bytes);
    if cursor.take_u8()? != META_VERSION {
        return Err(invalid("unsupported meta encoding version"));
    }
    let entry_point = NodeId(cursor.take_u64()?);
    // Go stores this as an int32; a negative value would be a layer that cannot
    // exist, so it is rejected rather than wrapped into a huge `usize`.
    let top_layer = cursor.take_u32()? as i32;
    if top_layer < 0 {
        return Err(invalid("meta has a negative top layer"));
    }
    Ok(Meta {
        entry_point,
        top_layer: top_layer as usize,
    })
}

fn invalid(reason: &str) -> Error {
    Error::Other(format!("vector index: {reason}"))
}

/// Reads forward through a buffer, refusing to run off the end.
struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Errors unless `width` more bytes remain.
    fn ensure(&self, width: usize) -> Result<()> {
        if self.bytes.len() - self.offset < width {
            return Err(invalid("encoding is truncated"));
        }
        Ok(())
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.ensure(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(out)
    }

    fn take_u8(&mut self) -> Result<u8> {
        Ok(self.take::<1>()?[0])
    }

    fn take_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take::<4>()?))
    }

    fn take_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take::<8>()?))
    }
}
