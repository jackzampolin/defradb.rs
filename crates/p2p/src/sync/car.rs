//! CARv1 encoding/decoding for DAG transfer.
//!
//! CARv1 (Content ARchive) packs a set of IPLD blocks with their CIDs into a
//! single byte stream, enabling single-round-trip DAG transfer over P2P.

use std::collections::HashSet;

use blockstore::Blockstore;
use cid::Cid;

use crate::error::{Error, Result};

/// Encode blocks as a CARv1 byte stream.
///
/// Format: varint-prefixed DAG-CBOR header, then varint-prefixed (CID + data) sections.
pub fn encode_car(roots: &[Cid], blocks: &[(&Cid, &[u8])]) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    // Header: DAG-CBOR map {version: 1, roots: [CID]}
    let header = encode_car_header(roots)?;
    write_varint_prefixed(&mut out, &header);

    // Each block: varint(len(cid_bytes + data)) + cid_bytes + data
    for (cid, data) in blocks {
        let cid_bytes = cid.to_bytes();
        let section_len = cid_bytes.len() + data.len();
        write_varint(&mut out, section_len as u64);
        out.extend_from_slice(&cid_bytes);
        out.extend_from_slice(data);
    }

    Ok(out)
}

/// Decoded CAR contents: root CIDs and blocks (CID + data pairs).
pub type CarContents = (Vec<Cid>, Vec<(Cid, Vec<u8>)>);

/// Decode a CARv1 byte stream into roots + blocks.
pub fn decode_car(data: &[u8]) -> Result<CarContents> {
    let mut cursor = data;

    // Read header
    let header_len = read_varint(&mut cursor)?;
    if header_len as usize > cursor.len() {
        return Err(Error::Codec("CAR header length exceeds data".into()));
    }
    let header_bytes = &cursor[..header_len as usize];
    cursor = &cursor[header_len as usize..];
    let roots = decode_car_header(header_bytes)?;

    // Read blocks
    let mut blocks = Vec::new();
    while !cursor.is_empty() {
        let section_len = read_varint(&mut cursor)?;
        if section_len as usize > cursor.len() {
            return Err(Error::Codec("CAR block section length exceeds data".into()));
        }
        let section = &cursor[..section_len as usize];
        cursor = &cursor[section_len as usize..];

        let cid = Cid::read_bytes(section)
            .map_err(|e| Error::InvalidCid(format!("failed to read CID from CAR block: {}", e)))?;
        let cid_len = cid.to_bytes().len();
        let block_data = section[cid_len..].to_vec();
        blocks.push((cid, block_data));
    }

    Ok((roots, blocks))
}

/// Traverse DAG from root, collect all reachable blocks from blockstore.
pub async fn collect_dag_blocks<B: Blockstore>(
    blockstore: &B,
    root_cid: &Cid,
) -> Result<Vec<(Cid, Vec<u8>)>> {
    let mut blocks = Vec::new();
    let mut visited = HashSet::new();
    collect_recursive(blockstore, root_cid, &mut blocks, &mut visited).await?;
    Ok(blocks)
}

fn collect_recursive<'a, B: Blockstore + 'a>(
    blockstore: &'a B,
    cid: &'a Cid,
    blocks: &'a mut Vec<(Cid, Vec<u8>)>,
    visited: &'a mut HashSet<Cid>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if !visited.insert(*cid) {
            return Ok(());
        }

        let data = match blockstore.get(cid).await {
            Ok(Some(d)) => d,
            Ok(None) => return Ok(()),
            Err(e) => {
                return Err(Error::BlockstoreError(format!(
                    "failed to get block {}: {}",
                    cid, e
                )));
            }
        };

        // Extract child links
        let refs = extract_links(&data);
        blocks.push((*cid, data));

        for child_cid in refs {
            collect_recursive(blockstore, &child_cid, blocks, visited).await?;
        }

        Ok(())
    })
}

/// Extract CID links from a DAG-CBOR block.
fn extract_links(block_data: &[u8]) -> Vec<Cid> {
    use libipld::multihash::{Code, MultihashDigest};
    use libipld::{Block, DefaultParams};

    let hash = Code::Sha2_256.digest(block_data);
    let dummy_cid = Cid::new_v1(0x71, hash);

    let mut refs = Vec::new();
    let block = Block::<DefaultParams>::new_unchecked(dummy_cid, block_data.to_vec());
    if block.references(&mut refs).is_err() {
        return Vec::new();
    }
    refs
}

// --- CARv1 header encode/decode ---
//
// Uses CBOR map {"roots": [<cid bytes>, ...], "version": 1}.
// CIDs are stored as plain CBOR byte strings (no tag 42) since this
// is an internal Rust-to-Rust protocol.

fn encode_car_header(roots: &[Cid]) -> Result<Vec<u8>> {
    use serde_cbor::Value;
    let roots_val: Vec<Value> = roots
        .iter()
        .map(|cid| Value::Bytes(cid.to_bytes()))
        .collect();

    let mut map = std::collections::BTreeMap::new();
    map.insert(Value::Text("roots".into()), Value::Array(roots_val));
    map.insert(Value::Text("version".into()), Value::Integer(1));

    serde_cbor::to_vec(&Value::Map(map))
        .map_err(|e| Error::Codec(format!("failed to encode CAR header: {}", e)))
}

fn decode_car_header(data: &[u8]) -> Result<Vec<Cid>> {
    use serde_cbor::Value;

    let value: Value = serde_cbor::from_slice(data)
        .map_err(|e| Error::Codec(format!("invalid CAR header: {}", e)))?;

    let map = match value {
        Value::Map(m) => m,
        _ => return Err(Error::Codec("CAR header is not a CBOR map".into())),
    };

    let version_key = Value::Text("version".into());
    match map.get(&version_key) {
        Some(Value::Integer(1)) => {}
        _ => return Err(Error::Codec("CAR header version must be 1".into())),
    }

    let roots_key = Value::Text("roots".into());
    let roots_array = match map.get(&roots_key) {
        Some(Value::Array(a)) => a,
        _ => return Err(Error::Codec("CAR header 'roots' must be an array".into())),
    };

    let mut roots = Vec::new();
    for val in roots_array {
        let bytes = match val {
            Value::Bytes(b) => b,
            Value::Tag(42, inner) => match inner.as_ref() {
                Value::Bytes(b) => {
                    // DAG-CBOR tag 42: strip 0x00 prefix if present
                    if b.first() == Some(&0x00) {
                        &b[1..]
                    } else {
                        b
                    }
                }
                _ => return Err(Error::Codec("CAR root tag 42 does not wrap bytes".into())),
            },
            _ => return Err(Error::Codec("CAR root is not a byte string".into())),
        };
        let cid = Cid::read_bytes(std::io::Cursor::new(bytes))
            .map_err(|e| Error::InvalidCid(format!("invalid CID in CAR roots: {}", e)))?;
        roots.push(cid);
    }

    Ok(roots)
}

// --- Varint helpers ---

fn write_varint(buf: &mut Vec<u8>, value: u64) {
    let mut tmp = unsigned_varint::encode::u64_buffer();
    let encoded = unsigned_varint::encode::u64(value, &mut tmp);
    buf.extend_from_slice(encoded);
}

fn write_varint_prefixed(buf: &mut Vec<u8>, data: &[u8]) {
    write_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

fn read_varint(cursor: &mut &[u8]) -> Result<u64> {
    let (value, rest) = unsigned_varint::decode::u64(cursor)
        .map_err(|e| Error::Codec(format!("failed to read varint: {}", e)))?;
    *cursor = rest;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::multihash::{Code, MultihashDigest};

    fn make_cid(data: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x71, hash)
    }

    #[test]
    fn round_trip_single_block() {
        let data = b"hello world";
        let cid = make_cid(data);
        let encoded = encode_car(&[cid], &[(&cid, data.as_slice())]).unwrap();
        let (roots, blocks) = decode_car(&encoded).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], cid);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].0, cid);
        assert_eq!(blocks[0].1, data);
    }

    #[test]
    fn round_trip_multi_block() {
        let data1 = b"block one";
        let data2 = b"block two";
        let data3 = b"block three";
        let cid1 = make_cid(data1);
        let cid2 = make_cid(data2);
        let cid3 = make_cid(data3);

        let blocks_in = vec![
            (&cid1, data1.as_slice()),
            (&cid2, data2.as_slice()),
            (&cid3, data3.as_slice()),
        ];
        let encoded = encode_car(&[cid1], &blocks_in).unwrap();
        let (roots, blocks) = decode_car(&encoded).unwrap();

        assert_eq!(roots, vec![cid1]);
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].1, data1);
        assert_eq!(blocks[1].1, data2);
        assert_eq!(blocks[2].1, data3);
    }

    #[test]
    fn round_trip_empty_blocks() {
        let cid = make_cid(b"root");
        let encoded = encode_car(&[cid], &[]).unwrap();
        let (roots, blocks) = decode_car(&encoded).unwrap();
        assert_eq!(roots, vec![cid]);
        assert!(blocks.is_empty());
    }

    #[test]
    fn round_trip_multiple_roots() {
        let data1 = b"root1";
        let data2 = b"root2";
        let cid1 = make_cid(data1);
        let cid2 = make_cid(data2);
        let encoded = encode_car(
            &[cid1, cid2],
            &[(&cid1, data1.as_slice()), (&cid2, data2.as_slice())],
        )
        .unwrap();
        let (roots, blocks) = decode_car(&encoded).unwrap();
        assert_eq!(roots, vec![cid1, cid2]);
        assert_eq!(blocks.len(), 2);
    }
}
