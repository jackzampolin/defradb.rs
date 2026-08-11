//! CARv1 encoding/decoding for DAG transfer.
//!
//! CARv1 (Content ARchive) packs a set of IPLD blocks with their CIDs into a
//! single byte stream, enabling single-round-trip DAG transfer over P2P.

use std::collections::{HashSet, VecDeque};

use blockstore::Blockstore;
use bytes::Bytes;
use cid::Cid;

use crate::error::{Error, Result};

/// Maximum number of blocks allowed in a single CAR response.
///
/// Prevents a malicious or faulty peer from causing the server to collect and
/// send an arbitrarily large DAG in a single response.
pub const CAR_MAX_BLOCKS: usize = 10_000;

/// Maximum total byte size of a single CAR response (16 MiB).
pub const CAR_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Result of collecting blocks for a CAR response.
#[derive(Debug, Clone, Default)]
pub struct CarCollectOutcome {
    pub blocks: Vec<(Cid, Bytes)>,
    pub truncated_by_blocks: bool,
    pub truncated_by_bytes: bool,
}

impl CarCollectOutcome {
    pub fn truncated(&self) -> bool {
        self.truncated_by_blocks || self.truncated_by_bytes
    }
}

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

/// Cheap peek: does this CAR byte stream contain at least one block section?
///
/// Reads only the header-length varint and checks whether any bytes remain
/// after the header. Used by the iroh requester to distinguish a header-only
/// "no blocks" response from a usable fetch without paying full decode cost.
/// Any malformed input — unreadable varint, or declared header length
/// exceeding the remaining bytes — reports `true` so the caller forwards
/// the bytes and lets the coordinator surface the decode error.
#[cfg(feature = "iroh-transport")]
pub(crate) fn car_has_any_block(data: &[u8]) -> bool {
    let mut cursor = data;
    let Ok(header_len) = read_varint(&mut cursor) else {
        return true;
    };
    let header_len = header_len as usize;
    if header_len > cursor.len() {
        return true;
    }
    cursor.len() > header_len
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

    // Read blocks. Cap at CAR_MAX_BLOCKS to prevent a hostile peer from
    // sending millions of tiny block headers that allocate unbounded memory
    // before any downstream limit fires (#840).
    let mut blocks = Vec::new();
    while !cursor.is_empty() {
        if blocks.len() >= CAR_MAX_BLOCKS {
            return Err(Error::Codec(format!(
                "CAR file exceeds maximum block count of {}",
                CAR_MAX_BLOCKS
            )));
        }

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
///
/// Collection is capped at [`CAR_MAX_BLOCKS`] blocks and [`CAR_MAX_BYTES`] total
/// bytes.  If either limit is reached the function returns the blocks collected
/// so far without error; the caller can detect truncation by checking whether
/// the returned slice represents a complete DAG.
pub async fn collect_dag_blocks<B: Blockstore>(
    blockstore: &B,
    root_cid: &Cid,
) -> Result<CarCollectOutcome> {
    let mut outcome = CarCollectOutcome::default();
    let mut visited = HashSet::new();
    let mut total_bytes: usize = 0;
    let mut queue = VecDeque::from([*root_cid]);

    while let Some(cid) = queue.pop_front() {
        if !visited.insert(cid) {
            continue;
        }

        if outcome.blocks.len() >= CAR_MAX_BLOCKS {
            outcome.truncated_by_blocks = true;
            break;
        }

        let data = match blockstore.get(&cid).await {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => {
                return Err(Error::BlockstoreError(format!(
                    "failed to get block {}: {}",
                    cid, e
                )));
            }
        };

        if total_bytes + data.len() > CAR_MAX_BYTES {
            outcome.truncated_by_bytes = true;
            break;
        }
        total_bytes += data.len();

        let refs = extract_links(&data);
        outcome.blocks.push((cid, data));

        for child_cid in refs {
            if !visited.contains(&child_cid) {
                queue.push_back(child_cid);
            }
        }
    }

    Ok(outcome)
}

/// Collect the exact requested blocks without walking descendant links.
pub async fn collect_exact_blocks<B: Blockstore>(
    blockstore: &B,
    cids: &[Cid],
) -> Result<CarCollectOutcome> {
    let mut outcome = CarCollectOutcome::default();
    let mut visited = HashSet::new();
    let mut total_bytes: usize = 0;

    for cid in cids {
        if !visited.insert(*cid) {
            continue;
        }
        if outcome.blocks.len() >= CAR_MAX_BLOCKS {
            outcome.truncated_by_blocks = true;
            break;
        }

        let data = match blockstore.get(cid).await {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => {
                return Err(Error::BlockstoreError(format!(
                    "failed to get block {}: {}",
                    cid, e
                )));
            }
        };

        if total_bytes + data.len() > CAR_MAX_BYTES {
            outcome.truncated_by_bytes = true;
            break;
        }

        total_bytes += data.len();
        outcome.blocks.push((*cid, data));
    }

    Ok(outcome)
}

/// Walk the DAG rooted at `root`, collecting every reachable CID for an
/// access grant (not the block bytes themselves).
///
/// Used to authorize a receiver's post-push selective-CAR recovery pull from
/// the DAG's actual shape, independent of whatever subset of blocks the
/// pusher happened to send in this job (root-only pushes still let the
/// receiver recover missing ancestors/dependents via CAR). Mirrors the
/// receiver-side `find_all_missing_links` walk by following
/// [`extract_ipld_links`](crate::sync::manager::links::extract_ipld_links),
/// which excludes the `encryption` link (#976): that block is never served
/// over CAR (KMS-only, ECIES-wrapped, permission-gated), so it must never be
/// included in a grant's explicit CID set either — `collect_exact_blocks`
/// will happily serve any CID the grant allows.
///
/// Capped at `max_blocks`; absent blocks are skipped (not an error) so a
/// partial local DAG still yields a best-effort grant.
pub async fn collect_dag_cids<B: Blockstore>(
    blockstore: &B,
    root: &Cid,
    max_blocks: usize,
) -> Result<Vec<Cid>> {
    let mut collected = Vec::new();
    let mut visited = HashSet::new();
    let mut queue = VecDeque::from([*root]);

    while let Some(cid) = queue.pop_front() {
        if !visited.insert(cid) {
            continue;
        }

        if collected.len() >= max_blocks {
            break;
        }

        let data = match blockstore.get(&cid).await {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => {
                return Err(Error::BlockstoreError(format!(
                    "failed to get block {}: {}",
                    cid, e
                )));
            }
        };

        collected.push(cid);

        let refs = crate::sync::manager::links::extract_ipld_links(&data).unwrap_or_default();
        for child_cid in refs {
            if !visited.contains(&child_cid) {
                queue.push_back(child_cid);
            }
        }
    }

    Ok(collected)
}

/// Extract CID links from a DAG-CBOR block.
fn extract_links(block_data: &[u8]) -> Vec<Cid> {
    use ipld_core::codec::Links;
    use serde_ipld_dagcbor::codec::DagCborCodec;

    let mut refs: Vec<Cid> = match DagCborCodec::links(block_data) {
        Ok(links) => links.collect(),
        Err(_) => return Vec::new(),
    };

    // Drop the encryption-metadata link. The encryption block holds the
    // plaintext DEK and is gated by the KMS access policy (NacDacPolicy): it
    // must travel ONLY over the KMS `encryption` topic (ECIES-wrapped,
    // permission-checked), never bundled into a CAR DAG transfer. Including it
    // here ships the DEK to a peer that may lack DAC read permission, bypassing
    // the dual-gate (issue #976). Mirrors the Bitswap link walker in
    // crates/p2p/src/sync/manager/links.rs.
    if let Ok(defra_block) = defra_core::Block::from_dag_cbor(block_data) {
        if let Some(enc_cid) = defra_block.encryption {
            refs.retain(|cid| *cid != enc_cid);
        }
    }

    refs
}

// --- CARv1 header encode/decode ---
//
// Uses CBOR map {"roots": [<cid bytes>, ...], "version": 1}.
// CIDs are stored as plain CBOR byte strings (no tag 42) since this
// is an internal Rust-to-Rust protocol.

fn encode_car_header(roots: &[Cid]) -> Result<Vec<u8>> {
    use ciborium::Value;
    let roots_val: Vec<Value> = roots
        .iter()
        .map(|cid| Value::Bytes(cid.to_bytes()))
        .collect();

    // ciborium's Value::Map is an ordered Vec, so key order is written out
    // explicitly here. It previously came from a BTreeMap, which sorted the
    // keys; "roots" < "version" alphabetically, so this order reproduces the
    // existing bytes exactly. `car_header_bytes_unchanged_by_encoder` pins it.
    let header = Value::Map(vec![
        (Value::Text("roots".into()), Value::Array(roots_val)),
        (Value::Text("version".into()), Value::Integer(1.into())),
    ]);

    let mut out = Vec::new();
    ciborium::into_writer(&header, &mut out)
        .map_err(|e| Error::Codec(format!("failed to encode CAR header: {}", e)))?;
    Ok(out)
}

fn decode_car_header(data: &[u8]) -> Result<Vec<Cid>> {
    use ciborium::Value;

    let value: Value = defra_core::cbor::from_slice(data)
        .map_err(|e| Error::Codec(format!("invalid CAR header: {}", e)))?;

    // ciborium models a CBOR map as an ordered Vec of pairs rather than a
    // keyed map, so entries are looked up by scanning.
    let map = match value {
        Value::Map(m) => m,
        _ => return Err(Error::Codec("CAR header is not a CBOR map".into())),
    };
    let field = |name: &str| {
        map.iter()
            .find(|(key, _)| matches!(key, Value::Text(text) if text == name))
            .map(|(_, value)| value)
    };

    match field("version") {
        Some(Value::Integer(v)) if i128::from(*v) == 1 => {}
        _ => return Err(Error::Codec("CAR header version must be 1".into())),
    }

    let roots_array = match field("roots") {
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
    use blockstore::{Blockstore, DefraBlockstore};
    use ipld_core::{codec::Codec, ipld, ipld::Ipld};
    use multihash_codetable::{Code, MultihashDigest};
    use serde_ipld_dagcbor::codec::DagCborCodec;
    use std::sync::Arc;
    use storage::backends::MemoryStore;

    fn make_cid(data: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x71, hash)
    }

    fn encode_ipld(ipld: Ipld) -> Vec<u8> {
        DagCborCodec::encode_to_vec(&ipld).unwrap()
    }

    /// The `encryption` link of an encrypted block must NOT be walked when
    /// building a CAR. The encryption block holds the plaintext DEK and is
    /// distributed only over the gated KMS topic — bundling it into a CAR would
    /// ship the key to a peer that may lack DAC read permission (issue #976).
    /// Named field links are still walked.
    #[test]
    fn extract_links_excludes_encryption_link() {
        use defra_core::{Block as DefraBlock, CrdtDelta, DAGLink, LwwDeltaPayload};

        let field_link = make_cid(b"field-block");
        let enc_cid = make_cid(b"encryption-block");

        let block = DefraBlock::new_with_options(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "secret".to_string(),
                priority: 1,
                schema_version_id: "schema1".to_string(),
                data: b"ciphertext".to_vec(),
            }),
            vec![],
            vec![DAGLink::new("secret", field_link)],
            Some(enc_cid),
            None,
        );
        let bytes = block.to_dag_cbor().expect("encode encrypted block");

        let links = extract_links(&bytes);
        assert!(links.contains(&field_link), "named field link must be kept");
        assert!(
            !links.contains(&enc_cid),
            "encryption link must be excluded from CAR DAG walk"
        );
    }

    #[cfg(feature = "iroh-transport")]
    #[test]
    fn car_has_any_block_detects_header_only_and_populated_cars() {
        let cid = make_cid(b"root");
        let header_only = encode_car(&[cid], &[]).unwrap();
        assert!(!car_has_any_block(&header_only));

        let data = b"payload";
        let populated = encode_car(&[cid], &[(&cid, data.as_slice())]).unwrap();
        assert!(car_has_any_block(&populated));

        // Malformed input: no varint → report `true` so the caller forwards
        // to the coordinator instead of silently dropping.
        assert!(car_has_any_block(&[]));

        // Malformed subtype: varint declares a header longer than the
        // remaining bytes. Also report `true` (fail-safe).
        // Varint 0x7f = 127, but only 1 byte follows.
        assert!(car_has_any_block(&[0x7f, 0xaa]));
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

    #[tokio::test]
    async fn collect_exact_blocks_does_not_walk_descendants() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let grandchild_data = encode_ipld(ipld!({ "kind": "grandchild" }));
        let grandchild_cid = make_cid(&grandchild_data);
        blockstore
            .put(&grandchild_cid, &grandchild_data)
            .await
            .unwrap();

        let child_data = encode_ipld(ipld!({ "child": grandchild_cid }));
        let child_cid = make_cid(&child_data);
        blockstore.put(&child_cid, &child_data).await.unwrap();

        let root_data = encode_ipld(ipld!({ "children": [child_cid] }));
        let root_cid = make_cid(&root_data);
        blockstore.put(&root_cid, &root_data).await.unwrap();

        let collected = collect_exact_blocks(&blockstore, &[root_cid])
            .await
            .unwrap();

        assert_eq!(collected.blocks.len(), 1);
        assert_eq!(collected.blocks[0].0, root_cid);
        assert_eq!(collected.blocks[0].1, root_data);
        assert!(!collected.truncated());
    }

    #[tokio::test]
    async fn collect_exact_blocks_serves_field_and_composite_history_subsets() {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);
        let mut previous_field = None;
        let mut previous_composite = None;
        let mut field_blocks = Vec::new();
        let mut composite_blocks = Vec::new();

        for height in 1..=17 {
            let field = Block::new(
                CrdtDelta::Lww(LwwDeltaPayload {
                    field_name: "status".to_string(),
                    priority: height,
                    schema_version_id: "version1".to_string(),
                    data: vec![height as u8],
                }),
                previous_field.into_iter().collect(),
                vec![],
            );
            let field_data = field.to_dag_cbor().unwrap();
            let field_cid = field.generate_cid().unwrap();
            blockstore.put(&field_cid, &field_data).await.unwrap();
            field_blocks.push((field_cid, field_data));
            previous_field = Some(field_cid);

            let composite = Block::new(
                CrdtDelta::Composite(CompositeDeltaPayload {
                    schema_version_id: "version1".to_string(),
                    priority: height,
                    status: 1,
                }),
                previous_composite.into_iter().collect(),
                vec![DAGLink::new("status", field_cid)],
            );
            let composite_data = composite.to_dag_cbor().unwrap();
            let composite_cid = composite.generate_cid().unwrap();
            blockstore
                .put(&composite_cid, &composite_data)
                .await
                .unwrap();
            composite_blocks.push((composite_cid, composite_data));
            previous_composite = Some(composite_cid);
        }

        let expected: Vec<(Cid, Bytes)> =
            [&field_blocks[3], &field_blocks[13], &composite_blocks[16]]
                .into_iter()
                .map(|(cid, data)| (*cid, Bytes::from(data.clone())))
                .collect();
        let wanted: Vec<Cid> = expected.iter().map(|(cid, _)| *cid).collect();
        let collected = collect_exact_blocks(&blockstore, &wanted).await.unwrap();

        assert_eq!(collected.blocks, expected);
        assert!(!collected.truncated());
    }

    #[tokio::test]
    async fn collect_dag_blocks_handles_deep_chains_iteratively() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let mut next: Option<Cid> = None;
        let mut root_cid: Option<Cid> = None;
        let depth = 5_000usize;

        for idx in (0..depth).rev() {
            let data = match next {
                Some(child) => encode_ipld(ipld!({ "idx": idx as i64, "next": child })),
                None => encode_ipld(ipld!({ "idx": idx as i64 })),
            };
            let cid = make_cid(&data);
            blockstore.put(&cid, &data).await.unwrap();
            next = Some(cid);
            root_cid = Some(cid);
        }

        let root_cid = root_cid.expect("deep chain root");
        let collected = collect_dag_blocks(&blockstore, &root_cid).await.unwrap();

        assert_eq!(collected.blocks.len(), depth);
        assert!(!collected.truncated());
    }

    #[tokio::test]
    async fn collect_dag_cids_walks_all_reachable_and_caps() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let grandchild_data = encode_ipld(ipld!({ "kind": "grandchild" }));
        let grandchild_cid = make_cid(&grandchild_data);
        blockstore
            .put(&grandchild_cid, &grandchild_data)
            .await
            .unwrap();

        let child_data = encode_ipld(ipld!({ "child": grandchild_cid }));
        let child_cid = make_cid(&child_data);
        blockstore.put(&child_cid, &child_data).await.unwrap();

        let root_data = encode_ipld(ipld!({ "children": [child_cid] }));
        let root_cid = make_cid(&root_data);
        blockstore.put(&root_cid, &root_data).await.unwrap();

        let all = collect_dag_cids(&blockstore, &root_cid, CAR_MAX_BLOCKS)
            .await
            .unwrap();
        let all_set: HashSet<Cid> = all.iter().copied().collect();
        assert_eq!(
            all_set,
            HashSet::from([root_cid, child_cid, grandchild_cid]),
            "expected root, child, and grandchild to be reachable"
        );

        let capped = collect_dag_cids(&blockstore, &root_cid, 2).await.unwrap();
        assert_eq!(capped.len(), 2, "cap of 2 must yield exactly 2 CIDs");
    }

    #[test]
    fn decode_car_rejects_block_count_exceeding_cap() {
        let root = make_cid(b"root");
        let blocks: Vec<(&Cid, &[u8])> = (0..CAR_MAX_BLOCKS + 1)
            .map(|_| (&root, b"x".as_slice()))
            .collect();
        let encoded = encode_car(&[root], &blocks).unwrap();

        let result = decode_car(&encoded);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("maximum block count"),
            "expected block-count error, got: {err}"
        );
    }

    #[test]
    fn decode_car_accepts_exactly_max_blocks() {
        let root = make_cid(b"root");
        let blocks: Vec<(&Cid, &[u8])> = (0..CAR_MAX_BLOCKS)
            .map(|_| (&root, b"x".as_slice()))
            .collect();
        let encoded = encode_car(&[root], &blocks).unwrap();

        let (roots, decoded) = decode_car(&encoded).unwrap();
        assert_eq!(roots, vec![root]);
        assert_eq!(decoded.len(), CAR_MAX_BLOCKS);
    }
}
