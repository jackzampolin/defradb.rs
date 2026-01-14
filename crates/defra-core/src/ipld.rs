//! libipld integration for full IPLD data model support
//!
//! This module provides conversions between DefraDB block types and the
//! libipld `Ipld` data model, enabling link traversal and IPLD-native operations.

use std::collections::BTreeMap;

use libipld::Ipld;

use crate::block::{
    Block, CollectionDeltaPayload, CompositeDeltaPayload, CounterDeltaPayload, CrdtDelta, DAGLink,
    Encryption, LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
};
use crate::{Error, Result};

// ============================================================================
// CID Conversion Helpers
// ============================================================================

/// Convert our cid::Cid (0.11) to libipld's cid::Cid (0.10)
fn cid_to_libipld(cid: &cid::Cid) -> libipld::cid::Cid {
    // CIDs are byte-compatible, convert via bytes
    let bytes = cid.to_bytes();
    libipld::cid::Cid::try_from(bytes).expect("valid CID should convert")
}

/// Convert libipld's cid::Cid (0.10) to our cid::Cid (0.11)
fn cid_from_libipld(cid: &libipld::cid::Cid) -> cid::Cid {
    let bytes = cid.to_bytes();
    cid::Cid::try_from(bytes).expect("valid CID should convert")
}

// ============================================================================
// Block -> Ipld Conversion
// ============================================================================

impl From<&Block> for Ipld {
    fn from(block: &Block) -> Self {
        let mut map = BTreeMap::new();

        // Convert delta
        map.insert("delta".to_string(), Ipld::from(&block.delta));

        // Convert heads (optional)
        if let Some(ref heads) = block.heads {
            let heads_ipld: Vec<Ipld> = heads
                .iter()
                .map(|cid| Ipld::Link(cid_to_libipld(cid)))
                .collect();
            map.insert("heads".to_string(), Ipld::List(heads_ipld));
        }

        // Convert links (optional)
        if let Some(ref links) = block.links {
            let links_ipld: Vec<Ipld> = links.iter().map(Ipld::from).collect();
            map.insert("links".to_string(), Ipld::List(links_ipld));
        }

        // Convert encryption link (optional)
        if let Some(ref enc) = block.encryption {
            map.insert("encryption".to_string(), Ipld::Link(cid_to_libipld(enc)));
        }

        // Convert signature link (optional)
        if let Some(ref sig) = block.signature {
            map.insert("signature".to_string(), Ipld::Link(cid_to_libipld(sig)));
        }

        Ipld::Map(map)
    }
}

impl From<Block> for Ipld {
    fn from(block: Block) -> Self {
        Ipld::from(&block)
    }
}

impl From<&DAGLink> for Ipld {
    fn from(link: &DAGLink) -> Self {
        let mut map = BTreeMap::new();
        map.insert("name".to_string(), Ipld::String(link.name.clone()));
        map.insert("link".to_string(), Ipld::Link(cid_to_libipld(&link.link)));
        Ipld::Map(map)
    }
}

impl From<&CrdtDelta> for Ipld {
    fn from(delta: &CrdtDelta) -> Self {
        let mut map = BTreeMap::new();
        match delta {
            CrdtDelta::Lww(payload) => {
                map.insert("lww".to_string(), Ipld::from(payload));
            }
            CrdtDelta::Counter(payload) => {
                map.insert("counter".to_string(), Ipld::from(payload));
            }
            CrdtDelta::Composite(payload) => {
                map.insert("composite".to_string(), Ipld::from(payload));
            }
            CrdtDelta::Collection(payload) => {
                map.insert("collection".to_string(), Ipld::from(payload));
            }
        }
        Ipld::Map(map)
    }
}

impl From<&LwwDeltaPayload> for Ipld {
    fn from(payload: &LwwDeltaPayload) -> Self {
        let mut map = BTreeMap::new();
        map.insert("docID".to_string(), Ipld::Bytes(payload.doc_id.clone()));
        map.insert(
            "fieldName".to_string(),
            Ipld::String(payload.field_name.clone()),
        );
        map.insert(
            "priority".to_string(),
            Ipld::Integer(payload.priority as i128),
        );
        map.insert(
            "schemaVersionID".to_string(),
            Ipld::String(payload.schema_version_id.clone()),
        );
        map.insert("data".to_string(), Ipld::Bytes(payload.data.clone()));
        Ipld::Map(map)
    }
}

impl From<&CounterDeltaPayload> for Ipld {
    fn from(payload: &CounterDeltaPayload) -> Self {
        let mut map = BTreeMap::new();
        map.insert("docID".to_string(), Ipld::Bytes(payload.doc_id.clone()));
        map.insert(
            "fieldName".to_string(),
            Ipld::String(payload.field_name.clone()),
        );
        map.insert(
            "priority".to_string(),
            Ipld::Integer(payload.priority as i128),
        );
        map.insert("nonce".to_string(), Ipld::Integer(payload.nonce as i128));
        map.insert(
            "schemaVersionID".to_string(),
            Ipld::String(payload.schema_version_id.clone()),
        );
        map.insert("data".to_string(), Ipld::Bytes(payload.data.clone()));
        Ipld::Map(map)
    }
}

impl From<&CompositeDeltaPayload> for Ipld {
    fn from(payload: &CompositeDeltaPayload) -> Self {
        let mut map = BTreeMap::new();
        map.insert("docID".to_string(), Ipld::Bytes(payload.doc_id.clone()));
        map.insert(
            "schemaVersionID".to_string(),
            Ipld::String(payload.schema_version_id.clone()),
        );
        map.insert(
            "priority".to_string(),
            Ipld::Integer(payload.priority as i128),
        );
        map.insert("status".to_string(), Ipld::Integer(payload.status as i128));
        Ipld::Map(map)
    }
}

impl From<&CollectionDeltaPayload> for Ipld {
    fn from(payload: &CollectionDeltaPayload) -> Self {
        let mut map = BTreeMap::new();
        map.insert(
            "schemaVersionID".to_string(),
            Ipld::String(payload.schema_version_id.clone()),
        );
        map.insert(
            "priority".to_string(),
            Ipld::Integer(payload.priority as i128),
        );
        Ipld::Map(map)
    }
}

// ============================================================================
// Ipld -> Block Conversion
// ============================================================================

impl TryFrom<&Ipld> for Block {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => return Err(Error::IpldError("Expected IPLD map for Block".to_string())),
        };

        // Parse delta (required)
        let delta_ipld = map
            .get("delta")
            .ok_or_else(|| Error::IpldError("Missing 'delta' field in Block".to_string()))?;
        let delta = CrdtDelta::try_from(delta_ipld)?;

        // Parse heads (optional)
        let heads = if let Some(heads_ipld) = map.get("heads") {
            Some(parse_cid_list(heads_ipld)?)
        } else {
            None
        };

        // Parse links (optional)
        let links = if let Some(links_ipld) = map.get("links") {
            Some(parse_dag_links(links_ipld)?)
        } else {
            None
        };

        // Parse encryption link (optional)
        let encryption = if let Some(enc_ipld) = map.get("encryption") {
            Some(parse_cid(enc_ipld)?)
        } else {
            None
        };

        // Parse signature link (optional)
        let signature = if let Some(sig_ipld) = map.get("signature") {
            Some(parse_cid(sig_ipld)?)
        } else {
            None
        };

        Ok(Block {
            delta,
            heads,
            links,
            encryption,
            signature,
        })
    }
}

impl TryFrom<Ipld> for Block {
    type Error = Error;

    fn try_from(ipld: Ipld) -> Result<Self> {
        Block::try_from(&ipld)
    }
}

impl TryFrom<&Ipld> for DAGLink {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for DAGLink".to_string(),
                ))
            }
        };

        let name = match map.get("name") {
            Some(Ipld::String(s)) => s.clone(),
            _ => {
                return Err(Error::IpldError(
                    "Missing or invalid 'name' in DAGLink".to_string(),
                ))
            }
        };

        let link = match map.get("link") {
            Some(Ipld::Link(cid)) => cid_from_libipld(cid),
            _ => {
                return Err(Error::IpldError(
                    "Missing or invalid 'link' in DAGLink".to_string(),
                ))
            }
        };

        Ok(DAGLink { name, link })
    }
}

impl TryFrom<&Ipld> for CrdtDelta {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for CrdtDelta".to_string(),
                ))
            }
        };

        if let Some(lww) = map.get("lww") {
            return Ok(CrdtDelta::Lww(LwwDeltaPayload::try_from(lww)?));
        }
        if let Some(counter) = map.get("counter") {
            return Ok(CrdtDelta::Counter(CounterDeltaPayload::try_from(counter)?));
        }
        if let Some(composite) = map.get("composite") {
            return Ok(CrdtDelta::Composite(CompositeDeltaPayload::try_from(
                composite,
            )?));
        }
        if let Some(collection) = map.get("collection") {
            return Ok(CrdtDelta::Collection(CollectionDeltaPayload::try_from(
                collection,
            )?));
        }

        Err(Error::IpldError(
            "Unknown CRDT delta type in IPLD".to_string(),
        ))
    }
}

impl TryFrom<&Ipld> for LwwDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for LwwDeltaPayload".to_string(),
                ))
            }
        };

        Ok(LwwDeltaPayload {
            doc_id: parse_bytes(map, "docID")?,
            field_name: parse_string(map, "fieldName")?,
            priority: parse_u64(map, "priority")?,
            schema_version_id: parse_string(map, "schemaVersionID")?,
            data: parse_bytes(map, "data")?,
        })
    }
}

impl TryFrom<&Ipld> for CounterDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for CounterDeltaPayload".to_string(),
                ))
            }
        };

        Ok(CounterDeltaPayload {
            doc_id: parse_bytes(map, "docID")?,
            field_name: parse_string(map, "fieldName")?,
            priority: parse_u64(map, "priority")?,
            nonce: parse_i64(map, "nonce")?,
            schema_version_id: parse_string(map, "schemaVersionID")?,
            data: parse_bytes(map, "data")?,
        })
    }
}

impl TryFrom<&Ipld> for CompositeDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for CompositeDeltaPayload".to_string(),
                ))
            }
        };

        Ok(CompositeDeltaPayload {
            doc_id: parse_bytes(map, "docID")?,
            schema_version_id: parse_string(map, "schemaVersionID")?,
            priority: parse_u64(map, "priority")?,
            status: parse_u8(map, "status")?,
        })
    }
}

impl TryFrom<&Ipld> for CollectionDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for CollectionDeltaPayload".to_string(),
                ))
            }
        };

        Ok(CollectionDeltaPayload {
            schema_version_id: parse_string(map, "schemaVersionID")?,
            priority: parse_u64(map, "priority")?,
        })
    }
}

// ============================================================================
// Encryption/Signature Ipld Conversions
// ============================================================================

impl From<&Encryption> for Ipld {
    fn from(enc: &Encryption) -> Self {
        let mut map = BTreeMap::new();
        map.insert("docID".to_string(), Ipld::Bytes(enc.doc_id.clone()));
        if let Some(ref field_name) = enc.field_name {
            map.insert("fieldName".to_string(), Ipld::String(field_name.clone()));
        }
        map.insert("key".to_string(), Ipld::Bytes(enc.key.clone()));
        Ipld::Map(map)
    }
}

impl TryFrom<&Ipld> for Encryption {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for Encryption".to_string(),
                ))
            }
        };

        Ok(Encryption {
            doc_id: parse_bytes(map, "docID")?,
            field_name: map.get("fieldName").and_then(|v| match v {
                Ipld::String(s) => Some(s.clone()),
                _ => None,
            }),
            key: parse_bytes(map, "key")?,
        })
    }
}

impl From<&Signature> for Ipld {
    fn from(sig: &Signature) -> Self {
        let mut map = BTreeMap::new();
        map.insert("header".to_string(), Ipld::from(&sig.header));
        map.insert("value".to_string(), Ipld::Bytes(sig.value.clone()));
        Ipld::Map(map)
    }
}

impl TryFrom<&Ipld> for Signature {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for Signature".to_string(),
                ))
            }
        };

        let header_ipld = map
            .get("header")
            .ok_or_else(|| Error::IpldError("Missing 'header' in Signature".to_string()))?;

        Ok(Signature {
            header: SignatureHeader::try_from(header_ipld)?,
            value: parse_bytes(map, "value")?,
        })
    }
}

impl From<&SignatureHeader> for Ipld {
    fn from(header: &SignatureHeader) -> Self {
        let mut map = BTreeMap::new();
        let type_str = match header.sig_type {
            SignatureType::ES256K => "ES256K",
            SignatureType::EdDSA => "EdDSA",
        };
        map.insert("type".to_string(), Ipld::String(type_str.to_string()));
        map.insert("identity".to_string(), Ipld::Bytes(header.identity.clone()));
        Ipld::Map(map)
    }
}

impl TryFrom<&Ipld> for SignatureHeader {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(
                    "Expected IPLD map for SignatureHeader".to_string(),
                ))
            }
        };

        let sig_type = match map.get("type") {
            Some(Ipld::String(s)) => match s.as_str() {
                "ES256K" => SignatureType::ES256K,
                "EdDSA" => SignatureType::EdDSA,
                other => {
                    return Err(Error::IpldError(format!(
                        "Unknown signature type: {}",
                        other
                    )))
                }
            },
            _ => {
                return Err(Error::IpldError(
                    "Missing or invalid 'type' in SignatureHeader".to_string(),
                ))
            }
        };

        Ok(SignatureHeader {
            sig_type,
            identity: parse_bytes(map, "identity")?,
        })
    }
}

// ============================================================================
// Link Traversal Helpers
// ============================================================================

/// Extract all CID links from an IPLD value recursively.
///
/// This walks the entire IPLD tree and collects all Link values found.
pub fn extract_links(ipld: &Ipld) -> Vec<cid::Cid> {
    let mut links = Vec::new();
    extract_links_recursive(ipld, &mut links);
    links
}

fn extract_links_recursive(ipld: &Ipld, links: &mut Vec<cid::Cid>) {
    match ipld {
        Ipld::Link(cid) => {
            links.push(cid_from_libipld(cid));
        }
        Ipld::List(items) => {
            for item in items {
                extract_links_recursive(item, links);
            }
        }
        Ipld::Map(map) => {
            for value in map.values() {
                extract_links_recursive(value, links);
            }
        }
        _ => {}
    }
}

/// Visitor trait for traversing IPLD structures.
///
/// Implement this trait to perform custom operations while walking an IPLD DAG.
pub trait IpldVisitor {
    /// Called for each IPLD value encountered during traversal.
    ///
    /// Return `true` to continue traversal into children, `false` to skip children.
    fn visit(&mut self, ipld: &Ipld) -> bool;

    /// Called when a link is encountered.
    ///
    /// Override this to handle link resolution (e.g., fetch linked blocks from storage).
    fn visit_link(&mut self, _cid: &cid::Cid) {
        // Default: do nothing
    }
}

/// Walk an IPLD tree with a visitor.
///
/// Calls visitor methods for each node in the tree. If the visitor's `visit`
/// method returns `false`, children of that node are skipped.
pub fn walk_ipld<V: IpldVisitor>(ipld: &Ipld, visitor: &mut V) {
    if !visitor.visit(ipld) {
        return;
    }

    match ipld {
        Ipld::Link(cid) => {
            visitor.visit_link(&cid_from_libipld(cid));
        }
        Ipld::List(items) => {
            for item in items {
                walk_ipld(item, visitor);
            }
        }
        Ipld::Map(map) => {
            for value in map.values() {
                walk_ipld(value, visitor);
            }
        }
        _ => {}
    }
}

/// Collect all links from a Block using the IPLD data model.
///
/// This is equivalent to `Block::all_links()` but uses the IPLD traversal pattern.
pub fn collect_block_links(block: &Block) -> Vec<cid::Cid> {
    let ipld = Ipld::from(block);
    extract_links(&ipld)
}

// ============================================================================
// Helper Functions for Parsing
// ============================================================================

fn parse_cid(ipld: &Ipld) -> Result<cid::Cid> {
    match ipld {
        Ipld::Link(cid) => Ok(cid_from_libipld(cid)),
        _ => Err(Error::IpldError("Expected IPLD Link".to_string())),
    }
}

fn parse_cid_list(ipld: &Ipld) -> Result<Vec<cid::Cid>> {
    match ipld {
        Ipld::List(items) => items.iter().map(parse_cid).collect(),
        _ => Err(Error::IpldError("Expected IPLD List of Links".to_string())),
    }
}

fn parse_dag_links(ipld: &Ipld) -> Result<Vec<DAGLink>> {
    match ipld {
        Ipld::List(items) => items.iter().map(DAGLink::try_from).collect(),
        _ => Err(Error::IpldError(
            "Expected IPLD List of DAGLinks".to_string(),
        )),
    }
}

fn parse_string(map: &BTreeMap<String, Ipld>, key: &str) -> Result<String> {
    match map.get(key) {
        Some(Ipld::String(s)) => Ok(s.clone()),
        Some(_) => Err(Error::IpldError(format!("Field '{}' is not a string", key))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_bytes(map: &BTreeMap<String, Ipld>, key: &str) -> Result<Vec<u8>> {
    match map.get(key) {
        Some(Ipld::Bytes(b)) => Ok(b.clone()),
        Some(_) => Err(Error::IpldError(format!("Field '{}' is not bytes", key))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_u64(map: &BTreeMap<String, Ipld>, key: &str) -> Result<u64> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < 0 || *i > u64::MAX as i128 {
                return Err(Error::IpldError(format!(
                    "Field '{}' out of u64 range",
                    key
                )));
            }
            Ok(*i as u64)
        }
        Some(_) => Err(Error::IpldError(format!(
            "Field '{}' is not an integer",
            key
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_i64(map: &BTreeMap<String, Ipld>, key: &str) -> Result<i64> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < i64::MIN as i128 || *i > i64::MAX as i128 {
                return Err(Error::IpldError(format!(
                    "Field '{}' out of i64 range",
                    key
                )));
            }
            Ok(*i as i64)
        }
        Some(_) => Err(Error::IpldError(format!(
            "Field '{}' is not an integer",
            key
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_u8(map: &BTreeMap<String, Ipld>, key: &str) -> Result<u8> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < 0 || *i > u8::MAX as i128 {
                return Err(Error::IpldError(format!("Field '{}' out of u8 range", key)));
            }
            Ok(*i as u8)
        }
        Some(_) => Err(Error::IpldError(format!(
            "Field '{}' is not an integer",
            key
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_cid() -> cid::Cid {
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    fn test_lww_delta() -> CrdtDelta {
        CrdtDelta::Lww(LwwDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 1,
            schema_version_id: "schema1".to_string(),
            data: b"John".to_vec(),
        })
    }

    #[test]
    fn test_block_ipld_roundtrip() {
        let block = Block::new(test_lww_delta(), vec![], vec![]);

        let ipld = Ipld::from(&block);
        let restored = Block::try_from(&ipld).unwrap();

        assert_eq!(block.delta.priority(), restored.delta.priority());
        if let (CrdtDelta::Lww(orig), CrdtDelta::Lww(rest)) = (&block.delta, &restored.delta) {
            assert_eq!(orig.doc_id, rest.doc_id);
            assert_eq!(orig.field_name, rest.field_name);
            assert_eq!(orig.data, rest.data);
        }
    }

    #[test]
    fn test_block_with_heads_ipld_roundtrip() {
        let head = test_cid();
        let block = Block::new(test_lww_delta(), vec![head], vec![]);

        let ipld = Ipld::from(&block);
        let restored = Block::try_from(&ipld).unwrap();

        assert!(restored.heads.is_some());
        assert_eq!(restored.heads.unwrap().len(), 1);
    }

    #[test]
    fn test_block_with_links_ipld_roundtrip() {
        let link = DAGLink::new("field", test_cid());
        let block = Block::new(test_lww_delta(), vec![], vec![link]);

        let ipld = Ipld::from(&block);
        let restored = Block::try_from(&ipld).unwrap();

        assert!(restored.links.is_some());
        let links = restored.links.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].name, "field");
    }

    #[test]
    fn test_extract_links() {
        let head = test_cid();
        let link_cid =
            cid::Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
                .unwrap();
        let link = DAGLink::new("field", link_cid);

        let block = Block::new(test_lww_delta(), vec![head], vec![link]);
        let ipld = Ipld::from(&block);

        let links = extract_links(&ipld);
        assert_eq!(links.len(), 2);
    }

    #[test]
    fn test_collect_block_links() {
        let head = test_cid();
        let link = DAGLink::new("field", test_cid());
        let block = Block::new(test_lww_delta(), vec![head], vec![link.clone()]);

        let links = collect_block_links(&block);
        assert_eq!(links.len(), 2);
    }

    #[test]
    fn test_encryption_ipld_roundtrip() {
        let enc =
            Encryption::new_for_field(b"doc1".to_vec(), "secret".to_string(), b"key123".to_vec());

        let ipld = Ipld::from(&enc);
        let restored = Encryption::try_from(&ipld).unwrap();

        assert_eq!(enc.doc_id, restored.doc_id);
        assert_eq!(enc.field_name, restored.field_name);
        assert_eq!(enc.key, restored.key);
    }

    #[test]
    fn test_signature_ipld_roundtrip() {
        let sig = Signature::new(
            SignatureHeader::new(SignatureType::EdDSA, b"pubkey".to_vec()),
            b"signature".to_vec(),
        );

        let ipld = Ipld::from(&sig);
        let restored = Signature::try_from(&ipld).unwrap();

        assert_eq!(sig.header.sig_type, restored.header.sig_type);
        assert_eq!(sig.header.identity, restored.header.identity);
        assert_eq!(sig.value, restored.value);
    }

    #[test]
    fn test_counter_delta_ipld_roundtrip() {
        let delta = CrdtDelta::Counter(CounterDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 5,
            nonce: -42,
            schema_version_id: "v1".to_string(),
            data: vec![1, 2, 3],
        });

        let block = Block::new(delta, vec![], vec![]);
        let ipld = Ipld::from(&block);
        let restored = Block::try_from(&ipld).unwrap();

        if let CrdtDelta::Counter(c) = &restored.delta {
            assert_eq!(c.nonce, -42);
            assert_eq!(c.priority, 5);
        } else {
            panic!("Expected Counter delta");
        }
    }

    #[test]
    fn test_composite_delta_ipld_roundtrip() {
        let delta = CrdtDelta::Composite(CompositeDeltaPayload {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 10,
            status: 2,
        });

        let block = Block::new(delta, vec![], vec![]);
        let ipld = Ipld::from(&block);
        let restored = Block::try_from(&ipld).unwrap();

        if let CrdtDelta::Composite(c) = &restored.delta {
            assert_eq!(c.status, 2);
            assert_eq!(c.priority, 10);
        } else {
            panic!("Expected Composite delta");
        }
    }

    #[test]
    fn test_collection_delta_ipld_roundtrip() {
        let delta = CrdtDelta::Collection(CollectionDeltaPayload {
            schema_version_id: "v2".to_string(),
            priority: 3,
        });

        let block = Block::new(delta, vec![], vec![]);
        let ipld = Ipld::from(&block);
        let restored = Block::try_from(&ipld).unwrap();

        if let CrdtDelta::Collection(c) = &restored.delta {
            assert_eq!(c.priority, 3);
            assert_eq!(c.schema_version_id, "v2");
        } else {
            panic!("Expected Collection delta");
        }
    }

    struct LinkCollector {
        links: Vec<cid::Cid>,
    }

    impl IpldVisitor for LinkCollector {
        fn visit(&mut self, _ipld: &Ipld) -> bool {
            true
        }

        fn visit_link(&mut self, cid: &cid::Cid) {
            self.links.push(*cid);
        }
    }

    #[test]
    fn test_walk_ipld_visitor() {
        let head = test_cid();
        let block = Block::new(test_lww_delta(), vec![head], vec![]);
        let ipld = Ipld::from(&block);

        let mut collector = LinkCollector { links: Vec::new() };
        walk_ipld(&ipld, &mut collector);

        assert_eq!(collector.links.len(), 1);
        assert_eq!(collector.links[0], head);
    }

    #[test]
    fn test_cid_conversion() {
        let cid = test_cid();
        let libipld_cid = cid_to_libipld(&cid);
        let back = cid_from_libipld(&libipld_cid);
        assert_eq!(cid, back);
    }
}
