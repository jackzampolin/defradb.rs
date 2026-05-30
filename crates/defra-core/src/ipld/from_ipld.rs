//! Conversions from IPLD to DefraDB types.

use std::collections::BTreeMap;

use libipld::Ipld;

use super::cid_convert::cid_from_libipld;
use crate::block::{
    Block, CollectionDefinitionDeltaPayload, CollectionDeltaPayload, CompositeDeltaPayload,
    CounterDeltaPayload, CrdtDelta, DAGLink, Encryption, FieldDefinitionDeltaPayload,
    LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
};
use crate::{Error, Result};

impl TryFrom<&Ipld> for Block {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for Block, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        let delta_ipld = map
            .get("delta")
            .ok_or_else(|| Error::IpldError("Missing 'delta' field in Block".to_string()))?;
        let delta = CrdtDelta::try_from(delta_ipld)?;

        let heads = if let Some(heads_ipld) = map.get("heads") {
            Some(parse_cid_list(heads_ipld)?)
        } else {
            None
        };

        let links = if let Some(links_ipld) = map.get("links") {
            Some(parse_dag_links(links_ipld)?)
        } else {
            None
        };

        let encryption = if let Some(enc_ipld) = map.get("encryption") {
            Some(parse_cid(enc_ipld)?)
        } else {
            None
        };

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
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for DAGLink, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        let name = parse_string(map, "name")?;
        let link = match map.get("link") {
            Some(Ipld::Link(cid)) => cid_from_libipld(cid)?,
            Some(other) => {
                return Err(Error::IpldError(format!(
                    "Field 'link' in DAGLink expected Link, got {}",
                    ipld_type_name(other)
                )))
            }
            None => return Err(Error::IpldError("Missing 'link' in DAGLink".to_string())),
        };

        Ok(DAGLink::new(name, link))
    }
}

impl TryFrom<&Ipld> for CrdtDelta {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for CrdtDelta, got {}",
                    ipld_type_name(ipld)
                )))
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
        if let Some(field_def) = map.get("fieldDefinition") {
            return Ok(CrdtDelta::FieldDefinition(
                FieldDefinitionDeltaPayload::try_from(field_def)?,
            ));
        }
        if let Some(col_set) = map.get("collectionSet") {
            let inner = match col_set {
                Ipld::Map(m) => m,
                _ => {
                    return Err(Error::IpldError(
                        "Expected IPLD map for CollectionSetDelta".to_string(),
                    ))
                }
            };
            let priority = match inner.get("priority") {
                Some(Ipld::Integer(n)) => *n as u64,
                _ => 0,
            };
            return Ok(CrdtDelta::CollectionSet(
                crate::CollectionSetDeltaPayload::new(priority),
            ));
        }
        if let Some(col_def) = map.get("collectionDefinition") {
            return Ok(CrdtDelta::CollectionDefinition(
                CollectionDefinitionDeltaPayload::try_from(col_def)?,
            ));
        }

        Err(Error::IpldError(format!(
            "Unknown CRDT delta type in IPLD map with keys: {:?}",
            map.keys().collect::<Vec<_>>()
        )))
    }
}

impl TryFrom<&Ipld> for LwwDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for LwwDeltaPayload, got {}",
                    ipld_type_name(ipld)
                )))
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
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for CounterDeltaPayload, got {}",
                    ipld_type_name(ipld)
                )))
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
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for CompositeDeltaPayload, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        let status = CompositeDeltaPayload::validate_status(parse_u8(map, "status")?)
            .map_err(Error::IpldError)?;

        Ok(CompositeDeltaPayload {
            doc_id: parse_bytes(map, "docID")?,
            schema_version_id: parse_string(map, "schemaVersionID")?,
            priority: parse_u64(map, "priority")?,
            status,
        })
    }
}

impl TryFrom<&Ipld> for CollectionDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for CollectionDeltaPayload, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        Ok(CollectionDeltaPayload {
            schema_version_id: parse_string(map, "schemaVersionID")?,
            priority: parse_u64(map, "priority")?,
        })
    }
}

impl TryFrom<&Ipld> for FieldDefinitionDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for FieldDefinitionDeltaPayload, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        Ok(FieldDefinitionDeltaPayload {
            priority: parse_u64(map, "priority")?,
            name: parse_optional_string(map, "name")?,
            crdt: parse_optional_u8(map, "crdt")?,
            scalar_kind: parse_optional_u8(map, "scalarKind")?,
            collection_id: parse_optional_string(map, "collectionID")?,
            relative_id: parse_optional_i32(map, "relativeID")?,
        })
    }
}

impl TryFrom<&Ipld> for CollectionDefinitionDeltaPayload {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for CollectionDefinitionDeltaPayload, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        Ok(CollectionDefinitionDeltaPayload {
            priority: parse_u64(map, "priority")?,
            name: parse_optional_string(map, "name")?,
            query_select: parse_optional_bytes(map, "querySelect")?,
            query_transform: parse_optional_cid(map, "queryTransform")?,
        })
    }
}

impl TryFrom<&Ipld> for Encryption {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for Encryption, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        Ok(Encryption {
            doc_id: parse_bytes(map, "docID")?,
            field_name: parse_optional_string(map, "fieldName")?,
            key: parse_bytes(map, "key")?,
        })
    }
}

impl TryFrom<&Ipld> for Signature {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for Signature, got {}",
                    ipld_type_name(ipld)
                )))
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

impl TryFrom<&Ipld> for SignatureHeader {
    type Error = Error;

    fn try_from(ipld: &Ipld) -> Result<Self> {
        let map = match ipld {
            Ipld::Map(m) => m,
            _ => {
                return Err(Error::IpldError(format!(
                    "Expected IPLD map for SignatureHeader, got {}",
                    ipld_type_name(ipld)
                )))
            }
        };

        let sig_type = match map.get("type") {
            Some(Ipld::String(s)) => match s.as_str() {
                "ES256K" => SignatureType::ES256K,
                "ES256" => SignatureType::ES256,
                "EdDSA" => SignatureType::EdDSA,
                "BLS" => SignatureType::BLS,
                other => {
                    return Err(Error::IpldError(format!(
                        "Unknown signature type: {}",
                        other
                    )))
                }
            },
            Some(other) => {
                return Err(Error::IpldError(format!(
                    "Field 'type' in SignatureHeader expected String, got {}",
                    ipld_type_name(other)
                )))
            }
            None => {
                return Err(Error::IpldError(
                    "Missing 'type' in SignatureHeader".to_string(),
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
// Helper Functions
// ============================================================================

/// Get a human-readable type name for IPLD values (for error messages)
fn ipld_type_name(ipld: &Ipld) -> &'static str {
    match ipld {
        Ipld::Null => "Null",
        Ipld::Bool(_) => "Bool",
        Ipld::Integer(_) => "Integer",
        Ipld::Float(_) => "Float",
        Ipld::String(_) => "String",
        Ipld::Bytes(_) => "Bytes",
        Ipld::List(_) => "List",
        Ipld::Map(_) => "Map",
        Ipld::Link(_) => "Link",
    }
}

fn parse_cid(ipld: &Ipld) -> Result<cid::Cid> {
    match ipld {
        Ipld::Link(cid) => cid_from_libipld(cid),
        _ => Err(Error::IpldError(format!(
            "Expected IPLD Link, got {}",
            ipld_type_name(ipld)
        ))),
    }
}

fn parse_cid_list(ipld: &Ipld) -> Result<Vec<cid::Cid>> {
    match ipld {
        Ipld::List(items) => items.iter().map(parse_cid).collect(),
        _ => Err(Error::IpldError(format!(
            "Expected IPLD List of Links, got {}",
            ipld_type_name(ipld)
        ))),
    }
}

fn parse_dag_links(ipld: &Ipld) -> Result<Vec<DAGLink>> {
    match ipld {
        Ipld::List(items) => items.iter().map(DAGLink::try_from).collect(),
        _ => Err(Error::IpldError(format!(
            "Expected IPLD List of DAGLinks, got {}",
            ipld_type_name(ipld)
        ))),
    }
}

fn parse_string(map: &BTreeMap<String, Ipld>, key: &str) -> Result<String> {
    match map.get(key) {
        Some(Ipld::String(s)) => Ok(s.clone()),
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected String, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_bytes(map: &BTreeMap<String, Ipld>, key: &str) -> Result<Vec<u8>> {
    match map.get(key) {
        Some(Ipld::Bytes(b)) => Ok(b.clone()),
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Bytes, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_u64(map: &BTreeMap<String, Ipld>, key: &str) -> Result<u64> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < 0 || *i > u64::MAX as i128 {
                return Err(Error::IpldError(format!(
                    "Field '{}' value {} out of u64 range (0 to {})",
                    key,
                    i,
                    u64::MAX
                )));
            }
            Ok(*i as u64)
        }
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Integer, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_i64(map: &BTreeMap<String, Ipld>, key: &str) -> Result<i64> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < i64::MIN as i128 || *i > i64::MAX as i128 {
                return Err(Error::IpldError(format!(
                    "Field '{}' value {} out of i64 range ({} to {})",
                    key,
                    i,
                    i64::MIN,
                    i64::MAX
                )));
            }
            Ok(*i as i64)
        }
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Integer, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

fn parse_u8(map: &BTreeMap<String, Ipld>, key: &str) -> Result<u8> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < 0 || *i > u8::MAX as i128 {
                return Err(Error::IpldError(format!(
                    "Field '{}' value {} out of u8 range (0 to {})",
                    key,
                    i,
                    u8::MAX
                )));
            }
            Ok(*i as u8)
        }
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Integer, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Err(Error::IpldError(format!("Missing field '{}'", key))),
    }
}

// ============================================================================
// Optional Field Parsers (with proper type checking)
// ============================================================================

/// Parse an optional string field. Returns error if field exists but has wrong type.
fn parse_optional_string(map: &BTreeMap<String, Ipld>, key: &str) -> Result<Option<String>> {
    match map.get(key) {
        Some(Ipld::String(s)) => Ok(Some(s.clone())),
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected String, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Ok(None),
    }
}

/// Parse an optional u8 field with bounds checking. Returns error if field exists but has wrong type or is out of range.
fn parse_optional_u8(map: &BTreeMap<String, Ipld>, key: &str) -> Result<Option<u8>> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < 0 || *i > u8::MAX as i128 {
                Err(Error::IpldError(format!(
                    "Field '{}' value {} out of u8 range (0 to {})",
                    key,
                    i,
                    u8::MAX
                )))
            } else {
                Ok(Some(*i as u8))
            }
        }
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Integer, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Ok(None),
    }
}

/// Parse an optional i32 field with bounds checking. Returns error if field exists but has wrong type or is out of range.
fn parse_optional_i32(map: &BTreeMap<String, Ipld>, key: &str) -> Result<Option<i32>> {
    match map.get(key) {
        Some(Ipld::Integer(i)) => {
            if *i < i32::MIN as i128 || *i > i32::MAX as i128 {
                Err(Error::IpldError(format!(
                    "Field '{}' value {} out of i32 range ({} to {})",
                    key,
                    i,
                    i32::MIN,
                    i32::MAX
                )))
            } else {
                Ok(Some(*i as i32))
            }
        }
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Integer, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Ok(None),
    }
}

/// Parse an optional bytes field. Returns error if field exists but has wrong type.
fn parse_optional_bytes(map: &BTreeMap<String, Ipld>, key: &str) -> Result<Option<Vec<u8>>> {
    match map.get(key) {
        Some(Ipld::Bytes(b)) => Ok(Some(b.clone())),
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Bytes, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Ok(None),
    }
}

/// Parse an optional CID/Link field. Returns error if field exists but has wrong type.
fn parse_optional_cid(map: &BTreeMap<String, Ipld>, key: &str) -> Result<Option<cid::Cid>> {
    match map.get(key) {
        Some(Ipld::Link(c)) => Ok(Some(cid_from_libipld(c)?)),
        Some(other) => Err(Error::IpldError(format!(
            "Field '{}' expected Link, got {}",
            key,
            ipld_type_name(other)
        ))),
        None => Ok(None),
    }
}
