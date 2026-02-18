//! Conversions from DefraDB types to IPLD.

use std::collections::BTreeMap;

use libipld::Ipld;

use super::cid_convert::cid_to_libipld;
use crate::block::{
    Block, CollectionDefinitionDeltaPayload, CollectionDeltaPayload, CompositeDeltaPayload,
    CounterDeltaPayload, CrdtDelta, DAGLink, Encryption, FieldDefinitionDeltaPayload,
    LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
};
use crate::{Error, Result};

impl TryFrom<&Block> for Ipld {
    type Error = Error;

    fn try_from(block: &Block) -> Result<Self> {
        let mut map = BTreeMap::new();

        map.insert("delta".to_string(), Ipld::try_from(&block.delta)?);

        if let Some(ref heads) = block.heads {
            let mut heads_ipld = Vec::with_capacity(heads.len());
            for cid in heads {
                heads_ipld.push(Ipld::Link(cid_to_libipld(cid)?));
            }
            map.insert("heads".to_string(), Ipld::List(heads_ipld));
        }

        if let Some(ref links) = block.links {
            let mut links_ipld = Vec::with_capacity(links.len());
            for link in links {
                links_ipld.push(Ipld::try_from(link)?);
            }
            map.insert("links".to_string(), Ipld::List(links_ipld));
        }

        if let Some(ref enc) = block.encryption {
            map.insert("encryption".to_string(), Ipld::Link(cid_to_libipld(enc)?));
        }

        if let Some(ref sig) = block.signature {
            map.insert("signature".to_string(), Ipld::Link(cid_to_libipld(sig)?));
        }

        Ok(Ipld::Map(map))
    }
}

impl TryFrom<Block> for Ipld {
    type Error = Error;

    fn try_from(block: Block) -> Result<Self> {
        Ipld::try_from(&block)
    }
}

impl TryFrom<&DAGLink> for Ipld {
    type Error = Error;

    fn try_from(link: &DAGLink) -> Result<Self> {
        let mut map = BTreeMap::new();
        map.insert("name".to_string(), Ipld::String(link.name.clone()));
        map.insert("link".to_string(), Ipld::Link(cid_to_libipld(&link.link)?));
        Ok(Ipld::Map(map))
    }
}

impl TryFrom<&CrdtDelta> for Ipld {
    type Error = Error;

    fn try_from(delta: &CrdtDelta) -> Result<Self> {
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
            CrdtDelta::CollectionSet(payload) => {
                let mut inner = BTreeMap::new();
                inner.insert(
                    "priority".to_string(),
                    Ipld::Integer(payload.priority as i128),
                );
                map.insert("collectionSet".to_string(), Ipld::Map(inner));
            }
            CrdtDelta::FieldDefinition(payload) => {
                map.insert("fieldDefinition".to_string(), Ipld::from(payload));
            }
            CrdtDelta::CollectionDefinition(payload) => {
                map.insert("collectionDefinition".to_string(), Ipld::try_from(payload)?);
            }
        }
        Ok(Ipld::Map(map))
    }
}

// Most payload types don't contain CIDs, so they can use infallible From.
// CollectionDefinitionDeltaPayload is an exception (has query_transform CID).

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

impl From<&FieldDefinitionDeltaPayload> for Ipld {
    fn from(payload: &FieldDefinitionDeltaPayload) -> Self {
        let mut map = BTreeMap::new();
        map.insert(
            "priority".to_string(),
            Ipld::Integer(payload.priority as i128),
        );
        if let Some(ref name) = payload.name {
            map.insert("name".to_string(), Ipld::String(name.clone()));
        }
        if let Some(crdt) = payload.crdt {
            map.insert("crdt".to_string(), Ipld::Integer(crdt as i128));
        }
        if let Some(sk) = payload.scalar_kind {
            map.insert("scalarKind".to_string(), Ipld::Integer(sk as i128));
        }
        if let Some(ref cid) = payload.collection_id {
            map.insert("collectionID".to_string(), Ipld::String(cid.clone()));
        }
        if let Some(rid) = payload.relative_id {
            map.insert("relativeID".to_string(), Ipld::Integer(rid as i128));
        }
        Ipld::Map(map)
    }
}

impl TryFrom<&CollectionDefinitionDeltaPayload> for Ipld {
    type Error = Error;

    fn try_from(payload: &CollectionDefinitionDeltaPayload) -> Result<Self> {
        let mut map = BTreeMap::new();
        map.insert(
            "priority".to_string(),
            Ipld::Integer(payload.priority as i128),
        );
        if let Some(ref name) = payload.name {
            map.insert("name".to_string(), Ipld::String(name.clone()));
        }
        if let Some(ref query_select) = payload.query_select {
            map.insert("querySelect".to_string(), Ipld::Bytes(query_select.clone()));
        }
        if let Some(ref query_transform) = payload.query_transform {
            map.insert(
                "queryTransform".to_string(),
                Ipld::Link(cid_to_libipld(query_transform)?),
            );
        }
        Ok(Ipld::Map(map))
    }
}

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

impl From<&Signature> for Ipld {
    fn from(sig: &Signature) -> Self {
        let mut map = BTreeMap::new();
        map.insert("header".to_string(), Ipld::from(&sig.header));
        map.insert("value".to_string(), Ipld::Bytes(sig.value.clone()));
        Ipld::Map(map)
    }
}

impl From<&SignatureHeader> for Ipld {
    fn from(header: &SignatureHeader) -> Self {
        let mut map = BTreeMap::new();
        let type_str = match header.sig_type {
            SignatureType::ES256K => "ES256K",
            SignatureType::EdDSA => "EdDSA",
            SignatureType::BLS => "BLS",
        };
        map.insert("type".to_string(), Ipld::String(type_str.to_string()));
        map.insert("identity".to_string(), Ipld::Bytes(header.identity.clone()));
        Ipld::Map(map)
    }
}
