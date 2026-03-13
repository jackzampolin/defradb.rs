use crate::corekv::Key;
use cid::Cid;

const PRIORITY_HEX_WIDTH: usize = 16;

/// HeadstoreDocKey: Links documents to their current block head CID
///
/// Structure: /d/[DocID]/[FieldID]/[CID]
/// Example: /d/docid123/fieldname/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreDocKey {
    /// Document identifier
    pub doc_id: String,
    /// Field identifier (can be 'C' for collection)
    pub field_id: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreDocKey {
    /// Create a new HeadstoreDocKey
    pub fn new(doc_id: impl Into<String>, field_id: impl Into<String>, cid: Cid) -> Self {
        Self {
            doc_id: doc_id.into(),
            field_id: field_id.into(),
            cid,
        }
    }

    /// Create a prefix for all heads in a document
    pub fn document_prefix(doc_id: impl Into<String>) -> Vec<u8> {
        let doc_id = doc_id.into();
        format!("/d/{}/", doc_id).into_bytes()
    }

    /// Create a prefix for all heads of a specific field in a document
    pub fn field_prefix(doc_id: impl Into<String>, field_id: impl Into<String>) -> Vec<u8> {
        let doc_id = doc_id.into();
        let field_id = field_id.into();
        format!("/d/{}/{}/", doc_id, field_id).into_bytes()
    }
}

impl Key for HeadstoreDocKey {
    fn bytes(&self) -> Vec<u8> {
        format!("/d/{}/{}/{}", self.doc_id, self.field_id, self.cid).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/d/{}/{}/{}", self.doc_id, self.field_id, self.cid)
    }
}

/// HeadstorePriorityKey: Secondary index for document commit height lookups
///
/// Structure: /p/[DocID]/[PriorityHex16]/[CID bytes]
/// Example: /p/docid123/0000000000000042/<cid-bytes>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstorePriorityKey {
    /// Document identifier
    pub doc_id: String,
    /// Commit priority encoded in fixed-width hexadecimal
    pub priority: u64,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstorePriorityKey {
    /// Create a new HeadstorePriorityKey.
    pub fn new(doc_id: impl Into<String>, priority: u64, cid: Cid) -> Self {
        Self {
            doc_id: doc_id.into(),
            priority,
            cid,
        }
    }

    /// Create a prefix for all priority-indexed commits in a document.
    pub fn document_prefix(doc_id: impl Into<String>) -> Vec<u8> {
        let doc_id = doc_id.into();
        format!("/p/{}/", doc_id).into_bytes()
    }

    /// Create a prefix for a specific priority in a document.
    pub fn priority_prefix(doc_id: impl Into<String>, priority: u64) -> Vec<u8> {
        let doc_id = doc_id.into();
        format!(
            "/p/{}/{:0width$x}/",
            doc_id,
            priority,
            width = PRIORITY_HEX_WIDTH
        )
        .into_bytes()
    }

    /// Return the byte offset of the raw CID suffix within a serialized key.
    pub fn cid_offset(doc_id: &str) -> usize {
        Self::document_prefix(doc_id).len() + PRIORITY_HEX_WIDTH + 1
    }
}

impl Key for HeadstorePriorityKey {
    fn bytes(&self) -> Vec<u8> {
        let mut bytes = Self::priority_prefix(&self.doc_id, self.priority);
        bytes.extend_from_slice(&self.cid.to_bytes());
        bytes
    }

    fn to_string(&self) -> String {
        format!(
            "/p/{}/{:0width$x}/{}",
            self.doc_id,
            self.priority,
            self.cid,
            width = PRIORITY_HEX_WIDTH
        )
    }
}

/// HeadstoreColKey: Stores current collection head CID
///
/// Structure: /c/[CollectionShortID]/[CID]
/// Example: /c/1/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreColKey {
    /// Collection short ID (decimal format)
    pub collection_id: u32,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreColKey {
    /// Create a new HeadstoreColKey
    pub fn new(collection_id: u32, cid: Cid) -> Self {
        Self { collection_id, cid }
    }

    /// Create a prefix for all heads in a collection
    pub fn collection_prefix(collection_id: u32) -> Vec<u8> {
        format!("/c/{}/", collection_id).into_bytes()
    }
}

impl Key for HeadstoreColKey {
    fn bytes(&self) -> Vec<u8> {
        format!("/c/{}/{}", self.collection_id, self.cid).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/c/{}/{}", self.collection_id, self.cid)
    }
}

/// HeadstoreFieldDefinition: Maps field definitions to their block head
///
/// Structure: /f/[CollectionName]/[FieldName]/[CID]
/// Example: /f/users/email/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreFieldDefinition {
    /// Collection name
    pub collection_name: String,
    /// Field name
    pub field_name: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreFieldDefinition {
    /// Create a new HeadstoreFieldDefinition
    pub fn new(
        collection_name: impl Into<String>,
        field_name: impl Into<String>,
        cid: Cid,
    ) -> Self {
        Self {
            collection_name: collection_name.into(),
            field_name: field_name.into(),
            cid,
        }
    }

    /// Create a prefix for all field definitions
    pub fn field_definition_prefix() -> Vec<u8> {
        b"/f/".to_vec()
    }

    /// Create a prefix for a specific collection's field definitions
    pub fn collection_prefix(collection_name: impl Into<String>) -> Vec<u8> {
        let collection_name = collection_name.into();
        format!("/f/{}/", collection_name).into_bytes()
    }

    /// Create a prefix for a specific field in a collection
    pub fn field_prefix(
        collection_name: impl Into<String>,
        field_name: impl Into<String>,
    ) -> Vec<u8> {
        let collection_name = collection_name.into();
        let field_name = field_name.into();
        format!("/f/{}/{}/", collection_name, field_name).into_bytes()
    }
}

impl Key for HeadstoreFieldDefinition {
    fn bytes(&self) -> Vec<u8> {
        format!(
            "/f/{}/{}/{}",
            self.collection_name, self.field_name, self.cid
        )
        .into_bytes()
    }

    fn to_string(&self) -> String {
        format!(
            "/f/{}/{}/{}",
            self.collection_name, self.field_name, self.cid
        )
    }
}

/// HeadstoreCollectionDefinition: Maps collection definitions to their block head
///
/// Structure: /g/[CollectionName]/[CID]
/// Example: /g/users/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreCollectionDefinition {
    /// Collection name
    pub collection_name: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreCollectionDefinition {
    /// Create a new HeadstoreCollectionDefinition
    pub fn new(collection_name: impl Into<String>, cid: Cid) -> Self {
        Self {
            collection_name: collection_name.into(),
            cid,
        }
    }

    /// Create a prefix for all collection definitions
    pub fn collection_definition_prefix() -> Vec<u8> {
        b"/g/".to_vec()
    }

    /// Create a prefix for a specific collection
    pub fn collection_prefix(collection_name: impl Into<String>) -> Vec<u8> {
        let collection_name = collection_name.into();
        format!("/g/{}/", collection_name).into_bytes()
    }
}

impl Key for HeadstoreCollectionDefinition {
    fn bytes(&self) -> Vec<u8> {
        format!("/g/{}/{}", self.collection_name, self.cid).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/g/{}/{}", self.collection_name, self.cid)
    }
}

/// HeadstoreCollectionSetDefinition: Maps collection set definitions to their block head
///
/// Structure: /s/[FirstCollectionID]/[CID]
/// Example: /s/col_a/bafyreih7c4pdkyvosses56rmyomakxvicn4cehjrw3w3mmk57iagt6f4sq
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadstoreCollectionSetDefinition {
    /// ID of lexicographically smallest collection in set
    pub first_collection_id: String,
    /// IPFS Content Identifier
    pub cid: Cid,
}

impl HeadstoreCollectionSetDefinition {
    /// Create a new HeadstoreCollectionSetDefinition
    pub fn new(first_collection_id: impl Into<String>, cid: Cid) -> Self {
        Self {
            first_collection_id: first_collection_id.into(),
            cid,
        }
    }

    /// Create a prefix for all collection set definitions
    pub fn set_definition_prefix() -> Vec<u8> {
        b"/s/".to_vec()
    }

    /// Create a prefix for a specific collection set
    pub fn set_prefix(first_collection_id: impl Into<String>) -> Vec<u8> {
        let first_collection_id = first_collection_id.into();
        format!("/s/{}/", first_collection_id).into_bytes()
    }
}

impl Key for HeadstoreCollectionSetDefinition {
    fn bytes(&self) -> Vec<u8> {
        format!("/s/{}/{}", self.first_collection_id, self.cid).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/s/{}/{}", self.first_collection_id, self.cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Test CID (V1, dag-pb, sha2-256)
    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    #[test]
    fn test_headstore_doc_key() {
        let cid = test_cid();
        let key = HeadstoreDocKey::new("docid123", "fieldname", cid);

        let string = key.to_string();
        assert!(string.starts_with("/d/"));
        assert!(string.contains("docid123"));
        assert!(string.contains("fieldname"));
        assert!(string.contains("bafy"));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_col_key() {
        let cid = test_cid();
        let key = HeadstoreColKey::new(1, cid);

        let string = key.to_string();
        assert_eq!(string, format!("/c/1/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_priority_key() {
        let cid = test_cid();
        let key = HeadstorePriorityKey::new("docid123", 66, cid);

        let string = key.to_string();
        assert_eq!(string, format!("/p/docid123/0000000000000042/{}", cid));

        let bytes = key.bytes();
        assert_eq!(
            &bytes[..HeadstorePriorityKey::cid_offset("docid123")],
            b"/p/docid123/0000000000000042/"
        );
        assert_eq!(
            &bytes[HeadstorePriorityKey::cid_offset("docid123")..],
            cid.to_bytes()
        );
    }

    #[test]
    fn test_headstore_field_definition() {
        let cid = test_cid();
        let key = HeadstoreFieldDefinition::new("users", "email", cid);

        let string = key.to_string();
        assert_eq!(string, format!("/f/users/email/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_collection_definition() {
        let cid = test_cid();
        let key = HeadstoreCollectionDefinition::new("users", cid);

        let string = key.to_string();
        assert_eq!(string, format!("/g/users/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_collection_set_definition() {
        let cid = test_cid();
        let key = HeadstoreCollectionSetDefinition::new("col_a", cid);

        let string = key.to_string();
        assert_eq!(string, format!("/s/col_a/{}", cid));

        let bytes = key.bytes();
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_headstore_prefixes() {
        let prefix = HeadstoreDocKey::document_prefix("docid");
        assert_eq!(prefix, b"/d/docid/");

        let prefix = HeadstoreDocKey::field_prefix("docid", "field");
        assert_eq!(prefix, b"/d/docid/field/");

        let prefix = HeadstorePriorityKey::document_prefix("docid");
        assert_eq!(prefix, b"/p/docid/");

        let prefix = HeadstorePriorityKey::priority_prefix("docid", 66);
        assert_eq!(prefix, b"/p/docid/0000000000000042/");

        let prefix = HeadstoreColKey::collection_prefix(1);
        assert_eq!(prefix, b"/c/1/");

        let prefix = HeadstoreFieldDefinition::field_definition_prefix();
        assert_eq!(prefix, b"/f/");

        let prefix = HeadstoreCollectionDefinition::collection_definition_prefix();
        assert_eq!(prefix, b"/g/");

        let prefix = HeadstoreCollectionSetDefinition::set_definition_prefix();
        assert_eq!(prefix, b"/s/");
    }
}
